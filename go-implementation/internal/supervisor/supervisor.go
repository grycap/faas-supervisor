package supervisor

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/faassupervisor/go/internal/config"
	"github.com/faassupervisor/go/internal/errors"
	"github.com/faassupervisor/go/internal/events"
	"github.com/faassupervisor/go/internal/logger"
	"github.com/faassupervisor/go/internal/storage"
	"github.com/faassupervisor/go/internal/utils"
)

const (
	scriptFileName  = "script.sh"
	oscarScriptPath = "/oscar/config/script.sh"
)

// SupervisorExecutor is the concrete supervisor interface (DefaultSupervisor).
type SupervisorExecutor interface {
	ExecuteFunction() error
	CreateResponse() interface{}
	CreateErrorResponse() interface{}
}

// GenericSupervisor mirrors the Python Supervisor class.
type GenericSupervisor struct {
	Event       interface{}
	Context     interface{}
	ParsedEvent events.Event
	Cfg         *config.Config
	StgConfig   *storage.StorageConfig
	Executor    SupervisorExecutor
	EventType   string
}

func NewGenericSupervisor(event, context interface{}) *GenericSupervisor {
	g := &GenericSupervisor{Event: event, Context: context}
	g.createTmpDirs()
	g.Cfg = config.LoadConfig()
	// The log level comes from the configuration (custom variables can be
	// overridden by the environment, which ReadCfgString already honours).
	logger.GetLogger().SetLevel(g.Cfg.ReadCfgString("log_level"))
	g.ParsedEvent = events.ParseEvent(event, "default", g.Cfg)
	if g.ParsedEvent != nil {
		g.EventType = g.ParsedEvent.GetType()
	}
	g.StgConfig = storage.NewStorageConfig(g.Cfg)
	g.Executor = g.createSupervisor(event, context)
	return g
}

func (g *GenericSupervisor) createTmpDirs() {
	inputDir, err := utils.CreateTmpDir()
	if err != nil {
		panic(err)
	}
	outputDir, err := utils.CreateTmpDir()
	if err != nil {
		panic(err)
	}
	os.Setenv("TMP_INPUT_DIR", inputDir)
	os.Setenv("TMP_OUTPUT_DIR", outputDir)
}

// recoverAndExit implements the "@exception" decorator semantics: warnings are
// logged and swallowed, whereas FaasSupervisorError (and any other raised error)
// logged and abort the process with exit code 1.
func recoverAndExit(fn func()) {
	defer func() {
		switch r := recover().(type) {
		case nil:
		case *errors.Warning:
			logger.GetLogger().Warning("%v", r)
		default:
			logger.GetLogger().Error("%v", r)
			fmt.Fprintf(os.Stderr, "%v\n", r)
			os.Exit(1)
		}
	}()
	fn()
}

func (g *GenericSupervisor) createSupervisor(event, context interface{}) (ex SupervisorExecutor) {
	recoverAndExit(func() {
		if utils.IsLambdaEnvironment() {
			ex = NewLambdaSupervisor(event, context, g.Cfg, g.ParsedEvent)
		} else {
			ex = NewBinarySupervisor(g.EventType, g.Cfg)
		}
	})
	return ex
}

// Run executes the whole flow following the specification.
func (g *GenericSupervisor) Run() (result interface{}) {
	defer func() {
		if r := recover(); r != nil {
			if faasErr, ok := r.(*errors.FaasSupervisorError); ok {
				logger.GetLogger().Error("Exception: %v", faasErr)
				result = g.Executor.CreateErrorResponse()
			} else if w, ok := r.(*errors.Warning); ok {
				logger.GetLogger().Warning("%v", w)
				result = nil
			} else {
				panic(r)
			}
		}
	}()

	if g.isBatchExecution() && utils.IsLambdaEnvironment() {
		g.Executor.ExecuteFunction()
	} else {
		g.parseInput()
		g.Executor.ExecuteFunction()
		g.parseOutput()
	}
	logger.GetLogger().Info("Creating response")
	return g.Executor.CreateResponse()
}

func (g *GenericSupervisor) isBatchExecution() bool {
	return g.Cfg.ReadCfgString("execution_mode") == "batch"
}

func (g *GenericSupervisor) parseInput() {
	var err error
	recoverAndExit(func() {
		if g.Cfg.ReadCfgBool("file_stage_in") {
			logger.GetLogger().Info("Skipping download of input file.")
			return
		}
		inputPath, e := g.StgConfig.DownloadInput(g.ParsedEvent, os.Getenv("TMP_INPUT_DIR"))
		err = e
		if e != nil {
			return
		}
		if utils.IsFile(inputPath) {
			os.Setenv("INPUT_FILE_PATH", inputPath)
		} else if utils.IsDirectory(inputPath) {
			os.Setenv("INPUT_FILE_PATH", os.Getenv("TMP_INPUT_DIR"))
		}
	})
	if err != nil {
		logger.GetLogger().Error("There was an exception in _parse_input: %v", err)
		fmt.Fprintf(os.Stderr, "There was an exception in _parse_input\n")
		os.Exit(1)
	}
}

func (g *GenericSupervisor) parseOutput() {
	var err error
	recoverAndExit(func() {
		err = g.StgConfig.UploadOutput(os.Getenv("TMP_OUTPUT_DIR"), g.ParsedEvent)
	})
	if err != nil {
		logger.GetLogger().Error("There was an exception in _parse_output: %v", err)
		fmt.Fprintf(os.Stderr, "There was an exception in _parse_output\n")
		os.Exit(1)
	}
}

// BinarySupervisor handles the binary/OSCAR environment.
type BinarySupervisor struct {
	EventType string
	Output    string
	cfg       *config.Config
}

func NewBinarySupervisor(eventType string, cfg *config.Config) *BinarySupervisor {
	return &BinarySupervisor{EventType: eventType, cfg: cfg}
}

func (b *BinarySupervisor) getScriptPath() string {
	if utils.IsVarInEnv("SCRIPT") {
		decoded, err := base64.StdEncoding.DecodeString(os.Getenv("SCRIPT"))
		if err != nil {
			decoded = []byte(os.Getenv("SCRIPT"))
		}
		path := filepath.Join(os.Getenv("TMP_INPUT_DIR"), scriptFileName)
		if err := utils.CreateFileWithContent(path, string(decoded)); err != nil {
			logger.GetLogger().Error("Error writing script: %v", err)
			return ""
		}
		logger.GetLogger().Info("Script file created in '%s'", path)
		return path
	}
	if utils.IsFile(oscarScriptPath) {
		logger.GetLogger().Info("Script file found in '%s'", oscarScriptPath)
		return oscarScriptPath
	}
	return ""
}

func (b *BinarySupervisor) ExecuteFunction() error {
	script := b.getScriptPath()
	if script == "" {
		logger.GetLogger().Error("No user script found!")
		return nil
	}

	// Save the current LD_LIBRARY_PATH then restore/remove it for the script.
	origLibPath := os.Getenv("LD_LIBRARY_PATH")
	if os.Getenv("LD_LIBRARY_PATH_ORIG") != "" {
		os.Setenv("LD_LIBRARY_PATH", os.Getenv("LD_LIBRARY_PATH_ORIG"))
	} else {
		os.Unsetenv("LD_LIBRARY_PATH")
	}
	restore := func() {
		if origLibPath != "" {
			os.Setenv("LD_LIBRARY_PATH", origLibPath)
		} else {
			os.Unsetenv("LD_LIBRARY_PATH")
		}
	}

	var merged, stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command("/bin/sh", script)
	cmd.Stdout = io.MultiWriter(&stdoutBuf, &merged)
	cmd.Stderr = io.MultiWriter(&stderrBuf, &merged)

	if err := cmd.Start(); err != nil {
		restore()
		logger.GetLogger().Error("Error starting script: %v", err)
		return err
	}
	restore()

	err := cmd.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			logger.GetLogger().Error("Script exited with code %v", exitErr.ExitCode())
			os.Exit(exitErr.ExitCode())
		}
		return err
	}

	for _, line := range bytes.Split(stdoutBuf.Bytes(), []byte("\n")) {
		logger.GetLogger().Stdout("%s", string(line))
	}
	for _, line := range bytes.Split(stderrBuf.Bytes(), []byte("\n")) {
		logger.GetLogger().Stderr("%s", string(line))
	}
	b.Output = merged.String()
	return nil
}

func (b *BinarySupervisor) CreateResponse() interface{} {
	if b.EventType != "UNKNOWN" {
		return b.Output
	}
	outputDir := os.Getenv("TMP_OUTPUT_DIR")
	files, _ := utils.GetAllFilesInDir(outputDir)
	if len(files) == 1 {
		content, err := utils.ReadFileBytes(files[0])
		if err != nil {
			return ""
		}
		return base64.StdEncoding.EncodeToString(content)
	}
	if len(files) > 1 {
		zipPath := filepath.Join(outputDir, "output.zip")
		if err := zipFiles(files, zipPath); err != nil {
			return ""
		}
		content, err := utils.ReadFileBytes(zipPath)
		if err != nil {
			return ""
		}
		return base64.StdEncoding.EncodeToString(content)
	}
	return ""
}

func (b *BinarySupervisor) CreateErrorResponse() interface{} {
	return nil
}

// LambdaSupervisor handles AWS Lambda environments (kept minimal; the Go
// binary targets the binary/OSCAR mode).
type LambdaSupervisor struct {
	cfg         *config.Config
	parsedEvent events.Event
}

func NewLambdaSupervisor(event, context interface{}, cfg *config.Config, parsed events.Event) *LambdaSupervisor {
	if context == nil {
		panic(errors.NoLambdaContextError)
	}
	return &LambdaSupervisor{cfg: cfg, parsedEvent: parsed}
}

func (l *LambdaSupervisor) ExecuteFunction() error {
	logger.GetLogger().Error("Lambda execution mode is not implemented for the Go binary. Use the Python package in Lambda.")
	return nil
}

func (l *LambdaSupervisor) CreateResponse() interface{} {
	return map[string]interface{}{
		"statusCode": 200,
		"headers": map[string]interface{}{
			"amz-lambda-request-id":  "",
			"amz-log-group-name":     "",
			"amz-log-stream-name":    "",
		},
		"body":            "",
		"isBase64Encoded": true,
	}
}

func (l *LambdaSupervisor) CreateErrorResponse() interface{} {
	return map[string]interface{}{
		"statusCode": 500,
		"headers": map[string]interface{}{
			"amz-lambda-request-id":  "",
			"amz-log-group-name":     "",
			"amz-log-stream-name":    "",
		},
		"body":            base64.StdEncoding.EncodeToString([]byte(`{"exception": "error"}`)),
		"isBase64Encoded": true,
	}
}

func zipFiles(fileList []string, zipPath string) error {
	out, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer out.Close()
	zw := zip.NewWriter(out)
	defer zw.Close()
	baseDir := filepath.Dir(zipPath)
	for _, f := range fileList {
		src, err := os.Open(f)
		if err != nil {
			return err
		}
		info, _ := src.Stat()
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			src.Close()
			return err
		}
		// Entries are stored relative to the directory that holds the zip,
		// matching FileUtils.zip_file_list.
		rel, err := filepath.Rel(baseDir, f)
		if err != nil {
			rel = filepath.Base(f)
		}
		header.Name = filepath.ToSlash(rel)
		header.Method = zip.Deflate
		w, err := zw.CreateHeader(header)
		if err != nil {
			src.Close()
			return err
		}
		if _, err := io.Copy(w, src); err != nil {
			src.Close()
			return err
		}
		src.Close()
	}
	return nil
}