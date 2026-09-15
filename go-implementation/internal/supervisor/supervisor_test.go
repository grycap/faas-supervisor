package supervisor

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/faassupervisor/go/internal/config"
)

// newBinarySupervisor returns a BinarySupervisor configured to use the
// provided TMP_OUTPUT_DIR for CreateResponse tests.
func newBinarySupervisor(eventType string, cfg *config.Config, outputDir string) *BinarySupervisor {
	os.Setenv("TMP_OUTPUT_DIR", outputDir)
	os.Setenv("TMP_INPUT_DIR", filepath.Join(outputDir, "..", "tmp-input"))
	return NewBinarySupervisor(eventType, cfg)
}

func writeTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCreateResponseNonUnknownReturnsOutput(t *testing.T) {
	cfg := config.NewConfig(nil)
	outDir := t.TempDir()
	b := newBinarySupervisor("MINIO", cfg, outDir)
	b.Output = "script stdout lines"
	if got := b.CreateResponse(); got != "script stdout lines" {
		t.Errorf("CreateResponse = %v, want script output", got)
	}
}

func TestCreateResponseUnknownSingleFile(t *testing.T) {
	cfg := config.NewConfig(nil)
	outDir := t.TempDir()
	b := newBinarySupervisor("UNKNOWN", cfg, outDir)
	writeTestFile(t, outDir, "result.txt", "hello world")

	got := b.CreateResponse()
	enc, ok := got.(string)
	if !ok {
		t.Fatalf("CreateResponse type = %T, want string", got)
	}
	decoded, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("invalid base64: %v", err)
	}
	if string(decoded) != "hello world" {
		t.Errorf("decoded content = %q, want 'hello world'", decoded)
	}
}

func TestCreateResponseUnknownNoFiles(t *testing.T) {
	cfg := config.NewConfig(nil)
	outDir := t.TempDir()
	b := newBinarySupervisor("UNKNOWN", cfg, outDir)

	if got := b.CreateResponse(); got != "" {
		t.Errorf("CreateResponse with no files = %v, want empty", got)
	}
}

func TestCreateResponseUnknownMultipleFilesZip(t *testing.T) {
	cfg := config.NewConfig(nil)
	outDir := t.TempDir()
	b := newBinarySupervisor("UNKNOWN", cfg, outDir)
	writeTestFile(t, outDir, "a.txt", "content a")
	writeTestFile(t, outDir, "b.txt", "content b")

	got := b.CreateResponse()
	enc, ok := got.(string)
	if !ok {
		t.Fatalf("CreateResponse type = %T, want string", got)
	}
	zipBytes, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("invalid base64: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		t.Fatalf("invalid zip: %v", err)
	}
	names := map[string]bool{}
	for _, f := range zr.File {
		names[filepath.Base(f.Name)] = true
		if f.Name == "output.zip" {
			t.Errorf("zip should not contain itself")
		}
	}
	if !names["a.txt"] || !names["b.txt"] {
		t.Errorf("zip entries = %v, want a.txt and b.txt", names)
	}
	// Verify content round-trips.
	contents := map[string]string{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(rc)
		contents[filepath.Base(f.Name)] = string(data)
		rc.Close()
	}
	if contents["a.txt"] != "content a" || contents["b.txt"] != "content b" {
		t.Errorf("zip contents = %v", contents)
	}
}

func TestGetScriptPathFromScriptEnv(t *testing.T) {
	cfg := config.NewConfig(nil)
	inputDir := t.TempDir()
	os.Setenv("TMP_INPUT_DIR", inputDir)
	defer os.Unsetenv("TMP_INPUT_DIR")

	payload := base64.StdEncoding.EncodeToString([]byte("echo hello"))
	os.Setenv("SCRIPT", payload)
	defer os.Unsetenv("SCRIPT")

	b := NewBinarySupervisor("UNKNOWN", cfg)
	path := b.getScriptPath()
	if path == "" {
		t.Fatal("expected a script path from SCRIPT env")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "echo hello" {
		t.Errorf("script content = %q, want 'echo hello'", content)
	}
	if filepath.Base(path) != "script.sh" {
		t.Errorf("script file = %q, want script.sh", path)
	}
}

func TestGetScriptPathWhenNoScript(t *testing.T) {
	cfg := config.NewConfig(nil)
	os.Unsetenv("SCRIPT")
	b := NewBinarySupervisor("UNKNOWN", cfg)
	if path := b.getScriptPath(); path != "" {
		t.Errorf("expected empty script path, got %q", path)
	}
}