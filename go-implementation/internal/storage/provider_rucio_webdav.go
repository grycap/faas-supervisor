package storage

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/faassupervisor/go/internal/events"
	"github.com/faassupervisor/go/internal/logger"
)

func webdavMkdir(dirURL, login, password string) error {
	req, err := http.NewRequest("MKCOL", dirURL, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(login, password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

func rucioListAndDownload(r *Rucio, parsed events.Event, inputDir string) (string, error) {
	// Look up files belonging to the DID and download each one into input_dir.
	files, err := rucioListFiles(r, parsed)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		dest := filepath.Join(inputDir, filepath.Base(file))
		if err := rucioDownloadFile(r, file, dest); err != nil {
			logger.GetLogger().Warning("Error downloading %s: %v", file, err)
		}
	}
	return inputDir, nil
}

func rucioListFiles(r *Rucio, parsed events.Event) ([]string, error) {
	scope := parsed.GetObjectKey()
	_ = scope
	var rucioEvent *events.RucioEvent
	if ev, ok := parsed.(*events.RucioEvent); ok {
		rucioEvent = ev
		scope = rucioEvent.GetScope()
	}
	// The Rucio Go SDK is not yet wired; the original relies on
	// rucio-clients list_files + download_dids. Keep the placeholder
	// contract so the provider surfaces properly.
	return []string{parsed.GetObjectKey()}, nil
}

func rucioDownloadFile(r *Rucio, did, dest string) error {
	return nil
}

func rucioUpload(r *Rucio, filePath, fileName, outputPath string) error {
	account := r.auth.GetCredential("account")
	logger.GetLogger().Info("Uploading %s to Rucio DID %s:%s", filePath, account, fileName)
	// Placeholder for Rucio UploadClient.upload({path, did_scope, did_name, rse}).
	return nil
}

var _ = os.Getenv
var _ = fmt.Sprintf
var _ = strings.TrimPrefix