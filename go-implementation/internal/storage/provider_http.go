package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/faassupervisor/go/internal/errors"
	"github.com/faassupervisor/go/internal/events"
)

// Onedata CDMI provider.
type Onedata struct {
	auth *AuthData
}

func NewOnedata(auth *AuthData) *Onedata { return &Onedata{auth: auth} }

func (o *Onedata) GetType() string { return "ONEDATA" }

func (o *Onedata) host() string  { return o.auth.GetCredential("oneprovider_host") }
func (o *Onedata) space() string { return o.auth.GetCredential("space") }
func (o *Onedata) token() string { return o.auth.GetCredential("token") }

func (o *Onedata) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	url := fmt.Sprintf("https://%s/cdmi%s", o.host(), parsed.GetObjectKey())
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Auth-Token", o.token())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", errors.OnedataDownloadError(parsed.GetFileName(), resp.StatusCode)
	}
	localPath := filepath.Join(inputDir, parsed.GetFileName())
	if _, err := copyToFile(resp.Body, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

func (o *Onedata) UploadFile(filePath, fileName, outputPath string) error {
	fileName = strings.TrimLeft(strings.TrimRight(fileName, "/"), "/")
	outputPath = strings.TrimLeft(strings.TrimRight(outputPath, "/"), "/")
	uploadPath := outputPath + "/" + fileName

	// Ensure parent folders exist.
	folder := strings.TrimSuffix(uploadPath, fileName)
	if folder != "" {
		folder = strings.TrimSuffix(folder, "/") + "/"
		folderNoSpace := strings.TrimPrefix(folder, o.space()+"/")
		checkURL := fmt.Sprintf("https://%s/cdmi/%s/%s", o.host(), o.space(), folderNoSpace)
		if err := o.ensureFolder(checkURL, folder); err != nil {
			return err
		}
	}

	body, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	uploadURL := fmt.Sprintf("https://%s/cdmi/%s/%s", o.host(), o.space(), uploadPath)
	req, err := http.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Auth-Token", o.token())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 201, 202, 204:
		return nil
	}
	return errors.OnedataUploadError(fileName, resp.StatusCode)
}

func (o *Onedata) ensureFolder(checkURL, folderName string) error {
	req, err := http.NewRequest(http.MethodGet, checkURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Auth-Token", o.token())
	req.Header.Set("X-CDMI-Specification-Version", "1.1.1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()

	createReq, err := http.NewRequest(http.MethodPut, checkURL, nil)
	if err != nil {
		return err
	}
	createReq.Header.Set("X-Auth-Token", o.token())
	createReq.Header.Set("X-CDMI-Specification-Version", "1.1.1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()
	if createResp.StatusCode == http.StatusCreated {
		return nil
	}
	return errors.OnedataFolderCreationError(folderName, createResp.StatusCode)
}

// WebDav provider (dCache).
type WebDav struct {
	auth *AuthData
}

func NewWebDav(auth *AuthData) *WebDav { return &WebDav{auth: auth} }

func (w *WebDav) GetType() string { return "WEBDAV" }

func (w *WebDav) webdavClient() *http.Client {
	return http.DefaultClient
}

func (w *WebDav) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	hostname := w.auth.GetCredential("hostname")
	login := w.auth.GetCredential("login")
	password := w.auth.GetCredential("password")
	url := fmt.Sprintf("https://%s/%s", hostname, strings.TrimPrefix(parsed.GetObjectKey(), "/"))
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(login, password)
	resp, err := w.webdavClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	localPath := filepath.Join(inputDir, parsed.GetFileName())
	if _, err := copyToFile(resp.Body, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

func (w *WebDav) UploadFile(filePath, fileName, outputPath string) error {
	hostname := w.auth.GetCredential("hostname")
	login := w.auth.GetCredential("login")
	password := w.auth.GetCredential("password")
	base := fmt.Sprintf("https://%s/%s", hostname, strings.TrimPrefix(outputPath, "/"))
	destURL := base + "/" + fileName

	if err := w.mkdirIfMissing(base, login, password); err != nil {
		return err
	}

	body, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, destURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(login, password)
	resp, err := w.webdavClient().Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

func (w *WebDav) mkdirIfMissing(dirURL, login, password string) error {
	return webdavMkdir(dirURL, login, password)
}

// Rucio provider.
type Rucio struct {
	auth *AuthData
}

func NewRucio(auth *AuthData) *Rucio { return &Rucio{auth: auth} }

func (r *Rucio) GetType() string { return "RUCIO" }

func (r *Rucio) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	return rucioListAndDownload(r, parsed, inputDir)
}

func (r *Rucio) UploadFile(filePath, fileName, outputPath string) error {
	return rucioUpload(r, filePath, fileName, outputPath)
}