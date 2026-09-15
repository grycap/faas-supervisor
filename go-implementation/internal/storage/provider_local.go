package storage

import (
	"github.com/faassupervisor/go/internal/errors"
	"github.com/faassupervisor/go/internal/events"
)

// Provider is the storage provider interface (DefaultStorageProvider).
type Provider interface {
	GetType() string
	DownloadFile(parsed events.Event, inputDir string) (string, error)
	UploadFile(filePath, fileName, outputPath string) error
}

// CreateProvider is the factory for providers based on auth data type.
// A nil AuthData produces the Local provider; an unknown type raises the
// equivalent of InvalidStorageProviderError.
func CreateProvider(auth *AuthData) Provider {
	if auth == nil {
		return &Local{}
	}
	switch auth.Type {
	case "MINIO":
		return NewMinio(auth)
	case "S3":
		return NewS3(auth)
	case "ONEDATA":
		return NewOnedata(auth)
	case "WEBDAV":
		return NewWebDav(auth)
	case "RUCIO":
		return NewRucio(auth)
	case "LOCAL":
		return &Local{}
	default:
		panic(errors.InvalidStorageProviderError(auth.Type))
	}
}

// Local stores the event as the input file; upload is a no-op.
type Local struct{}

func (l *Local) GetType() string { return "LOCAL" }

func (l *Local) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	return events.SaveEvent(parsed, inputDir)
}

func (l *Local) UploadFile(filePath, fileName, outputPath string) error {
	return nil
}