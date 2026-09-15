package errors

import (
	"fmt"
	"os"
)

type FaasSupervisorError struct {
	Msg string
}

func (e *FaasSupervisorError) Error() string {
	return e.Msg
}

func NewFaasSupervisorError(msg string) *FaasSupervisorError {
	return &FaasSupervisorError{Msg: msg}
}

func NewFaasSupervisorErrorf(format string, args ...interface{}) *FaasSupervisorError {
	return &FaasSupervisorError{Msg: fmt.Sprintf(format, args...)}
}

type Warning struct {
	Msg string
}

func (e *Warning) Error() string {
	return e.Msg
}

func NewWarning(msg string) *Warning {
	return &Warning{Msg: msg}
}

func NewWarningf(format string, args ...interface{}) *Warning {
	return &Warning{Msg: fmt.Sprintf(format, args...)}
}

var (
	InvalidPlatformError               = NewFaasSupervisorError("This binary only works on a Linux Platform.\nTry executing the Python version.")
	InvalidSupervisorTypeError         = func(supTyp string) *FaasSupervisorError { return NewFaasSupervisorErrorf("The supervisor type '%s' is not allowed.", supTyp) }
	InvalidStoragePathTypeError        = func(storageType string) *FaasSupervisorError { return NewFaasSupervisorErrorf("The storage path type '%s' is not allowed.", storageType) }
	ContainerImageNotFoundError        = NewFaasSupervisorError("Container image id is not specified.")
	ContainerTimeoutExpiredWarning     = NewWarning("Container timeout expired.\nContainer execution stopped.")
	NoLambdaContextError               = NewFaasSupervisorError("No context found in the Lambda environment.")
	UnknowStorageEventWarning          = NewWarning("Unknown storage event detected.")
	InvalidStorageProviderError        = func(storageType string) *FaasSupervisorError { return NewFaasSupervisorErrorf("Invalid storage provider type defined: '%s'.", storageType) }
	NoStorageProviderDefinedWarning    = NewWarning("There is no storage provider defined for this function execution.")
	StorageTypeError                  = func(authType string) *FaasSupervisorError { return NewFaasSupervisorErrorf("The storage type '%s' is not allowed.", authType) }
	StorageAuthError                  = func(authType string) *FaasSupervisorError { return NewFaasSupervisorErrorf("The storage authentication of '%s' is not well-defined.", authType) }
	OnedataUploadError                = func(fileName string, statusCode int) *FaasSupervisorError { return NewFaasSupervisorErrorf("Uploading file '%s' to Onedata failed. Status code: %d", fileName, statusCode) }
	OnedataDownloadError              = func(fileName string, statusCode int) *FaasSupervisorError { return NewFaasSupervisorErrorf("Downloading file '%s' from Onedata failed. Status code: %d", fileName, statusCode) }
	OnedataFolderCreationError        = func(folderName string, statusCode int) *FaasSupervisorError { return NewFaasSupervisorErrorf("Folder '%s' creation in Onedata failed. Status code: %d", folderName, statusCode) }
	RucioDataIdentifierAlreadyExists  = func(scope, fileName string) *FaasSupervisorError { return NewFaasSupervisorErrorf("DID '%s:%s' already exists.", scope, fileName) }
	RucioNotRSE                       = func(msg string) *FaasSupervisorError { return NewFaasSupervisorErrorf("No RSEs available: %s.", msg) }
)

func HandleError(err error) {
	if err == nil {
		return
	}
	if _, ok := err.(*Warning); ok {
		// warnings don't exit
		return
	}
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
