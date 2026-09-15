package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/faassupervisor/go/internal/config"
	"github.com/faassupervisor/go/internal/errors"
	"github.com/faassupervisor/go/internal/events"
	"github.com/faassupervisor/go/internal/logger"
	"github.com/faassupervisor/go/internal/utils"
)

const minioCredentialsPath = "/var/run/secrets/providers/minio.default"

// providerFactory builds storage providers. It is overridable in tests to
// avoid real network calls.
var providerFactory = CreateProvider

type AuthData struct {
	Type  string
	Creds map[string]interface{}
}

func (a *AuthData) GetCredential(key string) string {
	if a == nil || a.Creds == nil {
		return ""
	}
	if v, ok := a.Creds[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

type OutputEntry struct {
	StorageProvider string
	Path            string
	Prefix          []string
	Suffix          []string
}

type StorageConfig struct {
	cfg         *config.Config
	S3Auth      map[string]*AuthData
	MinioAuth   map[string]*AuthData
	OnedataAuth map[string]*AuthData
	WebdavAuth  map[string]*AuthData
	RucioAuth   map[string]*AuthData
	Input       []OutputEntry
	Output      []OutputEntry
}

func NewStorageConfig(cfg *config.Config) (sc *StorageConfig) {
	sc = &StorageConfig{
		cfg:         cfg,
		S3Auth:      map[string]*AuthData{"default": {Type: "S3"}},
		MinioAuth:   map[string]*AuthData{},
		OnedataAuth: map[string]*AuthData{},
		WebdavAuth:  map[string]*AuthData{},
		RucioAuth:   map[string]*AuthData{},
	}
	if err := sc.parseConfig(); err != nil {
		if w, ok := err.(*errors.Warning); ok {
			logger.GetLogger().Warning("%v", w)
			return sc
		}
		// Mirrors the @exception decorator: FaasSupervisorError with "Error"
		// in the class name aborts the process.
		logger.GetLogger().Error("%v", err)
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	return sc
}

func (s *StorageConfig) parseConfig() error {
	outputVal := s.cfg.ReadCfgVar("output")
	if isMissing(outputVal) {
		logger.GetLogger().Warning("There is no output defined for this function.")
	} else {
		s.Output = parseIoEntries(outputVal)
	}

	inputVal := s.cfg.ReadCfgVar("input")
	if isMissing(inputVal) {
		logger.GetLogger().Warning("There is no input defined for this function.")
	} else {
		s.Input = parseIoEntries(inputVal)
	}

	storageProviders := s.cfg.ReadCfgVar("storage_providers")
	if isMissing(storageProviders) {
		logger.GetLogger().Warning("There is no storage provider defined for this function.")
		return nil
	}
	provMap, ok := storageProviders.(map[string]interface{})
	if !ok {
		return nil
	}
	for providerType, validate := range map[string]func(interface{}) error{
		"s3":     s.validateS3,
		"minio":  s.validateMinio,
		"onedata": s.validateOnedata,
		"webdav": s.validateWebdav,
		"rucio":  s.validateRucio,
	} {
		if v, present := provMap[providerType]; present && isTruthy(v) {
			if err := validate(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func isMissing(val interface{}) bool {
	if val == nil {
		return true
	}
	if s, ok := val.(string); ok {
		return s == ""
	}
	return false
}

func isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	if s, ok := val.(string); ok {
		return s != ""
	}
	if m, ok := val.(map[string]interface{}); ok {
		return len(m) > 0
	}
	if l, ok := val.([]interface{}); ok {
		return len(l) > 0
	}
	return true
}

func parseIoEntries(val interface{}) []OutputEntry {
	var entries []OutputEntry
	list, ok := val.([]interface{})
	if !ok {
		return entries
	}
	for _, item := range list {
		entryMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		entry := OutputEntry{
			StorageProvider: fmt.Sprintf("%v", entryMap["storage_provider"]),
			Path:            fmt.Sprintf("%v", entryMap["path"]),
		}
		entry.Prefix = stringList(entryMap["prefix"])
		entry.Suffix = stringList(entryMap["suffix"])
		entries = append(entries, entry)
	}
	return entries
}

func stringList(val interface{}) []string {
	var out []string
	list, ok := val.([]interface{})
	if !ok {
		return out
	}
	for _, item := range list {
		out = append(out, fmt.Sprintf("%v", item))
	}
	return out
}

func (s *StorageConfig) validateS3(val interface{}) error {
	entries, ok := val.(map[string]interface{})
	if !ok {
		return errors.StorageAuthError("S3")
	}
	for id, credsRaw := range entries {
		creds, ok := credsRaw.(map[string]interface{})
		if !ok {
			return errors.StorageAuthError("S3")
		}
		if !hasNonEmpty(creds, "access_key") || !hasNonEmpty(creds, "secret_key") {
			return errors.StorageAuthError("S3")
		}
		s.S3Auth[id] = &AuthData{Type: "S3", Creds: creds}
	}
	return nil
}

func (s *StorageConfig) validateMinio(val interface{}) error {
	entries, ok := val.(map[string]interface{})
	if !ok {
		return errors.StorageAuthError("MINIO")
	}
	for id, credsRaw := range entries {
		creds, ok := credsRaw.(map[string]interface{})
		if !ok {
			return errors.StorageAuthError("MINIO")
		}
		newCreds := make(map[string]interface{}, len(creds))
		for k, v := range creds {
			newCreds[k] = v
		}
		if id == "default" {
			// If the provider id is "default" the MinIO credentials are read
			// from the user secret mounted in the container.
			if !utils.IsDirectory(minioCredentialsPath) {
				logger.GetLogger().Info("No MinIO user configuration found")
				continue
			}
			accessKey, aerr := os.ReadFile(filepath.Join(minioCredentialsPath, "accessKey"))
			secretKey, serr := os.ReadFile(filepath.Join(minioCredentialsPath, "secretKey"))
			if aerr != nil || serr != nil || len(strings.TrimSpace(string(accessKey))) == 0 || len(strings.TrimSpace(string(secretKey))) == 0 {
				return errors.StorageAuthError("MINIO")
			}
			newCreds["access_key"] = strings.TrimSpace(string(accessKey))
			newCreds["secret_key"] = strings.TrimSpace(string(secretKey))
			s.MinioAuth[id] = &AuthData{Type: "MINIO", Creds: newCreds}
		} else {
			if !hasNonEmpty(creds, "access_key") || !hasNonEmpty(creds, "secret_key") {
				return errors.StorageAuthError("MINIO")
			}
			s.MinioAuth[id] = &AuthData{Type: "MINIO", Creds: newCreds}
		}
	}
	return nil
}

func hasNonEmpty(creds map[string]interface{}, key string) bool {
	v, present := creds[key]
	if !present || v == nil {
		return false
	}
	if s, ok := v.(string); ok {
		return s != ""
	}
	return true
}

func (s *StorageConfig) validateOnedata(val interface{}) error {
	entries, ok := val.(map[string]interface{})
	if !ok {
		return errors.StorageAuthError("ONEDATA")
	}
	for id, credsRaw := range entries {
		creds, ok := credsRaw.(map[string]interface{})
		if !ok {
			return errors.StorageAuthError("ONEDATA")
		}
		for _, k := range []string{"oneprovider_host", "token", "space"} {
			if !hasNonEmpty(creds, k) {
				return errors.StorageAuthError("ONEDATA")
			}
		}
		s.OnedataAuth[id] = &AuthData{Type: "ONEDATA", Creds: creds}
	}
	return nil
}

func (s *StorageConfig) validateWebdav(val interface{}) error {
	entries, ok := val.(map[string]interface{})
	if !ok {
		return errors.StorageAuthError("WEBDAV")
	}
	for id, credsRaw := range entries {
		creds, ok := credsRaw.(map[string]interface{})
		if !ok {
			return errors.StorageAuthError("WEBDAV")
		}
		for _, k := range []string{"hostname", "login", "password"} {
			if !hasNonEmpty(creds, k) {
				return errors.StorageAuthError("WEBDAV")
			}
		}
		s.WebdavAuth[id] = &AuthData{Type: "WEBDAV", Creds: creds}
	}
	return nil
}

func (s *StorageConfig) validateRucio(val interface{}) error {
	entries, ok := val.(map[string]interface{})
	if !ok {
		return errors.StorageAuthError("RUCIO")
	}
	for id, credsRaw := range entries {
		creds, ok := credsRaw.(map[string]interface{})
		if !ok {
			return errors.StorageAuthError("RUCIO")
		}
		for _, k := range []string{"host", "rse", "account", "auth_host"} {
			if !hasNonEmpty(creds, k) {
				return errors.StorageAuthError("RUCIO")
			}
		}
		if !hasNonEmpty(creds, "access_token") && !hasNonEmpty(creds, "refresh_token") {
			return errors.StorageAuthError("RUCIO")
		}
		s.RucioAuth[id] = &AuthData{Type: "RUCIO", Creds: creds}
	}
	return nil
}

func (s *StorageConfig) GetAuthData(storageType, providerId string) *AuthData {
	switch storageType {
	case "S3":
		return s.S3Auth[providerId]
	case "MINIO":
		return s.MinioAuth[providerId]
	case "ONEDATA":
		return s.OnedataAuth[providerId]
	case "WEBDAV":
		return s.WebdavAuth[providerId]
	case "RUCIO":
		return s.RucioAuth[providerId]
	}
	return nil
}

func (s *StorageConfig) getInputAuthData(parsed events.Event) (*AuthData, error) {
	storageType := parsed.GetType()
	// Change storage type for dCache events to use WebDav.
	if storageType == "DCACHE" {
		storageType = "WEBDAV"
	}
	if storageType == "ONEDATA" {
		objectKey := parsed.GetObjectKey()
		if objectKey == "" {
			return nil, errors.StorageAuthError("ONEDATA")
		}
		// Get the onedata space from the event object_key (first segment).
		eventSpace := strings.SplitN(strings.TrimLeft(objectKey, "/"), "/", 2)[0]
		for _, inputValue := range s.Input {
			pType := utils.GetStorageType(inputValue.StorageProvider)
			if pType != "ONEDATA" {
				continue
			}
			pId := utils.GetStorageId(inputValue.StorageProvider)
			if auth, ok := s.OnedataAuth[pId]; ok && auth.GetCredential("space") == eventSpace {
				return auth, nil
			}
		}
		return nil, errors.StorageAuthError("ONEDATA")
	}
	if storageType == "UNKNOWN" {
		// Unknown events have no storage provider (Local is used).
		return nil, nil
	}
	providerId := parsed.GetProviderId()
	if providerId == "" {
		providerId = "default"
	}
	return s.GetAuthData(storageType, providerId), nil
}

func (s *StorageConfig) DownloadInput(parsed events.Event, inputDir string) (string, error) {
	auth, err := s.getInputAuthData(parsed)
	if err != nil {
		return "", err
	}
	provider := providerFactory(auth)
	return provider.DownloadFile(parsed, inputDir)
}

func (s *StorageConfig) UploadOutput(outputDir string, parsed events.Event) error {
	outputFiles, err := utils.GetAllFilesInDir(outputDir)
	if err != nil {
		return err
	}
	if len(outputFiles) == 0 {
		return nil
	}
	providerInstances := map[string]Provider{}

	for _, output := range s.Output {
		providerType := utils.GetStorageType(output.StorageProvider)
		providerId := utils.GetStorageId(output.StorageProvider)

		currentPath := output.Path
		// Change the output path to a private bucket if isolation applies.
		if providerType == "MINIO" &&
			s.cfg.ReadCfgString("isolation_level") == "USER" &&
			parsed != nil &&
			parsed.GetBucketName() != "" &&
			utils.Contains(s.cfg.ReadCfgListString("bucket_list"), parsed.GetBucketName()) {
			currentPath = applyMinioIsolation(parsed.GetBucketName(), currentPath)
		}

		for _, filePath := range outputFiles {
			fileName := outputFileName(outputDir, filePath)
			if !matchesFilters(fileName, output.Prefix, output.Suffix) {
				continue
			}

			key := providerType + "." + providerId
			provider, ok := providerInstances[key]
			if !ok {
				auth := s.GetAuthData(providerType, providerId)
				provider = providerFactory(auth)
				providerInstances[key] = provider
			}
			if err := provider.UploadFile(filePath, fileName, currentPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// outputFileName returns the name of the file relative to the output dir,
// with new lines and leading slashes removed.
func outputFileName(outputDir, filePath string) string {
	name := strings.TrimSpace(filePath)
	name = strings.TrimLeft(name, "/")
	name = strings.TrimPrefix(name, strings.TrimPrefix(outputDir, "/")+"/")
	name = strings.TrimSpace(name)
	return strings.TrimLeft(name, "/")
}

// matchesFilters returns true when the file name matches any of the provided
// prefixes and suffixes. An absent/empty prefix or suffix matches everything.
func matchesFilters(fileName string, prefix, suffix []string) bool {
	prefixOK := len(prefix) == 0
	for _, p := range prefix {
		if strings.HasPrefix(fileName, p) {
			prefixOK = true
			break
		}
	}
	if !prefixOK {
		return false
	}
	suffixOK := len(suffix) == 0
	for _, sf := range suffix {
		if strings.HasSuffix(fileName, sf) {
			suffixOK = true
			break
		}
	}
	return suffixOK
}

func applyMinioIsolation(bucketName, path string) string {
	segments := strings.Split(path, "/")
	if len(segments) <= 1 {
		return path
	}
	return bucketName + "/" + strings.Join(segments[1:], "/")
}

func GetBucketName(outputPath string) string {
	segments := strings.SplitN(outputPath, "/", 2)
	return segments[0]
}

func GetFileKey(outputPath, fileName string) string {
	segments := strings.SplitN(outputPath, "/", 2)
	if len(segments) < 2 {
		return fileName
	}
	return segments[1] + "/" + fileName
}