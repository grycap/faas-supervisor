package utils

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func GetStdin() string {
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)
	for {
		n, err := os.Stdin.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	return string(buf)
}

func JoinPaths(paths ...string) string {
	return filepath.Join(paths...)
}

func IsVarInEnv(v string) bool {
	return os.Getenv(v) != ""
}

func SetEnvVar(k, v string) {
	if k != "" && v != "" {
		os.Setenv(k, v)
	}
}

func GetEnvVar(v string) string {
	return os.Getenv(v)
}

func GetEnvVarOrDefault(v, defaultVal string) string {
	val := os.Getenv(v)
	if val == "" {
		return defaultVal
	}
	return val
}

func DeleteEnvVar(v string) {
	os.Unsetenv(v)
}

func IsLambdaEnvironment() bool {
	env := os.Getenv("AWS_EXECUTION_ENV")
	return env != "" && strings.HasPrefix(env, "AWS_Lambda_")
}

func IsLambdaImageEnvironment() bool {
	return os.Getenv("AWS_EXECUTION_ENV") == "AWS_Lambda_Image"
}

func BytesToBase64Str(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func DictToBase64Str(d map[string]interface{}) string {
	jsonBytes, _ := json.Marshal(d)
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

func UTF8ToBase64String(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func Base64ToStr(s string) string {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s
	}
	return string(decoded)
}

func GetStorageId(storageProvider string) string {
	parts := strings.SplitN(storageProvider, ".", 2)
	if len(parts) < 2 {
		return "default"
	}
	return parts[1]
}

func GetStorageType(storageProvider string) string {
	parts := strings.SplitN(storageProvider, ".", 2)
	if len(parts) == 0 {
		return ""
	}
	return strings.ToUpper(parts[0])
}

func RemovePrefix(text, prefix string) string {
	return strings.TrimPrefix(text, prefix)
}

func GetAllFilesInDir(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func CreateTmpDir() (string, error) {
	return os.MkdirTemp("", "faas-*")
}

func CreateFolder(name string) error {
	return os.MkdirAll(name, 0755)
}

func CreateFileWithContent(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

func ReadFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func ReadFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func IsFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func IsDirectory(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func GetFileName(path string) string {
	return filepath.Base(path)
}

func GetDirName(path string) string {
	return filepath.Dir(path)
}

func FormatContainerEnvVars(envVars map[string]interface{}) string {
	var result []string
	for k, v := range envVars {
		result = append(result, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(result, "\n")
}

func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
