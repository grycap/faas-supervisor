package config

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/faassupervisor/go/internal/utils"
	"gopkg.in/yaml.v3"
)

const (
	lambdaStorageConfigPath = "/var/task/function_config.yaml"
	lambdaStorageConfigEnv  = "FDL"
	binaryStorageConfigEnv  = "FUNCTION_CONFIG"
	binaryOscarStoragePath  = "/oscar/config/function_config.yaml"
)

var customVariables = []string{
	"log_level", "execution_mode", "extra_payload", "udocker_exec",
	"udocker_dir", "udocker_bin", "udocker_lib", "download_input",
}

type Config struct {
	raw map[string]interface{}
}

// NewConfig builds a Config from an already-parsed YAML document. Useful for
// tests and for callers that already hold the raw configuration.
func NewConfig(raw map[string]interface{}) *Config {
	return &Config{raw: raw}
}

func LoadConfig() *Config {
	raw := readRawConfig()
	return &Config{raw: raw}
}

// FromBytes parses a YAML document and returns a Config.
func FromBytes(data []byte) *Config {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return &Config{raw: nil}
	}
	return &Config{raw: raw}
}

func readRawConfig() map[string]interface{} {
	var configBytes []byte
	var err error

	if utils.IsLambdaEnvironment() {
		if utils.IsFile(lambdaStorageConfigPath) {
			configBytes, err = os.ReadFile(lambdaStorageConfigPath)
			if err != nil {
				return nil
			}
		} else if utils.GetEnvVar(lambdaStorageConfigEnv) != "" {
			decoded, decErr := base64.StdEncoding.DecodeString(utils.GetEnvVar(lambdaStorageConfigEnv))
			if decErr != nil {
				return nil
			}
			configBytes = decoded
		}
	} else {
		if utils.IsFile(binaryOscarStoragePath) {
			configBytes, err = os.ReadFile(binaryOscarStoragePath)
			if err != nil {
				return nil
			}
		} else if utils.GetEnvVar(binaryStorageConfigEnv) != "" {
			decoded, decErr := base64.StdEncoding.DecodeString(utils.GetEnvVar(binaryStorageConfigEnv))
			if decErr != nil {
				return nil
			}
			configBytes = decoded
		}
	}

	if len(configBytes) == 0 {
		return nil
	}

	var raw map[string]interface{}
	if err := yaml.Unmarshal(configBytes, &raw); err != nil {
		return nil
	}
	return raw
}

func (c *Config) ReadCfgVar(variable string) interface{} {
	if c == nil {
		return ""
	}
	if utils.Contains(customVariables, variable) {
		envVal := os.Getenv(upperSnake(variable))
		if envVal != "" {
			return envVal
		}
	}
	val, ok := c.raw[variable]
	if !ok {
		return ""
	}
	return val
}

func (c *Config) ReadCfgString(variable string) string {
	val := c.ReadCfgVar(variable)
	if val == nil {
		return ""
	}
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

func (c *Config) ReadCfgBool(variable string) bool {
	val := c.ReadCfgVar(variable)
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "True"
	}
	return false
}

func (c *Config) ReadCfgMap(variable string) map[string]interface{} {
	val := c.ReadCfgVar(variable)
	if m, ok := val.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func (c *Config) ReadCfgList(variable string) []interface{} {
	val := c.ReadCfgVar(variable)
	if l, ok := val.([]interface{}); ok {
		return l
	}
	return nil
}

func (c *Config) ReadCfgListString(variable string) []string {
	var out []string
	list := c.ReadCfgList(variable)
	for _, item := range list {
		out = append(out, fmt.Sprintf("%v", item))
	}
	return out
}

func upperSnake(v string) string {
	out := make([]byte, 0, len(v))
	for _, r := range v {
		if r >= 'a' && r <= 'z' {
			out = append(out, byte(r-'a'+'A'))
		} else {
			out = append(out, byte(r))
		}
	}
	return string(out)
}