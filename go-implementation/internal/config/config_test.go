package config

import (
	"os"
	"testing"
)

func TestNewConfigNil(t *testing.T) {
	cfg := NewConfig(nil)
	if got := cfg.ReadCfgVar("anything"); got != "" {
		t.Errorf("ReadCfgVar on nil config: got %v, want empty", got)
	}
}

func TestFromBytes(t *testing.T) {
	data := []byte(`
name: grayify
log_level: DEBUG
execution_mode: batch
`)
	cfg := FromBytes(data)
	if got := cfg.ReadCfgString("name"); got != "grayify" {
		t.Errorf("name = %q, want grayify", got)
	}
	if got := cfg.ReadCfgString("log_level"); got != "DEBUG" {
		t.Errorf("log_level = %q, want DEBUG", got)
	}
	if got := cfg.ReadCfgString("execution_mode"); got != "batch" {
		t.Errorf("execution_mode = %q, want batch", got)
	}
}

func TestFromBytesInvalidYAML(t *testing.T) {
	cfg := FromBytes([]byte(":::not yaml:::"))
	// Should not panic; returns config with nil raw.
	if got := cfg.ReadCfgVar("anything"); got != "" {
		t.Errorf("expected empty, got %v", got)
	}
}

func TestReadCfgBool(t *testing.T) {
	cfg := FromBytes([]byte(`
flag_on: true
flag_off: false
flag_str: "true"
flag_yes: "True"
`))
	if !cfg.ReadCfgBool("flag_on") {
		t.Error("flag_on should be true")
	}
	if cfg.ReadCfgBool("flag_off") {
		t.Error("flag_off should be false")
	}
	if !cfg.ReadCfgBool("flag_str") {
		t.Error("flag_str \"true\" should be true")
	}
	if !cfg.ReadCfgBool("flag_yes") {
		t.Error("flag_yes \"True\" should be true")
	}
	if cfg.ReadCfgBool("missing") {
		t.Error("missing var should be false")
	}
}

func TestReadCfgMap(t *testing.T) {
	cfg := FromBytes([]byte(`
container:
  image: ghcr.io/test
  timeout_threshold: 10
`))
	m := cfg.ReadCfgMap("container")
	if m == nil {
		t.Fatal("expected container map")
	}
	if m["image"] != "ghcr.io/test" {
		t.Errorf("image = %v", m["image"])
	}
}

func TestReadCfgList(t *testing.T) {
	cfg := FromBytes([]byte(`
bucket_list:
  - alpha
  - beta
`))
	l := cfg.ReadCfgList("bucket_list")
	if len(l) != 2 {
		t.Fatalf("expected 2 items, got %d", len(l))
	}
}

func TestReadCfgListString(t *testing.T) {
	cfg := FromBytes([]byte(`
tags:
  - hello
  - world
`))
	sl := cfg.ReadCfgListString("tags")
	if len(sl) != 2 || sl[0] != "hello" || sl[1] != "world" {
		t.Errorf("ReadCfgListString = %v", sl)
	}
}

func TestReadCfgVarMissing(t *testing.T) {
	cfg := FromBytes([]byte(`name: test`))
	if got := cfg.ReadCfgVar("nonexistent"); got != "" {
		t.Errorf("expected empty, got %v", got)
	}
}

func TestCustomVarEnvOverride(t *testing.T) {
	cfg := FromBytes([]byte(`
log_level: WARNING
`))
	// Env var LOG_LEVEL should override the config value.
	os.Setenv("LOG_LEVEL", "DEBUG")
	defer os.Unsetenv("LOG_LEVEL")

	if got := cfg.ReadCfgVar("log_level"); got != "DEBUG" {
		t.Errorf("ReadCfgVar log_level = %v, want DEBUG (env override)", got)
	}
}

func TestCustomVarEnvEmptyFallsThrough(t *testing.T) {
	cfg := FromBytes([]byte(`
log_level: WARNING
`))
	// Empty env var should fall through to config value.
	os.Setenv("LOG_LEVEL", "")
	defer os.Unsetenv("LOG_LEVEL")

	if got := cfg.ReadCfgVar("log_level"); got != "WARNING" {
		t.Errorf("ReadCfgVar log_level = %v, want WARNING (empty env falls through)", got)
	}
}

func TestNonCustomVarNotOverriddenByEnv(t *testing.T) {
	cfg := FromBytes([]byte(`
name: grayify
`))
	// "name" is NOT a custom variable; env should not override it.
	os.Setenv("NAME", "other")
	defer os.Unsetenv("NAME")

	if got := cfg.ReadCfgVar("name"); got != "grayify" {
		t.Errorf("ReadCfgVar name = %v, want grayify (non-custom not overridden)", got)
	}
}

func TestUpperSnake(t *testing.T) {
	cases := map[string]string{
		"log_level":       "LOG_LEVEL",
		"execution_mode":  "EXECUTION_MODE",
		"udocker_exec":    "UDOCKER_EXEC",
		"download_input":  "DOWNLOAD_INPUT",
		"simple":          "SIMPLE",
	}
	for input, want := range cases {
		if got := upperSnake(input); got != want {
			t.Errorf("upperSnake(%q) = %q, want %q", input, got, want)
		}
	}
}
