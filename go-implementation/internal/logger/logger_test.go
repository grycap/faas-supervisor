package logger

import (
	"os"
	"strings"
	"testing"
)

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()
	fn()
	w.Close()
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	return string(buf[:n])
}

func TestSetLevelCRITICALFiltersNoise(t *testing.T) {
	l := &Logger{}
	l.SetLevel("CRITICAL")
	out := captureStderr(t, func() {
		l.Debug("debug-line")
		l.Info("info-line")
		l.Warning("warning-line")
		l.Error("error-line")
		l.Critical("critical-line")
	})
	if strings.Contains(out, "info-line") || strings.Contains(out, "debug-line") {
		t.Errorf("CRITICAL level should filter INFO/DEBUG, got:\n%s", out)
	}
	if !strings.Contains(out, "critical-line") {
		t.Errorf("CRITICAL level should let CRITICAL through, got:\n%s", out)
	}
}

func TestSetLevelInvalidStaysInfo(t *testing.T) {
	l := &Logger{}
	l.SetLevel("NOT-A-REAL-LEVEL")
	out := captureStderr(t, func() {
		l.Debug("debug-line")
		l.Info("info-line")
	})
	if strings.Contains(out, "debug-line") {
		t.Errorf("unknown log level should keep INFO (no debug), got:\n%s", out)
	}
	if !strings.Contains(out, "info-line") {
		t.Errorf("unknown log level should keep INFO (info passes), got:\n%s", out)
	}
}

func TestOutputAlwaysVisible(t *testing.T) {
	l := &Logger{}
	l.SetLevel("CRITICAL")
	out := captureStderr(t, func() {
		l.Output("some script output line")
		l.Debug("hidden")
	})
	if !strings.Contains(out, "some script output line") {
		t.Errorf("Output() must be visible at CRITICAL, got:\n%s", out)
	}
	if strings.Contains(out, "hidden") {
		t.Errorf("DEBUG must be filtered at CRITICAL, got:\n%s", out)
	}
}

func TestStdoutPlainMessageOnly(t *testing.T) {
	l := &Logger{}
	out := captureStderr(t, func() {
		l.Stdout("hello cow")
	})
	if strings.Contains(out, "supervisor -") {
		t.Errorf("Stdout() must not include the prefix, got:\n%s", out)
	}
	if !strings.Contains(out, "hello cow") {
		t.Errorf("Stdout() must print the message, got:\n%s", out)
	}
}

func TestStdoutSkipsEmptyLines(t *testing.T) {
	l := &Logger{}
	out := captureStderr(t, func() {
		l.Stdout("")
	})
	if strings.TrimSpace(out) != "" {
		t.Errorf("Stdout() must skip empty lines, got:\n%q", out)
	}
}

func TestStderrIsErrorTypedAndAlwaysVisible(t *testing.T) {
	l := &Logger{}
	l.SetLevel("CRITICAL")
	out := captureStderr(t, func() {
		l.Stderr("convert: some error")
	})
	if !strings.Contains(out, "- supervisor - ERROR - convert: some error") {
		t.Errorf("Stderr() must be labelled ERROR at CRITICAL, got:\n%s", out)
	}
	if strings.Contains(out, "OUTPUT") {
		t.Errorf("Stderr() must not be labelled OUTPUT, got:\n%s", out)
	}
}