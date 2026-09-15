package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/faassupervisor/go/internal/config"
	"github.com/faassupervisor/go/internal/events"
)

func TestGetBucketName(t *testing.T) {
	cases := map[string]string{
		"grayify/out": "grayify",
		"bucket":      "bucket",
		"a/b/c":       "a",
	}
	for in, want := range cases {
		if got := GetBucketName(in); got != want {
			t.Errorf("GetBucketName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestGetFileKey(t *testing.T) {
	cases := map[string]struct {
		outputPath string
		fileName   string
		want       string
	}{
		"with folder":    {"grayify/out", "a.jpg", "out/a.jpg"},
		"without folder": {"bucket", "a.jpg", "a.jpg"},
	}
	for name, c := range cases {
		if got := GetFileKey(c.outputPath, c.fileName); got != c.want {
			t.Errorf("%s: GetFileKey(%q, %q) = %q, want %q", name, c.outputPath, c.fileName, got, c.want)
		}
	}
}

func TestApplyMinioIsolation(t *testing.T) {
	cases := map[string]struct {
		bucket string
		path   string
		want   string
	}{
		"nested path":    {"jdoe", "grayify/out", "jdoe/out"},
		"single segment": {"jdoe", "out", "out"},
		"deep":           {"jdoe", "a/b/c", "jdoe/b/c"},
	}
	for name, c := range cases {
		if got := applyMinioIsolation(c.bucket, c.path); got != c.want {
			t.Errorf("%s: applyMinioIsolation = %q, want %q", name, got, c.want)
		}
	}
}

func TestOutputFileName(t *testing.T) {
	dir := filepath.Join(string(filepath.Separator)+"tmp", "faas-xyz")
	if got := outputFileName(dir, dir+"/result-a.txt"); got != "result-a.txt" {
		t.Errorf("outputFileName = %q, want result-a.txt", got)
	}
	if got := outputFileName(dir, dir+"/nested/sub.txt"); got != "nested/sub.txt" {
		t.Errorf("outputFileName nested = %q, want nested/sub.txt", got)
	}
}

func TestMatchesFilters(t *testing.T) {
	if !matchesFilters("result-a.txt", nil, nil) {
		t.Error("empty filters must match everything")
	}
	if !matchesFilters("result-a.txt", []string{"result-"}, nil) {
		t.Error("prefix result- should match")
	}
	if matchesFilters("other.txt", []string{"result-"}, nil) {
		t.Error("prefix result- should not match other.txt")
	}
	if !matchesFilters("photo.jpg", nil, []string{"jpg", "png"}) {
		t.Error("suffix jpg should match")
	}
	if matchesFilters("photo.gif", nil, []string{"jpg", "png"}) {
		t.Error("suffix gif should not match")
	}
	if !matchesFilters("result-a.txt", []string{"result-"}, []string{"txt"}) {
		t.Error("prefix+suffix should match result-a.txt")
	}
}

func TestParseIoEntries(t *testing.T) {
	val := []interface{}{
		map[string]interface{}{
			"storage_provider": "minio.cluster2",
			"path":             "grayify/out",
			"prefix":           []interface{}{"result-", "output-"},
			"suffix":           []interface{}{"txt"},
		},
	}
	entries := parseIoEntries(val)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.StorageProvider != "minio.cluster2" || e.Path != "grayify/out" {
		t.Errorf("entry = %+v", e)
	}
	if len(e.Prefix) != 2 || e.Prefix[0] != "result-" || len(e.Suffix) != 1 || e.Suffix[0] != "txt" {
		t.Errorf("entry prefix/suffix = %v %v", e.Prefix, e.Suffix)
	}
}

func TestStorageConfigMinioNonDefault(t *testing.T) {
	cfg := config.FromBytes([]byte(`
storage_providers:
  minio:
    cluster2:
      endpoint: http://minio2:9000
      access_key: ak
      secret_key: sk
`))
	sc := NewStorageConfig(cfg)
	if _, ok := sc.MinioAuth["cluster2"]; !ok {
		t.Fatal("expected minio.cluster2 auth data")
	}
}

func TestStorageConfigMissingMinioDefaultSkips(t *testing.T) {
	// When /var/run/secrets/providers/minio.default does not exist, the
	// "default" MinIO provider is skipped without error.
	cfg := config.FromBytes([]byte(`
storage_providers:
  minio:
    default:
      endpoint: http://localhost:9000
`))
	if _, err := statDefaultSecrets(); err != nil {
		sc := NewStorageConfig(cfg)
		if _, ok := sc.MinioAuth["default"]; ok {
			t.Fatal("expected minio default to be skipped when secrets dir is missing")
		}
	}
}

func TestStorageConfigS3Auth(t *testing.T) {
	cfg := config.FromBytes([]byte(`
storage_providers:
  s3:
    default:
      access_key: ak
      secret_key: sk
input:
- storage_provider: s3
  path: in
output:
- storage_provider: s3
  path: out
`))
	sc := NewStorageConfig(cfg)
	if _, ok := sc.S3Auth["default"]; !ok {
		t.Fatal("expected s3.default auth data")
	}
	if len(sc.Input) != 1 || len(sc.Output) != 1 {
		t.Errorf("input/output entries: %d %d", len(sc.Input), len(sc.Output))
	}
}

func TestStorageConfigDefaultS3WithNilCreds(t *testing.T) {
	cfg := config.NewConfig(nil)
	sc := NewStorageConfig(cfg)
	s3default := sc.GetAuthData("S3", "default")
	if s3default == nil || s3default.Creds != nil {
		t.Errorf("expected S3 default auth with nil creds, got %+v", s3default)
	}
}

func TestDownloadInputUnknownUsesLocal(t *testing.T) {
	cfg := config.NewConfig(nil)
	sc := NewStorageConfig(cfg)
	ev := events.ParseEvent("some-binary-data", "default", nil)
	if ev.GetType() != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN event, got %v", ev.GetType())
	}
	dir := t.TempDir()
	path, err := sc.DownloadInput(ev, dir)
	if err != nil {
		t.Fatalf("download input: %v", err)
	}
	if path == "" || !isFile(path) {
		t.Errorf("expected a saved input file, got %q", path)
	}
}

// fakeEvent implements events.Event minimally for upload tests.
type fakeEvent struct {
	events.BaseEvent
	bucket string
}

func (f *fakeEvent) GetBucketName() string { return f.bucket }

// uploadRecorder captures UploadFile calls made through providerFactory.
type uploadRecorder struct {
	capturedPath     string
	capturedFileName string
}

func TestUploadOutputMinioIsolationSelectsFiles(t *testing.T) {
	cfg := config.FromBytes([]byte(`
isolation_level: USER
bucket_list: [jdoe]
storage_providers:
  minio:
    default:
      endpoint: http://localhost:9000
      access_key: minioadmin
      secret_key: minioadmin
output:
- storage_provider: minio
  path: grayify/out
  prefix: [result-]
`))
	sc := NewStorageConfig(cfg)

	outputDir := t.TempDir()
	mustWrite(t, filepath.Join(outputDir, "result-a.txt"), "hello")
	mustWrite(t, filepath.Join(outputDir, "skip.txt"), "skip")

	recorder := &uploadRecorder{}
	origFactory := providerFactory
	providerFactory = func(auth *AuthData) Provider {
		return &recordingProvider{recorder: recorder}
	}
	defer func() { providerFactory = origFactory }()

	ev := &fakeEvent{bucket: "jdoe"}
	if err := sc.UploadOutput(outputDir, ev); err != nil {
		t.Fatalf("upload output: %v", err)
	}
	if got := recorder.capturedPath; got != "jdoe/out" {
		t.Errorf("isolated output path = %q, want jdoe/out", got)
	}
	if got := recorder.capturedFileName; got != "result-a.txt" {
		t.Errorf("uploaded file name = %q, want result-a.txt", got)
	}
}

// recordingProvider implements Provider and records upload calls.
type recordingProvider struct {
	recorder *uploadRecorder
}

func (r *recordingProvider) GetType() string                                    { return "MINIO" }
func (r *recordingProvider) DownloadFile(ev events.Event, dir string) (string, error) { return "", nil }
func (r *recordingProvider) UploadFile(filePath, fileName, outputPath string) error {
	r.recorder.capturedPath = outputPath
	r.recorder.capturedFileName = fileName
	return nil
}

func isFile(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func statDefaultSecrets() (os.FileInfo, error) {
	return os.Stat("/var/run/secrets/providers/minio.default")
}
