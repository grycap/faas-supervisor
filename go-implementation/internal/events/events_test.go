package events

import (
	"encoding/json"
	"os"
	"testing"
)

// testConfigReader is a minimal ConfigReader implementation for tests.
type testConfigReader struct {
	values map[string]interface{}
}

func (t *testConfigReader) ReadCfgVar(variable string) interface{} {
	if v, ok := t.values[variable]; ok {
		return v
	}
	return ""
}

var (
	minioEvent = map[string]interface{}{
		"Key": "images/nature-wallpaper-229.jpg",
		"Records": []interface{}{
			map[string]interface{}{
				"s3": map[string]interface{}{
					"object": map[string]interface{}{"key": "nature-wallpaper-229.jpg"},
					"bucket": map[string]interface{}{"name": "images", "arn": "arn:aws:s3:::images"},
				},
				"eventSource": "minio:s3",
				"eventTime":   "2018-06-29T10:23:44Z",
			},
		},
	}

	minioEventURLEncoded = map[string]interface{}{
		"Key": "grayify/in/koala.jpeg",
		"Records": []interface{}{
			map[string]interface{}{
				"s3": map[string]interface{}{
					"object": map[string]interface{}{"key": "in%2Fkoala.jpeg"},
					"bucket": map[string]interface{}{"name": "grayify", "arn": "arn:aws:s3:::grayify"},
				},
				"eventSource": "minio:s3",
				"eventTime":   "2026-07-10T11:55:11.046Z",
			},
		},
	}

	s3Event = map[string]interface{}{
		"Records": []interface{}{
			map[string]interface{}{
				"awsRegion":   "us-east-1",
				"eventSource": "aws:s3",
				"eventTime":   "2018-06-29T10:23:44Z",
				"s3": map[string]interface{}{
					"bucket": map[string]interface{}{"arn": "arn:aws:s3:::darknet-bucket", "name": "darknet-bucket"},
					"object": map[string]interface{}{"key": "darknet-s3/input/dog.jpg"},
				},
			},
		},
	}

	unknownEvent = map[string]interface{}{
		"Records": []interface{}{
			map[string]interface{}{"eventSource": "narnia"},
		},
	}

	apigtwEventWoJSON = map[string]interface{}{
		"body":        "aXQgd29ya3Mh",
		"headers":     map[string]interface{}{"Content-Type": "application/octet-stream"},
		"httpMethod":  "POST",
		"queryStringParameters": nil,
	}

	apigtwEventWJSON = map[string]interface{}{
		"body":                  s3Event,
		"headers":               map[string]interface{}{"Content-Type": "application/json"},
		"httpMethod":            "POST",
		"queryStringParameters": map[string]interface{}{"q1": "v1", "q2": "v2"},
	}

	dcacheEvent = map[string]interface{}{
		"event":        map[string]interface{}{"name": "image2.jpg", "mask": []interface{}{"IN_CREATE"}},
		"subscription": "https://prometheus.desy.de:3880/events/AAA",
	}

	rucioEvent = map[string]interface{}{
		"event_type": "close",
		"payload":    map[string]interface{}{"name": "dataset_name", "scope": "user.jdoe"},
	}
)

func dcacheCfg() ConfigReader {
	return &testConfigReader{values: map[string]interface{}{
		"input": []interface{}{
			map[string]interface{}{"storage_provider": "webdav.dcache", "path": "/pnfs/desy"},
		},
	}}
}

func TestParseMinioEvent(t *testing.T) {
	ev := ParseEvent(minioEvent, "default", nil)
	if ev == nil || ev.GetType() != "MINIO" {
		t.Fatalf("expected MINIO event, got %v", ev)
	}
	if ev.GetObjectKey() != "nature-wallpaper-229.jpg" {
		t.Errorf("object_key = %q", ev.GetObjectKey())
	}
	if ev.GetBucketName() != "images" {
		t.Errorf("bucket_name = %q", ev.GetBucketName())
	}
	if ev.GetFileName() != "nature-wallpaper-229.jpg" {
		t.Errorf("file_name = %q", ev.GetFileName())
	}
	if ev.GetEventTime() != "2018-06-29T10:23:44Z" {
		t.Errorf("event_time = %q", ev.GetEventTime())
	}
	if ev.GetProviderId() != "default" {
		t.Errorf("provider_id = %q", ev.GetProviderId())
	}
}

func TestParseMinioEventURLDecoding(t *testing.T) {
	ev := ParseEvent(minioEventURLEncoded, "default", nil)
	if ev.GetObjectKey() != "in/koala.jpeg" {
		t.Errorf("object_key = %q, want 'in/koala.jpeg'", ev.GetObjectKey())
	}
	if ev.GetFileName() != "koala.jpeg" {
		t.Errorf("file_name = %q", ev.GetFileName())
	}
	if ev.GetBucketName() != "grayify" {
		t.Errorf("bucket_name = %q", ev.GetBucketName())
	}
}

func TestParseS3Event(t *testing.T) {
	ev := ParseEvent(s3Event, "default", nil)
	if ev == nil || ev.GetType() != "S3" {
		t.Fatalf("expected S3 event, got %v", ev)
	}
	if ev.GetObjectKey() != "darknet-s3/input/dog.jpg" {
		t.Errorf("object_key = %q", ev.GetObjectKey())
	}
	if ev.GetBucketName() != "darknet-bucket" {
		t.Errorf("bucket_name = %q", ev.GetBucketName())
	}
	if ev.GetFileName() != "dog.jpg" {
		t.Errorf("file_name = %q", ev.GetFileName())
	}
}

func TestParseDelegatedEvent(t *testing.T) {
	delegated := map[string]interface{}{
		"storage_provider": "minio.cluster2",
		"event":            `{"EventName":"s3:ObjectCreated:Put","Records":[{"s3":{"object":{"key":"nature-wallpaper-229.jpg"},"bucket":{"name":"images"}},"eventSource":"minio:s3","eventTime":"2018-06-29T10:23:44Z"}]}`,
	}
	ev := ParseEvent(delegated, "default", nil)
	if ev == nil || ev.GetType() != "MINIO" {
		t.Fatalf("expected delegated MINIO event, got %v", ev)
	}
	if ev.GetProviderId() != "minio.cluster2" {
		t.Errorf("provider_id = %q, want minio.cluster2", ev.GetProviderId())
	}
	if ev.GetBucketName() != "images" {
		t.Errorf("bucket_name = %q", ev.GetBucketName())
	}
	if ev.GetObjectKey() != "nature-wallpaper-229.jpg" {
		t.Errorf("object_key = %q", ev.GetObjectKey())
	}
}

func TestParseDelegatedStringEvent(t *testing.T) {
	// The delegated event may arrive as a string already.
	b, err := json.Marshal(minioEvent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev := ParseEvent(string(b), "default", nil)
	if ev == nil || ev.GetType() != "MINIO" {
		t.Fatalf("expected MINIO event from JSON string, got %v", ev)
	}
}

func TestParseApiGatewayJSONBody(t *testing.T) {
	os.Unsetenv("CONT_VAR_q1")
	os.Unsetenv("CONT_VAR_q2")
	ev := ParseEvent(apigtwEventWJSON, "default", nil)
	if ev == nil || ev.GetType() != "S3" {
		t.Fatalf("expected S3 event from apigateway, got %v", ev)
	}
	if os.Getenv("CONT_VAR_q1") != "v1" || os.Getenv("CONT_VAR_q2") != "v2" {
		t.Errorf("CONT_VAR params not set: q1=%q q2=%q", os.Getenv("CONT_VAR_q1"), os.Getenv("CONT_VAR_q2"))
	}
}

func TestParseApiGatewayBinaryBody(t *testing.T) {
	ev := ParseEvent(apigtwEventWoJSON, "default", nil)
	if ev.GetType() != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN event from non-JSON apigateway body, got %v", ev.GetType())
	}
}

func TestParseUnknownEvent(t *testing.T) {
	ev := ParseEvent(unknownEvent, "default", nil)
	if ev == nil || ev.GetType() != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN event, got %v", ev)
	}
}

func TestParseUnknownNonJSONString(t *testing.T) {
	ev := ParseEvent("this is not json", "default", nil)
	if ev == nil || ev.GetType() != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN event for non-JSON string, got %v", ev)
	}
}

func TestParseDCacheEvent(t *testing.T) {
	ev := ParseEvent(dcacheEvent, "default", dcacheCfg())
	if ev == nil || ev.GetType() != "DCACHE" {
		t.Fatalf("expected DCACHE event, got %v", ev)
	}
	if ev.GetFileName() != "image2.jpg" {
		t.Errorf("file_name = %q", ev.GetFileName())
	}
	if ev.GetObjectKey() != "/pnfs/desy/image2.jpg" {
		t.Errorf("object_key = %q, want %q", ev.GetObjectKey(), "/pnfs/desy/image2.jpg")
	}
}

func TestParseDCacheEventWithoutInput(t *testing.T) {
	ev := ParseEvent(dcacheEvent, "default", nil)
	if ev == nil || ev.GetType() != "DCACHE" {
		t.Fatalf("expected DCACHE event, got %v", ev)
	}
	if ev.GetObjectKey() != "image2.jpg" {
		t.Errorf("object_key = %q, want file_name", ev.GetObjectKey())
	}
}

func TestParseRucioEvent(t *testing.T) {
	ev := ParseEvent(rucioEvent, "default", nil)
	if ev == nil || ev.GetType() != "RUCIO" {
		t.Fatalf("expected RUCIO event, got %v", ev)
	}
	rucio, ok := ev.(*RucioEvent)
	if !ok {
		t.Fatalf("expected *RucioEvent, got %T", ev)
	}
	if rucio.GetScope() != "user.jdoe" {
		t.Errorf("scope = %q", rucio.GetScope())
	}
	if ev.GetObjectKey() != "dataset_name" {
		t.Errorf("object_key = %q", ev.GetObjectKey())
	}
}

func TestStorageEnvVars(t *testing.T) {
	os.Unsetenv("STORAGE_OBJECT_KEY")
	os.Unsetenv("EVENT_TIME")
	os.Unsetenv("EVENT")
	ParseEvent(minioEvent, "default", nil)
	if os.Getenv("STORAGE_OBJECT_KEY") != "nature-wallpaper-229.jpg" {
		t.Errorf("STORAGE_OBJECT_KEY = %q", os.Getenv("STORAGE_OBJECT_KEY"))
	}
	if os.Getenv("EVENT_TIME") != "2018-06-29T10:23:44Z" {
		t.Errorf("EVENT_TIME = %q", os.Getenv("EVENT_TIME"))
	}
	if os.Getenv("EVENT") == "" {
		t.Error("EVENT env var not set")
	}
}