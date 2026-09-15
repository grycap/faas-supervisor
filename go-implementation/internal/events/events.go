package events

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/faassupervisor/go/internal/logger"
	"github.com/faassupervisor/go/internal/utils"
)

// ConfigReader allows event parsing to read function configuration
// without importing the config package (avoids dependency cycles).
type ConfigReader interface {
	ReadCfgVar(variable string) interface{}
}

type Event interface {
	GetType() string
	GetFileName() string
	GetProviderId() string
	GetObjectKey() string
	GetEventTime() string
	GetBucketName() string
	SaveEvent(inputDir string) (string, error)
	RawEvent() map[string]interface{}
}

// SaveEvent persists the event as the input file in inputDir and returns
// the resulting path (Local provider contract).
func SaveEvent(event Event, inputDir string) (string, error) {
	return event.SaveEvent(inputDir)
}

type BaseEvent struct {
	Type         string
	FileName     string
	ProviderId   string
	ObjectKey    string
	EventTime    string
	BucketName   string
	Event        map[string]interface{}
	EventRecords map[string]interface{}
	Raw          interface{}
}

func (e *BaseEvent) GetType() string {
	if e == nil {
		return "UNKNOWN"
	}
	return e.Type
}

func (e *BaseEvent) GetFileName() string  { return e.FileName }
func (e *BaseEvent) GetProviderId() string { return e.ProviderId }
func (e *BaseEvent) GetObjectKey() string  { return e.ObjectKey }
func (e *BaseEvent) GetEventTime() string  { return e.EventTime }
func (e *BaseEvent) GetBucketName() string { return e.BucketName }

func (e *BaseEvent) RawEvent() map[string]interface{} { return e.Event }

func (e *BaseEvent) SaveEvent(inputDir string) (string, error) {
	return e.saveEventRaw(inputDir)
}

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func mapFromRaw(raw interface{}) map[string]interface{} {
	if m, ok := raw.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func getNested(base, a string) map[string]interface{} {
	m := mapFromRaw(base)
	if m == nil {
		return nil
	}
	return mapFromRaw(m[a])
}

// ParseEvent identifies the event type following the strict order in the
// specification: API Gateway -> dCache -> Rucio -> delegated -> storage.
func ParseEvent(raw interface{}, storageProvider string, cfg ConfigReader) Event {
	var event map[string]interface{}
	switch v := raw.(type) {
	case map[string]interface{}:
		event = v
	case string:
		if err := json.Unmarshal([]byte(v), &event); err != nil {
			return NewUnknownEvent(v)
		}
	case []byte:
		if err := json.Unmarshal(v, &event); err != nil {
			return NewUnknownEvent(string(v))
		}
	default:
		return NewUnknownEvent(v)
	}

	if event == nil {
		return NewUnknownEvent(raw)
	}

	// 1. API Gateway
	if _, ok := event["httpMethod"]; ok {
		apigw := &ApiGatewayEvent{BaseEvent: BaseEvent{Type: "APIGATEWAY", FileName: "api_event_file", Event: event}}
		apigw.setEventParams()
		body := apigw.GetBody()
		return ParseEvent(body, storageProvider, cfg)
	}

	// 2. dCache (checked before delegated because it also has "event")
	if _, hasEvt := event["event"]; hasEvt {
		if _, hasSub := event["subscription"]; hasSub {
			return parseDCacheEvent(event, storageProvider, cfg)
		}
	}

	// 3. Rucio
	if payload := mapFromRaw(event["payload"]); payload != nil {
		if _, hasType := event["event_type"]; hasType {
			if _, hasScope := payload["scope"]; hasScope {
				e := &RucioEvent{BaseEvent: BaseEvent{Type: "RUCIO", ProviderId: "rucio", Event: event}}
				e.setEventParams()
				return e
			}
		}
	}

	// 4. Delegated event
	if evtRaw, ok := event["event"]; ok {
		sp := storageProvider
		if spVal, hasSp := event["storage_provider"]; hasSp {
			sp = fmt.Sprintf("%v", spVal)
		}
		return ParseEvent(evtRaw, sp, cfg)
	}

	// 5. Storage event
	if recordsRaw, ok := event["Records"]; ok {
		records, castOK := recordsRaw.([]interface{})
		if castOK && len(records) > 0 {
			record, recOK := records[0].(map[string]interface{})
			if recOK {
				if eventSource, hasSource := record["eventSource"]; hasSource {
					src := fmt.Sprintf("%v", eventSource)
					for _, candidate := range []string{"aws:s3", "minio:s3", "OneTrigger", "rucio"} {
						if src == candidate {
							parsed := parseStorageEvent(event, storageProvider, src)
							if parsed != nil {
								setStorageEnvVars(parsed, event)
							}
							return parsed
						}
					}
				}
			}
		}
	}

	return NewUnknownEvent(raw)
}

func setStorageEnvVars(parsed Event, event map[string]interface{}) {
	utils.SetEnvVar("STORAGE_OBJECT_KEY", parsed.GetObjectKey())
	utils.SetEnvVar("EVENT_TIME", parsed.GetEventTime())
	eventJSON, _ := json.Marshal(event)
	utils.SetEnvVar("EVENT", string(eventJSON))
}

func parseStorageEvent(event map[string]interface{}, storageProvider, src string) Event {
	switch src {
	case "aws:s3":
		e := &S3Event{BaseEvent: BaseEvent{Type: "S3", ProviderId: storageProvider, Event: event}}
		e.setEventParams()
		return e
	case "minio:s3":
		e := &MinioEvent{BaseEvent: BaseEvent{Type: "MINIO", ProviderId: storageProvider, Event: event}}
		e.setEventParams()
		return e
	case "OneTrigger":
		e := &OnedataEvent{BaseEvent: BaseEvent{Type: "ONEDATA", ProviderId: storageProvider, Event: event}}
		e.setEventParams()
		return e
	case "rucio":
		e := &RucioEvent{BaseEvent: BaseEvent{Type: "RUCIO", ProviderId: storageProvider, Event: event}}
		e.setEventParams()
		return e
	}
	return nil
}

type dcacheInputPath struct {
	StorageProvider string
	Path            string
}

func parseDCacheEvent(event map[string]interface{}, storageProvider string, cfg ConfigReader) Event {
	inputPath := ""
	if cfg != nil {
		inputList := cfg.ReadCfgVar("input")
		if list, ok := inputList.([]interface{}); ok {
			for _, item := range list {
				if entry, ok := item.(map[string]interface{}); ok {
					sp := fmt.Sprintf("%v", entry["storage_provider"])
					if sp == "webdav.dcache" {
						inputPath = fmt.Sprintf("%v", entry["path"])
						break
					}
				}
			}
		}
	}
	if inputPath == "" {
		logger.GetLogger().Warning("There is no dcache input defined for this function.")
	}
	e := &DCacheEvent{BaseEvent: BaseEvent{Type: "DCACHE", ProviderId: "dcache", Event: event}}
	e.setEventParams()
	e.SetPath(inputPath)
	return e
}

func (e *BaseEvent) saveEventRaw(inputDir string) (string, error) {
	dest := filepath.Join(inputDir, e.FileName)
	switch r := e.Raw.(type) {
	case string:
		if _, err := decodeJSON(r); err == nil {
			return dest, os.WriteFile(dest, []byte(r), 0644)
		}
		// If the string is not valid JSON, try base64-decoding it.
		// If that also fails, write the raw bytes (matches the Python spec).
		decoded, decErr := base64.StdEncoding.DecodeString(r)
		if decErr != nil {
			return dest, os.WriteFile(dest, []byte(r), 0644)
		}
		return dest, os.WriteFile(dest, decoded, 0644)
	default:
		if e.Event != nil {
			b, _ := json.Marshal(e.Event)
			return dest, os.WriteFile(dest, b, 0644)
		}
		b, _ := json.Marshal(r)
		return dest, os.WriteFile(dest, b, 0644)
	}
}

func decodeJSON(s string) (interface{}, error) {
	var v interface{}
	err := json.Unmarshal([]byte(s), &v)
	return v, err
}

type UnknownEvent struct {
	BaseEvent
}

func NewUnknownEvent(raw interface{}) Event {
	fileName := "event-file-" + newUUID()
	e := &UnknownEvent{BaseEvent: BaseEvent{Type: "UNKNOWN", FileName: fileName, Raw: raw}}
	if m, ok := raw.(map[string]interface{}); ok {
		e.Event = m
		if records, ok := m["Records"].([]interface{}); ok && len(records) > 0 {
			e.EventRecords, _ = records[0].(map[string]interface{})
		}
	} else if s, ok := raw.(string); ok {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			e.Event = m
			if records, ok := m["Records"].([]interface{}); ok && len(records) > 0 {
				e.EventRecords, _ = records[0].(map[string]interface{})
			}
		}
	}
	return e
}

func (e *UnknownEvent) SaveEvent(inputDir string) (string, error) {
	return e.saveEventRaw(inputDir)
}

type ApiGatewayEvent struct {
	BaseEvent
}

func (e *ApiGatewayEvent) setEventParams() {
	if e.Event == nil {
		return
	}
	if qsp, ok := e.Event["queryStringParameters"].(map[string]interface{}); ok {
		for k, v := range qsp {
			os.Setenv("CONT_VAR_"+k, fmt.Sprintf("%v", v))
		}
	}
}

func (e *ApiGatewayEvent) GetBody() interface{} {
	if e.Event == nil {
		return ""
	}
	body, ok := e.Event["body"]
	if !ok {
		return ""
	}
	return body
}

func (e *ApiGatewayEvent) HasJSONBody() bool {
	if e.Event == nil {
		return false
	}
	headers, ok := e.Event["headers"].(map[string]interface{})
	if !ok {
		return false
	}
	ct, ok := headers["Content-Type"].(string)
	if !ok {
		return false
	}
	return strings.TrimSpace(ct) == "application/json"
}

func (e *ApiGatewayEvent) SaveEvent(inputDir string) (string, error) {
	dest := filepath.Join(inputDir, "api_event_file")
	var body string
	if b, ok := e.GetBody().(string); ok {
		body = b
	}
	if e.HasJSONBody() {
		return dest, os.WriteFile(dest, []byte(body), 0644)
	}
	decoded, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return "", err
	}
	return dest, os.WriteFile(dest, decoded, 0644)
}

type S3Event struct {
	BaseEvent
}

func (e *S3Event) setEventParams() { e.setS3CommonParams() }

type MinioEvent struct {
	BaseEvent
}

func (e *MinioEvent) setEventParams() { e.setS3CommonParams() }

func (e *BaseEvent) setS3CommonParams() {
	records, _ := e.Event["Records"].([]interface{})
	if len(records) == 0 {
		return
	}
	record, _ := records[0].(map[string]interface{})
	s3 := mapFromRaw(record["s3"])
	bucket := mapFromRaw(s3["bucket"])
	object := mapFromRaw(s3["object"])
	e.BucketName = fmt.Sprintf("%v", bucket["name"])
	e.EventTime = fmt.Sprintf("%v", record["eventTime"])
	key := fmt.Sprintf("%v", object["key"])
	decodedKey, _ := url.QueryUnescape(key)
	e.ObjectKey = decodedKey
	e.FileName = filepath.Base(decodedKey)
}

type OnedataEvent struct {
	BaseEvent
}

func (e *OnedataEvent) setEventParams() {
	if e.Event == nil {
		return
	}
	if key, ok := e.Event["Key"]; ok {
		e.ObjectKey = fmt.Sprintf("%v", key)
	}
	records, _ := e.Event["Records"].([]interface{})
	if len(records) > 0 {
		record, _ := records[0].(map[string]interface{})
		e.FileName = fmt.Sprintf("%v", record["objectKey"])
		e.EventTime = fmt.Sprintf("%v", record["eventTime"])
	}
}

type DCacheEvent struct {
	BaseEvent
}

func (e *DCacheEvent) setEventParams() {
	if innerRaw, ok := e.Event["event"]; ok {
		var inner map[string]interface{}
		if m, ok2 := innerRaw.(map[string]interface{}); ok2 {
			inner = m
		} else if s, ok2 := innerRaw.(string); ok2 {
			json.Unmarshal([]byte(s), &inner)
		}
		if inner != nil {
			e.Event = inner
		}
	}
	e.FileName = fmt.Sprintf("%v", e.Event["name"])
	e.EventTime = ""
}

func (e *DCacheEvent) SetPath(path string) {
	if path == "" {
		e.ObjectKey = e.FileName
		return
	}
	e.ObjectKey = filepath.Join(path, e.FileName)
}

type RucioEvent struct {
	BaseEvent
	Scope string
}

func (e *RucioEvent) setEventParams() {
	if e.Scope != "" || e.Event == nil {
		return
	}
	payload := mapFromRaw(e.Event["payload"])
	if payload != nil {
		e.Scope = fmt.Sprintf("%v", payload["scope"])
		e.ObjectKey = fmt.Sprintf("%v", payload["name"])
	}
}

func (e *RucioEvent) GetScope() string { return e.Scope }