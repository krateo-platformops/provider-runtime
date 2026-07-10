package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// TestNewOTelJSONHandler_OTelModel verifies the consolidated handler emits the OTel log model:
// JSON line, `timestamp`, level + sibling SeverityNumber, and the service.name attribute.
func TestNewOTelJSONHandler_OTelModel(t *testing.T) {
	var buf bytes.Buffer
	h := NewOTelJSONHandler(slog.LevelDebug, &buf, ServiceNameAttr("my-svc")...)
	slog.New(h).Info("hello", "k", "v")

	line := strings.TrimSpace(buf.String())
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("output is not a JSON object: %v\n%s", err, line)
	}
	if _, ok := m["timestamp"]; !ok {
		t.Errorf("missing OTel `timestamp` field: %v", m)
	}
	if _, ok := m["time"]; ok {
		t.Errorf("raw `time` field should have been rewritten to `timestamp`: %v", m)
	}
	if sn, ok := m["SeverityNumber"]; !ok || sn.(float64) != 9 { // INFO=9
		t.Errorf("SeverityNumber for INFO should be 9, got %v", m["SeverityNumber"])
	}
	if m["service.name"] != "my-svc" || m["service"] != "my-svc" {
		t.Errorf("service.name/service not set: %v", m)
	}
	if m["level"] != "INFO" || m["msg"] != "hello" || m["k"] != "v" {
		t.Errorf("level/msg/attrs wrong: %v", m)
	}
}

func TestOTelSeverityNumber(t *testing.T) {
	cases := map[slog.Level]int{
		slog.LevelDebug - 1: 1, // TRACE-ish
		slog.LevelDebug:     5,
		slog.LevelInfo:      9,
		slog.LevelWarn:      13,
		slog.LevelError:     17,
	}
	for lvl, want := range cases {
		if got := otelSeverityNumber(lvl); got != want {
			t.Errorf("otelSeverityNumber(%v) = %d, want %d", lvl, got, want)
		}
	}
}

// TestNewOTelHandler_TeesJSON verifies the OTLP-capable handler still writes the OTel-model JSON
// stream (the otelslog bridge is a no-op with no LoggerProvider installed, so the record is not
// lost).
func TestNewOTelHandler_TeesJSON(t *testing.T) {
	var buf bytes.Buffer
	h := NewOTelHandler(slog.LevelInfo, &buf, "svc")
	slog.New(h).Info("teed", "a", "b")
	if !strings.Contains(buf.String(), `"msg":"teed"`) || !strings.Contains(buf.String(), `"SeverityNumber":9`) {
		t.Errorf("tee did not write the OTel-model JSON stream: %s", buf.String())
	}
}
