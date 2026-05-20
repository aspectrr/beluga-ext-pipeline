package pipeline

import "encoding/json"

// PipelineConfig holds configuration for creating a pipeline sandbox.
type PipelineConfig struct {
	RedpandaImage      string `json:"redpanda_image"`
	ElasticsearchImage string `json:"elasticsearch_image"`
	LogstashImage      string `json:"logstash_image"`
	LogstashConfig     string `json:"logstash_config,omitempty"` // Raw Logstash pipeline config
}

// Defaults.
const (
	DefaultRedpandaImage      = "docker.redpanda.com/redpandadata/redpanda:latest"
	DefaultElasticsearchImage = "docker.elastic.co/elasticsearch/elasticsearch:8.17.0"
	DefaultLogstashImage      = "docker.elastic.co/logstash/logstash:8.17.0"
)

// ComponentHealth describes the health of a single pipeline component.
type ComponentHealth struct {
	Name    string          `json:"name"`
	Status  string          `json:"status"` // "healthy", "degraded", "unhealthy", "unknown"
	Message string          `json:"message,omitempty"`
	Details json.RawMessage `json:"details,omitempty"`
}

// HealthReport is the overall health of the pipeline sandbox.
type HealthReport struct {
	SessionID  string            `json:"session_id"`
	Overall    string            `json:"overall"` // "healthy", "degraded", "unhealthy"
	Components []ComponentHealth `json:"components"`
}
