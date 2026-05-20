package pipeline

import (
	"fmt"
	"regexp"
	"strings"
)

// RewriteLogstashConfig replaces external host references in a Logstash config
// with the Docker service names used inside the pipeline sandbox network.
//
// It rewrites:
//   - Kafka bootstrap_servers → kafka:9092
//   - Elasticsearch hosts → elasticsearch:9200
//
// Everything else (filters, grok patterns, codecs) is preserved.
func RewriteLogstashConfig(config string) string {
	result := config

	// Rewrite Kafka bootstrap_servers.
	result = rewriteKafkaBootstrap(result, "kafka", "9092")

	// Rewrite Elasticsearch hosts.
	result = rewriteESHosts(result, "elasticsearch", "9200")

	return result
}

// rewriteKafkaBootstrap replaces bootstrap_servers values in Logstash config.
func rewriteKafkaBootstrap(config, host, port string) string {
	target := fmt.Sprintf("%s:%s", host, port)

	// Array form: bootstrap_servers => ["host1:9092", "host2:9092"]
	arrayRe := regexp.MustCompile(`(bootstrap_servers\s*=>\s*)\[[^\]]*\]`)
	config = arrayRe.ReplaceAllString(config, fmt.Sprintf(`${1}["%s"]`, target))

	// String form: bootstrap_servers => "host:9092" or "host:9092,other:9092"
	stringRe := regexp.MustCompile(`(bootstrap_servers\s*=>\s*)"[^"]*"`)
	config = stringRe.ReplaceAllString(config, fmt.Sprintf(`${1}"%s"`, target))

	return config
}

// rewriteESHosts replaces Elasticsearch hosts in Logstash config.
func rewriteESHosts(config, host, port string) string {
	esURL := fmt.Sprintf("http://%s:%s", host, port)

	// Array form: hosts => ["http://host:9200", ...]
	arrayRe := regexp.MustCompile(`(hosts\s*=>\s*)\[[^\]]*\]`)
	config = arrayRe.ReplaceAllString(config, fmt.Sprintf(`${1}["%s"]`, esURL))

	// String form: hosts => "http://host:9200"
	stringRe := regexp.MustCompile(`(hosts\s*=>\s*)"[^"]*"`)
	config = stringRe.ReplaceAllString(config, fmt.Sprintf(`${1}"%s"`, esURL))

	return config
}

// ValidateLogstashConfig performs basic validation on a Logstash config.
func ValidateLogstashConfig(config string) error {
	trimmed := strings.TrimSpace(config)
	if trimmed == "" {
		return fmt.Errorf("empty Logstash config")
	}

	// Check for balanced braces.
	depth := 0
	for _, ch := range config {
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced braces: extra closing brace")
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unbalanced braces: %d unclosed", depth)
	}

	// Check that at least an input or output block exists.
	hasInput := strings.Contains(config, "input {") || strings.Contains(config, "input{")
	hasOutput := strings.Contains(config, "output {") || strings.Contains(config, "output{")

	if !hasInput && !hasOutput {
		return fmt.Errorf("config must have at least an input or output block")
	}

	return nil
}

// DefaultLogstashConfig returns a simple passthrough config for testing.
func DefaultLogstashConfig() string {
	return `input {
  kafka {
    bootstrap_servers => "kafka:9092"
    topics => ["test"]
    group_id => "logstash"
  }
}

filter {
  mutate {
    add_field => { "[@metadata][processed]" => "true" }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "logstash-%{+YYYY.MM.dd}"
  }
}
`
}
