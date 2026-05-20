# beluga-ext-pipeline

A Beluga extension that provides pipeline sandbox environments with Redpanda (Kafka-compatible), Logstash, and Elasticsearch containers.

## Tools

| Tool | Description |
|------|-------------|
| `pipeline_send_data` | Send test data through the pipeline's Kafka broker |
| `pipeline_query_es` | Query the pipeline's Elasticsearch instance |
| `pipeline_get_logstash_status` | Get Logstash logs and status |
| `pipeline_update_config` | Update Logstash pipeline config and restart |
| `pipeline_health` | Health check for all pipeline components |

## Install

```bash
beluga extend install github.com/collinpfeifer/beluga-ext-pipeline
```

Or from a local path:

```bash
beluga extend install ./beluga-ext-pipeline
```

## Config

```yaml
extensions:
  pipeline:
    enabled: true
    redpanda_image: "docker.redpanda.com/redpandadata/redpanda:latest"
    elasticsearch_image: "docker.elastic.co/elasticsearch/elasticsearch:8.17.0"
    logstash_image: "docker.elastic.co/logstash/logstash:8.17.0"
```

## How It Works

When the agent needs a pipeline sandbox, the extension creates a dedicated Docker network with three containers:

1. **Redpanda** — Kafka-compatible broker for ingesting data
2. **Logstash** — Processes and transforms the data streams
3. **Elasticsearch** — Indexes the processed data for querying

Each session gets its own isolated pipeline. The extension automatically rewrites Logstash config to use Docker service names so the containers can communicate.

## Development

```bash
go mod tidy
go build .
```

Requires Beluga core to be available at `../beluga` (via the `replace` directive in go.mod).
