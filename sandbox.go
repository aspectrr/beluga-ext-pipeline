package pipeline

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// PipelineSandbox represents a running Redpanda → Logstash → ES pipeline.
type PipelineSandbox struct {
	SessionID                string
	NetworkID                string
	RedpandaContainerID      string
	ElasticsearchContainerID string
	LogstashContainerID      string
	KafkaAddr                string // internal: redpanda:9092
	ESAddr                   string // host: http://127.0.0.1:PORT
	CreatedAt                time.Time
	LastUsedAt               time.Time

	cli *client.Client
}

// SendData produces a message to the pipeline's Redpanda broker using rpk.
func (ps *PipelineSandbox) SendData(ctx context.Context, topic, data string) error {
	produceCmd := []string{
		"/bin/bash", "-c",
		fmt.Sprintf("echo '%s' | rpk topic produce %s --format '%%v'",
			escapeForShell(data), topic,
		),
	}

	execResp, err := ps.cli.ContainerExecCreate(ctx, ps.RedpandaContainerID, container.ExecOptions{
		Cmd:          produceCmd,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return fmt.Errorf("creating producer exec: %w", err)
	}

	attachResp, err := ps.cli.ContainerExecAttach(ctx, execResp.ID, container.ExecStartOptions{})
	if err != nil {
		return fmt.Errorf("attaching producer exec: %w", err)
	}
	defer attachResp.Close()

	_, _ = io.ReadAll(attachResp.Reader)

	inspectResp, err := ps.cli.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return fmt.Errorf("inspecting producer exec: %w", err)
	}
	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("rpk topic produce exited with code %d", inspectResp.ExitCode)
	}

	return nil
}

// QueryES queries the pipeline's Elasticsearch instance.
// query can be a simple text search string or a JSON ES query DSL body.
func (ps *PipelineSandbox) QueryES(ctx context.Context, index string, query string) (json.RawMessage, error) {
	url := fmt.Sprintf("%s/%s/_search", ps.ESAddr, index)

	var body string
	if json.Valid([]byte(query)) {
		body = query
	} else {
		b, _ := json.Marshal(map[string]interface{}{
			"query": map[string]interface{}{
				"query_string": map[string]interface{}{
					"query": "*" + query + "*",
				},
			},
		})
		body = string(b)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating ES request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying ES: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	result, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading ES response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ES returned status %d: %s", resp.StatusCode, string(result))
	}

	return json.RawMessage(result), nil
}

// GetLogstashLogs retrieves recent Logstash container logs.
func (ps *PipelineSandbox) GetLogstashLogs(ctx context.Context, tail string) (string, error) {
	if tail == "" {
		tail = "100"
	}

	logs, err := ps.cli.ContainerLogs(ctx, ps.LogstashContainerID, container.LogsOptions{
		Tail:       tail,
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return "", fmt.Errorf("getting Logstash logs: %w", err)
	}
	defer func() { _ = logs.Close() }()

	data, err := io.ReadAll(logs)
	if err != nil {
		return "", fmt.Errorf("reading Logstash logs: %w", err)
	}

	return stripDockerLogHeaders(data), nil
}

// UpdateLogstashConfig replaces the Logstash pipeline config and restarts the container.
func (ps *PipelineSandbox) UpdateLogstashConfig(ctx context.Context, config string) error {
	if err := ValidateLogstashConfig(config); err != nil {
		return fmt.Errorf("invalid Logstash config: %w", err)
	}

	rewritten := RewriteLogstashConfig(config)

	if err := ps.writeLogstashConfig(ctx, rewritten); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	if err := ps.cli.ContainerRestart(ctx, ps.LogstashContainerID, container.StopOptions{}); err != nil {
		return fmt.Errorf("restarting Logstash: %w", err)
	}

	return nil
}

// Health returns a full health report for the pipeline sandbox.
func (ps *PipelineSandbox) Health(ctx context.Context) (*HealthReport, error) {
	report := &HealthReport{
		SessionID:  ps.SessionID,
		Components: make([]ComponentHealth, 0, 3),
	}

	allHealthy := true

	rpHealth := ps.checkRedpanda(ctx)
	report.Components = append(report.Components, rpHealth)
	if rpHealth.Status != "healthy" {
		allHealthy = false
	}

	esHealth := ps.checkElasticsearch(ctx)
	report.Components = append(report.Components, esHealth)
	if esHealth.Status != "healthy" {
		allHealthy = false
	}

	lsHealth := ps.checkLogstash(ctx)
	report.Components = append(report.Components, lsHealth)
	if lsHealth.Status != "healthy" {
		allHealthy = false
	}

	if allHealthy {
		report.Overall = "healthy"
	} else {
		healthyCount := 0
		for _, c := range report.Components {
			if c.Status == "healthy" {
				healthyCount++
			}
		}
		if healthyCount > 0 {
			report.Overall = "degraded"
		} else {
			report.Overall = "unhealthy"
		}
	}

	return report, nil
}

// --- Internal helpers ---

func (ps *PipelineSandbox) writeLogstashConfig(ctx context.Context, config string) error {
	// Use tar copy to write the config into the Logstash container.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: "usr/share/logstash/pipeline/logstash.conf",
		Mode: 0o644,
		Size: int64(len(config)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("writing tar header: %w", err)
	}
	if _, err := tw.Write([]byte(config)); err != nil {
		return fmt.Errorf("writing tar content: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("closing tar writer: %w", err)
	}
	return ps.cli.CopyToContainer(ctx, ps.LogstashContainerID, "/", &buf, container.CopyToContainerOptions{})
}

func (ps *PipelineSandbox) checkRedpanda(ctx context.Context) ComponentHealth {
	health := ComponentHealth{Name: "redpanda"}

	inspect, err := ps.cli.ContainerInspect(ctx, ps.RedpandaContainerID)
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("container inspect failed: %v", err)
		return health
	}
	if inspect.State == nil || !inspect.State.Running {
		status := "stopped"
		if inspect.State != nil {
			status = inspect.State.Status
		}
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("container not running (status: %s)", status)
		return health
	}

	execResp, err := ps.cli.ContainerExecCreate(ctx, ps.RedpandaContainerID, container.ExecOptions{
		Cmd:          []string{"rpk", "cluster", "health"},
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("exec create failed: %v", err)
		return health
	}

	attachResp, err := ps.cli.ContainerExecAttach(ctx, execResp.ID, container.ExecStartOptions{})
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("exec attach failed: %v", err)
		return health
	}
	defer attachResp.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, attachResp.Reader); err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("reading output failed: %v", err)
		return health
	}

	execInspect, err := ps.cli.ContainerExecInspect(ctx, execResp.ID)
	if err != nil || execInspect.ExitCode != 0 {
		exitCode := -1
		if err == nil {
			exitCode = execInspect.ExitCode
		}
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("rpk cluster health exited %d: %s", exitCode, stderrBuf.String())
		return health
	}

	health.Status = "healthy"
	health.Message = "redpanda broker is running and accepting connections"
	return health
}

func (ps *PipelineSandbox) checkElasticsearch(ctx context.Context) ComponentHealth {
	health := ComponentHealth{Name: "elasticsearch"}

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", ps.ESAddr+"/_cluster/health", nil)
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("creating request: %v", err)
		return health
	}

	resp, err := client.Do(req)
	if err != nil {
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("connection failed: %v", err)
		return health
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("reading response: %v", err)
		return health
	}

	health.Details = json.RawMessage(body)

	if resp.StatusCode != 200 {
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("ES returned status %d", resp.StatusCode)
		return health
	}

	var esHealth struct {
		Status           string `json:"status"`
		NumberOfNodes    int    `json:"number_of_nodes"`
		ActiveShards     int    `json:"active_shards"`
		UnassignedShards int    `json:"unassigned_shards"`
	}
	if err := json.Unmarshal(body, &esHealth); err != nil {
		health.Status = "degraded"
		health.Message = "could not parse cluster health response"
		return health
	}

	switch esHealth.Status {
	case "green":
		health.Status = "healthy"
		health.Message = fmt.Sprintf("cluster is green (%d nodes, %d shards)", esHealth.NumberOfNodes, esHealth.ActiveShards)
	case "yellow":
		health.Status = "degraded"
		health.Message = fmt.Sprintf("cluster is yellow (%d unassigned shards)", esHealth.UnassignedShards)
	case "red":
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("cluster is red (%d unassigned shards)", esHealth.UnassignedShards)
	default:
		health.Status = "unknown"
		health.Message = fmt.Sprintf("unexpected cluster status: %s", esHealth.Status)
	}

	return health
}

func (ps *PipelineSandbox) checkLogstash(ctx context.Context) ComponentHealth {
	health := ComponentHealth{Name: "logstash"}

	inspect, err := ps.cli.ContainerInspect(ctx, ps.LogstashContainerID)
	if err != nil {
		health.Status = "unknown"
		health.Message = fmt.Sprintf("container inspect failed: %v", err)
		return health
	}
	if inspect.State == nil || !inspect.State.Running {
		status := "stopped"
		if inspect.State != nil {
			status = inspect.State.Status
		}
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("container not running (status: %s)", status)
		return health
	}

	// Check Logstash API via exec.
	execResp, err := ps.cli.ContainerExecCreate(ctx, ps.LogstashContainerID, container.ExecOptions{
		Cmd:          []string{"bash", "-c", "curl -sf http://localhost:9600/_node/pipelines/main 2>&1 || echo 'api-unavailable'"},
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		health.Status = "degraded"
		health.Message = fmt.Sprintf("exec failed: %v", err)
		return health
	}

	attachResp, err := ps.cli.ContainerExecAttach(ctx, execResp.ID, container.ExecStartOptions{})
	if err != nil {
		health.Status = "degraded"
		health.Message = fmt.Sprintf("exec attach failed: %v", err)
		return health
	}
	defer attachResp.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, attachResp.Reader); err != nil {
		health.Status = "degraded"
		health.Message = fmt.Sprintf("reading output failed: %v", err)
		return health
	}

	execInspect, err := ps.cli.ContainerExecInspect(ctx, execResp.ID)
	if err != nil || execInspect.ExitCode != 0 {
		health.Status = "degraded"
		health.Message = "Logstash API not responding (still starting up?)"
		return health
	}

	output := stdoutBuf.String()
	if strings.Contains(output, "api-unavailable") {
		health.Status = "degraded"
		health.Message = "Logstash API not responding (still starting up?)"
		return health
	}

	// Parse pipeline status.
	var pipelineInfo struct {
		Pipelines struct {
			Main struct {
				Status string `json:"status"`
				Events struct {
					Filtered int64 `json:"filtered"`
					Output   int64 `json:"out"`
				} `json:"events"`
			} `json:"main"`
		} `json:"pipelines"`
	}

	if err := json.Unmarshal([]byte(output), &pipelineInfo); err == nil {
		mainPipe := pipelineInfo.Pipelines.Main
		if mainPipe.Status == "running" {
			health.Status = "healthy"
			health.Message = fmt.Sprintf("pipeline running (filtered: %d, output: %d)",
				mainPipe.Events.Filtered, mainPipe.Events.Output)
		} else {
			health.Status = "degraded"
			health.Message = fmt.Sprintf("pipeline status: %s", mainPipe.Status)
		}
	} else {
		health.Status = "healthy"
		health.Message = "Logstash is running and API is responding"
	}

	return health
}

func escapeForShell(s string) string {
	return strings.ReplaceAll(s, "'", "'\\''")
}

func stripDockerLogHeaders(data []byte) string {
	var result bytes.Buffer
	reader := bytes.NewReader(data)
	for reader.Len() > 0 {
		header := make([]byte, 8)
		if _, err := io.ReadFull(reader, header); err != nil {
			break
		}
		size := int(header[4]) | int(header[5])<<8 | int(header[6])<<16 | int(header[7])<<24
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break
		}
		result.Write(payload)
	}
	return result.String()
}
