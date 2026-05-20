package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// PipelineManager manages the lifecycle of pipeline sandboxes.
// Each pipeline sandbox gets its own Docker network and 3 containers:
// Redpanda (Kafka-compatible), Logstash, Elasticsearch.
type PipelineManager struct {
	cli       *client.Client
	logger    *slog.Logger
	pipelines map[string]*PipelineSandbox // sessionID → PipelineSandbox
	// Default images from extension config.
	redpandaImage      string
	elasticsearchImage string
	logstashImage      string
}

// NewPipelineManager creates a new pipeline sandbox manager.
func NewPipelineManager(redpandaImage, elasticsearchImage, logstashImage string, logger *slog.Logger) (*PipelineManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("creating Docker client: %w", err)
	}

	if redpandaImage == "" {
		redpandaImage = DefaultRedpandaImage
	}
	if elasticsearchImage == "" {
		elasticsearchImage = DefaultElasticsearchImage
	}
	if logstashImage == "" {
		logstashImage = DefaultLogstashImage
	}

	return &PipelineManager{
		cli:                cli,
		logger:             logger,
		pipelines:          make(map[string]*PipelineSandbox),
		redpandaImage:      redpandaImage,
		elasticsearchImage: elasticsearchImage,
		logstashImage:      logstashImage,
	}, nil
}

// Create spins up a full Redpanda → Logstash → ES pipeline for a session.
func (pm *PipelineManager) Create(ctx context.Context, sessionID string, cfg PipelineConfig) (_ *PipelineSandbox, retErr error) {
	if _, exists := pm.pipelines[sessionID]; exists {
		return nil, fmt.Errorf("pipeline already exists for session %s", sessionID)
	}

	// Apply defaults from manager config.
	if cfg.RedpandaImage == "" {
		cfg.RedpandaImage = pm.redpandaImage
	}
	if cfg.ElasticsearchImage == "" {
		cfg.ElasticsearchImage = pm.elasticsearchImage
	}
	if cfg.LogstashImage == "" {
		cfg.LogstashImage = pm.logstashImage
	}

	// Validate and rewrite Logstash config.
	if strings.TrimSpace(cfg.LogstashConfig) == "" {
		cfg.LogstashConfig = DefaultLogstashConfig()
	} else {
		if err := ValidateLogstashConfig(cfg.LogstashConfig); err != nil {
			return nil, fmt.Errorf("invalid Logstash config: %w", err)
		}
	}
	rewrittenConfig := RewriteLogstashConfig(cfg.LogstashConfig)

	prefix := fmt.Sprintf("beluga-pipe-%s", sessionID)
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}

	// 1. Create dedicated Docker network.
	_ = pm.cli.NetworkRemove(ctx, prefix) // clean stale

	netResp, err := pm.cli.NetworkCreate(ctx, prefix, network.CreateOptions{
		Labels: map[string]string{
			"beluga-session-id": sessionID,
			"beluga-type":       "pipeline-network",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating network: %w", err)
	}
	networkID := netResp.ID

	// Cleanup helper.
	cleanup := func(rpID, esID, lsID string) {
		if rpID != "" {
			_ = pm.cli.ContainerRemove(ctx, rpID, container.RemoveOptions{Force: true})
		}
		if esID != "" {
			_ = pm.cli.ContainerRemove(ctx, esID, container.RemoveOptions{Force: true})
		}
		if lsID != "" {
			_ = pm.cli.ContainerRemove(ctx, lsID, container.RemoveOptions{Force: true})
		}
		_ = pm.cli.NetworkRemove(ctx, networkID)
	}

	// 2. Start Redpanda.
	rpID, err := pm.startRedpanda(ctx, prefix+"-redpanda", networkID, cfg.RedpandaImage)
	if err != nil {
		cleanup("", "", "")
		return nil, fmt.Errorf("starting Redpanda: %w", err)
	}

	// 3. Start Elasticsearch.
	esID, err := pm.startElasticsearch(ctx, prefix+"-es", networkID, cfg.ElasticsearchImage)
	if err != nil {
		cleanup(rpID, "", "")
		return nil, fmt.Errorf("starting Elasticsearch: %w", err)
	}

	// 4. Wait for ES to be healthy before starting Logstash.
	esAddr, err := pm.getESHostAddr(ctx, esID)
	if err != nil {
		cleanup(rpID, esID, "")
		return nil, fmt.Errorf("getting ES address: %w", err)
	}

	tmpPS := &PipelineSandbox{
		ElasticsearchContainerID: esID,
		ESAddr:                   esAddr,
		cli:                      pm.cli,
	}

	pm.logger.Info("waiting for ES before starting Logstash")
	if err := pm.waitForES(ctx, tmpPS, time.Now().Add(3*time.Minute)); err != nil {
		cleanup(rpID, esID, "")
		return nil, fmt.Errorf("waiting for ES: %w", err)
	}

	// 5. Start Logstash with rewritten config.
	lsID, err := pm.startLogstash(ctx, prefix+"-logstash", networkID, cfg.LogstashImage, rewrittenConfig)
	if err != nil {
		cleanup(rpID, esID, "")
		return nil, fmt.Errorf("starting Logstash: %w", err)
	}

	now := time.Now()
	ps := &PipelineSandbox{
		SessionID:                sessionID,
		NetworkID:                networkID,
		RedpandaContainerID:      rpID,
		ElasticsearchContainerID: esID,
		LogstashContainerID:      lsID,
		KafkaAddr:                "redpanda:9092",
		ESAddr:                   esAddr,
		cli:                      pm.cli,
		CreatedAt:                now,
		LastUsedAt:               now,
	}

	pm.pipelines[sessionID] = ps

	pm.logger.Info("pipeline sandbox created",
		"session_id", sessionID,
		"redpanda_id", rpID,
		"es_id", esID,
		"logstash_id", lsID,
		"network_id", networkID,
	)

	return ps, nil
}

// Get retrieves a pipeline sandbox by session ID.
func (pm *PipelineManager) Get(sessionID string) (*PipelineSandbox, error) {
	ps, ok := pm.pipelines[sessionID]
	if !ok {
		return nil, fmt.Errorf("no pipeline sandbox for session %s", sessionID)
	}
	return ps, nil
}

// Destroy tears down the pipeline sandbox.
func (pm *PipelineManager) Destroy(ctx context.Context, sessionID string) error {
	ps, ok := pm.pipelines[sessionID]
	if !ok {
		return fmt.Errorf("no pipeline sandbox for session %s", sessionID)
	}

	removeOpts := container.RemoveOptions{Force: true}
	if ps.LogstashContainerID != "" {
		_ = pm.cli.ContainerRemove(ctx, ps.LogstashContainerID, removeOpts)
	}
	if ps.ElasticsearchContainerID != "" {
		_ = pm.cli.ContainerRemove(ctx, ps.ElasticsearchContainerID, removeOpts)
	}
	if ps.RedpandaContainerID != "" {
		_ = pm.cli.ContainerRemove(ctx, ps.RedpandaContainerID, removeOpts)
	}
	if ps.NetworkID != "" {
		_ = pm.cli.NetworkRemove(ctx, ps.NetworkID)
	}

	delete(pm.pipelines, sessionID)
	pm.logger.Info("pipeline sandbox destroyed", "session_id", sessionID)
	return nil
}

// Close tears down all pipeline sandboxes.
func (pm *PipelineManager) Close(ctx context.Context) error {
	for sessionID := range pm.pipelines {
		_ = pm.Destroy(ctx, sessionID)
	}
	return pm.cli.Close()
}

// --- Container starters ---

func (pm *PipelineManager) startRedpanda(ctx context.Context, name, networkID, image string) (string, error) {
	createResp, err := pm.cli.ContainerCreate(ctx,
		&container.Config{
			Image: image,
			Cmd: []string{
				"redpanda", "start",
				"--mode", "dev-container",
				"--node-id", "0",
				"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
				"--advertise-kafka-addr", "PLAINTEXT://redpanda:9092",
				"--smp", "1",
				"--memory", "512M",
			},
			Labels: map[string]string{
				"beluga-type":       "pipeline-redpanda",
				"beluga-session-id": name,
			},
		},
		&container.HostConfig{},
		nil, nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("creating Redpanda container: %w", err)
	}

	if err := pm.cli.NetworkConnect(ctx, networkID, createResp.ID, &network.EndpointSettings{
		Aliases: []string{"redpanda", "kafka"},
	}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("connecting Redpanda to network: %w", err)
	}

	if err := pm.cli.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("starting Redpanda container: %w", err)
	}

	return createResp.ID, nil
}

func (pm *PipelineManager) startElasticsearch(ctx context.Context, name, networkID, image string) (string, error) {
	createResp, err := pm.cli.ContainerCreate(ctx,
		&container.Config{
			Image: image,
			Env: []string{
				"discovery.type=single-node",
				"xpack.security.enabled=false",
				"xpack.security.http.ssl.enabled=false",
				"ES_JAVA_OPTS=-Xms512m -Xmx512m",
				"cluster.routing.allocation.disk.threshold_enabled=false",
			},
			Labels: map[string]string{
				"beluga-type":       "pipeline-es",
				"beluga-session-id": name,
			},
			ExposedPorts: nat.PortSet{
				"9200/tcp": struct{}{},
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				"9200/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: "0"}},
			},
		},
		nil, nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("creating ES container: %w", err)
	}

	if err := pm.cli.NetworkConnect(ctx, networkID, createResp.ID, &network.EndpointSettings{
		Aliases: []string{"elasticsearch"},
	}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("connecting ES to network: %w", err)
	}

	if err := pm.cli.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("starting ES container: %w", err)
	}

	return createResp.ID, nil
}

func (pm *PipelineManager) startLogstash(ctx context.Context, name, networkID, image, config string) (string, error) {
	createResp, err := pm.cli.ContainerCreate(ctx,
		&container.Config{
			Image: image,
			Env: []string{
				"LS_JAVA_OPTS=-Xms256m -Xmx256m",
			},
			Labels: map[string]string{
				"beluga-type":       "pipeline-logstash",
				"beluga-session-id": name,
			},
		},
		&container.HostConfig{},
		nil, nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("creating Logstash container: %w", err)
	}

	if err := pm.cli.NetworkConnect(ctx, networkID, createResp.ID, &network.EndpointSettings{
		Aliases: []string{"logstash"},
	}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("connecting Logstash to network: %w", err)
	}

	// Write the pipeline config BEFORE starting.
	ls := &PipelineSandbox{LogstashContainerID: createResp.ID, cli: pm.cli}
	if err := ls.writeLogstashConfig(ctx, config); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("writing Logstash config: %w", err)
	}

	if err := pm.cli.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		_ = pm.cli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("starting Logstash container: %w", err)
	}

	return createResp.ID, nil
}

// --- Helpers ---

func (pm *PipelineManager) getESHostAddr(ctx context.Context, containerID string) (string, error) {
	inspect, err := pm.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", err
	}
	if inspect.NetworkSettings != nil && inspect.NetworkSettings.Ports != nil {
		bindings := inspect.NetworkSettings.Ports["9200/tcp"]
		if len(bindings) > 0 {
			return fmt.Sprintf("http://127.0.0.1:%s", bindings[0].HostPort), nil
		}
	}
	return "", fmt.Errorf("ES port 9200 not bound for container %s", containerID)
}

func (pm *PipelineManager) waitForES(ctx context.Context, ps *PipelineSandbox, deadline time.Time) error {
	for i := 0; time.Now().Before(deadline); i++ {
		req, err := http.NewRequestWithContext(ctx, "GET", ps.ESAddr+"/_cluster/health", nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			pm.logger.Info("ES not ready", "status", resp.StatusCode, "addr", ps.ESAddr)
		} else if i%10 == 0 {
			pm.logger.Info("ES not reachable", "addr", ps.ESAddr, "error", err)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("elasticsearch did not become healthy within timeout (addr=%s)", ps.ESAddr)
}
