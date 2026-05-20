package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/collinpfeifer/beluga/pkg/extension"
	"github.com/collinpfeifer/beluga/pkg/tools"
)

// Extension implements the pipeline sandbox extension for Beluga.
// It manages Docker containers for Redpanda → Logstash → Elasticsearch pipelines.
type Extension struct {
	manager *PipelineManager
	logger  *slog.Logger
}

// Name returns the extension identifier.
func (e *Extension) Name() string { return "pipeline" }

// Init parses config and registers pipeline tools.
func (e *Extension) Init(ctx extension.ExtensionContext) error {
	var cfg struct {
		RedpandaImage      string `json:"redpanda_image"`
		ElasticsearchImage string `json:"elasticsearch_image"`
		LogstashImage      string `json:"logstash_image"`
	}
	if len(ctx.Config) > 0 {
		if err := json.Unmarshal(ctx.Config, &cfg); err != nil {
			return err
		}
	}

	e.logger = ctx.Logger

	// Create the pipeline manager.
	mgr, err := NewPipelineManager(cfg.RedpandaImage, cfg.ElasticsearchImage, cfg.LogstashImage, ctx.Logger)
	if err != nil {
		return err
	}
	e.manager = mgr

	// Register pipeline tools.
	provider := &managerProvider{mgr: mgr}
	pipelineTools := []tools.Tool{
		&PipelineSendDataTool{Provider: provider},
		&PipelineQueryESTool{Provider: provider},
		&PipelineGetLogstashStatusTool{Provider: provider},
		&PipelineUpdateConfigTool{Provider: provider},
		&PipelineHealthTool{Provider: provider},
	}
	for _, t := range pipelineTools {
		if err := ctx.Registry.Register(t); err != nil {
			return err
		}
	}

	e.logger.Info("pipeline extension initialized",
		"redpanda_image", cfg.RedpandaImage,
		"elasticsearch_image", cfg.ElasticsearchImage,
		"logstash_image", cfg.LogstashImage,
		"tools", len(pipelineTools),
	)
	return nil
}

// Start blocks until context is cancelled. Pipeline containers are created on-demand.
func (e *Extension) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Stop tears down all pipeline sandboxes.
func (e *Extension) Stop(ctx context.Context) error {
	if e.manager != nil {
		return e.manager.Close(ctx)
	}
	return nil
}

// managerProvider adapts PipelineManager to the PipelineProvider interface
// used by tools to look up sandboxes by session ID.
type managerProvider struct {
	mgr *PipelineManager
}

func (p *managerProvider) Get(sessionID string) (*PipelineSandbox, error) {
	return p.mgr.Get(sessionID)
}
