// ── Pipeline Extension ────────────────────────────────────────
// Ported from github.com/aspectrr/beluga-ext-pipeline (Go → TS)
//
// Isolated Docker-based data pipeline sandboxes. Each session gets
// a dedicated Docker network running Redpanda (Kafka), Logstash,
// and Elasticsearch. 5 tools for data production, querying, config
// updates, and health checks.

import Docker from "dockerode";
import type {
	Extension,
	ExtensionContext,
	Tool,
	ToolDef,
	ToolContext,
} from "@beluga/sdk";

// ── Defaults ───────────────────────────────────────────────────

const DEFAULT_REDPANDA_IMAGE =
	"docker.redpanda.com/redpandadata/redpanda:latest";
const DEFAULT_ES_IMAGE =
	"docker.elastic.co/elasticsearch/elasticsearch:8.17.0";
const DEFAULT_LOGSTASH_IMAGE =
	"docker.elastic.co/logstash/logstash:8.17.0";

// ── Types ──────────────────────────────────────────────────────

interface PipelineConfig {
	redpanda_image?: string;
	elasticsearch_image?: string;
	logstash_image?: string;
}

interface ComponentHealth {
	name: string;
	status: "healthy" | "degraded" | "unhealthy" | "unknown";
	message: string;
	details?: Record<string, unknown>;
}

interface HealthReport {
	session_id: string;
	overall: "healthy" | "degraded" | "unhealthy";
	components: ComponentHealth[];
}

interface PipelineSandbox {
	sessionId: string;
	networkId: string;
	redpandaId: string;
	elasticsearchId: string;
	logstashId: string;
	esAddr: string;
	createdAt: Date;
	lastUsedAt: Date;
}

function dryRun(): boolean {
	return process.env.BELUGA_DRY_RUN === "true";
}

// ── Config rewrite ─────────────────────────────────────────────

function rewriteLogstashConfig(config: string): string {
	let result = config;
	result = result.replace(
		/bootstrap_servers\s*=>\s*\[([^\]]*)\]/g,
		'bootstrap_servers => ["kafka:9092"]',
	);
	result = result.replace(
		/bootstrap_servers\s*=>\s*"[^"]*"/g,
		'bootstrap_servers => "kafka:9092"',
	);
	result = result.replace(
		/hosts\s*=>\s*\[([^\]]*)\]/g,
		() => 'hosts => ["http://elasticsearch:9200"]',
	);
	result = result.replace(
		/hosts\s*=>\s*"[^"]*"/g,
		'hosts => "http://elasticsearch:9200"',
	);
	return result;
}

function validateLogstashConfig(config: string): void {
	if (!config.trim()) throw new Error("config is empty");
	const openBraces = (config.match(/{/g) || []).length;
	const closeBraces = (config.match(/}/g) || []).length;
	if (openBraces !== closeBraces)
		throw new Error("unbalanced braces in config");
	if (!config.includes("input") && !config.includes("output")) {
		throw new Error("config must have at least input or output block");
	}
}

function defaultLogstashConfig(): string {
	return `
input {
  kafka {
    bootstrap_servers => "kafka:9092"
    topics => ["beluga"]
  }
}
filter {
  mutate {
    add_field => { "pipeline" => "beluga" }
  }
}
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "beluga-%{+YYYY.MM.dd}"
  }
}
`.trim();
}

// ── Pipeline Manager ───────────────────────────────────────────

class PipelineManager {
	private docker: Docker;
	private logger: import("pino").Logger;
	private pipelines = new Map<string, PipelineSandbox>();
	private redpandaImage: string;
	private esImage: string;
	private logstashImage: string;

	constructor(cfg: PipelineConfig, logger: import("pino").Logger) {
		this.docker = new Docker();
		this.logger = logger;
		this.redpandaImage = cfg.redpanda_image || DEFAULT_REDPANDA_IMAGE;
		this.esImage = cfg.elasticsearch_image || DEFAULT_ES_IMAGE;
		this.logstashImage = cfg.logstash_image || DEFAULT_LOGSTASH_IMAGE;
	}

	async create(
		sessionId: string,
		logstashConfig?: string,
	): Promise<PipelineSandbox> {
		if (this.pipelines.has(sessionId)) return this.pipelines.get(sessionId)!;

		const prefix = `beluga-pipe-${sessionId.slice(0, 12)}`;
		let networkId = "";
		let redpandaId = "";
		let esId = "";
		let logstashId = "";

		try {
			// 1. Create network
			const network = await this.docker.createNetwork({
				Name: prefix,
				Labels: { "beluga.session": sessionId },
			});
			networkId = network.id;

			// 2. Start Redpanda
			const redpanda = await this.docker.createContainer({
				Image: this.redpandaImage,
				name: `${prefix}-redpanda`,
				Labels: {
					"beluga.session": sessionId,
					"beluga.component": "redpanda",
				},
				Env: [
					"REDPANDA_NODE_ID=0",
					"REDPANDA_SEEDS=kafka:9092",
					"REDPANDA_ADVERTISE_KAFKA_ADDRESS=kafka:9092",
				],
				Cmd: [
					"redpanda",
					"start",
					"--mode",
					"dev",
					"--smp",
					"1",
					"--memory",
					"512M",
					"--advertise-kafka-addr",
					"kafka:9092",
					"--advertise-rpc-addr",
					"redpanda:33145",
				],
				HostConfig: {
					Memory: 512 * 1024 * 1024,
					NetworkMode: prefix,
				},
				NetworkingConfig: {
					EndpointsConfig: {
						[prefix]: { Aliases: ["redpanda", "kafka"] },
					},
				},
			});
			await redpanda.start();
			redpandaId = redpanda.id;

			// 3. Start Elasticsearch
			const esPort = Math.floor(Math.random() * 10000) + 40000;
			const es = await this.docker.createContainer({
				Image: this.esImage,
				name: `${prefix}-es`,
				Labels: {
					"beluga.session": sessionId,
					"beluga.component": "elasticsearch",
				},
				Env: [
					"discovery.type=single-node",
					"xpack.security.enabled=false",
					"ES_JAVA_OPTS=-Xms512m -Xmx512m",
				],
				HostConfig: {
					Memory: 1024 * 1024 * 1024,
					PortBindings: {
						"9200/tcp": [
							{ HostPort: String(esPort), HostIp: "127.0.0.1" },
						],
					},
					NetworkMode: prefix,
				},
				NetworkingConfig: {
					EndpointsConfig: {
						[prefix]: { Aliases: ["elasticsearch"] },
					},
				},
			});
			await es.start();
			esId = es.id;

			// Wait for ES healthy
			const esAddr = `http://127.0.0.1:${esPort}`;
			await this.waitForES(esAddr, 180_000);

			// 4. Start Logstash
			const lsConfig = rewriteLogstashConfig(
				logstashConfig || defaultLogstashConfig(),
			);
			validateLogstashConfig(lsConfig);

			const ls = await this.docker.createContainer({
				Image: this.logstashImage,
				name: `${prefix}-logstash`,
				Labels: {
					"beluga.session": sessionId,
					"beluga.component": "logstash",
				},
				Env: ["LS_JAVA_OPTS=-Xms256m -Xmx256m"],
				HostConfig: {
					Memory: 512 * 1024 * 1024,
					NetworkMode: prefix,
				},
				NetworkingConfig: {
					EndpointsConfig: {
						[prefix]: { Aliases: ["logstash"] },
					},
				},
			});
			await ls.start();
			logstashId = ls.id;

			// Copy config into Logstash container
			const tarBuffer = this.createTarBuffer("logstash.conf", lsConfig);
			await ls.putArchive(tarBuffer, {
				path: "/usr/share/logstash/pipeline/",
			});

			// Restart Logstash to pick up config
			await ls.restart();

			const sandbox: PipelineSandbox = {
				sessionId,
				networkId,
				redpandaId,
				elasticsearchId: esId,
				logstashId,
				esAddr,
				createdAt: new Date(),
				lastUsedAt: new Date(),
			};
			this.pipelines.set(sessionId, sandbox);
			return sandbox;
		} catch (err) {
			this.logger.error(
				{ err, sessionId },
				"pipeline creation failed, cleaning up",
			);
			await this.removeContainer(logstashId);
			await this.removeContainer(esId);
			await this.removeContainer(redpandaId);
			if (networkId) await this.removeNetwork(networkId);
			throw err;
		}
	}

	get(sessionId: string): PipelineSandbox | undefined {
		return this.pipelines.get(sessionId);
	}

	async destroy(sessionId: string): Promise<void> {
		const sandbox = this.pipelines.get(sessionId);
		if (!sandbox) return;

		await this.removeContainer(sandbox.logstashId);
		await this.removeContainer(sandbox.elasticsearchId);
		await this.removeContainer(sandbox.redpandaId);
		await this.removeNetwork(sandbox.networkId);
		this.pipelines.delete(sessionId);
	}

	async close(): Promise<void> {
		for (const id of this.pipelines.keys()) {
			await this.destroy(id).catch(() => {});
		}
	}

	// ── Sandbox operations ────────────────────────────────────

	async sendData(
		sandbox: PipelineSandbox,
		topic: string,
		data: string,
	): Promise<void> {
		sandbox.lastUsedAt = new Date();
		const container = this.docker.getContainer(sandbox.redpandaId);
		const escapedData = data.replace(/'/g, "'\\''");
		const exec = await container.exec({
			Cmd: [
				"/bin/sh",
				"-c",
				`echo '${escapedData}' | rpk topic produce ${topic}`,
			],
			AttachStdout: true,
			AttachStderr: true,
		});
		const stream = await exec.start({ hijack: true, stdin: false });
		await this.collectStream(stream);
	}

	async queryES(
		sandbox: PipelineSandbox,
		index: string,
		query?: string,
	): Promise<Record<string, unknown>> {
		sandbox.lastUsedAt = new Date();
		let esQuery: string;
		if (query) {
			try {
				JSON.parse(query);
				esQuery = query;
			} catch {
				esQuery = JSON.stringify({
					query: { query_string: { query: `*${query}*` } },
				});
			}
		} else {
			esQuery = JSON.stringify({ query: { match_all: {} } });
		}

		const resp = await fetch(`${sandbox.esAddr}/${index}/_search`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: esQuery,
		});
		if (!resp.ok) throw new Error(`ES query failed: ${resp.status}`);
		return resp.json() as Promise<Record<string, unknown>>;
	}

	async getLogstashLogs(
		sandbox: PipelineSandbox,
		tail = 100,
	): Promise<string> {
		sandbox.lastUsedAt = new Date();
		const container = this.docker.getContainer(sandbox.logstashId);
		const logs = await container.logs({ stdout: true, stderr: true, tail });
		return typeof logs === "string"
			? logs
			: this.stripDockerFrames(logs);
	}

	async updateConfig(
		sandbox: PipelineSandbox,
		config: string,
	): Promise<void> {
		sandbox.lastUsedAt = new Date();
		validateLogstashConfig(config);
		const rewritten = rewriteLogstashConfig(config);

		const container = this.docker.getContainer(sandbox.logstashId);
		const tarBuffer = this.createTarBuffer("logstash.conf", rewritten);
		await container.putArchive(tarBuffer, {
			path: "/usr/share/logstash/pipeline/",
		});
		await container.restart();
	}

	async health(sandbox: PipelineSandbox): Promise<HealthReport> {
		sandbox.lastUsedAt = new Date();
		const components: ComponentHealth[] = [];

		// Redpanda health
		try {
			const container = this.docker.getContainer(sandbox.redpandaId);
			const exec = await container.exec({
				Cmd: [
					"/bin/sh",
					"-c",
					"rpk cluster health 2>&1 || true",
				],
				AttachStdout: true,
				AttachStderr: true,
			});
			const stream = await exec.start({ hijack: true, stdin: false });
			const output = await this.collectStream(stream);
			components.push({
				name: "redpanda",
				status:
					output.includes("healthy") || output.includes("leader")
						? "healthy"
						: "degraded",
				message: output.slice(0, 500),
			});
		} catch (err) {
			components.push({
				name: "redpanda",
				status: "unhealthy",
				message: String(err),
			});
		}

		// ES health
		try {
			const resp = await fetch(`${sandbox.esAddr}/_cluster/health`);
			const data = (await resp.json()) as Record<string, unknown>;
			components.push({
				name: "elasticsearch",
				status:
					data.status === "green"
						? "healthy"
						: data.status === "yellow"
							? "degraded"
							: "unhealthy",
				message: `status: ${data.status}`,
				details: data,
			});
		} catch (err) {
			components.push({
				name: "elasticsearch",
				status: "unhealthy",
				message: String(err),
			});
		}

		// Logstash health
		try {
			const container = this.docker.getContainer(sandbox.logstashId);
			const exec = await container.exec({
				Cmd: [
					"/bin/sh",
					"-c",
					"curl -s http://localhost:9600/_node/pipelines/main 2>&1 || true",
				],
				AttachStdout: true,
				AttachStderr: true,
			});
			const stream = await exec.start({ hijack: true, stdin: false });
			const output = await this.collectStream(stream);
			components.push({
				name: "logstash",
				status: output.includes("running") ? "healthy" : "degraded",
				message: output.slice(0, 500),
			});
		} catch (err) {
			components.push({
				name: "logstash",
				status: "unhealthy",
				message: String(err),
			});
		}

		const overall = components.some((c) => c.status === "unhealthy")
			? "unhealthy"
			: components.some((c) => c.status === "degraded")
				? "degraded"
				: "healthy";

		return { session_id: sandbox.sessionId, overall, components };
	}

	// ── Helpers ────────────────────────────────────────────────

	private async waitForES(
		esAddr: string,
		timeoutMs: number,
	): Promise<void> {
		const start = Date.now();
		while (Date.now() - start < timeoutMs) {
			try {
				const resp = await fetch(`${esAddr}/_cluster/health`);
				if (resp.ok) return;
			} catch {
				// Not ready yet
			}
			await new Promise((r) => setTimeout(r, 2000));
		}
		throw new Error(
			"Elasticsearch failed to become healthy within timeout",
		);
	}

	private async removeContainer(id: string): Promise<void> {
		if (!id) return;
		try {
			const c = this.docker.getContainer(id);
			await c.stop().catch(() => {});
			await c.remove().catch(() => {});
		} catch {
			// Best effort
		}
	}

	private async removeNetwork(id: string): Promise<void> {
		if (!id) return;
		try {
			await this.docker
				.getNetwork(id)
				.remove()
				.catch(() => {});
		} catch {
			// Best effort
		}
	}

	private collectStream(stream: NodeJS.ReadableStream): Promise<string> {
		return new Promise((resolve) => {
			const chunks: Buffer[] = [];
			stream.on("data", (chunk: Buffer) => chunks.push(chunk));
			stream.on("end", () =>
				resolve(Buffer.concat(chunks).toString("utf-8")),
			);
			stream.on("error", () => resolve(""));
		});
	}

	private stripDockerFrames(buf: Buffer): string {
		let offset = 0;
		let result = "";
		while (offset < buf.length) {
			if (offset + 8 > buf.length) break;
			const size = buf.readUInt32BE(offset + 4);
			offset += 8;
			if (offset + size > buf.length) {
				result += buf.slice(offset).toString("utf-8");
				break;
			}
			result += buf.slice(offset, offset + size).toString("utf-8");
			offset += size;
		}
		return result;
	}

	private createTarBuffer(filename: string, content: string): Buffer {
		const contentBuf = Buffer.from(content, "utf-8");
		const headerSize = 512;
		const header = Buffer.alloc(headerSize);

		header.write(filename, 0);
		header.write("0000644\0", 100, 8);
		header.write("0000000\0", 108, 8);
		header.write("0000000\0", 116, 8);
		header.write(
			contentBuf.length.toString(8).padStart(11, "0") + "\0",
			124,
			12,
		);
		header.write(
			Math.floor(Date.now() / 1000)
				.toString(8)
				.padStart(11, "0") + "\0",
			136,
			12,
		);
		header.write("0", 156, 1);
		header.write("ustar\0", 257, 6);
		header.write("00", 263, 2);

		let checksum = 0;
		for (let i = 0; i < headerSize; i++) {
			checksum += header[i];
		}
		const originalChecksum =
			header.readUInt8(148) +
			header.readUInt8(149) +
			header.readUInt8(150) +
			header.readUInt8(151) +
			header.readUInt8(152) +
			header.readUInt8(153) +
			header.readUInt8(154) +
			header.readUInt8(155);
		checksum = checksum - originalChecksum + 32 * 8;
		header.write(
			checksum.toString(8).padStart(6, "0") + "\0 ",
			148,
			8,
		);

		const contentPadded = Buffer.alloc(
			Math.ceil(contentBuf.length / 512) * 512,
		);
		contentBuf.copy(contentPadded);
		const endMarker = Buffer.alloc(1024);

		return Buffer.concat([header, contentPadded, endMarker]);
	}
}

// ── Tools ──────────────────────────────────────────────────────

class PipelineSendDataTool implements Tool {
	private manager: PipelineManager;

	constructor(manager: PipelineManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "pipeline_send_data",
			description:
				"Send test data through the pipeline's Kafka broker.",
			parameters: {
				type: "object",
				properties: {
					topic: { type: "string", description: "Kafka topic name" },
					data: { type: "string", description: "Data to send" },
					format: {
						type: "string",
						description: "plain, json, or syslog",
						enum: ["plain", "json", "syslog"],
					},
				},
				required: ["topic", "data"],
			},
		};
	}

	async execute(
		args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		if (dryRun()) return { status: "dry_run" };
		const sandbox = this.manager.get(ctx.sessionId);
		if (!sandbox)
			throw new Error("no pipeline sandbox for this session");
		await this.manager.sendData(
			sandbox,
			String(args.topic),
			String(args.data),
		);
		return { status: "sent", topic: args.topic };
	}
}

class PipelineQueryESTool implements Tool {
	private manager: PipelineManager;

	constructor(manager: PipelineManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "pipeline_query_es",
			description:
				"Query the pipeline's Elasticsearch instance.",
			parameters: {
				type: "object",
				properties: {
					index: {
						type: "string",
						description: "ES index name",
					},
					query: {
						type: "string",
						description: "ES JSON query DSL or plain text",
					},
				},
				required: ["index"],
			},
		};
	}

	async execute(
		args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		if (dryRun()) return { hits: [], total: 0 };
		const sandbox = this.manager.get(ctx.sessionId);
		if (!sandbox)
			throw new Error("no pipeline sandbox for this session");
		return this.manager.queryES(
			sandbox,
			String(args.index),
			args.query as string | undefined,
		);
	}
}

class PipelineLogstashStatusTool implements Tool {
	private manager: PipelineManager;

	constructor(manager: PipelineManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "pipeline_get_logstash_status",
			description: "Get Logstash logs and status.",
			parameters: {
				type: "object",
				properties: {
					tail: {
						type: "string",
						description: "Number of lines (default: 100)",
					},
				},
			},
		};
	}

	async execute(
		args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		if (dryRun()) return { logs: "dry run" };
		const sandbox = this.manager.get(ctx.sessionId);
		if (!sandbox)
			throw new Error("no pipeline sandbox for this session");
		const logs = await this.manager.getLogstashLogs(
			sandbox,
			parseInt(String(args.tail)) || 100,
		);
		return { logs };
	}
}

class PipelineUpdateConfigTool implements Tool {
	private manager: PipelineManager;

	constructor(manager: PipelineManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "pipeline_update_config",
			description:
				"Update Logstash pipeline configuration and restart Logstash.",
			parameters: {
				type: "object",
				properties: {
					config: {
						type: "string",
						description: "Logstash input/filter/output config",
					},
				},
				required: ["config"],
			},
		};
	}

	async execute(
		args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		if (dryRun()) return { status: "dry_run" };
		const sandbox = this.manager.get(ctx.sessionId);
		if (!sandbox)
			throw new Error("no pipeline sandbox for this session");
		await this.manager.updateConfig(sandbox, String(args.config));
		return { status: "updated" };
	}
}

class PipelineHealthTool implements Tool {
	private manager: PipelineManager;

	constructor(manager: PipelineManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "pipeline_health",
			description: "Health check for all pipeline components.",
			parameters: { type: "object", properties: {} },
		};
	}

	async execute(
		_args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		if (dryRun()) {
			return { session_id: "dry", overall: "healthy", components: [] };
		}
		const sandbox = this.manager.get(ctx.sessionId);
		if (!sandbox)
			throw new Error("no pipeline sandbox for this session");
		return this.manager.health(
			sandbox,
		) as unknown as Promise<Record<string, unknown>>;
	}
}

// ── Extension ──────────────────────────────────────────────────

class PipelineExtension implements Extension {
	name = "pipeline";
	private manager?: PipelineManager;

	async init(ctx: ExtensionContext): Promise<void> {
		const cfg = ctx.config as unknown as PipelineConfig;
		this.manager = new PipelineManager(cfg, ctx.logger);

		ctx.registry.register(new PipelineSendDataTool(this.manager));
		ctx.registry.register(new PipelineQueryESTool(this.manager));
		ctx.registry.register(new PipelineLogstashStatusTool(this.manager));
		ctx.registry.register(new PipelineUpdateConfigTool(this.manager));
		ctx.registry.register(new PipelineHealthTool(this.manager));

		ctx.logger.info("pipeline extension initialized");
	}

	async start(_signal: AbortSignal): Promise<void> {
		// Containers created on-demand
	}

	async stop(): Promise<void> {
		await this.manager?.close();
	}
}

export default new PipelineExtension();
