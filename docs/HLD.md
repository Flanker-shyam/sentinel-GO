# Sentinel-GO: High Level Design

## 1. Overview

Sentinel-GO is a Go-based log streaming and anomaly detection system that continuously ingests logs from multiple AWS EKS/ECS pods, applies filtering/deduplication/cleaning pipelines, and leverages an LLM to analyze error/warning patterns — firing alerts when anomalies are detected.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS Cloud                                       │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                                  │
│  │  Pod A   │  │  Pod B   │  │  Pod N   │                                  │
│  │ (stdout) │  │ (stdout) │  │ (stdout) │                                  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                                  │
│       │              │              │                                        │
│       └──────────────┼──────────────┘                                       │
│                      │                                                      │
│              CloudWatch Logs / K8s API                                       │
└──────────────────────┼──────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Sentinel-GO Service                                  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                     Log Ingestion Layer                                │  │
│  │                                                                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                    │  │
│  │  │ Pod Watcher │  │ Pod Watcher │  │ Pod Watcher │  (1 goroutine/pod) │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                    │  │
│  │         └─────────────────┼───────────────┘                           │  │
│  │                           ▼                                            │  │
│  │                   Raw Log Channel                                      │  │
│  └───────────────────────────┼────────────────────────────────────────────┘  │
│                              ▼                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                   Processing Pipeline                                  │  │
│  │                                                                        │  │
│  │  ┌──────────┐   ┌──────────────┐   ┌──────────┐   ┌───────────────┐   │  │
│  │  │  Filter  │──▶│ Deduplicator │──▶│ Cleaner  │──▶│ Severity      │   │  │
│  │  │  (noise) │   │ (bloom/hash) │   │ (parser) │   │ Classifier    │   │  │
│  │  └──────────┘   └──────────────┘   └──────────┘   └───────┬───────┘   │  │
│  │                                                            │           │  │
│  └────────────────────────────────────────────────────────────┼───────────┘  │
│                                                               ▼              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    LLM Analysis Engine                                 │  │
│  │                                                                        │  │
│  │  ┌─────────────┐   ┌──────────────┐   ┌────────────────────────────┐  │  │
│  │  │   Batcher   │──▶│ LLM Client   │──▶│ Anomaly Scoring & Decision │  │  │
│  │  │ (time/size) │   │ (Bedrock/    │   │                            │  │  │
│  │  │             │   │  OpenAI)     │   └─────────────┬──────────────┘  │  │
│  │  └─────────────┘   └──────────────┘                 │                 │  │
│  └─────────────────────────────────────────────────────┼─────────────────┘  │
│                                                        ▼                     │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                     Alerting Layer                                     │  │
│  │                                                                        │  │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   │  │
│  │  │   Slack   │  │  PagerDuty│  │    SNS    │  │  Webhook (generic)│   │  │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────────────┘   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components

### 3.1 Log Ingestion Layer

| Aspect | Detail |
|--------|--------|
| **Source** | Kubernetes API (`/api/v1/namespaces/{ns}/pods/{pod}/log?follow=true`) — long-lived chunked HTTP stream, same mechanism as `kubectl logs -f` |
| **Concurrency Model** | One goroutine per matched pod, managed by a Pod Discovery controller |
| **Pod Discovery** | Namespace + pod name regex pattern matching. K8s Watch API detects new/terminated pods in configured namespaces; regex filter determines which pods to stream from |
| **Backpressure** | Bounded channel (`chan LogEntry`, configurable buffer size) between ingestion and processing |
| **Reconnection** | Exponential backoff with jitter (capped at 30s) on stream disconnects |
| **Streaming** | Uses `client-go` `GetLogs().Stream(ctx)` — `bufio.Scanner.Scan()` blocks until next line arrives (zero CPU waste, no polling) |

#### Pod Discovery via Regex

Configuration drives the scope — you specify namespace and a pod name regex pattern. The discovery controller watches those namespaces and streams logs from any pod whose name matches the pattern:

```yaml
ingestion:
  targets:
    - namespace: "production"
      pod_pattern: "payment-.*"        # matches payment-service-7b4f9-xk2z, payment-worker-abc123
    - namespace: "production"
      pod_pattern: "order-.*"
      exclude_pattern: ".*-canary-.*"  # optional: skip canary pods
    - namespace: "staging"
      pod_pattern: ".*"                # everything in staging
```

This approach:
- Is intuitive (teams already think in terms of pod prefixes like `payment-*`, `order-*`)
- Requires zero config changes on rolling deploys or scaling events
- Provides namespace scoping to avoid accidental noise
- Supports full regex for OR patterns, wildcards, exclusions

```go
type LogEntry struct {
    Timestamp   time.Time
    PodName     string
    Namespace   string
    Container   string
    Message     string
```

### 3.2 Processing Pipeline

A chain-of-responsibility pattern where each stage is a `Processor` interface:

```go
type Processor interface {
    Process(ctx context.Context, entry LogEntry) (*LogEntry, error)
}
```

#### 3.2.1 Filter
- Drops known noise (health checks, readiness probes, debug-level spam)
- Configurable via regex/glob allowlist and denylist in YAML config

#### 3.2.2 Deduplicator
- Uses a time-windowed bloom filter (or sliding-window hash set) to suppress duplicate log lines
- Window size configurable (default: 60s)
- Key = hash(pod_name + normalized_message)

#### 3.2.3 Cleaner / Parser
- Strips ANSI escape codes, trims whitespace
- Attempts structured parsing (JSON logs → extract fields)
- Normalizes timestamps to UTC

#### 3.2.4 Severity Classifier
- Regex-based first pass: matches `ERROR`, `WARN`, `FATAL`, `PANIC`, stack traces
- Outputs only `ERROR` and `WARNING` severity entries to the next stage
- Attaches severity label to the LogEntry

### 3.3 LLM Analysis Engine

#### 3.3.1 Batcher
- Collects filtered log entries into batches by:
  - **Time window**: flush every N seconds (default: 30s)
  - **Size threshold**: flush when batch reaches M entries (default: 50)
- Groups logs by namespace/service for contextual analysis

#### 3.3.2 LLM Client
- Abstracts the LLM provider behind an interface:

```go
type Analyzer interface {
    Analyze(ctx context.Context, batch []LogEntry) (*AnalysisResult, error)
}
```

- Supported backends:
  - **AWS Bedrock** (Claude, Titan) — preferred for staying within AWS
  - **OpenAI API** (GPT-4) — fallback/alternative
- Prompt engineering:
  - System prompt defines the role: "You are a production incident analyst..."
  - Provides log batch as context
  - Asks for: anomaly detection, root cause hypothesis, severity score (1-10), recommended action

#### 3.3.3 Anomaly Scoring & Decision
- LLM returns structured JSON response:

```go
type AnalysisResult struct {
    IsAnomaly       bool     `json:"is_anomaly"`
    Severity        int      `json:"severity"` // 1-10
    Summary         string   `json:"summary"`
    RootCause       string   `json:"root_cause"`
    AffectedPods    []string `json:"affected_pods"`
    Recommendation  string   `json:"recommendation"`
}
```

- Alert fires when `IsAnomaly == true && Severity >= threshold` (configurable, default: 6)
- Cooldown period per service to avoid alert fatigue (default: 5 min)

### 3.4 Alerting Layer

- Pluggable alert sinks via interface:

```go
type AlertSink interface {
    Send(ctx context.Context, alert Alert) error
}
```

- Built-in sinks: Slack webhook, AWS SNS, PagerDuty, generic HTTP webhook
- Alert payload includes: summary, affected pods, severity, timestamp, raw log sample, LLM analysis

---

## 4. Data Flow

```
Pod Logs (stream)
    │
    ▼
[Ingestion] ──buffered channel──▶ [Filter] ──▶ [Dedup] ──▶ [Clean] ──▶ [Classify]
                                                                            │
                                                              (errors/warnings only)
                                                                            │
                                                                            ▼
                                                                       [Batcher]
                                                                            │
                                                              (time or size trigger)
                                                                            │
                                                                            ▼
                                                                     [LLM Analysis]
                                                                            │
                                                                  (anomaly detected?)
                                                                            │
                                                                   YES      │     NO
                                                                    ▼       │      ▼
                                                              [Fire Alert]  │  [Discard]
                                                                            │
                                                                         [Metrics]
```

---

## 5. Project Structure

```
sentinel-GO/
├── cmd/
│   └── sentinel/
│       └── main.go              # Entrypoint, config loading, DI wiring
├── internal/
│   ├── config/
│   │   └── config.go           # YAML config struct and loader
│   ├── ingestion/
│   │   ├── watcher.go          # Pod log watcher (K8s API / CloudWatch)
│   │   ├── discovery.go        # Pod discovery controller
│   │   └── checkpoint.go       # Stream position checkpointing
│   ├── pipeline/
│   │   ├── pipeline.go         # Pipeline orchestrator
│   │   ├── filter.go           # Noise filter
│   │   ├── dedup.go            # Deduplicator (bloom filter)
│   │   ├── cleaner.go          # Log cleaner/parser
│   │   └── classifier.go       # Severity classifier
│   ├── analysis/
│   │   ├── batcher.go          # Log batching logic
│   │   ├── analyzer.go         # Analyzer interface
│   │   ├── bedrock.go          # AWS Bedrock implementation
│   │   └── openai.go           # OpenAI implementation
│   ├── alerting/
│   │   ├── manager.go          # Alert routing, cooldowns, dedup
│   │   ├── slack.go            # Slack sink
│   │   ├── sns.go              # SNS sink
│   │   └── webhook.go          # Generic webhook sink
│   └── models/
│       └── types.go            # Shared types (LogEntry, Alert, etc.)
├── configs/
│   └── sentinel.yaml           # Default configuration
├── docs/
│   └── HLD.md                  # This document
├── go.mod
├── go.sum
└── Makefile
```

---

## 6. Configuration

```yaml
# configs/sentinel.yaml
ingestion:
  buffer_size: 10000
  reconnect_backoff_max: 30s
  targets:
    - namespace: "production"
      pod_pattern: "payment-.*"
    - namespace: "production"
      pod_pattern: "order-.*"
      exclude_pattern: ".*-canary-.*"
    - namespace: "staging"
      pod_pattern: ".*"

pipeline:
  filter:
    deny_patterns:
      - "health.*check"
      - "GET /ready"
      - "kube-probe"
  dedup:
    window: 60s
    max_entries: 100000
  classifier:
    min_severity: "WARN"       # Only pass WARN+ to analysis

analysis:
  provider: "bedrock"          # "bedrock" | "openai"
  model: "anthropic.claude-3-sonnet"
  batch_window: 30s
  batch_size: 50
  anomaly_threshold: 6         # severity 1-10
  cooldown_per_service: 5m
  max_tokens: 2048

alerting:
  sinks:
    - type: "slack"
      webhook_url: "${SLACK_WEBHOOK_URL}"
      channel: "#incidents"
    - type: "sns"
      topic_arn: "arn:aws:sns:us-east-1:123456789:sentinel-alerts"

observability:
  metrics_port: 9090
  log_level: "info"
```

---

## 7. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Go language** | Low memory footprint, excellent concurrency primitives (goroutines/channels), fast startup for containerized deployment |
| **Goroutine-per-pod** | Simple model; Go scheduler handles thousands of goroutines efficiently |
| **Bloom filter for dedup** | O(1) lookup, memory-efficient for high-throughput log streams; acceptable false-positive rate (~1%) |
| **Batch before LLM** | Reduces API calls/cost; provides context window for pattern detection across related logs |
| **Structured LLM output** | JSON schema enforcement allows programmatic decision-making on anomaly scores |
| **Cooldown per service** | Prevents alert storms during cascading failures |
| **Interface-driven design** | Swappable LLM providers, alert sinks, and ingestion sources without code changes |

---

## 8. Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| **Throughput** | 10,000+ log lines/sec across all pods |
| **Latency (ingestion → alert)** | < 60s for critical anomalies |
| **Memory** | < 512 MB for 100 pods |
| **Availability** | Stateless design; horizontal scaling via pod replicas |
| **LLM Cost Control** | Batching + severity pre-filter reduces API calls by ~90% |
| **Fault Tolerance** | Graceful degradation if LLM is unavailable (queue and retry) |

---

## 9. Deployment Model

- **Containerized**: Single Docker image, deploy as a Deployment in the same EKS cluster
- **IAM**: Requires `logs:FilterLogEvents`, `logs:DescribeLogGroups`, `bedrock:InvokeModel`, `sns:Publish`
- **Service Account**: IRSA (IAM Roles for Service Accounts) for pod-level AWS permissions
- **Scaling**: Horizontal — shard by namespace/label selector across replicas
- **Health**: `/healthz` and `/readyz` endpoints; Prometheus metrics at `/metrics`

---

## 10. Future Enhancements

- **Correlation engine**: Group related errors across services into a single incident
- **Feedback loop**: Allow operators to mark false positives → fine-tune severity thresholds
- **Historical context**: Feed past incident patterns to LLM for better detection
- **Multi-cluster support**: Aggregate logs across multiple EKS clusters
- **Cost dashboard**: Track LLM API usage and per-service analysis costs
- **Local LLM option**: Support Ollama/vLLM for air-gapped environments
