# Sentinel-GO

A Go-based log streamer that continuously ingests logs from Kubernetes pods on AWS, filters errors/warnings by severity, and uses an LLM (AWS Bedrock - Claude) to detect anomalies and fire alerts.

## Architecture

```
K8s Pods → Log Stream (with retry) → Severity Filter → Dedup → Batch → LLM Analysis → Alert → Google Chat
     ↕
Dynamic Discovery (Watch API — auto-tracks scale up/down/deploy)
```

## Prerequisites

- **Go 1.22+**
- **AWS CLI** configured (via `saml2aws login`)
- **kubectl** access to your EKS cluster
- **AWS Bedrock** model access enabled (Claude 3.5 Sonnet in your region)

## Setup

### 1. Clone

```bash
git clone https://github.com/Flanker-shyam/sentinel-GO.git
cd sentinel-GO
```

### 2. Install dependencies

```bash
go mod download
```

### 3. Authenticate

```bash
# Login via SAML (populates ~/.aws/credentials and ~/.kube/config)
saml2aws login

# Verify K8s access
kubectl get pods -n tp-rc

# Verify Bedrock access
echo '{"anthropic_version":"bedrock-2023-05-31","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}' > /tmp/test.json
aws bedrock-runtime invoke-model \
  --model-id anthropic.claude-3-5-sonnet-20241022-v2:0 \
  --region eu-west-1 \
  --content-type application/json \
  --body fileb:///tmp/test.json \
  /tmp/output.json
cat /tmp/output.json
```

### 4. Configure

Edit `configs/sentinel.yaml` to match your environment:

```yaml
targets:
  - namespace: "tp-rc"
    pod_patterns:
      - "plan-outcome-reporting-compute-.*"
      - "plan-outcome-reporting-api-.*"
    container_patterns:
      - "plan-outcome-reporting-compute"
      - "plan-outcome-reporting-api"

streaming:
  buffer_size: 1000
  tail_lines: 50

dedup:
  enabled: true
  window: "60s"

analysis:
  batch_size: 50
  flush_interval: "30s"
  region: "eu-west-1"
  model_id: "anthropic.claude-3-5-sonnet-20241022-v2:0"
  max_tokens: 2048
  anomaly_threshold: 6
  min_call_interval: "10s"

notification:
  google_chat:
    enabled: true
    webhook_url: "${GCHAT_WEBHOOK_URL}"
```

| Field | Description |
|-------|-------------|
| `targets[].namespace` | K8s namespace to watch |
| `targets[].pod_patterns` | Regex patterns to match pod names |
| `targets[].container_patterns` | Regex patterns to match containers (empty = auto-pick non-sidecar) |
| `streaming.buffer_size` | Channel buffer between streamers and batcher |
| `streaming.tail_lines` | Number of historical lines to fetch on stream start |
| `dedup.enabled` | Enable/disable log deduplication |
| `dedup.window` | Suppress duplicate lines within this time window |
| `analysis.batch_size` | Max logs per LLM call |
| `analysis.flush_interval` | Max time to wait before sending a batch |
| `analysis.anomaly_threshold` | Only alert if LLM severity score >= this (1-10) |
| `analysis.min_call_interval` | Rate limit — minimum time between LLM calls |
| `notification.google_chat.webhook_url` | Google Chat space webhook URL (use env var) |

### 5. Set Environment Variables

```bash
cp .env.example .env
# Edit .env with your Google Chat webhook URL
export GCHAT_WEBHOOK_URL="https://chat.googleapis.com/v1/spaces/SPACE_ID/messages?key=KEY&token=TOKEN"
```

## Run

```bash
# Build and run
go build -o sentinel ./cmd/sentinel/
./sentinel

# Or directly
go run ./cmd/sentinel/

# With custom config path
./sentinel -config /path/to/custom.yaml
```

## Output

```
sentinel-go starting (dynamic discovery mode)
2026/08/22 20:00:00 [main] Google Chat notifications enabled
2026/08/22 20:00:00 [main] deduplication enabled (window=1m0s)
2026/08/22 20:00:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-compute-7c97c-6gbz2/plan-outcome-reporting-compute
2026/08/22 20:00:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-compute-7c97c-xk2z1/plan-outcome-reporting-compute
2026/08/22 20:00:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-api-5b8f4-mn9q3/plan-outcome-reporting-api

2026/08/22 20:00:30 [analysis] ✅ Batch of 12 logs — no anomaly (severity=2)
2026/08/22 20:01:00 [analysis] 🚨 ANOMALY DETECTED [severity=8]: Database connection failure causing cascading errors
2026/08/22 20:01:00 [analysis]    Root cause: PostgreSQL unreachable - connection pool exhausted
2026/08/22 20:01:00 [analysis]    Affected: [plan-outcome-reporting-compute]
2026/08/22 20:01:00 [analysis]    Action: Check RDS instance status and security groups

# On rolling deploy:
2026/08/22 20:05:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-compute-8d4a1-abc12/plan-outcome-reporting-compute
2026/08/22 20:05:02 [discovery] ■ stopped streaming tp-rc/plan-outcome-reporting-compute-7c97c-6gbz2/plan-outcome-reporting-compute

# On stream failure:
2026/08/22 20:10:00 [streamer] stream broke for tp-rc/compute-7c97c-xk2z1/compute: EOF — reconnecting in 1s
2026/08/22 20:10:01 [streamer] stream broke for tp-rc/compute-7c97c-xk2z1/compute: EOF — reconnecting in 2s
```

## Project Structure

```
├── cmd/sentinel/main.go               # Entrypoint — wires all components
├── internal/
│   ├── k8s/client.go                  # Kubernetes client (kubeconfig / in-cluster)
│   ├── discovery/
│   │   ├── types.go                  # Shared types, constants, helpers
│   │   ├── discovery.go              # One-shot pod discovery
│   │   └── controller.go            # Dynamic controller (Watch API, lifecycle mgmt)
│   ├── streamer/streamer.go           # Log streaming + severity filtering + retry
│   ├── dedup/dedup.go                 # Log deduplication (sliding window hash set)
│   ├── analysis/
│   │   ├── batch.go                  # Time/size-based batching
│   │   └── bedrock.go                # AWS Bedrock (Claude) LLM + rate limiting
│   ├── notify/gchat.go               # Google Chat webhook notifications
│   └── config/config.go              # YAML config loader
├── configs/sentinel.yaml              # Default configuration
├── docs/HLD.md                        # High Level Design document
└── .env.example                       # Environment variable template
```

## How It Works

1. **Discovery** — Uses K8s Watch API to continuously track pods matching your regex patterns. Automatically handles scale up/down and rolling deploys.
2. **Streaming** — Opens a `follow=true` log stream per pod/container (like `kubectl logs -f`). Reconnects automatically with exponential backoff on failure.
3. **Filtering** — Only passes ERROR/WARN/FATAL/PANIC level logs (JSON `"level"` field).
4. **Deduplication** — Suppresses duplicate log lines within a configurable time window (default 60s). Prevents error storms from flooding the LLM.
5. **Batching** — Collects filtered logs until batch is full (50) or time window expires (30s).
6. **Analysis** — Sends batch to Claude on Bedrock with a structured prompt. Rate-limited to prevent cost blowup.
7. **Alerting** — If Claude detects an anomaly with severity >= threshold, sends alert to Google Chat space.

## Dynamic Discovery

The discovery controller handles pod lifecycle events automatically:

| Scenario | Behavior |
|----------|----------|
| **Scale up** (3→5 replicas) | New pods detected via Watch → streaming starts automatically |
| **Scale down** (5→3 replicas) | Pods deleted → goroutines cancelled, cleaned up |
| **Rolling deploy** | New pods start streaming before old pods are terminated — no log gap |
| **Pod crash/restart** | Stream reconnects with backoff; next reconcile ensures it's tracked |
| **Watch expires** (~5 min) | Re-lists pods, reconciles state, re-establishes watch |

## IAM Permissions Required

Your SAML role needs:

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-*"
}
```

## Known Limitations

- Only detects severity in JSON logs with `"level"` field (no plain-text support yet)
- No alert cooldown per service (same anomaly can re-alert after each batch)
- No Prometheus metrics endpoint

## Roadmap

- [ ] Alert cooldown per service
- [ ] Plain-text log severity detection
- [ ] Prometheus metrics endpoint
- [ ] Multiple notification sinks (Slack, PagerDuty)
- [ ] Historical context — feed past incidents to LLM
