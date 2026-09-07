# Sentinel-GO

A Go-based log streamer that continuously ingests logs from Kubernetes pods on AWS, filters errors/warnings by severity, and uses an LLM (AWS Bedrock - Claude) to detect anomalies and fire alerts to Google Chat.

## Architecture

```
K8s Pods → Log Stream (with retry) → Severity Filter → Dedup → Batch → LLM Analysis → Cooldown → Alert → Google Chat
     ↕
Dynamic Discovery (Watch API — auto-tracks scale up/down/deploy)
```

## Prerequisites

- **Go 1.22+**
- **AWS CLI** configured (via `saml2aws login`)
- **kubectl** access to your EKS cluster
- **AWS Bedrock** model access enabled in your region

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

# Verify Bedrock access (region-pinned model)
echo '{"anthropic_version":"bedrock-2023-05-31","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}' > /tmp/test.json
aws bedrock-runtime invoke-model \
  --model-id anthropic.claude-3-haiku-20240307-v1:0 \
  --region eu-west-1 \
  --content-type application/json \
  --body fileb:///tmp/test.json \
  /tmp/output.json
cat /tmp/output.json
```

> **Note on model IDs:** Cross-region inference profiles (e.g., `eu.anthropic.claude-...`) route requests across multiple EU regions. If an account has a region-scoped deny policy (e.g., denying `eu-north-1`), use a region-pinned `ON_DEMAND` model like `anthropic.claude-3-haiku-20240307-v1:0` instead.

### 4. Configure

Sentinel uses **environment-specific config files** selected by the `SENTINEL_ENV` variable:

| `SENTINEL_ENV` | Config file loaded |
|----------------|--------------------|
| unset or `dev` | `configs/sentinel.dev.yaml` |
| `prod` | `configs/sentinel.prod.yaml` |
| `<other>` | `configs/sentinel.<other>.yaml` |
| (`-config` flag) | explicit path — overrides `SENTINEL_ENV` |

Each environment can target a different account, namespace, and Bedrock model. Example config:

```yaml
targets:
  - namespace: "tp-rc"
    pod_patterns:
      - "plan-outcome-reporting-.*"
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
  model_id: "anthropic.claude-3-haiku-20240307-v1:0"
  max_tokens: 2048
  anomaly_threshold: 6
  min_call_interval: "10s"
  alert_cooldown: "15m"

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
| `dedup.enabled` | Enable/disable log line deduplication |
| `dedup.window` | Suppress duplicate identical lines within this time window |
| `analysis.batch_size` | Max logs per LLM call |
| `analysis.flush_interval` | Max time to wait before sending a batch |
| `analysis.region` | AWS region for Bedrock |
| `analysis.model_id` | Bedrock model ID or inference profile |
| `analysis.anomaly_threshold` | Only alert if LLM severity score >= this (1-10) |
| `analysis.min_call_interval` | Rate limit — minimum time between LLM calls |
| `analysis.alert_cooldown` | Suppress repeat alerts for the same issue within this window |
| `notification.google_chat.webhook_url` | Google Chat space webhook URL (use env var) |

### 5. Set Environment Variables

The `.env` file is loaded automatically at startup (real env vars take precedence).

```bash
cp .env.example .env
# Edit .env with your Google Chat webhook URL:
# GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/SPACE_ID/messages?key=KEY&token=TOKEN
```

## Run

```bash
# Dev (default)
go run ./cmd/sentinel/

# Prod
SENTINEL_ENV=prod go run ./cmd/sentinel/

# Build binary
go build -o sentinel ./cmd/sentinel/ && ./sentinel

# Explicit config override
./sentinel -config /path/to/custom.yaml
```

## Output

```
2026/09/02 20:00:00 [main] using config: configs/sentinel.dev.yaml
2026/09/02 20:00:00 [main] Google Chat notifications enabled
2026/09/02 20:00:00 [main] deduplication enabled (window=1m0s)
sentinel-go starting (dynamic discovery mode)
2026/09/02 20:00:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-compute-7c97c-6gbz2/plan-outcome-reporting-compute
2026/09/02 20:00:00 [discovery] ▶ start streaming tp-rc/plan-outcome-reporting-api-5b8f4-mn9q3/plan-outcome-reporting-api

2026/09/02 20:00:30 [analysis] ✅ Batch of 12 logs — no anomaly (severity=2)
2026/09/02 20:01:00 [analysis] 🚨 ANOMALY DETECTED [severity=8, type=db_connection_failure]: Database connection failure causing cascading errors
2026/09/02 20:01:00 [analysis]    Affected: [plan-outcome-reporting-compute]
2026/09/02 20:01:30 [analysis] 🚨 ANOMALY DETECTED [severity=7, type=db_connection_failure]: ...
2026/09/02 20:01:30 [analysis]    (suppressed — [plan-outcome-reporting-compute]/db_connection_failure in cooldown window)
```

## Notifications

Sentinel sends three kinds of Google Chat messages:

| Type | When |
|------|------|
| **Startup** | `✅ Sentinel-GO is up and watching for errors.` — sent once on start |
| **Heartbeat** | `💚 all good, no anomalies in the last 30m` — sent after 30 min of quiet; timer resets whenever an anomaly alert fires |
| **Anomaly alert** | `🚨 ANOMALY DETECTED` with severity, summary, and affected services |

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
│   │   └── bedrock.go                # AWS Bedrock (Claude) LLM + rate limit + cooldown
│   ├── notify/gchat.go               # Google Chat webhook notifications
│   └── config/
│       ├── config.go                 # YAML config loader
│       └── dotenv.go                 # .env file loader
├── configs/
│   ├── sentinel.dev.yaml              # Dev environment config
│   └── sentinel.prod.yaml             # Prod environment config
├── docs/HLD.md                        # High Level Design document
└── .env.example                       # Environment variable template
```

## How It Works

1. **Discovery** — K8s Watch API continuously tracks pods matching your regex patterns; auto-handles scale up/down and rolling deploys.
2. **Streaming** — Opens a `follow=true` log stream per pod/container (like `kubectl logs -f`). Reconnects with exponential backoff on failure.
3. **Filtering** — Only passes ERROR/WARN/FATAL/PANIC level logs (JSON `"level"` field).
4. **Deduplication** — Suppresses identical duplicate log lines within a configurable window (default 60s).
5. **Batching** — Collects filtered logs until batch is full (50) or time window expires (30s).
6. **Analysis** — Sends batch to Claude on Bedrock with a structured prompt; rate-limited to control cost. Claude returns a structured verdict including an `issue_type` category.
7. **Cooldown** — Suppresses repeat alerts for the same `(service, issue_type)` within the cooldown window. A *different* issue on the same service still alerts immediately.
8. **Alerting** — Fires to Google Chat when an anomaly's severity >= threshold and it's not in cooldown.

## Alert Cooldown (Avoiding Spam Without Missing Incidents)

Log-line dedup alone doesn't prevent alert spam, because a single ongoing incident produces many *different* log lines (varying request IDs, timestamps). Sentinel therefore applies a cooldown at the **alert** level, keyed on **service + issue type**:

| Scenario | Behavior |
|----------|----------|
| Same issue repeating on a service | Alert once, then suppress for the cooldown window |
| **Different** issue on the same service (during cooldown) | **Alerts immediately** — not suppressed |
| Same issue on a different service | Alerts (separate key) |
| Issue still ongoing after cooldown expires | Alerts again (reminder) |

The `issue_type` (e.g., `http_404_errors`, `db_connection_failure`, `out_of_memory`) is assigned by the LLM as a stable snake_case category, so recurring problems map to the same key.

## Cross-Account / Multi-Environment

Each environment (dev/prod) uses its own config file and can point at a different AWS account, namespace, and Bedrock model. For monitoring multiple accounts, run **one deployment per account**, each with its own `SENTINEL_ENV`, all pointing at the same Google Chat webhook.

## IAM Permissions Required

Your role needs:

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-*"
}
```

## Known Limitations

- Only detects severity in JSON logs with a `"level"` field (no plain-text support yet)
- No Prometheus metrics endpoint
- Alert cooldown relies on the LLM producing consistent `issue_type` values (fails toward over-alerting, not under-alerting)

## Roadmap

- [ ] Plain-text log severity detection
- [ ] Prometheus metrics endpoint
- [ ] Multiple notification sinks (Slack, PagerDuty)
- [ ] Historical context — feed past incidents to the LLM
- [ ] Richer alerts (root cause + recommendation, currently logged only)
```