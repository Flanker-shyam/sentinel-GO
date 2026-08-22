# Sentinel-GO

A Go-based log streamer that continuously ingests logs from Kubernetes pods on AWS, filters errors/warnings by severity, and uses an LLM (AWS Bedrock - Claude) to detect anomalies and fire alerts.

## Architecture

```
K8s Pods → Log Stream → Severity Filter → Batch → LLM Analysis → Alert
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

analysis:
  batch_size: 50
  flush_interval: "30s"
  region: "eu-west-1"
  model_id: "anthropic.claude-3-5-sonnet-20241022-v2:0"
  max_tokens: 2048
  anomaly_threshold: 6
```

| Field | Description |
|-------|-------------|
| `targets[].namespace` | K8s namespace to watch |
| `targets[].pod_patterns` | Regex patterns to match pod names |
| `targets[].container_patterns` | Regex patterns to match containers (empty = auto-pick non-sidecar) |
| `streaming.buffer_size` | Channel buffer between streamers and batcher |
| `streaming.tail_lines` | Number of historical lines to fetch on stream start |
| `analysis.batch_size` | Max logs per LLM call |
| `analysis.flush_interval` | Max time to wait before sending a batch |
| `analysis.anomaly_threshold` | Only alert if LLM severity score >= this (1-10) |

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
Streaming from 3 pods:
  - tp-rc/plan-outcome-reporting-compute-7c97c-6gbz2 [plan-outcome-reporting-compute]
  - tp-rc/plan-outcome-reporting-compute-7c97c-xk2z1 [plan-outcome-reporting-compute]
  - tp-rc/plan-outcome-reporting-api-5b8f4-mn9q3 [plan-outcome-reporting-api]

2026/08/22 20:00:30 ✅ Batch of 12 logs analyzed — no anomaly (severity=2)
2026/08/22 20:01:00 🚨 ANOMALY DETECTED [severity=8]: Database connection failure causing cascading errors
   Root cause: PostgreSQL unreachable - connection pool exhausted
   Affected: [plan-outcome-reporting-compute]
   Action: Check RDS instance status and security groups
```

## Project Structure

```
├── cmd/sentinel/main.go           # Entrypoint — wires all components
├── internal/
│   ├── k8s/client.go              # Kubernetes client (kubeconfig / in-cluster)
│   ├── discovery/discovery.go     # Pod discovery via regex pattern matching
│   ├── streamer/streamer.go       # Log streaming + severity filtering
│   ├── analysis/
│   │   ├── batch.go              # Time/size-based batching
│   │   └── bedrock.go            # AWS Bedrock (Claude) LLM integration
│   └── config/config.go          # YAML config loader
├── configs/sentinel.yaml          # Default configuration
├── docs/HLD.md                    # High Level Design document
└── .env.example                   # Environment variable template
```

## How It Works

1. **Discovery** — Finds running pods matching your regex patterns in configured namespaces
2. **Streaming** — Opens a `follow=true` log stream per pod (like `kubectl logs -f`)
3. **Filtering** — Only passes ERROR/WARN/FATAL/PANIC level logs (JSON `"level"` field)
4. **Batching** — Collects filtered logs until batch is full (50) or time window expires (30s)
5. **Analysis** — Sends batch to Claude on Bedrock with a structured prompt
6. **Alerting** — If Claude detects an anomaly with severity >= threshold, logs an alert

## IAM Permissions Required

Your SAML role needs:

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-*"
}
```

## Known Limitations (v1)

- Discovery is one-shot (no dynamic pod tracking on scale up/down/deploy)
- No stream retry on disconnection
- No log deduplication
- Only detects severity in JSON logs with `"level"` field
- No notification sinks yet (Slack, SNS — coming soon)
- No rate limiting on LLM calls

## Roadmap

- [ ] Dynamic pod discovery (K8s Watch API)
- [ ] Stream reconnection with backoff
- [ ] Log deduplication (bloom filter)
- [ ] Notification sinks (Slack, SNS, PagerDuty)
- [ ] LLM call rate limiting
- [ ] Alert cooldown per service
- [ ] Plain-text log severity detection
- [ ] Prometheus metrics endpoint
