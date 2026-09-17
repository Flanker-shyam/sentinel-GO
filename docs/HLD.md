# Sentinel-GO: High Level Design

## 1. Overview

Sentinel-GO is a Go-based log streaming and anomaly detection system that continuously ingests logs from AWS EKS pods, filters by severity, deduplicates, batches, and leverages an LLM (AWS Bedrock — Claude) to analyze error/warning patterns — firing alerts to Google Chat when anomalies are detected. It applies a per-issue cooldown to avoid alert spam while still surfacing distinct incidents.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS EKS Cluster                                 │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐                                  │
│   │  Pod A   │  │  Pod B   │  │  Pod N   │   (stdout, JSON logs)             │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘                                  │
│        └─────────────┼─────────────┘                                        │
│                      │  K8s API (Watch + follow log streams)                │
└──────────────────────┼──────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Sentinel-GO Service                                  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Discovery Controller (Watch API)                                     │  │
│  │  - list-then-watch per namespace                                      │  │
│  │  - regex match on pod + container names                               │  │
│  │  - starts/stops one streamer goroutine per matched container          │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
│                                   ▼                                          │
│  ┌───────────────┐   streamChan   ┌───────────┐   analyzeChan   ┌─────────┐  │
│  │  Streamers    │───────────────▶│  Dedup    │────────────────▶│ Batcher │  │
│  │ (per pod,     │  (severity-    │ (sliding  │  (unique lines) │(time/   │  │
│  │  retry+filter)│   filtered)    │  window)  │                 │ size)   │  │
│  └───────────────┘                └───────────┘                 └────┬────┘  │
│                                                                       ▼       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Analysis Engine (Bedrock / Claude)                                   │  │
│  │  - rate limiter → InvokeModel → structured JSON verdict               │  │
│  │  - verdict: is_anomaly, severity, issue_type, summary, affected       │  │
│  │  - per-(service, issue_type) cooldown gate                            │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Notifier (Google Chat webhook)                                       │  │
│  │  - startup message, 30-min heartbeat (reset on alert), anomaly alerts │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components

### 3.1 Discovery Controller (`internal/discovery`)

| Aspect | Detail |
|--------|--------|
| **Mechanism** | K8s Watch API with a **list-then-watch** loop per namespace |
| **Matching** | Regex on pod names (`pod_patterns`) and container names (`container_patterns`); empty container patterns → auto-pick first non-sidecar container |
| **Lifecycle** | Maintains an `active` map keyed `namespace/pod/container`; starts a streamer goroutine per new match, cancels it on pod deletion/termination |
| **Reconciliation** | On each LIST, reconciles desired vs active (starts missing, stops stale) — catches anything missed while a watch was down |
| **Resilience** | Watch expiry (~5 min) and connectivity failures (e.g., VPN down) are handled with exponential backoff, not a busy-loop |

Handles scale up/down and rolling deploys automatically: new pods → new streamers, terminated pods → cancelled streamers, no log gap during rollouts.

### 3.2 Streamer (`internal/streamer`)

| Aspect | Detail |
|--------|--------|
| **Source** | K8s API `GetLogs(...).Stream(ctx)` with `Follow: true` (like `kubectl logs -f`) |
| **Blocking read** | `bufio.Scanner.Scan()` blocks until the next line arrives — no polling, zero CPU waste |
| **Severity filter** | Inline — parses the `"level"` field from JSON logs; forwards only `ERROR`/`WARN`/`FATAL`/`PANIC` |
| **Reconnection** | Exponential backoff with jitter (1s → 2s → … → 30s cap) on stream failure; only exits on context cancellation |
| **Output** | Writes filtered lines to a shared `chan string` (`streamChan`) |

### 3.3 Deduplication (`internal/dedup`)

- Sliding-window hash set (SHA-256 of the full line)
- Suppresses **identical** duplicate lines within a configurable window (default 60s)
- Background cleanup goroutine evicts expired entries
- Sits as a pipeline stage between `streamChan` and `analyzeChan`
- Note: line-level dedup handles exact repeats; incident-level spam is handled by the alert cooldown (see 3.5)

### 3.4 Batcher (`internal/analysis/batch.go`)

- Flushes a batch when **either**: batch size reached (default 50) **or** time window elapsed (default 30s)
- Empty windows are skipped (no wasted LLM calls)

### 3.5 Analysis Engine (`internal/analysis/bedrock.go`)

**LLM client**
- AWS Bedrock via `bedrock-runtime:InvokeModel`
- Credentials from the default AWS chain (saml2aws / IRSA) — no keys in code
- Rate limiter (`time.Tick`) enforces a minimum interval between calls (default 10s) to control cost/throttling
- Response text is defensively unwrapped (strips markdown fences / trailing prose) before JSON parsing

**Structured verdict**

```go
type AnalysisResult struct {
    IsAnomaly        bool     `json:"is_anomaly"`
    Severity         int      `json:"severity"`   // 1-10
    IssueType        string   `json:"issue_type"` // stable snake_case category
    Summary          string   `json:"summary"`
    RootCause        string   `json:"root_cause"`
    AffectedServices []string `json:"affected_services"`
    Recommendation   string   `json:"recommendation"`
}
```

**Decision + cooldown**
- Alert fires when `IsAnomaly && Severity >= anomaly_threshold` (default 6) **and** the issue is not in cooldown
- Cooldown is keyed on **`(service, issue_type)`**, not service alone:
  - Same issue repeating on a service → alert once, suppress for the cooldown window (default 15m)
  - A **different** `issue_type` on the same service → alerts immediately (not suppressed)
  - After the window expires, the ongoing issue alerts again as a reminder
- `issue_type` is an LLM-assigned stable category (e.g., `http_404_errors`, `db_connection_failure`, `out_of_memory`) so recurring problems map to the same key

### 3.6 Notifier (`internal/notify`)

- Google Chat via incoming webhook (`SendText` / `Send`)
- Three message types:
  - **Startup**: `✅ Sentinel-GO is up and watching for errors.`
  - **Heartbeat**: `💚 all good` every 30 min of quiet; the timer resets whenever an anomaly alert fires
  - **Anomaly alert**: severity, summary, affected services (root cause/recommendation currently logged only, not sent)

---

## 4. Data Flow

```
Pods ──(Watch)──▶ Discovery Controller ──spawns──▶ Streamers (per pod/container)
                                                        │ severity filter
                                                        ▼
                                                   streamChan
                                                        │
                                                    [Dedup]  (sliding window)
                                                        │
                                                   analyzeChan
                                                        │
                                                    [Batcher] (time or size)
                                                        │
                                                  [Rate limiter]
                                                        │
                                                  [Bedrock/Claude]
                                                        │
                                              is_anomaly && severity>=T ?
                                                        │ yes
                                                  [Cooldown gate]  (service+issue_type)
                                                        │ not suppressed
                                                        ▼
                                                 [Google Chat alert]
                                                        │
                                                 (resets heartbeat timer)
```

---

## 5. Project Structure

```
sentinel-GO/
├── cmd/sentinel/main.go               # Entrypoint — config selection + DI wiring
├── internal/
│   ├── k8s/client.go                  # K8s clientset (kubeconfig / in-cluster)
│   ├── discovery/
│   │   ├── types.go                   # Target, PodStream, StreamFunc, helpers
│   │   ├── discovery.go               # One-shot Discover()
│   │   └── controller.go              # Dynamic list-then-watch controller
│   ├── streamer/streamer.go           # Follow-stream + severity filter + retry
│   ├── dedup/dedup.go                 # Sliding-window dedup
│   ├── analysis/
│   │   ├── batch.go                   # Time/size batching
│   │   └── bedrock.go                 # Bedrock client, prompt, cooldown, rate limit
│   ├── notify/gchat.go                # Google Chat webhook notifier
│   └── config/
│       ├── config.go                  # YAML loader + env expansion + defaults
│       └── dotenv.go                  # .env loader
├── configs/
│   ├── sentinel.dev.yaml
│   └── sentinel.prod.yaml
├── docs/HLD.md
├── .env.example
├── go.mod
└── go.sum
```

---

## 6. Configuration

Config file is selected by `SENTINEL_ENV` (`dev` default, `prod`, or any `<env>` → `configs/sentinel.<env>.yaml`); the `-config` flag overrides. `${VAR}` references are expanded from the environment, with a `.env` file auto-loaded at startup (real env vars take precedence).

```yaml
targets:
  - namespace: ""
    pod_patterns:
      - ".*"
    container_patterns:
      - ""
      - ""

streaming:
  buffer_size: 1000
  tail_lines: 50

dedup:
  enabled: true
  window: "60s"

analysis:
  batch_size: 50
  flush_interval: "30s"
  region: ""
  model_id: ""
  max_tokens: 2048
  anomaly_threshold: 6
  min_call_interval: "10s"
  alert_cooldown: "15m"

notification:
  google_chat:
    enabled: true
    webhook_url: "${GCHAT_WEBHOOK_URL}"
```

---

## 7. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Go language** | Low memory footprint, first-class concurrency (goroutines/channels), fast container startup |
| **Goroutine-per-container** | Simple model; Go scheduler handles many streams efficiently |
| **Watch API + list-then-watch** | Real-time pod lifecycle tracking; reconciliation closes gaps from expired/broken watches |
| **Inline severity filter** | Cheap first pass; only errors/warnings reach the LLM, cutting cost |
| **Sliding-window dedup** | Removes exact-duplicate lines cheaply before batching |
| **Batch before LLM** | Fewer API calls, and richer context for cross-line pattern detection |
| **Rate limiter on LLM** | Bounds cost and avoids Bedrock throttling during error storms |
| **Structured JSON verdict** | Enables programmatic thresholding and cooldown keys |
| **Cooldown on (service, issue_type)** | Prevents spam from one ongoing incident while still alerting on *different* problems — fails toward over-alerting, never silently dropping a new issue |
| **Region-pinned model option** | Avoids cross-region inference profiles that can hit region-scoped IAM deny policies |
| **Env-based config** | One image, many environments/accounts; deploy once per account |

---

## 8. Resilience

| Failure | Handling |
|---------|----------|
| Log stream breaks (pod restart, network blip) | Streamer exponential-backoff reconnect |
| K8s watch expires (~5 min) | Re-list + reconcile + re-watch |
| API connectivity lost (e.g., VPN down locally) | Watch loop backs off instead of busy-looping; auto-recovers when connectivity returns |
| Pod scaled/redeployed | Discovery starts/stops streamers automatically |
| LLM returns fenced/annotated JSON | Response unwrapped before parsing |
| LLM unavailable / error | Logged; batch dropped for that cycle (no crash) |
| Bedrock throttling | Rate limiter caps call frequency |

---

## 9. Deployment Model

- **Containerized**: single image; deploy as a Deployment in each target EKS cluster
- **Auth**: in-cluster service account via IRSA (no kubeconfig needed in-cluster); Bedrock uses the pod role
- **Cross-account**: run **one deployment per account**, each with its own `SENTINEL_ENV` config (namespace, region, model), all pointing at the same Google Chat webhook — keeps accounts isolated and avoids cross-account IAM complexity
- **IAM**: `bedrock:InvokeModel` on the Claude model/profile in use

---

## 10. Non-Functional Targets

| Requirement | Target |
|-------------|--------|
| Latency (ingestion → alert) | < ~60s for critical anomalies |
| Memory | Low — bounded channels + streaming reads |
| Availability | Stateless; restart-safe |
| LLM cost control | Severity filter + dedup + batching + rate limit |
| Fault tolerance | Graceful degradation; auto-reconnect at every layer |

---

## 11. Future Enhancements

- Plain-text (non-JSON) log severity detection
- Prometheus metrics endpoint (`/metrics`) + health probes
- Additional notification sinks (Slack, PagerDuty, SNS)
- Richer alerts (include root cause + recommendation, currently logged only)
- Historical incident context fed to the LLM
- Correlation engine to group related errors across services into one incident
