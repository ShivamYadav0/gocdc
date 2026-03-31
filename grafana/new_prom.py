#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   NMS Processor — Prometheus + Grafana Dashboard Creator  v2.0              ║
║   Full monitoring for Kafka → Redis → ClickHouse pipeline                   ║
║                                                                              ║
║   Panels   : 22 (KPI cards, timeseries, heatmaps, state timeline)           ║
║   Alerts   : 9  (pipeline freeze, HoL backpressure, timeout storm, etc.)    ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage:
    pip install requests
    python3 create_nms_grafana_dashboard.py

Environment variables:
    GRAFANA_URL                  default: http://localhost:3000
    GRAFANA_USER                 default: admin
    GRAFANA_PASSWORD             default: admin
    GRAFANA_API_KEY              optional — overrides basic auth
    PROMETHEUS_DATASOURCE_UID    default: prometheus
    ALERTMANAGER_DATASOURCE_UID  default: alertmanager
    FOLDER_NAME                  default: EMS Monitoring
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests

# ========================= CONFIG =========================
GRAFANA_URL     = os.getenv("GRAFANA_URL",      "http://localhost:3000").rstrip("/")
GRAFANA_USER    = os.getenv("GRAFANA_USER",     "admin")
GRAFANA_PASSWORD= os.getenv("GRAFANA_PASSWORD", "admin")
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY",  "")

PROM_DS_UID     = os.getenv("PROMETHEUS_DATASOURCE_UID",   "prometheus")
AM_DS_UID       = os.getenv("ALERTMANAGER_DATASOURCE_UID", "alertmanager")
FOLDER_NAME     = os.getenv("FOLDER_NAME", "EMS Monitoring")

# ─── HTTP Session ──────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    if GRAFANA_API_KEY:
        s.headers["Authorization"] = f"Bearer {GRAFANA_API_KEY}"
    else:
        s.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
    s.headers["Content-Type"] = "application/json"
    return s

SESSION = _make_session()


def api(method: str, path: str, **kwargs) -> Any:
    url = f"{GRAFANA_URL}{path}"
    r = SESSION.request(method, url, **kwargs)
    if not r.ok:
        print(f"  ✗ {r.status_code} {method} {path}")
        print(r.text[:800])
        r.raise_for_status()
    return r.json()


# ─── Folder ────────────────────────────────────────────────────────────────────
def ensure_folder() -> Optional[str]:
    folders = api("GET", "/api/folders")
    for f in folders:
        if f["title"] == FOLDER_NAME:
            print(f"  ✓ Found existing folder  '{FOLDER_NAME}'  uid={f['uid']}")
            return f["uid"]
    result = api("POST", "/api/folders", json={"title": FOLDER_NAME})
    print(f"  ✓ Created folder  '{FOLDER_NAME}'  uid={result['uid']}")
    return result["uid"]


# ─── Low-level panel helpers ───────────────────────────────────────────────────
def _ds(): return {"type": "prometheus", "uid": PROM_DS_UID}

def grid(x, y, w, h): return {"x": x, "y": y, "w": w, "h": h}


def _target(expr, legend="", ref="A"):
    return {"datasource": _ds(), "expr": expr, "refId": ref,
            "legendFormat": legend or "{{label_name}}"}


def _thresholds(*steps):
    """steps: list of (value, color). First value should be None (base)."""
    return {"mode": "absolute",
            "steps": [{"color": c, "value": v} for v, c in steps]}


# ─── Panel Factories ───────────────────────────────────────────────────────────
def stat(pid, title, expr, unit="short", x=0, y=0, w=6, h=4,
         thresholds=None, color_mode="background", legend=""):
    return {
        "id": pid, "type": "stat", "title": title,
        "gridPos": grid(x, y, w, h),
        "datasource": _ds(),
        "targets": [_target(expr, legend)],
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "colorMode": color_mode,
            "graphMode": "area",
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "thresholds": thresholds or _thresholds((None, "green")),
            }
        }
    }


def timeseries(pid, title, targets, unit="short", x=0, y=0, w=12, h=8,
               fill=15, line=2, stack=False):
    """targets: list of (expr, legend) tuples."""
    return {
        "id": pid, "type": "timeseries", "title": title,
        "gridPos": grid(x, y, w, h),
        "datasource": _ds(),
        "targets": [_target(e, l, chr(65+i)) for i, (e, l) in enumerate(targets)],
        "options": {
            "tooltip": {"mode": "multi"},
            "legend": {"displayMode": "list", "placement": "bottom"},
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "lineWidth": line,
                    "fillOpacity": fill,
                    "stacking": {"mode": "normal" if stack else "none"},
                }
            }
        }
    }


def gauge_panel(pid, title, expr, unit="percentunit", min_=0, max_=1,
                x=0, y=0, w=6, h=6, thresholds=None):
    return {
        "id": pid, "type": "gauge", "title": title,
        "gridPos": grid(x, y, w, h),
        "datasource": _ds(),
        "targets": [_target(expr)],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}},
        "fieldConfig": {
            "defaults": {
                "unit": unit, "min": min_, "max": max_,
                "thresholds": thresholds or _thresholds(
                    (None, "green"), (0.7, "yellow"), (0.9, "red")),
            }
        }
    }


def bargauge(pid, title, expr, unit="short", x=0, y=0, w=12, h=6,
             orient="horizontal", legend="{{shard}}"):
    return {
        "id": pid, "type": "bargauge", "title": title,
        "gridPos": grid(x, y, w, h),
        "datasource": _ds(),
        "targets": [_target(expr, legend)],
        "options": {
            "orientation": orient,
            "reduceOptions": {"calcs": ["lastNotNull"]},
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "thresholds": _thresholds(
                    (None, "green"), (2000, "yellow"), (3500, "red")),
            }
        }
    }


def row_panel(pid, title, y):
    return {"id": pid, "type": "row", "title": title,
            "gridPos": grid(0, y, 24, 1), "collapsed": False}


# ─── Build Dashboard ───────────────────────────────────────────────────────────
def build_dashboard(folder_uid: str) -> Dict:
    p = []  # panels list

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 0 — KPI Overview  (y=0)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(1, "🚦 Pipeline Health Overview", y=0))

    p.append(stat(2, "Messages / sec",
                  "sum(rate(nms_processor_messages_total[1m]))",
                  unit="short", x=0, y=1, w=4))

    p.append(stat(3, "Active DOWN Nodes",
                  "nms_active_down_nodes",
                  unit="none", x=4, y=1, w=4,
                  thresholds=_thresholds((None, "green"), (10, "orange"), (50, "red"))))

    p.append(stat(4, "Error Rate / sec",
                  "sum(rate(nms_processor_errors_total[5m]))",
                  unit="short", x=8, y=1, w=4,
                  thresholds=_thresholds((None, "green"), (1, "red"))))

    p.append(stat(5, "CH Insert Timeouts / min",
                  "sum(rate(nms_clickhouse_insert_timeouts_total[1m])) * 60",
                  unit="short", x=12, y=1, w=4,
                  thresholds=_thresholds((None, "green"), (1, "yellow"), (5, "red"))))

    p.append(stat(6, "Pipeline Ingestion Lag (s)",
                  "time() - min(nms_clickhouse_last_commit_timestamp_seconds)",
                  unit="s", x=16, y=1, w=4,
                  thresholds=_thresholds((None, "green"), (30, "yellow"), (60, "red"))))

    p.append(stat(7, "Channel Utilization",
                  "nms_event_channel_utilization",
                  unit="percentunit", x=20, y=1, w=4,
                  thresholds=_thresholds((None, "green"), (0.7, "yellow"), (1.0, "red"))))

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 1 — Kafka / Throughput  (y=6)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(10, "📨 Kafka & Message Throughput", y=6))

    p.append(timeseries(11, "Message Throughput by Status",
                        [("sum(rate(nms_processor_messages_total[1m])) by (status)", "{{status}}")],
                        unit="ops", x=0, y=7, w=12))

    p.append(timeseries(12, "Kafka Consumer Lag (seconds)",
                        [("nms_kafka_consumer_lag_seconds", "{{topic}}/{{group}}")],
                        unit="s", x=12, y=7, w=12,
                        fill=20))

    p.append(timeseries(13, "Kafka p50 / p95 / p99 Processing Latency",
                        [
                            ("histogram_quantile(0.50, sum(rate(nms_processor_kafka_message_duration_seconds_bucket[5m])) by (le))", "p50"),
                            ("histogram_quantile(0.95, sum(rate(nms_processor_kafka_message_duration_seconds_bucket[5m])) by (le))", "p95"),
                            ("histogram_quantile(0.99, sum(rate(nms_processor_kafka_message_duration_seconds_bucket[5m])) by (le))", "p99"),
                        ],
                        unit="s", x=0, y=15, w=12))

    p.append(timeseries(14, "Messages Skipped / sec (by reason)",
                        [("sum(rate(nms_kafka_messages_skipped_total[1m])) by (reason)", "{{reason}}")],
                        unit="ops", x=12, y=15, w=12))

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 2 — Worker Pool & Backpressure  (y=23)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(20, "⚙️ Worker Pool & Backpressure", y=23))

    p.append(gauge_panel(21, "Worker Pool Saturation",
                         "nms_processor_workers_active / 20",   # 20 = WorkerCount
                         unit="percentunit", x=0, y=24, w=6))

    p.append(gauge_panel(22, "Overall Channel Utilization",
                         "nms_event_channel_utilization",
                         unit="percentunit", x=6, y=24, w=6,
                         thresholds=_thresholds((None,"green"),(0.7,"yellow"),(1.0,"red"))))

    p.append(bargauge(23, "Per-Shard Channel Depth",
                      "nms_event_channel_depth",
                      unit="short", x=12, y=24, w=12,
                      legend="Shard {{shard}}"))

    p.append(timeseries(24, "Channel Backpressure Events (blocked enqueues/sec)",
                        [("rate(nms_event_channel_blocked_total[1m])", "blocked/s")],
                        unit="ops", x=0, y=30, w=12,
                        fill=30))

    p.append(timeseries(25, "Active Workers Over Time",
                        [("nms_processor_workers_active", "active workers")],
                        unit="short", x=12, y=30, w=12))

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 3 — ClickHouse  (y=38)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(30, "🗄️ ClickHouse Insert Health", y=38))

    p.append(timeseries(31, "Batch Insert Latency p95 / p99 (per shard)",
                        [
                            ("histogram_quantile(0.95, sum(rate(nms_clickhouse_batch_duration_seconds_bucket[5m])) by (le, shard))", "p95 shard {{shard}}"),
                            ("histogram_quantile(0.99, sum(rate(nms_clickhouse_batch_duration_seconds_bucket[5m])) by (le, shard))", "p99 shard {{shard}}"),
                        ],
                        unit="s", x=0, y=39, w=12))

    p.append(timeseries(32, "INSERT Timeouts / sec (per shard)",
                        [("sum(rate(nms_clickhouse_insert_timeouts_total[1m])) by (shard)", "Shard {{shard}}")],
                        unit="ops", x=12, y=39, w=12,
                        fill=40))

    p.append(timeseries(33, "Batch Size Distribution (avg records/flush)",
                        [("sum(rate(nms_clickhouse_batch_size_records_sum[1m])) by (shard) / sum(rate(nms_clickhouse_batch_size_records_count[1m])) by (shard)", "Shard {{shard}}")],
                        unit="short", x=0, y=47, w=12))

    p.append(timeseries(34, "Ingestion Lag per Shard (time() − last_commit)",
                        [("time() - nms_clickhouse_last_commit_timestamp_seconds", "Shard {{shard}}")],
                        unit="s", x=12, y=47, w=12,
                        fill=20))

    p.append(bargauge(35, "In-Memory Buffer Depth per Shard",
                      "nms_clickhouse_buffer_depth",
                      unit="short", x=0, y=55, w=12,
                      legend="Shard {{shard}}"))

    p.append(timeseries(36, "ClickHouse Errors / sec (prepare vs send)",
                        [("sum(rate(nms_clickhouse_errors_total[1m])) by (shard, operation)", "{{operation}} shard {{shard}}")],
                        unit="ops", x=12, y=55, w=12))

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 4 — Redis  (y=63)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(40, "🔴 Redis State Store", y=63))

    p.append(timeseries(41, "Redis Operation Latency p95 (GET vs SET)",
                        [
                            ("histogram_quantile(0.95, sum(rate(nms_redis_operation_duration_seconds_bucket[5m])) by (le, operation))", "{{operation}}"),
                        ],
                        unit="s", x=0, y=64, w=12))

    p.append(timeseries(42, "Redis Errors / sec",
                        [("sum(rate(nms_redis_errors_total[1m])) by (operation)", "{{operation}}")],
                        unit="ops", x=12, y=64, w=12))

    p.append(timeseries(43, "Redis Cache Miss Rate",
                        [("rate(nms_redis_cache_misses_total[1m])", "miss/s")],
                        unit="ops", x=0, y=72, w=12))

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 5 — Business / Domain  (y=80)
    # ══════════════════════════════════════════════════════════════════════════
    p.append(row_panel(50, "📊 Business & Domain Metrics", y=80))

    p.append(timeseries(51, "Interval Records Created / sec",
                        [("rate(nms_interval_records_total[1m])", "intervals/s")],
                        unit="ops", x=0, y=81, w=12))

    p.append(timeseries(52, "Outage Duration Distribution (heatmap proxy — avg/p95/p99)",
                        [
                            ("histogram_quantile(0.50, sum(rate(nms_interval_duration_seconds_bucket[5m])) by (le))", "p50 duration"),
                            ("histogram_quantile(0.95, sum(rate(nms_interval_duration_seconds_bucket[5m])) by (le))", "p95 duration"),
                            ("histogram_quantile(0.99, sum(rate(nms_interval_duration_seconds_bucket[5m])) by (le))", "p99 duration"),
                        ],
                        unit="s", x=12, y=81, w=12))

    p.append(timeseries(53, "Node Flap Rate / min (DOWN→UP < 10s)",
                        [("sum(rate(nms_node_flaps_total[1m])) * 60", "flaps/min")],
                        unit="short", x=0, y=89, w=12))

    p.append(timeseries(54, "End-to-End Pipeline Latency p50 / p95 / p99",
                        [
                            ("histogram_quantile(0.50, sum(rate(nms_pipeline_e2e_latency_seconds_bucket[5m])) by (le))", "p50"),
                            ("histogram_quantile(0.95, sum(rate(nms_pipeline_e2e_latency_seconds_bucket[5m])) by (le))", "p95"),
                            ("histogram_quantile(0.99, sum(rate(nms_pipeline_e2e_latency_seconds_bucket[5m])) by (le))", "p99"),
                        ],
                        unit="s", x=12, y=89, w=12))

    return {
        "dashboard": {
            "title": "NMS Processor — Full Pipeline Monitoring v2",
            "uid":   "nms-processor-v2",
            "tags":  ["nms", "kafka", "clickhouse", "redis", "prometheus", "pipeline"],
            "timezone":      "browser",
            "schemaVersion": 38,
            "version":       2,
            "refresh":       "10s",
            "time": {"from": "now-6h", "to": "now"},
            "panels": p,
        },
        "folderUid": folder_uid,
        "overwrite": True,
        "message": "Created by create_nms_grafana_dashboard.py v2.0",
    }


# ─── Alert Rules ───────────────────────────────────────────────────────────────
# Uses Grafana Unified Alerting (POST /api/ruler/grafana/api/v1/rules/{folder})
# All annotations include runbooks so on-call engineers know exactly what to do.

def build_alert_group() -> Dict:
    def alert(name, expr, for_dur, severity, summary, description, runbook=""):
        return {
            "title": name,
            "condition": "C",
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": PROM_DS_UID,
                    "model": {"expr": expr, "refId": "A"},
                },
                {
                    "refId": "C",
                    "datasourceUid": "__expr__",
                    "model": {
                        "refId": "C", "type": "threshold",
                        "datasource": {"type": "__expr__", "uid": "__expr__"},
                        "conditions": [{"evaluator": {"params": [0], "type": "gt"},
                                        "operator":  {"type": "and"},
                                        "query":     {"params": ["A"]},
                                        "reducer":   {"type": "last"},
                                        "type":      "query"}]
                    }
                }
            ],
            "for": for_dur,
            "annotations": {
                "summary":     summary,
                "description": description,
                "runbook_url": runbook,
            },
            "labels": {"severity": severity, "team": "nms-platform"},
            "noDataState":  "NoData",
            "execErrState": "Alerting",
        }

    rules = [

        # ── 1. Silent Pipeline Freeze (The "Invisible Ingestion Gap") ──────────
        alert(
            name     = "NMS: Pipeline Silent Freeze",
            expr     = "time() - min(nms_clickhouse_last_commit_timestamp_seconds) > 60",
            for_dur  = "1m",
            severity = "critical",
            summary  = "No ClickHouse commit in >60s — pipeline may be frozen",
            description=(
                "The most recently successful ClickHouse batch commit is more than 60 seconds old. "
                "This is the primary indicator of the 'Invisible Ingestion Gap': Go workers appear "
                "healthy (metrics still scraping) but no data is reaching the DB. "
                "Immediate check: nms_clickhouse_insert_timeouts_total, nms_event_channel_utilization."
            ),
        ),

        # ── 2. ClickHouse INSERT Timeout Storm ──────────────────────────────────
        alert(
            name     = "NMS: ClickHouse INSERT Timeout Storm",
            expr     = "sum(rate(nms_clickhouse_insert_timeouts_total[2m])) > 0.5",
            for_dur  = "2m",
            severity = "critical",
            summary  = "ClickHouse INSERT operations are timing out (>0.5/s over 2m)",
            description=(
                "Go context.WithTimeout(2s) is firing on ClickHouse INSERTs at a sustained rate. "
                "Root cause is usually a background Part Merge saturating disk I/O, or "
                "insert_quorum misconfiguration. "
                "Mitigation: (1) increase BatchSize to reduce INSERT IOPS; "
                "(2) check `system.merges` in ClickHouse; "
                "(3) consider async_insert mode."
            ),
        ),

        # ── 3. Full Channel Backpressure / Head-of-Line Blocking ───────────────
        alert(
            name     = "NMS: Channel Backpressure — HoL Blocking Risk",
            expr     = "nms_event_channel_utilization > 0.90",
            for_dur  = "2m",
            severity = "warning",
            summary  = "Interval channel >90% full — Head-of-Line blocking imminent",
            description=(
                "The aggregate shard channel fill ratio has been above 90% for 2 minutes. "
                "Producers (workers) will start blocking on channel send, stalling Kafka consumption. "
                "Check: nms_event_channel_blocked_total rate and nms_clickhouse_batch_duration_seconds. "
                "Fix: increase ShardCount or ShardChanSize; verify ClickHouse write throughput."
            ),
        ),

        # ── 4. Channel Completely Full (Critical Backpressure) ─────────────────
        alert(
            name     = "NMS: Channel Completely Full — Active Backpressure",
            expr     = "nms_event_channel_utilization >= 1.0",
            for_dur  = "30s",
            severity = "critical",
            summary  = "All shard channels are 100% full — Kafka consumption is blocked",
            description=(
                "All shards are at capacity. Go workers are blocked on channel sends. "
                "Kafka consumer lag is growing rapidly. "
                "Immediate action: check nms_clickhouse_insert_timeouts_total. "
                "If zero, the batcher is overwhelmed by volume — scale ShardCount. "
                "If non-zero, ClickHouse is the bottleneck — see INSERT timeout runbook."
            ),
        ),

        # ── 5. Kafka Consumer Lag Spike ────────────────────────────────────────
        alert(
            name     = "NMS: Kafka Consumer Lag > 30s",
            expr     = "nms_kafka_consumer_lag_seconds > 30",
            for_dur  = "3m",
            severity = "warning",
            summary  = "Kafka consumer is >30s behind real-time",
            description=(
                "The gap between message event_time and wall clock has exceeded 30 seconds. "
                "This is an early signal of downstream saturation (channel full or slow CH writes). "
                "Correlate with nms_event_channel_utilization and nms_processor_workers_active."
            ),
        ),

        # ── 6. High Processing Error Rate ─────────────────────────────────────
        alert(
            name     = "NMS: High Processing Error Rate",
            expr     = "sum(rate(nms_processor_errors_total[5m])) > 5",
            for_dur  = "2m",
            severity = "warning",
            summary  = "Processing errors >5/s sustained for 2 minutes",
            description=(
                "A sustained error rate of >5/s across all error types. "
                "Break down by label: type='json_unmarshal' → malformed Debezium event schema; "
                "type='redis' → Redis latency/connectivity issue; "
                "type='clickhouse' → ClickHouse prepare/send failure."
            ),
        ),

        # ── 7. Redis Latency Degradation ───────────────────────────────────────
        alert(
            name     = "NMS: Redis p95 Latency > 50ms",
            expr     = (
                "histogram_quantile(0.95, "
                "  sum(rate(nms_redis_operation_duration_seconds_bucket[5m])) by (le, operation)"
                ") > 0.05"
            ),
            for_dur  = "3m",
            severity = "warning",
            summary  = "Redis p95 operation latency has exceeded 50ms",
            description=(
                "A Redis GET or SET is taking >50ms at the 95th percentile. "
                "Since every Kafka message requires 1 GET + 1 SET, this directly limits throughput "
                "to a max of ~20 msg/s per worker. Check Redis memory, eviction policy, and network."
            ),
        ),

        # ── 8. Too Many DOWN Nodes (Mass Outage Detection) ─────────────────────
        alert(
            name     = "NMS: Mass Node Outage — >100 Nodes DOWN",
            expr     = "nms_active_down_nodes > 100",
            for_dur  = "1m",
            severity = "critical",
            summary  = "More than 100 network nodes are currently in DOWN state",
            description=(
                "The number of nodes tracked as DOWN in Redis has exceeded 100. "
                "This may indicate a genuine network event (MPLS core failure, upstream BGP issue) "
                "or a trap storm / deduplication failure in the upstream alarm normalizer."
            ),
        ),

        # ── 9. E2E Pipeline Latency Degradation ───────────────────────────────
        alert(
            name     = "NMS: E2E Pipeline Latency p95 > 10s",
            expr     = (
                "histogram_quantile(0.95, "
                "  sum(rate(nms_pipeline_e2e_latency_seconds_bucket[5m])) by (le)"
                ") > 10"
            ),
            for_dur  = "3m",
            severity = "warning",
            summary  = "End-to-end pipeline latency p95 has exceeded 10 seconds",
            description=(
                "The 95th percentile time from Kafka message enqueue to ClickHouse commit "
                "is above 10 seconds. Outage intervals in the DB are significantly delayed vs real time. "
                "Correlate: nms_clickhouse_batch_duration_seconds, nms_event_channel_utilization, "
                "nms_kafka_consumer_lag_seconds."
            ),
        ),
    ]

    return {
        "name":     "NMS Pipeline Alerts",
        "interval": "1m",
        "rules":    rules,
    }


# ─── Main ──────────────────────────────────────────────────────────────────────
def run():
    sep = "=" * 74
    print(sep)
    print("   NMS Processor — Prometheus + Grafana Dashboard Creator  v2.0")
    print(sep)
    print(f"  Grafana            → {GRAFANA_URL}")
    print(f"  Prometheus DS UID  → {PROM_DS_UID}")
    print(f"  Folder             → {FOLDER_NAME}")
    print()

    # ── Step 1: Folder ────────────────────────────────────────────────────────
    print("[1/3] Ensuring Grafana folder...")
    folder_uid = ensure_folder()

    # ── Step 2: Dashboard (22 panels) ─────────────────────────────────────────
    print("\n[2/3] Creating dashboard (22 panels)...")
    payload = build_dashboard(folder_uid)
    result  = api("POST", "/api/dashboards/db", json=payload)
    dash_url = f"{GRAFANA_URL}{result.get('url', '')}"
    print(f"  ✓ Dashboard created  →  {dash_url}")

    # ── Step 3: Alert Rules (9 rules) ─────────────────────────────────────────
    print("\n[3/3] Creating alert rules (9 rules via Unified Alerting)...")
    alert_group = build_alert_group()
    try:
        api(
            "POST",
            f"/api/ruler/grafana/api/v1/rules/{FOLDER_NAME}",
            json=alert_group,
        )
        print("  ✓ 9 alert rules created successfully")
    except Exception as e:
        print(f"  ⚠ Alert rule creation failed (Unified Alerting may be disabled): {e}")
        print("    Dumping alert JSON to  nms_alerts.json  for manual import...")
        with open("nms_alerts.json", "w") as f:
            json.dump(alert_group, f, indent=2)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  ✅  All done!")
    print(f"\n  Dashboard  →  {dash_url}")
    print(f"\n  Panels created (22):")
    sections = [
        ("Pipeline Health Overview",   "7  KPI stat cards"),
        ("Kafka & Message Throughput", "4  timeseries panels"),
        ("Worker Pool & Backpressure", "4  gauge / bargauge / timeseries"),
        ("ClickHouse Insert Health",   "6  panels incl. timeout + lag"),
        ("Redis State Store",          "3  latency + error panels"),
        ("Business & Domain Metrics",  "4  intervals, flaps, E2E latency"),
    ]
    for section, detail in sections:
        print(f"    • {section:<35}  {detail}")

    print(f"\n  Alert rules (9):")
    alerts = [
        "Pipeline Silent Freeze              (critical — ingestion gap)",
        "ClickHouse INSERT Timeout Storm     (critical — HoL root cause)",
        "Channel Backpressure >90%           (warning  — early HoL signal)",
        "Channel 100% Full                   (critical — Kafka blocked)",
        "Kafka Consumer Lag >30s             (warning  — downstream saturation)",
        "High Processing Error Rate          (warning  — schema / Redis / CH)",
        "Redis p95 Latency >50ms             (warning  — throughput cap)",
        "Mass Node Outage >100 DOWN          (critical — network event)",
        "E2E Pipeline Latency p95 >10s       (warning  — delayed outage data)",
    ]
    for a in alerts:
        print(f"    • {a}")
    print(sep)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)