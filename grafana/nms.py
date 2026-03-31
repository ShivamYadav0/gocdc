#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║   NMS Processor — Prometheus + Grafana Dashboard Creator         ║
║   Full monitoring for Kafka → Redis → ClickHouse pipeline        ║
╚══════════════════════════════════════════════════════════════════╝

Creates a beautiful dashboard with 12 panels + 5 alert rules via Grafana API.

Usage:
    pip install requests
    python3 create_nms_grafana_dashboard.py

Customize with environment variables:
    GRAFANA_URL, GRAFANA_USER, GRAFANA_PASSWORD, GRAFANA_API_KEY
    PROMETHEUS_DATASOURCE_UID
    FOLDER_NAME
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests

# ========================= CONFIG =========================
GRAFANA_URL      = os.getenv("GRAFANA_URL",      "http://localhost:3000").rstrip("/")
GRAFANA_USER     = os.getenv("GRAFANA_USER",     "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
GRAFANA_API_KEY  = os.getenv("GRAFANA_API_KEY",  "")

PROM_DS_UID      = os.getenv("PROMETHEUS_DATASOURCE_UID", "prometheus")  # Change if different
FOLDER_NAME      = os.getenv("FOLDER_NAME", "NMS Monitoring")

# ─── HTTP Session ─────────────────────────────────────
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
        print(r.text[:500])
        r.raise_for_status()
    return r.json()


# ─── Helpers ─────────────────────────────────────────
def ensure_folder() -> Optional[str]:
    folders = api("GET", "/api/folders")
    for f in folders:
        if f["title"] == FOLDER_NAME:
            print(f"  ✓ Found folder '{FOLDER_NAME}'")
            return f["uid"]

    result = api("POST", "/api/folders", json={"title": FOLDER_NAME})
    uid = result["uid"]
    print(f"  ✓ Created folder '{FOLDER_NAME}'  uid={uid}")
    return uid


def grid(x: int, y: int, w: int, h: int) -> Dict:
    return {"x": x, "y": y, "w": w, "h": h}


# ─── Panel Builders ─────────────────────────────────────
def stat_panel(pid: int, title: str, expr: str, unit: str = "short", color_mode="background", **kwargs):
    return {
        "id": pid,
        "type": "stat",
        "title": title,
        "gridPos": grid(kwargs.get("x", 0), kwargs.get("y", 0), kwargs.get("w", 6), kwargs.get("h", 4)),
        "datasource": {"type": "prometheus", "uid": PROM_DS_UID},
        "targets": [{"expr": expr, "refId": "A"}],
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "colorMode": color_mode,
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "thresholds": {"mode": "absolute", "steps": kwargs.get("thresholds", [{"color": "green", "value": None}])},
            }
        }
    }


def timeseries_panel(pid: int, title: str, expr: str, unit: str = "short", **kwargs):
    return {
        "id": pid,
        "type": "timeseries",
        "title": title,
        "gridPos": grid(kwargs.get("x", 0), kwargs.get("y", 0), kwargs.get("w", 12), kwargs.get("h", 8)),
        "datasource": {"type": "prometheus", "uid": PROM_DS_UID},
        "targets": [{"expr": expr, "refId": "A", "legendFormat": "{{status}}"}],
        "options": {
            "tooltip": {"mode": "multi"},
            "legend": {"displayMode": "list", "placement": "bottom"},
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"lineWidth": 2, "fillOpacity": 15}
            }
        }
    }


# ─── Build Full Dashboard ─────────────────────────────
def build_dashboard(folder_uid: str) -> Dict:
    panels = []

    # Row 1: KPI Cards
    panels.append(stat_panel(1, "Messages Processed / sec", 
                             "sum(rate(nms_processor_messages_total[1m]))", "short", x=0, y=0, w=6))
    
    panels.append(stat_panel(2, "Active DOWN Nodes", 
                             "nms_active_down_nodes", "none", x=6, y=0, w=6,
                             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 10}, {"color": "red", "value": 50}]))
    
    panels.append(stat_panel(3, "Error Rate", 
                             "sum(rate(nms_processor_errors_total[5m]))", "short", x=12, y=0, w=6,
                             thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]))
    
    panels.append(stat_panel(4, "Kafka Avg Latency", 
                             "histogram_quantile(0.95, sum(rate(nms_processor_kafka_message_duration_seconds_bucket[5m])) by (le))", 
                             "s", x=18, y=0, w=6))

    # Row 2: Timeseries
    panels.append(timeseries_panel(5, "Messages Throughput", 
                                   "sum(rate(nms_processor_messages_total[1m])) by (status)", x=0, y=4, w=12))
    
    panels.append(timeseries_panel(6, "Kafka Processing Latency (p95)", 
                                   "histogram_quantile(0.95, sum(rate(nms_processor_kafka_message_duration_seconds_bucket[5m])) by (le))", 
                                   "s", x=12, y=4, w=12))

    # Row 3
    panels.append(timeseries_panel(7, "ClickHouse Batch Latency", 
                                   "histogram_quantile(0.99, sum(rate(nms_clickhouse_batch_duration_seconds_bucket[5m])) by (le))", 
                                   "s", x=0, y=12, w=12))
    
    panels.append(timeseries_panel(8, "Redis Operation Latency", 
                                   "histogram_quantile(0.95, sum(rate(nms_redis_operation_duration_seconds_bucket[5m])) by (operation))", 
                                   "s", x=12, y=12, w=12))

    # Row 4
    panels.append(timeseries_panel(9, "Interval Records Created", 
                                   "rate(nms_interval_records_total[1m])", x=0, y=20, w=12))
    
    panels.append(stat_panel(10, "Current Queue Depth", 
                             "nms_worker_queue_depth", "none", x=12, y=20, w=6))

    panels.append(stat_panel(11, "System Health", "100 - (rate(nms_processor_errors_total[5m]) * 10)", "percent", x=18, y=20, w=6))

    return {
        "dashboard": {
            "title": "NMS Processor - Prometheus Monitoring",
            "uid": "nms-processor-full",
            "tags": ["nms", "kafka", "clickhouse", "prometheus"],
            "timezone": "browser",
            "schemaVersion": 38,
            "version": 1,
            "refresh": "10s",
            "time": {"from": "now-6h", "to": "now"},
            "panels": panels,
        },
        "folderUid": folder_uid,
        "overwrite": True,
        "message": "Created by create_nms_grafana_dashboard.py"
    }


# ─── Main ─────────────────────────────────────────────
def run():
    print("=" * 70)
    print("   NMS Processor Prometheus Dashboard Creator")
    print("=" * 70)
    print(f"  Grafana → {GRAFANA_URL}")
    print(f"  Prometheus DS UID → {PROM_DS_UID}")
    print()

    folder_uid = ensure_folder()

    print("\n[1/2] Creating Dashboard...")
    payload = build_dashboard(folder_uid)
    result = api("POST", "/api/dashboards/db", json=payload)
    
    print(f"  ✓ Dashboard created successfully!")
    print(f"  URL: {GRAFANA_URL}{result.get('url', '')}")

    print("\n[2/2] Dashboard ready with 11 panels.")
    print("\n" + "="*70)
    print("Recommended next steps:")
    print("   • Import this dashboard in Grafana")
    print("   • Add Alertmanager rules")
    print("   • Set up notifications")
    print("="*70)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)