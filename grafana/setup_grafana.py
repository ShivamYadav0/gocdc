#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║   Node Downtime Analytics — Grafana Dashboard Creator           ║
║   Targets: ClickHouse  •  grafana-clickhouse-datasource plugin  ║
╚══════════════════════════════════════════════════════════════════╝

Creates 10 panels + 3 alert rules in one shot via Grafana HTTP API.

Schema expected (configure TABLE below if yours differs):
  node_id       String / VARCHAR
  down_at       DateTime
  up_at         DateTime
  duration_sec  Float64

Usage
─────
  pip install requests
  python3 create_grafana_dashboard.py

Override defaults with environment variables:

  GRAFANA_URL          http://localhost:3000       Grafana base URL
  GRAFANA_USER         admin                       Basic-auth user
  GRAFANA_PASSWORD     admin                       Basic-auth password
  GRAFANA_API_KEY      <token>                     Bearer token (overrides user/pass)

  CLICKHOUSE_HOST      localhost                   ClickHouse HTTP host
  CLICKHOUSE_PORT      8123                        ClickHouse HTTP port
  CLICKHOUSE_DB        default                     Database name
  CLICKHOUSE_TABLE     node_intervals                 Table name
  CLICKHOUSE_USER      default                     ClickHouse user
  CLICKHOUSE_PASSWORD  (empty)                     ClickHouse password

  ALERT_EMAIL          ops@example.com             Email for alert notifications
  FOLDER_NAME          Node Monitoring             Grafana folder
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests

# ─── Configuration ─────────────────────────────────────────────────────────────

GRAFANA_URL      = os.getenv("GRAFANA_URL",      "http://localhost:3000").rstrip("/")
GRAFANA_USER     = os.getenv("GRAFANA_USER",     "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
GRAFANA_API_KEY  = os.getenv("GRAFANA_API_KEY",  "")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "localhost")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB       = os.getenv("CLICKHOUSE_DB",       "default")
CH_TABLE    = os.getenv("CLICKHOUSE_TABLE",    "node_intervals")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
FOLDER_NAME = os.getenv("FOLDER_NAME", "Node Monitoring")

# Full qualified table name used in every query
TABLE = f"{CH_DB}.{CH_TABLE}"

# ─── HTTP helpers ───────────────────────────────────────────────────────────────

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
    """Call Grafana API, raise on error, return parsed JSON."""
    url = f"{GRAFANA_URL}{path}"
    r = SESSION.request(method, url, **kwargs)
    if not r.ok:
        print(f"  ✗ {r.status_code} {method.upper()} {path}")
        print(f"    {r.text[:400]}")
        r.raise_for_status()
    return r.json()


# ─── Datasource ─────────────────────────────────────────────────────────────────

DS_TYPE = "grafana-clickhouse-datasource"


def ensure_datasource() -> str:
    """Return UID of the ClickHouse datasource, auto-creating if missing."""
    datasources = api("GET", "/api/datasources")
    for ds in datasources:
        if ds.get("type") == DS_TYPE:
            print(f"  ✓ Found datasource '{ds['name']}'  uid={ds['uid']}")
            return ds["uid"]

    print("  → No ClickHouse datasource found — creating one …")
    payload = {
        "name": "ClickHouse",
        "type": DS_TYPE,
        "access": "proxy",
        "isDefault": True,
        "jsonData": {
            "server":          CH_HOST,
            "port":            CH_PORT,
            "username":        CH_USER,
            "defaultDatabase": CH_DB,
            "protocol":        "http",
            "tlsSkipVerify":   True,
        },
        "secureJsonData": {"password": CH_PASSWORD},
    }
    result = api("POST", "/api/datasources", json=payload)
    uid = (result.get("datasource") or {}).get("uid") or result.get("uid", "")
    print(f"  ✓ Created datasource  uid={uid}")
    return uid


def ds_ref(uid: str) -> Dict:
    return {"type": DS_TYPE, "uid": uid}


# ─── Folder ──────────────────────────────────────────────────────────────────────

def ensure_folder() -> Optional[str]:
    """Return UID of the monitoring folder, creating it if absent."""
    try:
        folders = api("GET", "/api/folders")
        for f in folders:
            if f["title"] == FOLDER_NAME:
                print(f"  ✓ Using existing folder '{FOLDER_NAME}'  uid={f['uid']}")
                return f["uid"]
        result = api("POST", "/api/folders", json={"title": FOLDER_NAME})
        uid = result["uid"]
        print(f"  ✓ Created folder '{FOLDER_NAME}'  uid={uid}")
        return uid
    except Exception as exc:
        print(f"  ⚠ Could not create folder, using General  ({exc})")
        return None


# ─── Low-level query/target builder ─────────────────────────────────────────────

def target(uid: str, sql: str, ref: str = "A", fmt: int = 1) -> Dict:
    """
    Build a ClickHouse datasource target.
      fmt=0  TimeSeries (needs a 'time' column + value columns)
      fmt=1  Table (stat, table, barchart, gauge)
    """
    return {
        "refId":     ref,
        "datasource": ds_ref(uid),
        "rawSql":    sql,
        "format":    fmt,
        "queryType": "sql",
    }


def grid(x: int, y: int, w: int, h: int) -> Dict:
    return {"x": x, "y": y, "w": w, "h": h}


def threshold_steps(steps: List[Dict]) -> Dict:
    """steps: [{"color": "green", "value": None}, {"color": "red", "value": 10}, …]"""
    return {"mode": "absolute", "steps": steps}


# ─── Panel builders ──────────────────────────────────────────────────────────────

def stat_panel(
    pid: int, uid: str, title: str, sql: str,
    unit:        str  = "short",
    color_mode:  str  = "background",
    thresholds:  Optional[List] = None,
    description: str  = "",
    decimals:    int  = 0,
    x=0, y=0, w=5, h=4,
) -> Dict:
    """Single-value KPI panel."""
    if thresholds is None:
        thresholds = [{"color": "blue", "value": None}]
    return {
        "id":          pid,
        "type":        "stat",
        "title":       title,
        "description": description,
        "gridPos":     grid(x, y, w, h),
        "datasource":  ds_ref(uid),
        "targets":     [target(uid, sql, fmt=1)],
        "options": {
            "reduceOptions": {
                "calcs":  ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "orientation": "auto",
            "textMode":    "auto",
            "colorMode":   color_mode,
            "graphMode":   "none",
            "justifyMode": "center",
        },
        "fieldConfig": {
            "defaults": {
                "unit":     unit,
                "decimals": decimals,
                "color":    {"mode": "thresholds"},
                "thresholds": threshold_steps(thresholds),
            },
            "overrides": [],
        },
    }


def timeseries_panel(
    pid: int, uid: str, title: str, sql: str,
    unit:        str  = "short",
    fill:        float = 0.12,
    description: str  = "",
    x=0, y=0, w=14, h=9,
) -> Dict:
    """Multi-series line chart with time on X axis."""
    return {
        "id":          pid,
        "type":        "timeseries",
        "title":       title,
        "description": description,
        "gridPos":     grid(x, y, w, h),
        "datasource":  ds_ref(uid),
        # Two series from one query: count + avg_duration
        "targets": [target(uid, sql, fmt=0)],
        "options": {
            "tooltip": {"mode": "multi",   "sort": "desc"},
            "legend":  {"displayMode": "list", "placement": "bottom"},
        },
        "fieldConfig": {
            "defaults": {
                "unit":  unit,
                "color": {"mode": "palette-classic"},
                "custom": {
                    "lineWidth":    2,
                    "fillOpacity":  int(fill * 100),
                    "gradientMode": "none",
                    "spanNulls":    True,
                    "showPoints":   "never",
                    "lineInterpolation": "smooth",
                },
            },
            "overrides": [
                # Second series (avg_duration_sec) → right Y axis, seconds unit
                {
                    "matcher": {"id": "byName", "options": "avg_duration_sec"},
                    "properties": [
                        {"id": "unit",    "value": "s"},
                        {"id": "custom.axisPlacement", "value": "right"},
                        {"id": "color",   "value": {"fixedColor": "orange", "mode": "fixed"}},
                        {"id": "custom.lineWidth", "value": 1},
                        {"id": "custom.lineStyle",
                         "value": {"dash": [6, 4], "fill": "dash"}},
                    ],
                }
            ],
        },
    }


def barchart_panel(
    pid: int, uid: str, title: str, sql: str,
    orientation: str  = "horizontal",
    unit:        str  = "s",
    description: str  = "",
    x=0, y=0, w=10, h=9,
) -> Dict:
    """Bar chart — works for both sorted rankings and histograms."""
    return {
        "id":          pid,
        "type":        "barchart",
        "title":       title,
        "description": description,
        "gridPos":     grid(x, y, w, h),
        "datasource":  ds_ref(uid),
        "targets": [target(uid, sql, fmt=1)],
        "options": {
            "orientation":         orientation,
            "xTickLabelRotation":  -30,
            "xTickLabelMaxLength": 20,
            "groupWidth":          0.7,
            "barRadius":           0.04,
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "single"},
        },
        "fieldConfig": {
            "defaults": {
                "unit":  unit,
                "color": {"mode": "palette-classic"},
                "custom": {
                    "fillOpacity": 80,
                    "lineWidth":   0,
                },
            },
            "overrides": [],
        },
    }


def table_panel(
    pid: int, uid: str, title: str, sql: str,
    description: str = "",
    x=0, y=0, w=24, h=8,
) -> Dict:
    """Interactive sortable + filterable table."""
    return {
        "id":          pid,
        "type":        "table",
        "title":       title,
        "description": description,
        "gridPos":     grid(x, y, w, h),
        "datasource":  ds_ref(uid),
        "targets": [target(uid, sql, fmt=1)],
        "options": {
            "showHeader": True,
            "sortBy": [{"displayName": "down_at", "desc": True}],
            "footer": {"show": False},
        },
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "align":       "auto",
                    "displayMode": "auto",
                    "filterable":  True,
                },
            },
            "overrides": [
                # Color-code the duration column
                {
                    "matcher":    {"id": "byName", "options": "duration_sec"},
                    "properties": [
                        {"id": "unit",         "value": "s"},
                        {"id": "decimals",     "value": 3},
                        {"id": "custom.displayMode", "value": "color-background"},
                        {"id": "thresholds",   "value": threshold_steps([
                            {"color": "green",  "value": None},
                            {"color": "yellow", "value": 1},
                            {"color": "orange", "value": 5},
                            {"color": "red",    "value": 30},
                        ])},
                        {"id": "color", "value": {"mode": "thresholds"}},
                    ],
                },
                # Humanise timestamps
                {
                    "matcher":    {"id": "byName", "options": "down_at"},
                    "properties": [{"id": "unit", "value": "dateTimeAsIso"}],
                },
                {
                    "matcher":    {"id": "byName", "options": "up_at"},
                    "properties": [{"id": "unit", "value": "dateTimeAsIso"}],
                },
            ],
        },
    }


def gauge_panel(
    pid: int, uid: str, title: str, sql: str,
    min_val:     float = 0,
    max_val:     float = 100,
    unit:        str   = "percent",
    description: str   = "",
    x=0, y=0, w=5, h=4,
) -> Dict:
    """Radial gauge with red-yellow-green threshold banding."""
    return {
        "id":          pid,
        "type":        "gauge",
        "title":       title,
        "description": description,
        "gridPos":     grid(x, y, w, h),
        "datasource":  ds_ref(uid),
        "targets": [target(uid, sql, fmt=1)],
        "options": {
            "reduceOptions": {
                "calcs":  ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "orientation":          "auto",
            "showThresholdLabels":  False,
            "showThresholdMarkers": True,
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min":  min_val,
                "max":  max_val,
                "color": {"mode": "thresholds"},
                "thresholds": threshold_steps([
                    {"color": "red",    "value": None},
                    {"color": "yellow", "value": 80},
                    {"color": "green",  "value": 95},
                ]),
            },
            "overrides": [],
        },
    }


# ─── SQL library ─────────────────────────────────────────────────────────────────

def build_queries(table: str) -> Dict[str, str]:
    return {

        # ── KPI stats ──────────────────────────────────────────────────────────

        "total_outages": f"""
SELECT toUInt64(COUNT(*)) AS value
FROM {table}
""".strip(),

        "avg_duration": f"""
SELECT round(AVG(duration_sec), 3) AS value
FROM {table}
""".strip(),

        "max_duration": f"""
SELECT round(MAX(duration_sec), 3) AS value
FROM {table}
""".strip(),

        "affected_nodes": f"""
SELECT toUInt64(COUNT(DISTINCT node_id)) AS value
FROM {table}
""".strip(),

        # Uptime % = 1 - (total downtime / observation window)
        "uptime_pct": f"""
SELECT round(
  greatest(0.0,
    100.0 - (
      SUM(duration_sec) /
      nullIf(dateDiff('second', MIN(down_at), MAX(up_at)), 0) * 100.0
    )
  ), 2
) AS value
FROM {table}
""".strip(),

        # ── Time series ────────────────────────────────────────────────────────

        "outages_over_time": f"""
SELECT
  toStartOfMinute(down_at)        AS time,
  toUInt64(COUNT(*))              AS outages,
  round(AVG(duration_sec), 3)     AS avg_duration_sec
FROM {table}
WHERE $__timeFilter(down_at)
GROUP BY time
ORDER BY time ASC
""".strip(),

        # ── Node rankings ──────────────────────────────────────────────────────

        "top_nodes_downtime": f"""
SELECT
  node_id,
  toUInt64(COUNT(*))          AS outage_count,
  round(SUM(duration_sec), 2) AS total_downtime_sec
FROM {table}
GROUP BY node_id
ORDER BY total_downtime_sec DESC
LIMIT 15
""".strip(),

        # ── Duration histogram (pre-bucketed) ──────────────────────────────────

        "duration_histogram": f"""
SELECT
  multiIf(
    duration_sec <  0.5,  '1 <0.5s',
    duration_sec <  1.0,  '2 0.5-1s',
    duration_sec <  2.0,  '3 1-2s',
    duration_sec <  5.0,  '4 2-5s',
    duration_sec < 10.0,  '5 5-10s',
    duration_sec < 30.0,  '6 10-30s',
    duration_sec < 60.0,  '7 30-60s',
    '8 >60s'
  ) AS bucket,
  toUInt64(COUNT(*)) AS count
FROM {table}
GROUP BY bucket
ORDER BY bucket ASC
""".strip(),

        # ── Hourly pattern ─────────────────────────────────────────────────────

        "hourly_pattern": f"""
SELECT
  toUInt8(toHour(down_at))        AS hour_of_day,
  toUInt64(COUNT(*))              AS outage_count,
  round(AVG(duration_sec), 3)     AS avg_duration_sec
FROM {table}
GROUP BY hour_of_day
ORDER BY hour_of_day ASC
""".strip(),

        # ── Recent events table ────────────────────────────────────────────────

        "recent_outages": f"""
SELECT
  node_id,
  down_at,
  up_at,
  round(duration_sec, 3) AS duration_sec
FROM {table}
ORDER BY down_at DESC
LIMIT 200
""".strip(),

        # ── Alert queries (used in alerting rules) ─────────────────────────────

        "alert_max_duration_5m": f"""
SELECT round(MAX(duration_sec), 3) AS value
FROM {table}
WHERE down_at >= now() - INTERVAL 5 MINUTE
""".strip(),

        "alert_count_5m": f"""
SELECT toUInt64(COUNT(*)) AS value
FROM {table}
WHERE down_at >= now() - INTERVAL 5 MINUTE
""".strip(),

        "alert_max_per_node_1h": f"""
SELECT toUInt64(MAX(cnt)) AS value
FROM (
  SELECT node_id, COUNT(*) AS cnt
  FROM {table}
  WHERE down_at >= now() - INTERVAL 1 HOUR
  GROUP BY node_id
)
""".strip(),
    }


# ─── Dashboard assembly ──────────────────────────────────────────────────────────

def build_dashboard(ds_uid: str, folder_uid: Optional[str]) -> Dict:
    Q = build_queries(TABLE)
    panels: List[Dict] = []

    # ── Row 1: KPI stat cards (y=0, h=4) ─────────────────────────────────────

    panels.append(stat_panel(
        pid=1, uid=ds_uid, x=0, y=0, w=5, h=4,
        title="Total Outages",
        description="Lifetime count of node downtime events in the table.",
        sql=Q["total_outages"],
        unit="none",
        thresholds=[
            {"color": "green",  "value": None},
            {"color": "yellow", "value": 100},
            {"color": "orange", "value": 500},
            {"color": "red",    "value": 2000},
        ],
    ))

    panels.append(stat_panel(
        pid=2, uid=ds_uid, x=5, y=0, w=5, h=4,
        title="Avg Outage Duration",
        description="Mean duration (seconds) across all recorded outage events.",
        sql=Q["avg_duration"],
        unit="s", decimals=3,
        thresholds=[
            {"color": "green",  "value": None},
            {"color": "yellow", "value": 1},
            {"color": "orange", "value": 5},
            {"color": "red",    "value": 30},
        ],
    ))

    panels.append(stat_panel(
        pid=3, uid=ds_uid, x=10, y=0, w=4, h=4,
        title="Worst Single Outage",
        description="Maximum duration of any single recorded outage event.",
        sql=Q["max_duration"],
        unit="s", decimals=3,
        thresholds=[
            {"color": "green", "value": None},
            {"color": "yellow","value": 5},
            {"color": "red",   "value": 30},
        ],
    ))

    panels.append(stat_panel(
        pid=4, uid=ds_uid, x=14, y=0, w=5, h=4,
        title="Affected Nodes",
        description="Number of distinct nodes that experienced at least one outage.",
        sql=Q["affected_nodes"],
        unit="none",
        thresholds=[
            {"color": "green",  "value": None},
            {"color": "yellow", "value": 10},
            {"color": "red",    "value": 50},
        ],
    ))

    panels.append(gauge_panel(
        pid=5, uid=ds_uid, x=19, y=0, w=5, h=4,
        title="Estimated Uptime %",
        description=(
            "Rough uptime % = 1 − (total downtime ÷ observation window). "
            "Assumes a single shared observation window; per-node view is in Row 2."
        ),
        sql=Q["uptime_pct"],
        min_val=0, max_val=100, unit="percent",
    ))

    # ── Row 2: Trend + Node ranking (y=4, h=9) ────────────────────────────────

    panels.append(timeseries_panel(
        pid=6, uid=ds_uid, x=0, y=4, w=14, h=9,
        title="Outages & Avg Duration Over Time",
        description=(
            "Left Y: outage count per minute.  "
            "Right Y (dashed): average duration_sec per minute.  "
            "Use the time-range picker to zoom."
        ),
        sql=Q["outages_over_time"],
        unit="short", fill=0.15,
    ))

    panels.append(barchart_panel(
        pid=7, uid=ds_uid, x=14, y=4, w=10, h=9,
        title="Top 15 Nodes by Total Downtime",
        description=(
            "Cumulative downtime per node — your most unreliable nodes at a glance. "
            "Bar length = SUM(duration_sec). Number above bar = outage count."
        ),
        sql=Q["top_nodes_downtime"],
        orientation="horizontal", unit="s",
    ))

    # ── Row 3: Histogram + Hourly pattern (y=13, h=8) ─────────────────────────

    panels.append(barchart_panel(
        pid=8, uid=ds_uid, x=0, y=13, w=12, h=8,
        title="Outage Duration Distribution",
        description=(
            "How long do outages last? "
            "Short spikes vs prolonged incidents. "
            "Buckets: <0.5 s | 0.5–1 s | 1–2 s | 2–5 s | 5–10 s | 10–30 s | 30–60 s | >60 s."
        ),
        sql=Q["duration_histogram"],
        orientation="auto", unit="short",
    ))

    panels.append(barchart_panel(
        pid=9, uid=ds_uid, x=12, y=13, w=12, h=8,
        title="Outages by Hour of Day",
        description=(
            "Which hours see the most failures? "
            "Useful for tuning maintenance windows and staffing alert rotas."
        ),
        sql=Q["hourly_pattern"],
        orientation="auto", unit="short",
    ))

    # ── Row 4: Live event log (y=21, h=8) ─────────────────────────────────────

    panels.append(table_panel(
        pid=10, uid=ds_uid, x=0, y=21, w=24, h=8,
        title="Recent Outages — Latest 200 Events",
        description=(
            "Raw event log sorted by down_at DESC.  "
            "duration_sec is color-coded green→yellow→red.  "
            "Click any column header to re-sort; use the filter icon to filter inline."
        ),
        sql=Q["recent_outages"],
    ))

    # ── Template variable: node_id filter ─────────────────────────────────────
    node_var = {
        "name":        "node_id",
        "type":        "query",
        "label":       "Node",
        "description": "Filter all panels to specific nodes (All = no filter).",
        "datasource":  ds_ref(ds_uid),
        "query": {
            "rawSql": f"SELECT DISTINCT node_id FROM {TABLE} ORDER BY node_id",
            "format": 1,
        },
        "includeAll": True,
        "multi":      True,
        "allValue":   ".*",
        "current":    {"selected": True, "text": "All", "value": "$__all"},
        "options":    [],
        "refresh":    2,
        "sort":       1,
    }

    return {
        "dashboard": {
            "id":          None,
            "uid":         None,
            "title":       "Node Downtime Analytics",
            "description": (
                "Real-time visibility into node availability sourced from ClickHouse. "
                "CDC pipeline: MySQL → Debezium → Kafka → ClickHouse."
            ),
            "tags":        ["node-monitoring", "clickhouse", "sre", "availability", "cdc"],
            "timezone":    "browser",
            "schemaVersion": 38,
            "version":     1,
            "refresh":     "30s",
            "time":        {"from": "now-24h", "to": "now"},
            "timepicker":  {},
            "panels":      panels,
            "templating":  {"list": [node_var]},
            "annotations": {
                "list": [
                    {
                        "builtIn":   1,
                        "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                        "enable":    True,
                        "hide":      True,
                        "iconColor": "rgba(0,211,255,1)",
                        "name":      "Annotations & Alerts",
                        "type":      "dashboard",
                    }
                ]
            },
            "links": [],
        },
        "folderUid": folder_uid or "",
        "overwrite": True,
        "message":   "Provisioned by create_grafana_dashboard.py",
    }


# ─── Alert rules ─────────────────────────────────────────────────────────────────

def _expr_reduce(ref_in: str, ref_out: str) -> Dict:
    return {
        "refId":     ref_out,
        "queryType": "",
        "relativeTimeRange": {"from": 300, "to": 0},
        "datasourceUid": "__expr__",
        "model": {
            "type":       "reduce",
            "refId":      ref_out,
            "conditions": [],
            "reducer":    "last",
            "expression": ref_in,
        },
    }


def _expr_threshold(ref_in: str, ref_out: str, op: str, val: float) -> Dict:
    return {
        "refId":     ref_out,
        "queryType": "",
        "relativeTimeRange": {"from": 300, "to": 0},
        "datasourceUid": "__expr__",
        "model": {
            "type":  "threshold",
            "refId": ref_out,
            "conditions": [
                {
                    "type":      "query",
                    "evaluator": {"type": op, "params": [val]},
                    "operator":  {"type": "and"},
                    "reducer":   {"type": "last"},
                    "query":     {"params": [ref_in]},
                }
            ],
            "expression": ref_in,
        },
    }


def _ch_query_node(ds_uid: str, sql: str, ref: str = "A",
                   look_back: int = 300) -> Dict:
    return {
        "refId":     ref,
        "queryType": "",
        "relativeTimeRange": {"from": look_back, "to": 0},
        "datasourceUid": ds_uid,
        "model": {
            "rawSql":    sql,
            "format":    1,
            "refId":     ref,
            "queryType": "sql",
        },
    }


def create_alerts(ds_uid: str, folder_uid: Optional[str]) -> None:
    Q = build_queries(TABLE)
    f_uid = folder_uid or "general"

    # ── Optional email contact point ───────────────────────────────────────────
    if ALERT_EMAIL:
        try:
            api("POST", "/api/v1/provisioning/contact-points", json={
                "name":     "node-downtime-email",
                "type":     "email",
                "settings": {"addresses": ALERT_EMAIL},
            })
            print(f"  ✓ Email contact point → {ALERT_EMAIL}")
        except Exception as exc:
            print(f"  ⚠ Contact point skipped ({exc})")

    # ── Alert definitions ──────────────────────────────────────────────────────
    alert_defs = [
        {
            "title":     "Node Downtime | High Single Outage Duration",
            "condition": "C",
            "data": [
                _ch_query_node(ds_uid, Q["alert_max_duration_5m"], "A", look_back=300),
                _expr_reduce("A", "B"),
                _expr_threshold("B", "C", "gt", 10.0),
            ],
            "for": "1m",
            "annotations": {
                "summary":     "Node outage duration exceeded 10 s",
                "description": (
                    "MAX(duration_sec) > 10 s in the last 5 minutes. "
                    "Check the affected node immediately."
                ),
                "runbook_url": "",
            },
            "labels": {"severity": "critical", "team": "sre"},
            "folderUID":  f_uid,
            "ruleGroup":  "node-downtime",
            "noDataState":  "NoData",
            "execErrState": "Error",
        },
        {
            "title":     "Node Downtime | Outage Storm (>20 events / 5 min)",
            "condition": "C",
            "data": [
                _ch_query_node(ds_uid, Q["alert_count_5m"], "A", look_back=300),
                _expr_reduce("A", "B"),
                _expr_threshold("B", "C", "gt", 20),
            ],
            "for": "2m",
            "annotations": {
                "summary":     "More than 20 outage events in the last 5 minutes",
                "description": (
                    "Possible cascading failure or CDC replay. "
                    "Review Kafka lag and Debezium connector status."
                ),
            },
            "labels": {"severity": "warning", "team": "sre"},
            "folderUID":  f_uid,
            "ruleGroup":  "node-downtime",
            "noDataState":  "NoData",
            "execErrState": "Error",
        },
        {
            "title":     "Node Downtime | Node Flapping (>5 outages / hour)",
            "condition": "C",
            "data": [
                _ch_query_node(ds_uid, Q["alert_max_per_node_1h"], "A", look_back=3600),
                _expr_reduce("A", "B"),
                _expr_threshold("B", "C", "gt", 5),
            ],
            "for": "5m",
            "annotations": {
                "summary":     "Single node went down more than 5 times in the last hour",
                "description": (
                    "Repeated failure pattern detected. "
                    "Likely hardware instability, network flap, or misconfigured health-check."
                ),
            },
            "labels": {"severity": "warning", "team": "sre"},
            "folderUID":  f_uid,
            "ruleGroup":  "node-downtime",
            "noDataState":  "NoData",
            "execErrState": "Error",
        },
    ]

    for alert in alert_defs:
        try:
            api("POST", "/api/v1/provisioning/alert-rules", json=alert)
            print(f"  ✓ Alert: {alert['title']}")
        except Exception as exc:
            print(f"  ⚠ Alert skipped '{alert['title']}'  ({exc})")


# ─── Entrypoint ──────────────────────────────────────────────────────────────────

def run() -> None:
    banner = "Node Downtime Analytics — Grafana Dashboard Creator"
    print("=" * 62)
    print(f"  {banner}")
    print("=" * 62)
    print(f"  Grafana : {GRAFANA_URL}")
    print(f"  Table   : {TABLE}")
    print(f"  Folder  : {FOLDER_NAME}")
    print()

    # 1. Datasource
    print("[1/4] Ensuring ClickHouse datasource …")
    ds_uid = ensure_datasource()

    # 2. Folder
    print("\n[2/4] Ensuring Grafana folder …")
    folder_uid = ensure_folder()

    # 3. Dashboard
    print("\n[3/4] Creating dashboard …")
    payload = build_dashboard(ds_uid, folder_uid)
    result  = api("POST", "/api/dashboards/db", json=payload)
    dash_url = result.get("url", "/")
    dash_uid = result.get("uid", "?")
    print(f"  ✓ Dashboard uid={dash_uid}")
    print(f"  ✓ URL: {GRAFANA_URL}{dash_url}")

    # 4. Alerts
    print("\n[4/4] Creating alert rules …")
    create_alerts(ds_uid, folder_uid)

    print()
    print("=" * 62)
    print("  Done! Open Grafana:")
    print(f"  {GRAFANA_URL}{dash_url}")
    print()
    print("  Panels created:")
    print("   1  Total Outages              (stat)")
    print("   2  Avg Outage Duration        (stat)")
    print("   3  Worst Single Outage        (stat)")
    print("   4  Affected Nodes             (stat)")
    print("   5  Estimated Uptime %         (gauge)")
    print("   6  Outages & Duration / Time  (timeseries)")
    print("   7  Top 15 Nodes              (bar chart)")
    print("   8  Duration Distribution      (histogram)")
    print("   9  Outages by Hour of Day     (bar chart)")
    print("  10  Recent 200 Outages         (table)")
    print()
    print("  Alert rules:")
    print("   •  High single outage duration  (> 10 s, critical)")
    print("   •  Outage storm                 (> 20 / 5 min, warning)")
    print("   •  Node flapping                (> 5 / hour, warning)")
    print("=" * 62)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
    except requests.HTTPError:
        sys.exit(1)