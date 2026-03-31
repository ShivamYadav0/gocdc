import requests
import json

# --- Configuration ---
GRAFANA_URL = "http://localhost:3000"
# Use your Service Account Token or admin:admin
AUTH = ("admin", "admin") 

def get_prometheus_uid():
    resp = requests.get(f"{GRAFANA_URL}/api/datasources", auth=AUTH)
    for ds in resp.json():
        if ds['type'] == 'prometheus':
            return ds['uid']
    return None

def create_promql_dashboard():
    prom_uid = get_prometheus_uid()
    if not prom_uid:
        print("❌ Error: Prometheus datasource not found. Please add it in Grafana first.")
        return

    dashboard_json = {
        "dashboard": {
            "id": None,
            "title": "Go Service & System Metrics (PromQL)",
            "tags": ["golang", "prometheus", "infrastructure"],
            "timezone": "browser",
            "refresh": "5s",
            "panels": [
                # --- ROW: APPLICATION THROUGHPUT ---
                {
                    "title": "Ingestion Rate (Messages/sec)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "rate(nms_processor_messages_total[1m])", "legendFormat": "Messages/s"}]
                },
                {
                    "title": "Error Rate",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "rate(nms_processor_errors_total[1m])", "legendFormat": "Errors/s"}],
                    "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "red"}}}
                },
                # --- ROW: GO RUNTIME HEALTH ---
                {
                    "title": "Active Goroutines",
                    "type": "stat",
                    "gridPos": {"h": 6, "w": 6, "x": 0, "y": 8},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "go_goroutines"}]
                },
                {
                    "title": "Heap Usage",
                    "type": "gauge",
                    "gridPos": {"h": 6, "w": 6, "x": 6, "y": 8},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "go_memstats_heap_alloc_bytes"}],
                    "fieldConfig": {"defaults": {"unit": "decbytes"}}
                },
                {
                    "title": "GC Duration (P99)",
                    "type": "timeseries",
                    "gridPos": {"h": 6, "w": 12, "x": 12, "y": 8},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "histogram_quantile(0.99, sum by (le) (rate(go_gc_duration_seconds_bucket[5m])))", "legendFormat": "99th Percentile"}]
                },
                # --- ROW: SYSTEM RESOURCE (NODE EXPORTER) ---
                {
                    "title": "System CPU Usage (%)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 14},
                    "datasource": {"uid": prom_uid},
                    "targets": [{"expr": "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[5m])) * 100)", "legendFormat": "CPU Load"}]
                }
            ]
        },
        "overwrite": True
    }

    response = requests.post(f"{GRAFANA_URL}/api/dashboards/db", 
                             headers={"Content-Type": "application/json"},
                             json=dashboard_json, 
                             auth=AUTH)
    
    if response.status_code == 200:
        print(f"✅ Dashboard Created Successfully! URL: {GRAFANA_URL}{response.json()['url']}")
    else:
        print(f"❌ Failed to create dashboard: {response.text}")

if __name__ == "__main__":
    create_promql_dashboard()