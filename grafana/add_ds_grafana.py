import requests
import json

GRAFANA_URL = "http://localhost:3000"
AUTH = ("admin", "admin")

def setup_datasources():
    datasources = [
        {
            "name": "Prometheus",
            "type": "prometheus",
            "url": "http://prometheus:9090", # Internal Docker network name
            "access": "proxy",
            "basicAuth": False,
            "isDefault": True,
            "uid": "prometheus-ds" # This MUST match the dashboard JSON
        },
        {
            "name": "ClickHouse",
            "type": "grafana-clickhouse-datasource",
            "url": "http://clickhouse:8123", # Internal Docker network name
            "access": "proxy",
            "jsonData": {
                "server": "clickhouse",
                "port": 9000,
                "username": "default",
                "protocol": "native"
            },
            "secureJsonData": {
                "password": "" # Add your password here if set
            },
            "uid": "clickhouse-ds" # This MUST match the dashboard JSON
        }
    ]

    for ds in datasources:
        resp = requests.post(f"{GRAFANA_URL}/api/datasources", json=ds, auth=AUTH)
        if resp.status_code == 200:
            print(f"✅ Data source '{ds['name']}' created.")
        elif resp.status_code == 409:
            print(f"ℹ️ Data source '{ds['name']}' already exists.")
        else:
            print(f"❌ Error creating {ds['name']}: {resp.text}")

def create_dashboard():
    # ... (Keep the dashboard_json from the previous step)
    # Ensure the 'uid' fields in your dashboard panels match 'prometheus-ds' and 'clickhouse-ds'
    pass 

if __name__ == "__main__":
    setup_datasources()
    create_dashboard()