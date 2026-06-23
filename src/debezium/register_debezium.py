import urllib.request
import json
import sys
import os

# Helper to load .env manually into os.environ if it exists
def load_dotenv():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # The .env file is located at the workspace root, which is 2 levels up from src/debezium/
    env_path = os.path.abspath(os.path.join(script_dir, "..", "..", ".env"))
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip('"').strip("'")
                        if key not in os.environ:
                            os.environ[key] = val

load_dotenv()

# Resolve path relative to script directory to ensure portability
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "debezium_connector.json")

try:
    with open(config_path, "r", encoding="utf-8") as f:
        connector_config = json.load(f)
except Exception as e:
    print(f"Failed to read config file {config_path}: {e}", file=sys.stderr)
    sys.exit(1)

# Dynamically inject database password from environment variable if set
postgres_password = os.getenv("POSTGRES_PASSWORD")
if postgres_password:
    connector_config["config"]["database.password"] = postgres_password

url = "http://localhost:8083/connectors"
req = urllib.request.Request(
    url,
    data=json.dumps(connector_config).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST"
)

try:
    with urllib.request.urlopen(req) as res:
        print("Debezium connector registered successfully!")
        print(res.read().decode("utf-8"))
except Exception as e:
    print(f"Failed to register connector: {e}", file=sys.stderr)
    # If already registered, it returns HTTP 409
    if hasattr(e, "code") and e.code == 409:
        print("Connector might already be registered. Trying to check...")
    sys.exit(1)
