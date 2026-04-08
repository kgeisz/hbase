#!/usr/bin/env python3
import os
import time
import subprocess
import sys
import requests
from dotenv import load_dotenv

# Constants for retries
SLEEP_TIME = 5
MAX_RETRIES = 12


def get_env(key, default=None):
  """Retrieve environment variables, ensuring they are loaded from the GitHub Actions runner."""
  val = os.environ.get(key, default)
  if val is None:
    print(f"Error: Environment variable {key} is not set.")
    sys.exit(1)
  return val


def wait_for_hbase_ui(port, cluster_name):
  """Checks for a 200 OK on the HBase Master UI."""
  url = f"http://localhost:{port}"
  print(f"--- Waiting for HBase UI: {cluster_name} on {url} ---")

  for attempt in range(1, MAX_RETRIES + 1):
    response = requests.get(url)
    if response.status_code == 200:
        print(f"SUCCESS: {cluster_name} UI is up.")
        return True
    else:
        print(".", end="", flush=True)
    time.sleep(SLEEP_TIME)

  raise RuntimeError(f"\nTIMEOUT: {cluster_name} UI failed to respond after {MAX_RETRIES} attempts.")


def check_server_status(container_name, cluster_name):
  """Runs 'status' inside the HBase shell and validates the output."""
  print(f"--- Validating Cluster Status: {cluster_name} ({container_name}) ---")

  # Runs 'status' in the HBase shell within the Docker container
  cmd = ["docker", "exec", container_name, "bash", "-c", "hbase shell -n <<< status"]

  for attempt in range(1, MAX_RETRIES + 1):
    try:
      process = subprocess.run(cmd, capture_output=True, check=True)
      output = process.stdout.decode('utf-8')

      # The cluster's status should have 1 active master, 1 region server, and no dead servers
      validations = {
        "Active Master": "1 active master" in output,
        "Region Server": "1 servers" in output,
        "No Dead Server": "0 dead" in output
      }

      if all(validations.values()):
        for check, status in validations.items():
          print(f"  [PASS] {check}")
        print(f"SUCCESS: {cluster_name} is fully operational.")
        return True
      else:
        print(f"\nWARN: {cluster_name} responding but not all components are ready...")
        print(f"HBase 'status' command output: {output}")

    except subprocess.CalledProcessError as e:
      print(".", end="", flush=True)

    time.sleep(SLEEP_TIME)

  raise RuntimeError(f"\nTIMEOUT: {cluster_name} shell check failed after {MAX_RETRIES} attempts.")


if __name__ == "__main__":
  # Load setting from .env file
  load_dotenv()
  active_port = get_env('ACTIVE_CLUSTER_PORT')
  replica_port = get_env('REPLICA_CLUSTER_PORT')
  container_base = get_env('HBASE_CONTAINER_NAME')

  # Check Active Cluster
  wait_for_hbase_ui(active_port, "Active Cluster")
  check_server_status(container_base, "Active Cluster")

  # Check Read-Replica Cluster
  wait_for_hbase_ui(replica_port, "Read-Replica Cluster")
  check_server_status(f"{container_base}-2", "Read-Replica Cluster")

  print("\n" + "=" * 40)
  print("ALL CLUSTERS VERIFIED AND READY")
  print("=" * 40)

