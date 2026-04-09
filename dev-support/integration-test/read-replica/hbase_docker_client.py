#!/usr/bin/env python3
import ast
import re
import subprocess
import time
import requests

from dotenv import load_dotenv
from environment_loader import get_env


class HBaseDockerClient:
    def __init__(self, container_name, hbase_ui_port=16010, cluster_name="HBase Cluster",
                 max_retries=12, sleep_time=5):
        self.container_name = container_name
        self.hbase_ui_port = hbase_ui_port
        self.cluster_name = cluster_name
        self.max_retries = max_retries
        self.sleep_time = sleep_time

    def wait_for_hbase_ui(self):
        """Checks for a 200 OK on the HBase Master UI."""
        url = f"http://localhost:{self.hbase_ui_port}"
        print(f"--- Waiting for HBase UI: {self.cluster_name} on {url} ---")
        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    print(f"SUCCESS: {self.cluster_name} UI is up.")
                    return True
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                print(".", end="", flush=True)
            time.sleep(self.sleep_time)

        raise RuntimeError(f"\nTIMEOUT: {self.cluster_name} UI failed to respond after "
                           f"{self.max_retries} attempts. "
                           f"Last raised exception was: {last_exception}")

    def check_server_status(self):
        """Runs 'status' inside the HBase shell and validates the output."""
        print(f"--- Validating Cluster Status: {self.cluster_name} ({self.container_name}) ---")
        for attempt in range(1, self.max_retries + 1):
            try:
                output = self.get_hbase_status()

                # The cluster's status should have 1 active master, 1 region server,
                # and no dead servers
                validations = {
                    "Active Master": "1 active master" in output,
                    "Region Server": "1 servers" in output,
                    "No Dead Servers": "0 dead" in output
                }

                if all(validations.values()):
                    for check, status in validations.items():
                        print(f"    [PASS] {check}")
                    print(f"SUCCESS: {self.cluster_name} is fully operational.")
                    return True
                else:
                    print(f"\nWARN: {self.cluster_name} responding but not all components are ready...")
                    print(f"HBase 'status' command output: {output}")

            except subprocess.CalledProcessError as e:
                print(".", end="", flush=True)

            time.sleep(self.sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self.cluster_name} shell check failed after {self.max_retries} attempts.")

    def __run_hbase_command(self, hbase_cmd):
        # In the Terminal, we usually put double quotes around everything after "-c", but doing that
        # with subprocess.run() results in a failure.
        cmd = ["docker", "exec", self.container_name, "bash", "-c",
               f'''hbase shell -n <<< "{hbase_cmd}"''']
        cmd_str = ' '.join(cmd)

        print(f"Running: {cmd_str}")
        process = subprocess.run(cmd, capture_output=True)
        stdout = process.stdout.decode('utf-8')
        if process.returncode != 0:
            raise RuntimeError(f"The following HBase shell command failed: {hbase_cmd}\n"
                               f"The docker command used to run this was: {cmd_str}\n"
                               f"The shell command's STDERR was:\n{process.stderr.decode('utf-8')}"
                               f"The shell command's STDOUT was:\n{stdout}")
        return stdout

    def create_table(self, table_name, column_family):
        create_cmd = f"create '{table_name}', '{column_family}'"
        output = self.__run_hbase_command(create_cmd)

        if f"Created table {table_name}" not in output:
            return False
        return True

    def list_tables(self):
        """Gets the list of HBase tables and returns it as a Python list"""
        pattern = r'\[(.*?)\]'
        output = self.__run_hbase_command("list")
        match = re.search(pattern, output)
        return ast.literal_eval(match.group(0))

    def verify_table_exists(self, table_name):
        return table_name in self.list_tables()

    def get_hbase_status(self):
        return self.__run_hbase_command("status")


if __name__ == "__main__":
    load_dotenv()
    active_cluster = HBaseDockerClient(get_env("HBASE_CONTAINER_NAME"))
    table_name = "t1"
    column_family = "cf"
    active_cluster.create_table(table_name, column_family)
    tables = active_cluster.list_tables()
    print(f"{table_name} exists? {active_cluster.verify_table_exists(table_name)}")
#     output = """hbase:001:0> list
# TABLE
# t1
# t2
# t3
# 3 row(s)
# Took 0.8066 seconds
# => ["t1", "t2", "t3"]
# hbase:002:0> """
#     pattern = r'\[(.*?)\]'
#     match = re.search(pattern, output)
#     tables = ast.literal_eval(match.group(0))
#     # matches = matches.split(",")
    print(tables)
