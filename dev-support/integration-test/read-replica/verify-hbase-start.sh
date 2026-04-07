#!/usr/bin/env bash
set -x
source "$(dirname ${0})/.env"

wait_for_hbase_ui() {
  until $(curl --output /dev/null --silent --head --fail "http://localhost:${1}"); do
    echo -n "."
    sleep 5
    done
  echo "HBase UI is up on ${2}"
}

check_server_status() {
  hbase_status=$(docker exec ${1} bash -c "echo \"status\" | hbase shell -n")

  if grep -q "1 active master" <<< $hbase_status; then
    echo "Active master is up for ${1}"
  else
    echo "Active master is not up for ${1}"
    exit 1
  fi

  if grep -q "1 servers" <<< $hbase_status; then
    echo "Region server is up for ${1}"
  else
    echo "Region server is not up for ${1}"
    exit 1
  fi

  if grep -q "0 dead" <<< $hbase_status; then
    echo "No dead servers for ${1}"
  else
    echo "There are dead servers for ${1}"
    exit 1
  fi
}

wait_for_hbase_ui ${ACTIVE_CLUSTER_PORT} "Active Cluster"
wait_for_hbase_ui ${REPLICA_CLUSTER_PORT} "Read Replica Cluster"
check_server_status ${HBASE_CONTAINER_NAME}
check_server_status ${HBASE_CONTAINER_NAME}-2
