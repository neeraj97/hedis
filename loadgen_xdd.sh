# !/bin/bash
#!/usr/bin/env bash

# ./loadgen_xdd.sh --host localhost --port 30001 --streams 128 --payload_size 2000 --count 10000 --parallelism 8

set -euo pipefail

# Default values
HOST="127.0.0.1"
PORT=6379
COUNT=10000
PARALLELISM=8
STREAMS=128
PAYLOAD_SIZE=3000

usage() {
  echo "Usage: $0 --host <host> --port <port> --count <messages per stream> --parallelism <parallel jobs> --streams <number of streams> --payload_size <size of each message in bytes>"
}

# if [[ $# -eq 0 ]]; then
#   usage
# fi

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --parallelism) PARALLELISM="$2"; shift 2 ;;
    --streams) STREAMS="$2"; shift 2 ;;
    --payload_size) PAYLOAD_SIZE="$2"; shift 2 ;;
    *) usage ;;
  esac
done

usage

echo "=== Valkey/Redis Stream Loader ==="
echo "Host: $HOST"
echo "Port: $PORT"
echo "Streams: $STREAMS"
echo "Messages per stream: $COUNT"
echo "Parallel jobs: $PARALLELISM"
echo "Payload size: $PAYLOAD_SIZE bytes"
echo "================================="

echo "Starting load with above configs"
echo "Writing $(($COUNT * $STREAMS)) messages of size $PAYLOAD_SIZE bytes each"
echo "Total Size in bytes $(($COUNT * $STREAMS * $PAYLOAD_SIZE)) -------"

sleep 1

# Worker function for one stream
worker() {
  local stream=$1
  local count=$2
  local host=$3
  local port=$4
  local payload_size=$5

  local RESP=$(seq 1 | awk -v s="$stream" '{ printf "XLEN %s\n", s}' | redis-cli -h "$host" -p "$port")

  echo $RESP
  if [[ "$RESP" =~ ^MOVED ]]; then
    echo "Redirect: $RESP"
    # Extract new target
    local NEW=$(echo "$RESP" | awk '{print $3}')
    local TARGET="$NEW"
    echo "Switching to $TARGET"
    # Change host and port command immediately
    host=${TARGET%:*}
    port=${TARGET#*:}
  fi

  seq "$count" | awk -v s="$stream" -v n="$payload_size" '{
      payload=""; 
      for (i=0; i<n; i++) {
        payload = payload "A";   # simple fixed payload
      }
      printf "XADD %s * payload %s%08d\n", s, payload, $1
  }' | redis-cli -h "$host" -p "$port" --pipe
}

export -f worker

# Run streams in parallel batches
seq 0 $((STREAMS-1)) | xargs -n1 -P"$PARALLELISM" bash -c 'worker "stream-{${0}}" '"$COUNT $HOST $PORT $PAYLOAD_SIZE" 

# Checker
# Verify XLEN of each stream
seq 0 $((STREAMS-1)) | xargs -n1  bash -c 'echo XLEN stream-{${0}} && redis-cli -h '"$HOST"' -p '"$PORT"' -c XLEN stream-{${0}}'

# cleanup
# redis-cli --cluster call --cluster-only-masters localhost:30001 FLUSHALL

# seq 0 $((STREAMS-1)) | xargs -n1 bash -c 'LENGTH=$(redis-cli -h '"$VALKEY_HOST"' -p '"$VALKEY_PORT"' -c XLEN '"$STREAM_PREFIX"'{${0}}) && echo "{\"command\": \"XLEN '"$STREAM_PREFIX"'{${0}}\",\"key\": \"'"${STREAM_PREFIX}"'{${0}}\",\"stream_index\":${0},\"length\": $LENGTH,\"action\": \"MONITOR_KV_STREAM_LENGTH\",\"service\": \"KV Stream Length checker\"}"'