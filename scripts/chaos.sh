#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-deposit}"
PUB_SERVICE="${STACK_NAME}_publisher"
DEP_SERVICE="${STACK_NAME}_depositor"
CHAOS_LOG="${CHAOS_LOG:-chaos.log}"
DASHBOARD_STATS_URL="${DASHBOARD_STATS_URL:-http://localhost:8080/api/stats}"
GATEWAY_STATS_URL="${GATEWAY_STATS_URL:-http://localhost:8081/stats}"
LOCK_FILE="/tmp/reliable_deposit_chaos.lock"

# Prevent concurrent executions of chaos.sh from colliding on Swarm services and chaos.log
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "Another instance of chaos.sh is already running (lock: $LOCK_FILE). Exiting."
    exit 0
fi

log_event() {
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] [CHAOS] $*" | tee -a "$CHAOS_LOG"
}

sample_cluster_state() {
    local ts pub_running stats_json gw_total gw_5xx
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    pub_running="$(docker ps -qf "name=${PUB_SERVICE}" | wc -l | tr -d ' ')"
    stats_json="$(curl -sf "$DASHBOARD_STATS_URL" || curl -sf "$GATEWAY_STATS_URL" || echo '{}')"
    gw_total="$(echo "$stats_json" | grep -o '"total_requests":[0-9]*' | head -n 1 | cut -d: -f2 || echo 0)"
    gw_5xx="$(echo "$stats_json" | grep -o '"upstream_error_5xx":[0-9]*' | head -n 1 | cut -d: -f2 || echo 0)"
    echo "[$ts] [TELEMETRY] publisher_replicas=${pub_running} gateway_total_reqs=${gw_total:-0} gateway_5xx=${gw_5xx:-0}" | tee -a "$CHAOS_LOG"
}

random_scale_publisher() {
    local target=$(( (RANDOM % 6) + 1 ))
    log_event "ACTION=SCALE_PUBLISHER target_replicas=${target} service=${PUB_SERVICE}"
    docker service scale "${PUB_SERVICE}=${target}" >/dev/null
    log_event "COMPLETED=SCALE_PUBLISHER target_replicas=${target}"
    sample_cluster_state
}

kill_one_publisher() {
    local label="$1"
    local running_count
    running_count="$(docker ps -qf "name=${PUB_SERVICE}" | wc -l | tr -d ' ')"
    if (( running_count < 2 )); then
        log_event "PRE_KILL_SCALE ensuring at least 2 publisher replicas before SIGKILL (${label})"
        docker service scale "${PUB_SERVICE}=2" >/dev/null
        for _ in {1..15}; do
            running_count="$(docker ps -qf "name=${PUB_SERVICE}" | wc -l | tr -d ' ')"
            if (( running_count >= 2 )); then
                break
            fi
            sleep 1
        done
        sleep 3
    fi
    local cid
    cid="$(docker ps -qf "name=${PUB_SERVICE}" | head -n 1)"
    if [[ -n "$cid" ]]; then
        log_event "ACTION=DOCKER_KILL_SIGKILL (${label}) container_id=${cid} service=${PUB_SERVICE}"
        docker kill -s KILL "$cid" >/dev/null
        log_event "COMPLETED=DOCKER_KILL_SIGKILL (${label}) container_id=${cid}"
    else
        log_event "SKIPPED=DOCKER_KILL_SIGKILL (${label}) reason=no_running_publisher_container"
    fi
    sample_cluster_state
}

force_update_publisher() {
    log_event "ACTION=FORCE_ROLLING_UPDATE service=${PUB_SERVICE}"
    docker service update --force "${PUB_SERVICE}" >/dev/null
    log_event "COMPLETED=FORCE_ROLLING_UPDATE service=${PUB_SERVICE}"
    sample_cluster_state
}

restart_one_depositor() {
    local cid
    cid="$(docker ps -qf "name=${DEP_SERVICE}" | head -n 1)"
    if [[ -n "$cid" ]]; then
        log_event "ACTION=RESTART_DEPOSITOR container_id=${cid} service=${DEP_SERVICE}"
        docker restart "$cid" >/dev/null
        log_event "COMPLETED=RESTART_DEPOSITOR container_id=${cid}"
    else
        log_event "SKIPPED=RESTART_DEPOSITOR reason=no_running_depositor_container"
    fi
    sample_cluster_state
}

sleep_random_45_to_90() {
    local delay=$(( (RANDOM % 46) + 45 ))
    log_event "SLEEPING duration=${delay}s before next chaos step"
    local elapsed=0
    while (( elapsed < delay )); do
        sleep 5
        elapsed=$(( elapsed + 5 ))
        sample_cluster_state
    done
}

log_event "CHAOS_RUNNER_STARTED stack=${STACK_NAME}"
sample_cluster_state

# Cycle 1: Random scale (1..6) + wait 45..90s + SIGKILL #1 on publisher
random_scale_publisher
sleep_random_45_to_90
kill_one_publisher "1/2"

# Cycle 2: Random scale (1..6) + wait 45..90s + Force Rolling Update on publisher
random_scale_publisher
sleep_random_45_to_90
force_update_publisher

# Cycle 3: Random scale (1..6) + wait 45..90s + Restart one depositor replica
random_scale_publisher
sleep_random_45_to_90
restart_one_depositor

# Cycle 4: Random scale (1..6) + wait 45..90s + SIGKILL #2 on publisher
random_scale_publisher
sleep_random_45_to_90
kill_one_publisher "2/2"

# Continue random scaling every 45..90s while loadgen is still active
while pgrep -f "cmd/loadgen|bin/loadgen" >/dev/null 2>&1; do
    sleep_random_45_to_90
    if ! pgrep -f "cmd/loadgen|bin/loadgen" >/dev/null 2>&1; then
        break
    fi
    random_scale_publisher
done

log_event "CHAOS_RUNNER_FINISHED"