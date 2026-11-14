#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-deposit}"
PUB_SERVICE="${STACK_NAME}_publisher"
DEP_SERVICE="${STACK_NAME}_depositor"
CHAOS_LOG="${CHAOS_LOG:-chaos.log}"
DASHBOARD_STATS_URL="${DASHBOARD_STATS_URL:-http://localhost:8080/api/stats}"
GATEWAY_STATS_URL="${GATEWAY_STATS_URL:-http://localhost:8081/stats}"
MIN_PUB_REPLICAS="${MIN_PUB_REPLICAS:-1}"
MAX_PUB_REPLICAS="${MAX_PUB_REPLICAS:-6}"
MIN_SLEEP_SECONDS="${MIN_SLEEP_SECONDS:-45}"
MAX_SLEEP_SECONDS="${MAX_SLEEP_SECONDS:-90}"
LOCK_FILE="/tmp/reliable_deposit_chaos.lock"

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

count_healthy_publishers() {
    docker ps -qf "name=${PUB_SERVICE}" --filter "health=healthy" | wc -l | tr -d ' '
}

sample_cluster_state() {
    local ts pub_running pub_healthy stats_json gw_total gw_5xx
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    pub_running="$(docker ps -qf "name=${PUB_SERVICE}" | wc -l | tr -d ' ')"
    pub_healthy="$(count_healthy_publishers)"

    stats_json="$(curl -sf --max-time 2 "$DASHBOARD_STATS_URL" || (sleep 0.2 && curl -sf --max-time 2 "$DASHBOARD_STATS_URL") || curl -sf --max-time 2 "$GATEWAY_STATS_URL" || echo '{}')"
    gw_total="$(echo "$stats_json" | grep -o '"total_requests":[0-9]*' | head -n 1 | cut -d: -f2 || echo 0)"
    gw_5xx="$(echo "$stats_json" | grep -o '"upstream_error_5xx":[0-9]*' | head -n 1 | cut -d: -f2 || echo 0)"
    echo "[$ts] [TELEMETRY] publisher_replicas=${pub_running} publisher_healthy=${pub_healthy} gateway_total_reqs=${gw_total:-0} gateway_5xx=${gw_5xx:-0}" | tee -a "$CHAOS_LOG"
}

wait_for_healthy_publishers() {
    local min_healthy="$1"
    local max_wait="${2:-25}"
    local elapsed=0
    while (( elapsed < max_wait )); do
        local hc
        hc="$(count_healthy_publishers)"
        if (( hc >= min_healthy )); then
            return 0
        fi
        sleep 1
        elapsed=$(( elapsed + 1 ))
    done
    return 1
}

random_scale_publisher() {
    local range=$(( MAX_PUB_REPLICAS - MIN_PUB_REPLICAS + 1 ))
    local target=$(( (RANDOM % range) + MIN_PUB_REPLICAS ))
    log_event "ACTION=SCALE_PUBLISHER target_replicas=${target} service=${PUB_SERVICE}"
    docker service scale "${PUB_SERVICE}=${target}" >/dev/null
    local expected_healthy="$target"
    if (( expected_healthy > 2 )); then
        expected_healthy=2
    fi
    wait_for_healthy_publishers "$expected_healthy" 20 || true
    log_event "COMPLETED=SCALE_PUBLISHER target_replicas=${target}"
    sample_cluster_state
}

kill_one_publisher() {
    local label="$1"
    local healthy_count
    healthy_count="$(count_healthy_publishers)"
    if (( healthy_count < 2 )); then
        log_event "PRE_KILL_SCALE ensuring at least 2 HEALTHY publisher replicas before SIGKILL (${label})"
        docker service scale "${PUB_SERVICE}=3" >/dev/null
        if ! wait_for_healthy_publishers 2 25; then
            log_event "SKIPPED=DOCKER_KILL_SIGKILL (${label}) reason=fewer_than_2_healthy_replicas_ready"
            sample_cluster_state
            return
        fi
        sleep 2
    fi

    local cid
    cid="$(docker ps -qf "name=${PUB_SERVICE}" --filter "health=healthy" | tail -n 1)"
    if [[ -n "$cid" ]]; then
        log_event "ACTION=DOCKER_KILL_SIGKILL (${label}) container_id=${cid} service=${PUB_SERVICE}"
        docker kill -s KILL "$cid" >/dev/null
        log_event "COMPLETED=DOCKER_KILL_SIGKILL (${label}) container_id=${cid}"
    else
        log_event "SKIPPED=DOCKER_KILL_SIGKILL (${label}) reason=no_healthy_publisher_container"
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

sleep_adaptive_chaos_window() {
    local sleep_range=$(( MAX_SLEEP_SECONDS - MIN_SLEEP_SECONDS + 1 ))
    local delay=$(( (RANDOM % sleep_range) + MIN_SLEEP_SECONDS ))
    log_event "SLEEPING duration=${delay}s before next chaos step"
    local elapsed=0
    while (( elapsed < delay )); do
        if ! pgrep -f "cmd/loadgen|bin/loadgen" >/dev/null 2>&1 && (( elapsed >= 5 )); then
            break
        fi
        sleep 5
        elapsed=$(( elapsed + 5 ))
        sample_cluster_state
    done
}

log_event "CHAOS_RUNNER_STARTED stack=${STACK_NAME}"
sample_cluster_state

# Cycle 1: Random scale (1..6) + SIGKILL #1 on publisher
random_scale_publisher
sleep_adaptive_chaos_window
kill_one_publisher "1/2"

# Cycle 2: Random scale (1..6) + Force Rolling Update on publisher
random_scale_publisher
sleep_adaptive_chaos_window
force_update_publisher

# Cycle 3: Random scale (1..6) + Restart one depositor replica
random_scale_publisher
sleep_adaptive_chaos_window
restart_one_depositor

# Cycle 4: Random scale (1..6) + SIGKILL #2 on publisher
random_scale_publisher
sleep_adaptive_chaos_window
kill_one_publisher "2/2"

while pgrep -f "cmd/loadgen|bin/loadgen" >/dev/null 2>&1; do
    sleep_adaptive_chaos_window
    if ! pgrep -f "cmd/loadgen|bin/loadgen" >/dev/null 2>&1; then
        break
    fi
    random_scale_publisher
done

log_event "CHAOS_RUNNER_FINISHED"