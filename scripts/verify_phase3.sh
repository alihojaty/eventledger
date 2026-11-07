#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?Usage: $0 <RUN_ID> [EXPECTED_COUNT]}"
EXPECTED_COUNT="${2:-5000000}"
STACK_NAME="${STACK_NAME:-deposit}"
DASHBOARD_URL="${DASHBOARD_URL:-http://localhost:8080/api/stats}"

echo "==> [1/5] Waiting for depositor queue to drain and PostgreSQL deposits count to reach ${EXPECTED_COUNT}..."
PG_CONTAINER="$(docker ps -qf "name=${STACK_NAME}_postgres" | head -n 1)"
if [[ -z "$PG_CONTAINER" ]]; then
    echo "ERROR: PostgreSQL container not found"
    exit 1
fi

while true; do
    CURRENT_COUNT="$(docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -tAc \
        "SELECT COUNT(*) FROM deposits WHERE event_id LIKE 'run-${RUN_ID}-%';" | tr -d '[:space:]')"
    echo "    Current deposits in DB for run-${RUN_ID}: ${CURRENT_COUNT} / ${EXPECTED_COUNT}"
    if [[ "$CURRENT_COUNT" == "$EXPECTED_COUNT" ]]; then
        break
    fi
    sleep 3
done

echo "==> [2/5] Verified exact row count in PostgreSQL: ${CURRENT_COUNT} == ${EXPECTED_COUNT}"

echo "==> [3/5] Querying per-user and per-currency SUM(amount) from PostgreSQL for run-${RUN_ID}..."
docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -c "
SELECT
    user_id,
    COALESCE(SUM(CASE WHEN currency = 'USDT' THEN ROUND(amount, 2) END), 0) AS usdt_sum,
    COALESCE(SUM(CASE WHEN currency = 'BTC'  THEN ROUND(amount, 8) END), 0) AS btc_sum,
    COALESCE(SUM(CASE WHEN currency = 'IRT'  THEN ROUND(amount, 0) END), 0) AS irt_sum
FROM deposits
WHERE event_id LIKE 'run-${RUN_ID}-%'
GROUP BY user_id
ORDER BY user_id ASC;
"

echo "==> [4/5] Checking Dashboard Automated Ledger Reconciliation (${DASHBOARD_URL})..."
STATS_JSON="$(curl -sf "$DASHBOARD_URL")"
IS_BALANCED="$(echo "$STATS_JSON" | grep -o '"is_balanced":[a-z]*' | cut -d: -f2)"
DISC_COUNT="$(echo "$STATS_JSON" | grep -o '"discrepancy_count":[0-9]*' | cut -d: -f2)"

echo "    Dashboard Reconciliation -> is_balanced=${IS_BALANCED}, discrepancy_count=${DISC_COUNT}"
if [[ "$IS_BALANCED" != "true" || "$DISC_COUNT" != "0" ]]; then
    echo "ERROR: Ledger reconciliation discrepancy detected!"
    exit 1
fi

echo "==> [5/5] Checking NATS JetStream DEPOSITS stream message count..."
NATS_CONTAINER="$(docker ps -qf "name=${STACK_NAME}_nats" | head -n 1)"
if [[ -n "$NATS_CONTAINER" ]]; then
    docker exec -i "$NATS_CONTAINER" wget -qO- "http://127.0.0.1:8222/jsz?streams=true" | grep -E '"name"|"messages"|"last_seq"' || true
fi

echo "==> ALL PHASE 3 VERIFICATION CHECKS PASSED SUCCESSFULLY!"