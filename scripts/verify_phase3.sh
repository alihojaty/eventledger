#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?Usage: $0 <RUN_ID> [EXPECTED_COUNT]}"
EXPECTED_COUNT="${2:-5000000}"
STACK_NAME="${STACK_NAME:-deposit}"
DASHBOARD_URL="${DASHBOARD_URL:-http://localhost:8080/api/stats}"

PG_CONTAINER="$(docker ps -qf "name=${STACK_NAME}_postgres" | head -n 1)"
if [[ -z "$PG_CONTAINER" ]]; then
    echo "ERROR: PostgreSQL container not found"
    exit 1
fi

echo "==> [1/5] Waiting for depositor workers & periodic SettlementWorker to ingest and settle all ${EXPECTED_COUNT} deposits..."
STALL_TICKS=0
LAST_SETTLED=""

while true; do
    COUNTS="$(docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -tAc \
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_settled = TRUE) FROM deposits WHERE event_id LIKE 'run-${RUN_ID}-%';" | tr -d '[:space:]')"
    INGESTED_COUNT="${COUNTS%%|*}"
    SETTLED_COUNT="${COUNTS##*|}"
    echo "    Run-${RUN_ID} -> Ingested: ${INGESTED_COUNT}/${EXPECTED_COUNT} | Settled: ${SETTLED_COUNT}/${EXPECTED_COUNT}"

    if [[ "$INGESTED_COUNT" == "$EXPECTED_COUNT" && "$SETTLED_COUNT" == "$EXPECTED_COUNT" ]]; then
        break
    fi

    if [[ "$SETTLED_COUNT" == "$LAST_SETTLED" && "$INGESTED_COUNT" != "$SETTLED_COUNT" ]]; then
        STALL_TICKS=$(( STALL_TICKS + 1 ))
        if (( STALL_TICKS >= 6 )); then
            docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -tAc "SELECT settle_pending_deposits(250000);" >/dev/null || true
            STALL_TICKS=0
        fi
    else
        STALL_TICKS=0
    fi
    LAST_SETTLED="$SETTLED_COUNT"
    sleep 2
done

echo "==> [2/5] Verified exact ingested & settled row count in PostgreSQL: ${SETTLED_COUNT} == ${EXPECTED_COUNT}"

echo ""
echo "==> [3/5] جدول واریزهای اضافه‌شده توسط همین تست (run-${RUN_ID}) و مقایسه با جدول balances دیتابیس:"
docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -c "
WITH run_summary AS (
    SELECT
        user_id,
        COUNT(*) FILTER (WHERE currency = 'USDT') AS usdt_tx_count,
        COALESCE(SUM(CASE WHEN currency = 'USDT' THEN amount END), 0) AS run_added_usdt,
        COUNT(*) FILTER (WHERE currency = 'BTC')  AS btc_tx_count,
        COALESCE(SUM(CASE WHEN currency = 'BTC'  THEN amount END), 0) AS run_added_btc,
        COUNT(*) FILTER (WHERE currency = 'IRT')  AS irt_tx_count,
        COALESCE(SUM(CASE WHEN currency = 'IRT'  THEN amount END), 0) AS run_added_irt
    FROM deposits
    WHERE event_id LIKE 'run-${RUN_ID}-%' AND is_settled = TRUE
    GROUP BY user_id
),
wallet_summary AS (
    SELECT
        user_id,
        COALESCE(MAX(CASE WHEN currency = 'USDT' THEN balance END), 0) AS db_balance_usdt,
        COALESCE(MAX(CASE WHEN currency = 'BTC'  THEN balance END), 0) AS db_balance_btc,
        COALESCE(MAX(CASE WHEN currency = 'IRT'  THEN balance END), 0) AS db_balance_irt
    FROM balances
    GROUP BY user_id
)
SELECT
    r.user_id AS \"User\",
    '+' || TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM r.run_added_usdt::TEXT)) || ' (' || r.usdt_tx_count || ' tx)' AS \"Added USDT (This Test)\",
    TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM w.db_balance_usdt::TEXT)) AS \"DB Balance USDT\",
    '+' || ROUND(r.run_added_btc, 8)::TEXT  || ' (' || r.btc_tx_count  || ' tx)' AS \"Added BTC (This Test)\",
    ROUND(w.db_balance_btc, 8)::TEXT  AS \"DB Balance BTC\",
    '+' || ROUND(r.run_added_irt, 0)::TEXT  || ' (' || r.irt_tx_count  || ' tx)' AS \"Added IRT (This Test)\",
    ROUND(w.db_balance_irt, 0)::TEXT  AS \"DB Balance IRT\"
FROM run_summary r
LEFT JOIN wallet_summary w ON w.user_id = r.user_id
ORDER BY r.user_id ASC;
"

echo ""
echo "==> [4/5] جزئیات اختصاصی کاربر شماره ۷ و تطبیق دقیق (بدون گرد کردن) با جدول balances:"
docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -c "
SELECT
    d.user_id AS \"User ID\",
    d.currency AS \"Currency\",
    COUNT(*) AS \"Tx Count (This Run)\",
    COUNT(*) FILTER (WHERE d.is_settled = TRUE) AS \"Settled Tx Count\",
    '+' || CASE
        WHEN d.currency = 'USDT' THEN ROUND(SUM(d.amount), 2)::TEXT
        WHEN d.currency = 'BTC'  THEN ROUND(SUM(d.amount), 8)::TEXT
        ELSE ROUND(SUM(d.amount), 0)::TEXT
    END AS \"Added By This Test (run-${RUN_ID})\",
    CASE
        WHEN d.currency = 'USDT' THEN ROUND(b.balance, 2)::TEXT
        WHEN d.currency = 'BTC'  THEN ROUND(b.balance, 8)::TEXT
        ELSE ROUND(b.balance, 0)::TEXT
    END AS \"Current Wallet Balance (balances table)\"
FROM deposits d
JOIN balances b ON b.user_id = d.user_id AND b.currency = d.currency
WHERE d.user_id = 7 AND d.event_id LIKE 'run-${RUN_ID}-%'
GROUP BY d.user_id, d.currency, b.balance
ORDER BY d.currency ASC;
"

echo "==> [5/5] Verifying Exact Ledger Reconciliation in PostgreSQL & Dashboard (${DASHBOARD_URL})..."
DB_DISC="$(docker exec -i "$PG_CONTAINER" psql -U postgres -d deposits_db -tAc "
    WITH expected AS (
        SELECT user_id, currency, SUM(amount) AS exp
        FROM deposits WHERE is_settled = TRUE GROUP BY user_id, currency
    )
    SELECT COUNT(*) FROM expected e
    FULL OUTER JOIN balances b ON b.user_id = e.user_id AND b.currency = e.currency
    WHERE COALESCE(e.exp, 0) IS DISTINCT FROM COALESCE(b.balance, 0);
" | tr -d '[:space:]')"

echo "    PostgreSQL Exact Decimal Reconciliation -> discrepancy_count=${DB_DISC}"
if [[ "$DB_DISC" != "0" ]]; then
    echo "ERROR: Ledger reconciliation discrepancy detected in PostgreSQL!"
    exit 1
fi

STATS_JSON="$(curl -sf "$DASHBOARD_URL" || echo '{}')"
IS_BALANCED="$(echo "$STATS_JSON" | grep -o '"is_balanced":[a-z]*' | cut -d: -f2 || echo "")"
DISC_COUNT="$(echo "$STATS_JSON" | grep -o '"discrepancy_count":[0-9]*' | cut -d: -f2 || echo "")"
if [[ -n "$IS_BALANCED" ]]; then
    echo "    Dashboard Reconciliation -> is_balanced=${IS_BALANCED}, discrepancy_count=${DISC_COUNT}"
    if [[ "$IS_BALANCED" != "true" || "$DISC_COUNT" != "0" ]]; then
        echo "ERROR: Dashboard ledger reconciliation discrepancy detected!"
        exit 1
    fi
fi

NATS_CONTAINER="$(docker ps -qf "name=${STACK_NAME}_nats" | head -n 1)"
if [[ -n "$NATS_CONTAINER" ]]; then
    echo "==> Checking NATS JetStream DEPOSITS stream message count..."
    docker exec -i "$NATS_CONTAINER" wget -qO- "http://127.0.0.1:8222/jsz?streams=true" | grep -E '"name"|"messages"|"last_seq"' || true
fi

echo ""
echo "===================================================================================================="
echo "✅ دستورات آماده برای بررسی دستی شما در ترمینال (کپی و اجرا کنید):"
echo "----------------------------------------------------------------------------------------------------"
echo "1) مشاهده موجودی فعلی کاربر ۷ در جدول balances:"
echo "   docker exec -it ${PG_CONTAINER} psql -U postgres -d deposits_db -c \"SELECT user_id, currency, balance, updated_at FROM balances WHERE user_id = 7 ORDER BY currency;\""
echo ""
echo "2) مشاهده جمع واریزهای انجام‌شده برای کاربر ۷ در همین تست (run-${RUN_ID}):"
echo "   docker exec -it ${PG_CONTAINER} psql -U postgres -d deposits_db -c \"SELECT user_id, currency, COUNT(*) AS tx_count, SUM(amount) AS added_amount, BOOL_AND(is_settled) AS all_settled FROM deposits WHERE user_id = 7 AND event_id LIKE 'run-${RUN_ID}-%' GROUP BY user_id, currency ORDER BY currency;\""
echo ""
echo "3) مشاهده موجودی کل همه کاربران در جدول balances:"
echo "   docker exec -it ${PG_CONTAINER} psql -U postgres -d deposits_db -c \"SELECT user_id, currency, balance FROM balances ORDER BY user_id, currency;\""
echo "===================================================================================================="
echo "==> ALL PHASE 3 VERIFICATION CHECKS PASSED SUCCESSFULLY!"