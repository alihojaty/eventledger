CREATE TABLE IF NOT EXISTS deposits (
                                        event_id     VARCHAR(255) PRIMARY KEY,
    user_id      INTEGER NOT NULL CHECK (user_id > 0),
    currency     VARCHAR(10) NOT NULL CHECK (currency IN ('USDT', 'BTC', 'IRT')),
    amount       NUMERIC(36, 18) NOT NULL CHECK (amount > 0),
    created_at   TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_id);
CREATE INDEX IF NOT EXISTS idx_deposits_processed_at_desc ON deposits(processed_at DESC);

CREATE TABLE IF NOT EXISTS balances (
                                        user_id    INTEGER NOT NULL CHECK (user_id > 0),
    currency   VARCHAR(10) NOT NULL CHECK (currency IN ('USDT', 'BTC', 'IRT')),
    balance    NUMERIC(36, 18) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, currency)
    );