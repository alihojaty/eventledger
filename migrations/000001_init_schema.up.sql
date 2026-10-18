CREATE TABLE IF NOT EXISTS deposits (
                                        event_id      VARCHAR(255) PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    currency     VARCHAR(10) NOT NULL,
    amount       NUMERIC(36, 18) NOT NULL CHECK (amount > 0),
    created_at   TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_id);


CREATE TABLE IF NOT EXISTS balances (
                                        user_id    INTEGER NOT NULL,
                                        currency   VARCHAR(10) NOT NULL,
    balance    NUMERIC(36, 18) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, currency)
    );