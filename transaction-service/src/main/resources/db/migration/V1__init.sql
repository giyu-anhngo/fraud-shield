CREATE TABLE accounts (
    id           UUID PRIMARY KEY,
    customer_id  VARCHAR(64)  NOT NULL,
    home_country VARCHAR(2)   NOT NULL,
    status       VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    avg_amount   NUMERIC(15,2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id                UUID PRIMARY KEY,
    account_id        UUID         NOT NULL REFERENCES accounts(id),
    amount            NUMERIC(15,2) NOT NULL,
    currency          VARCHAR(3)   NOT NULL,
    country           VARCHAR(2)   NOT NULL,
    merchant_id       VARCHAR(64),
    merchant_category VARCHAR(32),
    channel           VARCHAR(16)  NOT NULL,
    device_id         VARCHAR(64),
    ip                VARCHAR(45),
    occurred_at       TIMESTAMPTZ  NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_account_id ON transactions(account_id);

-- Seed one account so POST /transactions works out of the box.
INSERT INTO accounts (id, customer_id, home_country, status, avg_amount)
VALUES ('11111111-1111-1111-1111-111111111111', 'cust_0001', 'CH', 'ACTIVE', 200.00);