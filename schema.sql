-- =============================================================================
-- HF Group CDC Pipeline — Complete Schema DDL
-- Run this script after extensions are created (init-extensions.sql)
-- Order matters: reference tables → domain tables → partitioned tables → partman
-- =============================================================================

-- ── Extensions (idempotent — safe to re-run) ──────────────────────────────────
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_cron;
ALTER DATABASE hfgroup_cdc SET search_path TO public, partman;

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS ref_currencies (
    currency_id     INTEGER         PRIMARY KEY,
    iso_code        CHAR(3)         NOT NULL,
    description     VARCHAR(50)     NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_channels (
    channel_id      INTEGER         PRIMARY KEY,
    channel_name    VARCHAR(50)     NOT NULL,
    channel_type    VARCHAR(20)     NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_transaction_codes (
    trx_code        INTEGER         PRIMARY KEY,
    description     VARCHAR(100)    NOT NULL,
    trn_type        SMALLINT        NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_products (
    product_id      INTEGER         PRIMARY KEY,
    product_name    VARCHAR(100)    NOT NULL,
    product_type    VARCHAR(50)     NOT NULL
);

-- Seed reference data
INSERT INTO ref_channels (channel_id, channel_name, channel_type) VALUES
    (9907, 'Mobile Banking',    'MOBILE'),
    (9908, 'Branch',            'BRANCH'),
    (9909, 'ATM',               'ATM'),
    (9910, 'Agency Banking',    'AGENCY'),
    (9911, 'Internet Banking',  'INTERNET')
ON CONFLICT (channel_id) DO NOTHING;

INSERT INTO ref_currencies (currency_id, iso_code, description) VALUES
    (22,  'KES', 'Kenyan Shilling'),
    (840, 'USD', 'US Dollar'),
    (978, 'EUR', 'Euro')
ON CONFLICT (currency_id) DO NOTHING;

-- =============================================================================
-- DOMAIN TABLES — CUSTOMERS
-- =============================================================================

CREATE TABLE IF NOT EXISTS customers (
    id                      BIGSERIAL       PRIMARY KEY,
    customer_id             VARCHAR(20)     NOT NULL UNIQUE,
    customer_type           VARCHAR(5),
    customer_status         VARCHAR(5),
    entry_status            VARCHAR(5),
    title                   VARCHAR(10),
    first_name              VARCHAR(50),
    middle_name             VARCHAR(50),
    last_name               VARCHAR(100),
    sex                     CHAR(1),
    date_of_birth           DATE,
    birth_place             VARCHAR(100),
    email_primary           VARCHAR(100),
    email_secondary         VARCHAR(100),
    mobile_primary          VARCHAR(30),
    phone_secondary         VARCHAR(30),
    is_vip                  BOOLEAN         NOT NULL DEFAULT FALSE,
    is_blacklisted          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_non_resident         BOOLEAN         NOT NULL DEFAULT FALSE,
    is_pensioner            BOOLEAN         NOT NULL DEFAULT FALSE,
    is_business             BOOLEAN         NOT NULL DEFAULT FALSE,
    aml_status              VARCHAR(5),
    employer                VARCHAR(100),
    salary_amount           NUMERIC(18,2),
    branch_id               INTEGER,
    relationship_officer    VARCHAR(20),
    account_open_date       DATE,
    doc_expire_date         DATE,
    last_updated_at         DATE,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cust_last_name    ON customers (last_name);
CREATE INDEX IF NOT EXISTS idx_cust_mobile       ON customers (mobile_primary);
CREATE INDEX IF NOT EXISTS idx_cust_email        ON customers (email_primary);
CREATE INDEX IF NOT EXISTS idx_cust_status       ON customers (customer_status);
CREATE INDEX IF NOT EXISTS idx_cust_branch       ON customers (branch_id);
CREATE INDEX IF NOT EXISTS idx_cust_blacklisted  ON customers (is_blacklisted) WHERE is_blacklisted = TRUE;
CREATE INDEX IF NOT EXISTS idx_cust_vip          ON customers (is_vip) WHERE is_vip = TRUE;
CREATE INDEX IF NOT EXISTS idx_cust_fullname_trgm ON customers USING gin ((first_name || ' ' || last_name) gin_trgm_ops);

-- =============================================================================
-- DOMAIN TABLES — ACCOUNTS
-- =============================================================================

CREATE TABLE IF NOT EXISTS accounts (
    id                  BIGSERIAL       PRIMARY KEY,
    account_number      VARCHAR(20)     NOT NULL UNIQUE,
    account_serial_num  VARCHAR(20),
    customer_id         VARCHAR(20)     NOT NULL,
    product_id          INTEGER,
    account_status      VARCHAR(5),
    account_code        VARCHAR(5),
    branch_id           INTEGER,
    monitoring_unit     INTEGER,
    profit_system       VARCHAR(5),
    movement_currency   INTEGER,
    limit_currency      INTEGER,
    iban                VARCHAR(50),
    atm_card_flag       VARCHAR(5),
    is_secondary_acc    BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_acc_customer_id ON accounts (customer_id);
CREATE INDEX IF NOT EXISTS idx_acc_status      ON accounts (account_status);
CREATE INDEX IF NOT EXISTS idx_acc_branch_id   ON accounts (branch_id);

-- =============================================================================
-- DOMAIN TABLES — CHANGE TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_changes (
    change_id       BIGSERIAL       PRIMARY KEY,
    customer_id     VARCHAR(20)     NOT NULL,
    changed_fields  TEXT,
    old_values      TEXT,
    new_values      TEXT,
    changed_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cust_changes_customer   ON customer_changes (customer_id);
CREATE INDEX IF NOT EXISTS idx_cust_changes_changed_at ON customer_changes (changed_at);

CREATE TABLE IF NOT EXISTS account_changes (
    change_id       BIGSERIAL       PRIMARY KEY,
    account_number  VARCHAR(20)     NOT NULL,
    customer_id     VARCHAR(20),
    changed_fields  TEXT,
    old_values      TEXT,
    new_values      TEXT,
    changed_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_acc_changes_account    ON account_changes (account_number);
CREATE INDEX IF NOT EXISTS idx_acc_changes_customer   ON account_changes (customer_id);
CREATE INDEX IF NOT EXISTS idx_acc_changes_changed_at ON account_changes (changed_at);

-- =============================================================================
-- DOMAIN TABLES — DELETION TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_deletions (
    deletion_id         BIGSERIAL       PRIMARY KEY,
    customer_id         VARCHAR(20)     NOT NULL,
    first_name          VARCHAR(50),
    middle_name         VARCHAR(50),
    last_name           VARCHAR(100),
    email               VARCHAR(100),
    email_secondary     VARCHAR(100),
    mobile_number       VARCHAR(30),
    phone_number        VARCHAR(30),
    customer_type       VARCHAR(5),
    customer_status     VARCHAR(5),
    branch_id           VARCHAR(10),
    aml_status          VARCHAR(5),
    is_blacklisted      VARCHAR(5),
    date_of_birth       DATE,
    account_open_date   DATE,
    deleted_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cust_del_customer   ON customer_deletions (customer_id);
CREATE INDEX IF NOT EXISTS idx_cust_del_deleted_at ON customer_deletions (deleted_at);

CREATE TABLE IF NOT EXISTS account_deletions (
    deletion_id         BIGSERIAL       PRIMARY KEY,
    account_number      VARCHAR(20)     NOT NULL,
    account_serial_num  VARCHAR(20),
    customer_id         VARCHAR(20),
    product_id          VARCHAR(20),
    account_status      VARCHAR(5),
    account_code        VARCHAR(5),
    branch_id           VARCHAR(10),
    monitoring_unit     VARCHAR(10),
    movement_currency   VARCHAR(10),
    limit_currency      VARCHAR(10),
    iban                VARCHAR(50),
    atm_card_flag       VARCHAR(5),
    is_secondary_acc    VARCHAR(5),
    deleted_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_acc_del_account    ON account_deletions (account_number);
CREATE INDEX IF NOT EXISTS idx_acc_del_customer   ON account_deletions (customer_id);
CREATE INDEX IF NOT EXISTS idx_acc_del_deleted_at ON account_deletions (deleted_at);

-- =============================================================================
-- TIER 2 — SUMMARY TABLES (real-time upsert counters)
-- =============================================================================

CREATE TABLE IF NOT EXISTS summary_by_channel (
    channel_id          INTEGER         NOT NULL,
    summary_date        DATE            NOT NULL,
    transaction_count   BIGINT          NOT NULL DEFAULT 0,
    total_volume        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_debits        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_credits       NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_commission    NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_expenses      NUMERIC(18,2)   NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_sum_channel_date ON summary_by_channel (summary_date);

CREATE TABLE IF NOT EXISTS summary_by_branch (
    branch_id           INTEGER         NOT NULL,
    summary_date        DATE            NOT NULL,
    transaction_count   BIGINT          NOT NULL DEFAULT 0,
    total_volume        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_debits        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_credits       NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_commission    NUMERIC(18,2)   NOT NULL DEFAULT 0,
    unique_customers    BIGINT          NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (branch_id, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_sum_branch_date ON summary_by_branch (summary_date);

CREATE TABLE IF NOT EXISTS summary_by_deposit_type (
    deposit_type        SMALLINT        NOT NULL,
    summary_date        DATE            NOT NULL,
    transaction_count   BIGINT          NOT NULL DEFAULT 0,
    total_volume        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_debits        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_credits       NUMERIC(18,2)   NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deposit_type, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_sum_deptype_date ON summary_by_deposit_type (summary_date);

CREATE TABLE IF NOT EXISTS summary_by_channel_branch (
    channel_id          INTEGER         NOT NULL,
    branch_id           INTEGER         NOT NULL,
    summary_date        DATE            NOT NULL,
    transaction_count   BIGINT          NOT NULL DEFAULT 0,
    total_volume        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_debits        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_credits       NUMERIC(18,2)   NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, branch_id, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_sum_chbr_date ON summary_by_channel_branch (summary_date);

-- =============================================================================
-- TIER 1 — HOT TIER (6-hour partitions, 72-hour rolling window)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_transactions (
    id                      BIGSERIAL,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    account_number          VARCHAR(20),
    customer_id             VARCHAR(20),
    trx_date                DATE,
    production_date         DATE,
    value_date              DATE,
    trx_code                INTEGER,
    trx_unit                INTEGER,
    trx_user                VARCHAR(20),
    trx_user_sn             INTEGER,
    channel_id              INTEGER,
    trn_type                SMALLINT,
    deposit_type            SMALLINT,
    transaction_status      SMALLINT,
    confirmed               SMALLINT,
    accounted_flag          SMALLINT        NOT NULL DEFAULT 0,
    currency                INTEGER,
    product_id              INTEGER,
    amount                  NUMERIC(18,2),
    debit_amount            NUMERIC(18,2)   DEFAULT 0,
    credit_amount           NUMERIC(18,2)   DEFAULT 0,
    capital_debit           NUMERIC(18,2)   DEFAULT 0,
    capital_credit          NUMERIC(18,2)   DEFAULT 0,
    commission              NUMERIC(18,2)   DEFAULT 0,
    expenses                NUMERIC(18,2)   DEFAULT 0,
    penalty                 NUMERIC(18,2)   DEFAULT 0,
    balance_accounting      NUMERIC(18,2),
    balance_available       NUMERIC(18,2),
    balance_uncleared       NUMERIC(18,2)   DEFAULT 0,
    balance_blocked         NUMERIC(18,2)   DEFAULT 0,
    prev_balance_accounting NUMERIC(18,2),
    prev_balance_available  NUMERIC(18,2),
    gl_account              VARCHAR(25),
    gl_account_dr           VARCHAR(25),
    comments                VARCHAR(500),
    reference_number        VARCHAR(50),
    authorizer_1            VARCHAR(20),
    authorizer_2            VARCHAR(20),
    is_promoted             BOOLEAN         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, ingested_at)
) PARTITION BY RANGE (ingested_at);

CREATE INDEX IF NOT EXISTS idx_hot_account_time ON raw_transactions (account_number, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_hot_customer     ON raw_transactions (customer_id, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_hot_not_promoted ON raw_transactions (ingested_at) WHERE is_promoted = FALSE;
CREATE INDEX IF NOT EXISTS idx_hot_unaccounted  ON raw_transactions (accounted_flag) WHERE accounted_flag = 0;

-- Default partition: safety net for rows arriving before their partition exists
CREATE TABLE IF NOT EXISTS raw_transactions_default
    PARTITION OF raw_transactions DEFAULT;

-- Hand partition lifecycle to pg_partman
SELECT partman.create_parent(
    p_parent_table  => 'public.raw_transactions',
    p_control       => 'ingested_at',
    p_type          => 'native',
    p_interval      => '6 hours',
    p_premake       => 4
);

UPDATE partman.part_config SET
    retention                = '78 hours',
    retention_keep_table     = false,
    infinite_time_partitions = true,
    automatic_maintenance    = 'on'
WHERE parent_table = 'public.raw_transactions';

-- =============================================================================
-- TIER 3 — COLD TIER (quarterly partitions, indefinite retention)
-- =============================================================================

CREATE TABLE IF NOT EXISTS transactions_history (
    id                      BIGSERIAL,
    original_id             BIGINT          NOT NULL,
    account_number          VARCHAR(20)     NOT NULL,
    customer_id             VARCHAR(20),
    trx_date                DATE            NOT NULL,
    ingested_at             TIMESTAMPTZ     NOT NULL,
    promoted_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    channel_id              INTEGER,
    trn_type                SMALLINT,
    deposit_type            SMALLINT,
    branch_id               INTEGER,
    currency                INTEGER,
    product_id              INTEGER,
    amount                  NUMERIC(18,2),
    debit_amount            NUMERIC(18,2),
    credit_amount           NUMERIC(18,2),
    commission              NUMERIC(18,2),
    expenses                NUMERIC(18,2),
    penalty                 NUMERIC(18,2),
    balance_accounting      NUMERIC(18,2),
    balance_available       NUMERIC(18,2),
    prev_balance_accounting NUMERIC(18,2),
    prev_balance_available  NUMERIC(18,2),
    gl_account              VARCHAR(25),
    gl_account_dr           VARCHAR(25),
    comments                VARCHAR(500),
    reference_number        VARCHAR(50),
    PRIMARY KEY (id, trx_date)
) PARTITION BY RANGE (trx_date);

CREATE TABLE IF NOT EXISTS transactions_history_2025_q1
    PARTITION OF transactions_history FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS transactions_history_2025_q2
    PARTITION OF transactions_history FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS transactions_history_2025_q3
    PARTITION OF transactions_history FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS transactions_history_2025_q4
    PARTITION OF transactions_history FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS transactions_history_2026_q1
    PARTITION OF transactions_history FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS transactions_history_2026_q2
    PARTITION OF transactions_history FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS transactions_history_2026_q3
    PARTITION OF transactions_history FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS transactions_history_2026_q4
    PARTITION OF transactions_history FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');

CREATE INDEX IF NOT EXISTS idx_hist_account_date ON transactions_history (account_number, trx_date);
CREATE INDEX IF NOT EXISTS idx_hist_customer_date ON transactions_history (customer_id, trx_date);
CREATE INDEX IF NOT EXISTS idx_hist_branch_date   ON transactions_history (branch_id, trx_date);
CREATE INDEX IF NOT EXISTS idx_hist_channel_date  ON transactions_history (channel_id, trx_date);
CREATE INDEX IF NOT EXISTS idx_hist_deposit_date  ON transactions_history (deposit_type, trx_date);

-- =============================================================================
-- DEAD LETTER QUEUE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id                  BIGSERIAL       PRIMARY KEY,
    source_topic        VARCHAR(100),
    source_table        VARCHAR(100),
    exception_class     VARCHAR(200),
    exception_message   TEXT,
    raw_payload         TEXT,
    status              VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    retry_count         INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ,
    resolved_by         VARCHAR(100),
    resolution_notes    TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_status  ON dead_letter_events (status);
CREATE INDEX IF NOT EXISTS idx_dlq_topic   ON dead_letter_events (source_topic);
CREATE INDEX IF NOT EXISTS idx_dlq_created ON dead_letter_events (created_at);

-- =============================================================================
-- STATEMENT ENTRIES (insert-only ledger per transaction)
-- =============================================================================

CREATE TABLE IF NOT EXISTS account_statement_entries (
    account_number      VARCHAR(20)     NOT NULL,
    trans_ser_num       INTEGER         NOT NULL,
    entry_ser_num       INTEGER         NOT NULL DEFAULT 1,
    PRIMARY KEY (account_number, trans_ser_num, entry_ser_num),
    customer_id         VARCHAR(20),
    trx_code            INTEGER,
    trx_unit            INTEGER,
    account_unit        INTEGER,
    trx_user            VARCHAR(20),
    trx_sn              INTEGER,
    justification_id    INTEGER,
    currency            INTEGER,
    product_id          INTEGER,
    in_amount           NUMERIC(18,2)   NOT NULL,
    entry_amount        NUMERIC(18,2)   NOT NULL,
    prev_balance        NUMERIC(18,2),
    debit_credit_flag   SMALLINT        NOT NULL,
    trx_date            DATE            NOT NULL,
    value_date          DATE,
    availability_date   DATE,
    reverse_flag        BOOLEAN         NOT NULL DEFAULT FALSE,
    reversed_trx_flag   BOOLEAN         NOT NULL DEFAULT FALSE,
    pending_flag        BOOLEAN         NOT NULL DEFAULT FALSE,
    not_printed_flag    BOOLEAN         NOT NULL DEFAULT FALSE,
    transferred_flag    BOOLEAN         NOT NULL DEFAULT FALSE,
    comments            VARCHAR(500),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stmt_account     ON account_statement_entries (account_number);
CREATE INDEX IF NOT EXISTS idx_stmt_customer    ON account_statement_entries (customer_id);
CREATE INDEX IF NOT EXISTS idx_stmt_trx_date    ON account_statement_entries (trx_date);
CREATE INDEX IF NOT EXISTS idx_stmt_pending     ON account_statement_entries (pending_flag) WHERE pending_flag = TRUE;
CREATE INDEX IF NOT EXISTS idx_stmt_not_printed ON account_statement_entries (not_printed_flag) WHERE not_printed_flag = TRUE;

-- =============================================================================
-- pg_cron: schedule partition maintenance every 6 hours
-- =============================================================================

SELECT cron.schedule(
    'partman-raw-transactions',
    '0 */6 * * *',
    $$SELECT partman.run_maintenance_proc()$$
);

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================