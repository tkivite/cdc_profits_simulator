#!/usr/bin/env python3
"""
HF Group — CDC Kafka Simulator
Mimics Debezium CDC event output for all pipeline topics.

Usage:
    pip install kafka-python faker

    # Run all scenarios continuously
    python cdc_simulator.py

    # Run specific scenario
    python cdc_simulator.py --scenario transactions --count 100 --interval 0.5

    # Burst mode (no delay between messages)
    python cdc_simulator.py --scenario all --count 500 --interval 0

    # Simulate a specific customer journey
    python cdc_simulator.py --scenario journey --customer-id 1019810
"""

import json
import time
import random
import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from kafka import KafkaProducer
from faker import Faker

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKERS    = "localhost:9092"

TOPICS = {
    "transactions":  "cdc.PROD.DEP_TRX_RECORDING",
    "customers":     "cdc.PROD.CUSTOMER",
    "accounts":      "cdc.PROD.PROFITS_ACCOUNT",
    "logs":          "cdc.PROD.CUSTOMER",             # audit logs go to customer topic
}

# ── Reference data (mirrors ref tables in your DB) ───────────────────────────
CHANNELS = {
    9907: "Mobile Banking",
    9908: "Branch",
    9909: "ATM",
    9910: "Agency Banking",
    9911: "Internet Banking",
}

BRANCHES = [109, 200, 201, 202, 203, 204, 205]

DEPOSIT_TYPES = [1, 2, 3]   # 1=Cash, 2=Cheque, 3=Transfer

PRODUCTS = [31719, 31720, 31721, 31722]

CURRENCIES = {22: "KES", 840: "USD", 978: "EUR"}

TRANSACTION_CODES = {
    3191: "Bill Payment",
    3192: "Cash Withdrawal",
    3193: "Cash Deposit",
    3194: "Internal Transfer",
    3195: "Mobile Money",
}

TRN_TYPES = {1: "Deposit", 2: "Withdrawal", 3: "Transfer"}

# ── Seeded customers and accounts (for realistic journeys) ────────────────────
SEED_CUSTOMERS = [
    {"id": "1019810", "first": "BILL",    "middle": "KIVUNZYA", "surname": "NTHULI",   "branch": 109},
    {"id": "604017",  "first": "JANE",    "middle": "WANJIKU",  "surname": "KAMAU",    "branch": 200},
    {"id": "783201",  "first": "PETER",   "middle": "OTIENO",   "surname": "OMONDI",   "branch": 201},
    {"id": "991234",  "first": "GRACE",   "middle": "ACHIENG",  "surname": "ADHIAMBO", "branch": 202},
    {"id": "556677",  "first": "SAMUEL",  "middle": "KIPRONO",  "surname": "KOECH",    "branch": 203},
]

SEED_ACCOUNTS = {
    "1019810": ["9783751190", "9783751191"],
    "604017":  ["9783751192"],
    "783201":  ["9783751193", "9783751194"],
    "991234":  ["9783751195"],
    "556677":  ["9783751196", "9783751197"],
}

fake = Faker()


# ── Helpers ───────────────────────────────────────────────────────────────────

def to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def sentinel_date() -> int:
    """The CBS sentinel for NULL dates — year 0001."""
    return -62135596800000

def now_ms() -> int:
    return to_epoch_ms(datetime.now(timezone.utc))

def trx_date_epoch(days_ago: int = 0) -> int:
    dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    dt -= timedelta(days=days_ago)
    return to_epoch_ms(dt)

def random_amount(min_val: float = 50, max_val: float = 50000) -> str:
    return f"{random.uniform(min_val, max_val):.2f}"

def random_balance(base: float = 10000) -> float:
    return round(base + random.uniform(-5000, 20000), 2)

def make_source(table: str, op: str) -> dict:
    return {
        "version": "1.9.7.Final",
        "connector": "oracle",
        "name": "cdc.PROD",
        "ts_ms": now_ms(),
        "snapshot": "false",
        "db": "PROD",
        "sequence": None,
        "schema": "HFB",
        "table": table,
        "txId": random.randint(100000, 999999),
        "scn": str(random.randint(10000000, 99999999)),
        "commit_scn": str(random.randint(10000000, 99999999)),
        "lcr_position": None
    }

def make_schema(name: str) -> dict:
    return {
        "type": "struct",
        "fields": [],
        "optional": False,
        "name": name,
        "version": 2
    }

def wrap_envelope(table: str, op: str, before: Optional[dict], after: Optional[dict]) -> dict:
    """Wraps a CDC payload in the standard Debezium envelope."""
    envelope_name = f"cdc.PROD.{table}.Envelope"
    return {
        "schema": make_schema(envelope_name),
        "payload": {
            "before": before,
            "after":  after,
            "source": make_source(table, op),
            "op":     op,       # c=create, u=update, d=delete, r=read(snapshot)
            "ts_ms":  now_ms(),
            "transaction": None
        }
    }


# ── Event builders ────────────────────────────────────────────────────────────

def build_customer_create(customer: Optional[dict] = None) -> dict:
    c = customer or random.choice(SEED_CUSTOMERS)
    mobile = f"2547{random.randint(10000000, 99999999)}"
    after = {
        "CUST_ID":              c["id"],
        "C_DIGIT":              str(random.randint(0, 9)),
        "CUST_TYPE":            "1",
        "CUST_STATUS":          "2",
        "ENTRY_STATUS":         "1",
        "TITLE":                "MR.   ",
        "FIRST_NAME":           f"{c['first']:<20}",
        "MIDDLE_NAME":          c["middle"],
        "SURNAME":              f"{c['surname']:<70}",
        "LATIN_FIRSTNAME":      f"{c['first']:<20}",
        "LATIN_SURNAME":        f"{c['surname']:<70}",
        "SHORT_NAME":           f"{c['surname']}{c['first']}"[:15].ljust(15),
        "SEX":                  random.choice(["M", "F"]),
        "DATE_OF_BIRTH":        to_epoch_ms(
                                    datetime(random.randint(1960, 2000), 
                                    random.randint(1,12), random.randint(1,28),
                                    tzinfo=timezone.utc)),
        "BIRTHPLACE":           f"{fake.city():<20}",
        "E_MAIL":               fake.email(),
        "E_MAIL2":              fake.email(),
        "MOBILE_TEL":           mobile,
        "TELEPHONE_1":          f"2547{random.randint(10000000,99999999)}",
        "VIP_IND":              "0",
        "BLACKLISTED_IND":      "0",
        "NON_RESIDENT":         "0",
        "NON_RESIDENT_FOR_R":   "0",
        "PENSIONER_IND":        "0",
        "BUSINESS_IND":         "0",
        "AML_STATUS":           " ",
        "MAJOR_BENEFICIARY":    "0",
        "EMPLOYER":             f"{fake.company():<40}",
        "EMPLOYER_ADDRESS":     f"{fake.address()[:40]:<40}",
        "SALARY_AMN":           "0.00",
        "FK_BRANCH_PORTFBRA":   str(c["branch"]),
        "FKUNIT_BELONGS":       str(c["branch"]),
        "FKUNIT_IS_MONITORE":   "200",
        "FK_BANKEMPLOYEEID":    "KOCE0001",
        "FK_CUST_BANKEMPID":    "CM3010  ",
        "FKCURR_THINKS_IN":     "22",
        "CUSTOMER_BEGIN_DAT":   trx_date_epoch(random.randint(30, 1000)),
        "CUST_OPEN_DATE":       trx_date_epoch(random.randint(30, 1000)),
        "LAST_UPDATE":          now_ms(),
        "TMSTAMP":              now_ms(),
        "CERTIFIC_DATE":        sentinel_date(),
        "EXPIRE_DATE":          sentinel_date(),
        "DOC_EXPIRE_DATE":      sentinel_date(),
        "STATUS_DATE":          sentinel_date(),
        "LEGAL_EXPIRE_DATE":    sentinel_date(),
        "EMPLOYEMENT_START":    sentinel_date(),
        "MAIL_IND":             "1",
        "COMMUN_METHOD":        "S",
        "SELF_INDICATOR":       "R",
        "NON_PROFIT":           "2",
        "CUST_STATUS":          "2",
        "NO_AFM":               "0",
        "NON_REGISTERED":       "0",
        "CERTIFIC_CUST":        "0",
        "ATTRACTION_DETAILS":   "DIRECT",
        "IAPPLY_ID":            "KOCE0001",
        "TURNOVER_AMN":         "0.00",
        "LOANS_AMN":            "0.00",
        "PROFIT_AMN":           "0.00",
        "PROFITABILITY_AMN":    "0.00",
        "NO_OF_BUSINESSES":     "0",
        "OWNERSHIP_INDICATION": "0",
        "NO_OF_EMPLOYEES":      "0",
        "PERM_EMPLOYEES":       "0",
        "CONTRACT":             "0",
        "SEGM_FLAGS":           "00   ",
        "INCOMPLETE_U_COMNT":   " " * 30,
        "ALERT_MSG":            " " * 30,
        "PROMOCODE":            " ",
        "CBS_ID":               " ",
        "CRM_ID":               " ",
    }
    return wrap_envelope("CUSTOMER", "c", None, after)


def build_customer_update(customer_id: str, changes: dict) -> dict:
    """Simulate a customer field update (e.g. status change, contact update)."""
    # Build a minimal before/after pair — only changed fields differ
    before = {"CUST_ID": customer_id, "LAST_UPDATE": now_ms() - 86400000, **changes.get("before", {})}
    after  = {"CUST_ID": customer_id, "LAST_UPDATE": now_ms(), "TMSTAMP": now_ms(), **changes.get("after", {})}
    return wrap_envelope("CUSTOMER", "u", before, after)


def build_account_create(customer_id: str, account_number: str) -> dict:
    after = {
        "ACCOUNT_NUMBER":   f"{account_number:<40}",
        "ACCOUNT_SER_NUM":  str(random.randint(1000000, 9999999)),
        "ACCOUNT_CD":       "0",
        "CUST_ID":          customer_id,
        "C_DIGIT":          str(random.randint(0, 9)),
        "PRODUCT_ID":       str(random.choice(PRODUCTS)),
        "ACC_STATUS":       "1",
        "MOVEMENT_CURRENCY":"22",
        "LIMIT_CURRENCY":   "22",
        "DEP_OPEN_UNIT":    str(random.choice(BRANCHES)),
        "DEP_ACC_NUMBER":   account_number,
        "MONOTORING_UNIT":  "200",
        "PRFT_SYSTEM":      "3",
        "SECONDARY_ACC":    " ",
        "IBAN":             " " * 50,
        "EXTSYS_FLG":       " ",
        "EXTSYS_PROCESS_DT":sentinel_date(),
        **{f"EXTSYS_AMN_{i}": "0.00" for i in range(1, 11)},
    }
    return wrap_envelope("PROFITS_ACCOUNT", "c", None, after)


def build_customer_update_full(customer: dict, scenario: dict) -> dict:
    """
    Builds a realistic customer update event with full before/after snapshots.
    The before snapshot mirrors a real customer record — not just the changed fields.
    """
    mobile   = f"2547{random.randint(10000000, 99999999)}"
    base = {
        "CUST_ID":          customer["id"],
        "FIRST_NAME":       f"{customer['first']:<20}",
        "MIDDLE_NAME":      customer["middle"],
        "SURNAME":          f"{customer['surname']:<70}",
        "SEX":              "M",
        "CUST_TYPE":        "1",
        "CUST_STATUS":      "2",
        "ENTRY_STATUS":     "1",
        "VIP_IND":          "0",
        "BLACKLISTED_IND":  "0",
        "NON_RESIDENT":     "0",
        "PENSIONER_IND":    "0",
        "BUSINESS_IND":     "0",
        "AML_STATUS":       " ",
        "E_MAIL":           fake.email(),
        "E_MAIL2":          fake.email(),
        "MOBILE_TEL":       mobile,
        "TELEPHONE_1":      f"2547{random.randint(10000000, 99999999)}",
        "FK_BRANCH_PORTFBRA": str(customer["branch"]),
        "FK_BANKEMPLOYEEID":  "KOCE0001",
        "EMPLOYER":         f"{fake.company():<40}",
        "SALARY_AMN":       "0.00",
        "MAIL_IND":         "1",
        "COMMUN_METHOD":    "S",
        "LAST_UPDATE":      now_ms() - 86400000,
        "TMSTAMP":          now_ms() - 86400000,
        "CUSTOMER_BEGIN_DAT": trx_date_epoch(365),
        "DOC_EXPIRE_DATE":  sentinel_date(),
        "DATE_OF_BIRTH":    to_epoch_ms(datetime(1985, 6, 15, tzinfo=timezone.utc)),
    }
    before = {**base, **scenario.get("before", {})}
    after  = {**base,
              "LAST_UPDATE": now_ms(),
              "TMSTAMP":     now_ms(),
              **scenario.get("after", {})}
    return wrap_envelope("CUSTOMER", "u", before, after)


def build_customer_deletion(customer: dict) -> dict:
    """
    Builds a customer deletion event — all data in BEFORE, after is null.
    Mirrors a real customer record snapshot at time of deletion.
    """
    mobile = f"2547{random.randint(10000000, 99999999)}"
    before = {
        "CUST_ID":              customer["id"],
        "FIRST_NAME":           f"{customer['first']:<20}",
        "MIDDLE_NAME":          customer["middle"],
        "SURNAME":              f"{customer['surname']:<70}",
        "SEX":                  "M",
        "CUST_TYPE":            "1",
        "CUST_STATUS":          "2",
        "ENTRY_STATUS":         "1",
        "TITLE":                "MR.   ",
        "VIP_IND":              "0",
        "BLACKLISTED_IND":      "0",
        "NON_RESIDENT":         "0",
        "PENSIONER_IND":        "0",
        "BUSINESS_IND":         "0",
        "AML_STATUS":           " ",
        "E_MAIL":               fake.email(),
        "E_MAIL2":              fake.email(),
        "MOBILE_TEL":           mobile,
        "TELEPHONE_1":          f"2547{random.randint(10000000, 99999999)}",
        "FK_BRANCH_PORTFBRA":   str(customer["branch"]),
        "FK_BANKEMPLOYEEID":    "KOCE0001",
        "EMPLOYER":             f"{fake.company():<40}",
        "SALARY_AMN":           "0.00",
        "LAST_UPDATE":          now_ms(),
        "TMSTAMP":              now_ms(),
        "CUSTOMER_BEGIN_DAT":   trx_date_epoch(365),
        "DOC_EXPIRE_DATE":      sentinel_date(),
        "DATE_OF_BIRTH":        to_epoch_ms(datetime(1985, 6, 15, tzinfo=timezone.utc)),
        "COMMUN_METHOD":        "S",
        "MAIL_IND":             "1",
    }
    return wrap_envelope("CUSTOMER", "d", before, None)


def build_account_update(customer_id: str, account_number: str, scenario: dict) -> dict:
    """
    Builds a realistic account update event with full before/after snapshots.
    """
    branch = random.choice(BRANCHES)
    base = {
        "ACCOUNT_NUMBER":   f"{account_number:<40}",
        "ACCOUNT_SER_NUM":  str(random.randint(1000000, 9999999)),
        "ACCOUNT_CD":       "0",
        "CUST_ID":          customer_id,
        "C_DIGIT":          str(random.randint(0, 9)),
        "PRODUCT_ID":       str(random.choice(PRODUCTS)),
        "ACC_STATUS":       "1",
        "MOVEMENT_CURRENCY":"22",
        "LIMIT_CURRENCY":   "22",
        "DEP_OPEN_UNIT":    str(branch),
        "DEP_ACC_NUMBER":   account_number,
        "MONOTORING_UNIT":  "200",
        "PRFT_SYSTEM":      "3",
        "SECONDARY_ACC":    " ",
        "IBAN":             " " * 50,
        "ATM_CARD_FLAG":    "0",
    }
    before = {**base, **scenario.get("before", {})}
    after  = {**base, **scenario.get("after",  {})}
    return wrap_envelope("PROFITS_ACCOUNT", "u", before, after)


def build_account_deletion(customer_id: str, account_number: str) -> dict:
    """
    Builds an account deletion event — all data in BEFORE, after is null.
    """
    branch = random.choice(BRANCHES)
    before = {
        "ACCOUNT_NUMBER":   f"{account_number:<40}",
        "ACCOUNT_SER_NUM":  str(random.randint(1000000, 9999999)),
        "ACCOUNT_CD":       "0",
        "CUST_ID":          customer_id,
        "C_DIGIT":          str(random.randint(0, 9)),
        "PRODUCT_ID":       str(random.choice(PRODUCTS)),
        "ACC_STATUS":       "1",
        "MOVEMENT_CURRENCY":"22",
        "LIMIT_CURRENCY":   "22",
        "DEP_OPEN_UNIT":    str(branch),
        "DEP_ACC_NUMBER":   account_number,
        "MONOTORING_UNIT":  "200",
        "PRFT_SYSTEM":      "3",
        "SECONDARY_ACC":    " ",
        "IBAN":             " " * 50,
        "ATM_CARD_FLAG":    "0",
    }
    return wrap_envelope("PROFITS_ACCOUNT", "d", before, None)


def build_transaction(
    account_number: str,
    customer_id: str,
    channel_id: Optional[int] = None,
    deposit_type: Optional[int] = None,
    trn_type: Optional[int] = None,
    branch_id: Optional[int] = None,
    days_ago: int = 0
) -> tuple[dict, dict]:
    """
    Returns a DEP_TRX_RECORDING event mimicking FST_DEMAND_TUN.
    """
    channel_id   = channel_id   or random.choice(list(CHANNELS.keys()))
    deposit_type = deposit_type or random.choice(DEPOSIT_TYPES)
    trn_type     = trn_type     or random.choice(list(TRN_TYPES.keys()))
    branch_id    = branch_id    or random.choice(BRANCHES)
    trx_code     = random.choice(list(TRANSACTION_CODES.keys()))

    amount       = float(random_amount())
    commission   = round(amount * 0.01, 2)         # 1% commission
    expenses     = round(random.uniform(5, 50), 2)
    total_charge = round(amount + commission + expenses, 2)

    prev_balance = random_balance()
    new_balance  = round(prev_balance - total_charge, 2)

    txn_date     = trx_date_epoch(days_ago)
    trx_user     = random.choice(["KOCE0001", "KOCE0002", "KOCE0003", "SYSAUTO"])
    trx_user_sn  = random.randint(1, 999)
    gl_acc       = f"2.0.0.00.{random.randint(1000, 9999):<22}"
    gl_acc_dr    = f"2.0.1.00.{random.randint(1000, 9999):<22}"

    comment_templates = [
        f"BILL PAYMENT TO {fake.company().upper()[:20]}",
        f"TRANSFER TO {fake.last_name().upper()}",
        f"CASH DEPOSIT",
        f"ATM WITHDRAWAL",
        f"MOBILE MONEY - MPESA",
        f"SALARY CREDIT FROM {fake.company().upper()[:15]}",
        f"INSURANCE PREMIUM - {fake.company().upper()[:10]}",
    ]

    tun_after = {
        "TRX_DATE":             txn_date,
        "TRX_UNIT":             str(branch_id),
        "TRX_USR":              trx_user,
        "TRX_USR_SN":           str(trx_user_sn),
        "TUN_INTERNAL_SN":      "1",
        "CUST_ID1":             customer_id,
        "I_ACCOUNT_NUMBER":     account_number,
        "ACC_UNIT_CODE":        "200",
        "ACC_ID_CURRENCY":      "22",
        "ID_PRODUCT":           str(random.choice(PRODUCTS)),
        "CHANNEL_ID":           str(channel_id),
        "TRX_CODE":             str(trx_code),
        "GL_RULE_CODE":         str(random.randint(30000, 39999)),
        "TRN_TYPE":             str(trn_type),
        "DEPOSIT_TYPE":         str(deposit_type),
        "TRANSACTION_STATUS":   "0",
        "CONFIRMED":            "9",
        "ACCOUNTED_FLAG":       "0",
        "BORDEREAU_FLAG":       "0",
        "I_AMOUNT":             str(amount),
        "U_ACC_PROD_DB":        str(total_charge),
        "U_ACC_PROD_CR":        "0.00",
        "U_CAPITAL_DB":         str(amount),
        "U_CAPITAL_CR":         "0.00",
        "U_COMMISSION":         str(commission),
        "U_EXPENSES":           str(expenses),
        "U_PENALTY":            "0.00",
        "O_ACCOUNTING_BAL":     str(new_balance),
        "O_AVAILABLE_BAL":      str(new_balance),
        "O_UNCLEARED_BAL":      "0.00",
        "O_BLOCKED_BAL":        "0.00",
        "O_ACCOUNT_STATUS":     "1",
        "O_OVERDRAFT_SPREAD":   "50000.00",
        "O_FINAL_ACC_AMOUNT":   str(-total_charge),
        "O_ACCOUNT_LIMIT":      "0.00",
        "P_ACCOUNTING_BAL":     str(prev_balance),
        "P_AVAILABLE_BAL":      str(prev_balance),
        "P_UNCLEARED_BAL":      "0.00",
        "P_BLOCKED_BAL":        "0.00",
        "P_ACCOUNT_STATUS":     "1",
        "O_VALUE_DATE":         txn_date,
        "O_AVAIL_DATE":         txn_date,
        "PRODUCTION_DATE":      txn_date,
        "I_COMMENTS":           random.choice(comment_templates),
        "I_REFERENCE_NUMBER":   f"{random.randint(100000000000, 999999999999):<20}",
        "AUTHORIZER1":          "        ",
        "AUTHORIZER2":          "        ",
        "GL_ACC":               gl_acc,
        "DR_GL_ACCOUNT":        gl_acc_dr,
        "GL_INTEREST_ACC":      f"4.0.0.06.{random.randint(1000,9999):<16}",
        "TMSTAMP":              now_ms(),
        "CONTINUATION":         "2",
        "CUST_LANG":            "1",
        "MIN_NEGOT_UNIT_DC":    "2",
        "O_LAST_STATEM_NUM":    str(random.randint(1, 200)),
        "P_LAST_STATEM_NUM":    str(random.randint(1, 200)),
        "O_CR_INTEREST_RATE":   "0.0000",
        "O_DB_INTEREST_RATE":   "0.0000",
        "U_CREDIT_INTEREST":    "0.00",
        "U_DEBIT_INTEREST":     "0.00",
        "ACCOUNTING_SN":        "0",
        "I_TRX_DATE":           sentinel_date(),
        "I_AVAIL_DATE":         sentinel_date(),
        "I_VALUE_DATE":         sentinel_date(),
        "I_START_DATE":         sentinel_date(),
        "I_EXPIRE_DATE":        sentinel_date(),
        "O_TEMP_EXC_START":     sentinel_date(),
        "O_TEMP_EXC_END":       sentinel_date(),
        "I_TRX_UNIT":           "0",
        "I_TRX_SN":             "0",
        "I_TRX_USR":            "        ",
    }

    trans_ser_num = random.randint(1, 500)
    tun_event = wrap_envelope("DEP_TRX_RECORDING", "c", None, tun_after)
    return tun_event


def build_audit_log(customer_id: str, action: str) -> dict:
    after = {
        "LOG_ID":           str(random.randint(1000000, 9999999)),
        "LOG_DATE":         now_ms(),
        "LOG_USR":          "KOCE0001",
        "LOG_UNIT":         str(random.choice(BRANCHES)),
        "CUST_ID":          customer_id,
        "ACTION":           action,
        "LOG_COMMENTS":     f"Automated log: {action} for customer {customer_id}",
        "TMSTAMP":          now_ms(),
    }
    return wrap_envelope("AUDIT_LOG", "c", None, after)


# ── Producer ──────────────────────────────────────────────────────────────────

class CdcSimulator:

    def __init__(self, brokers: str, max_retries: int = 10, retry_delay: float = 5.0):
        log.info("Connecting to Kafka at %s", brokers)
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=brokers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_block_ms=10000,
                    request_timeout_ms=30000,
                )
                log.info("Kafka producer ready (attempt %d)", attempt)
                return
            except Exception as e:
                last_error = e
                log.warning("Attempt %d/%d — Kafka not ready yet: %s. Retrying in %.0fs...",
                            attempt, max_retries, e, retry_delay)
                time.sleep(retry_delay)
        raise RuntimeError(f"Could not connect to Kafka at {brokers} "
                           f"after {max_retries} attempts: {last_error}")

    def send(self, topic: str, key: str, payload: dict):
        future = self.producer.send(topic, key=key, value=payload)
        future.get(timeout=10)         # block until confirmed
        log.debug("Sent → %s [key=%s]", topic, key)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()

    # ── Scenario: seed all customers and accounts ─────────────────────────────
    def seed_customers_and_accounts(self):
        log.info("=== Seeding customers and accounts ===")
        for c in SEED_CUSTOMERS:
            event = build_customer_create(c)
            self.send(TOPICS["customers"], c["id"], event)
            log.info("  Created customer %s %s (%s)", c["first"], c["surname"], c["id"])
            time.sleep(0.1)

            for account_number in SEED_ACCOUNTS[c["id"]]:
                event = build_account_create(c["id"], account_number)
                self.send(TOPICS["accounts"], account_number, event)
                log.info("  Created account %s for customer %s", account_number, c["id"])
                time.sleep(0.1)

        log.info("Seeding complete")

    # ── Scenario: continuous transaction stream ───────────────────────────────
    def run_transactions(self, count: int = 100, interval: float = 1.0):
        log.info("=== Transaction stream: %d events, %.1fs interval ===", count, interval)
        sent = 0
        while sent < count:
            customer = random.choice(SEED_CUSTOMERS)
            accounts = SEED_ACCOUNTS[customer["id"]]
            account_number = random.choice(accounts)

            tun_event = build_transaction(
                account_number=account_number,
                customer_id=customer["id"],
                channel_id=random.choice(list(CHANNELS.keys())),
                branch_id=customer["branch"],
            )

            self.send(TOPICS["transactions"], account_number, tun_event)

            amount = tun_event["payload"]["after"]["I_AMOUNT"]
            channel = CHANNELS.get(
                int(tun_event["payload"]["after"]["CHANNEL_ID"]), "Unknown")
            log.info("[%d/%d] TXN | account=%s customer=%s amount=%s channel=%s",
                     sent + 1, count, account_number,
                     customer["id"], amount, channel)

            sent += 1
            if interval > 0:
                time.sleep(interval)

    # ── Scenario: customer update events ─────────────────────────────────────
    def run_customer_updates(self, count: int = 10, interval: float = 2.0):
        log.info("=== Customer update stream: %d events ===", count)
        update_scenarios = [
            {
                "description": "Status change to inactive",
                "before": {"CUST_STATUS": "2"},
                "after":  {"CUST_STATUS": "1"}
            },
            {
                "description": "Status change to active",
                "before": {"CUST_STATUS": "1"},
                "after":  {"CUST_STATUS": "2"}
            },
            {
                "description": "Email update",
                "before": {"E_MAIL": "old.email@example.com                                        "},
                "after":  {"E_MAIL": f"{fake.email():<64}"}
            },
            {
                "description": "Mobile number update",
                "before": {"MOBILE_TEL": "254700000000X"},
                "after":  {"MOBILE_TEL": f"2547{random.randint(10000000,99999999)}X"}
            },
            {
                "description": "AML flag raised",
                "before": {"AML_STATUS": " "},
                "after":  {"AML_STATUS": "S"}
            },
            {
                "description": "AML flag cleared",
                "before": {"AML_STATUS": "S"},
                "after":  {"AML_STATUS": " "}
            },
            {
                "description": "VIP upgrade",
                "before": {"VIP_IND": "0"},
                "after":  {"VIP_IND": "1"}
            },
            {
                "description": "Blacklist flag raised",
                "before": {"BLACKLISTED_IND": "0"},
                "after":  {"BLACKLISTED_IND": "1"}
            },
            {
                "description": "Employer change",
                "before": {"EMPLOYER": f"{fake.company():<40}"},
                "after":  {"EMPLOYER": f"{fake.company():<40}"}
            },
            {
                "description": "Branch transfer",
                "before": {"FK_BRANCH_PORTFBRA": str(random.choice(BRANCHES))},
                "after":  {"FK_BRANCH_PORTFBRA": str(random.choice(BRANCHES))}
            },
        ]

        for i in range(count):
            customer = random.choice(SEED_CUSTOMERS)
            scenario = random.choice(update_scenarios)
            event = build_customer_update_full(customer, scenario)
            self.send(TOPICS["customers"], customer["id"], event)
            log.info("[%d/%d] CUSTOMER UPDATE | customer=%s scenario='%s'",
                     i + 1, count, customer["id"], scenario["description"])
            if interval > 0:
                time.sleep(interval)

    # ── Scenario: account update events ──────────────────────────────────────
    def run_account_updates(self, count: int = 10, interval: float = 2.0):
        log.info("=== Account update stream: %d events ===", count)
        update_scenarios = [
            {
                "description": "Account status change to dormant",
                "before": {"ACC_STATUS": "1"},
                "after":  {"ACC_STATUS": "2"}
            },
            {
                "description": "Account status reactivated",
                "before": {"ACC_STATUS": "2"},
                "after":  {"ACC_STATUS": "1"}
            },
            {
                "description": "Account status blocked",
                "before": {"ACC_STATUS": "1"},
                "after":  {"ACC_STATUS": "3"}
            },
            {
                "description": "Product change",
                "before": {"PRODUCT_ID": str(PRODUCTS[0])},
                "after":  {"PRODUCT_ID": str(random.choice(PRODUCTS))}
            },
            {
                "description": "ATM card enabled",
                "before": {"ATM_CARD_FLAG": "0"},
                "after":  {"ATM_CARD_FLAG": "1"}
            },
            {
                "description": "ATM card disabled",
                "before": {"ATM_CARD_FLAG": "1"},
                "after":  {"ATM_CARD_FLAG": "0"}
            },
            {
                "description": "Branch transfer",
                "before": {"DEP_OPEN_UNIT": str(BRANCHES[0])},
                "after":  {"DEP_OPEN_UNIT": str(random.choice(BRANCHES))}
            },
        ]

        for i in range(count):
            customer  = random.choice(SEED_CUSTOMERS)
            account_number = random.choice(SEED_ACCOUNTS[customer["id"]])
            scenario  = random.choice(update_scenarios)
            event = build_account_update(customer["id"], account_number, scenario)
            self.send(TOPICS["accounts"], account_number, event)
            log.info("[%d/%d] ACCOUNT UPDATE | account=%s customer=%s scenario='%s'",
                     i + 1, count, account_number,
                     customer["id"], scenario["description"])
            if interval > 0:
                time.sleep(interval)

    # ── Scenario: customer deletions ──────────────────────────────────────────
    def run_customer_deletions(self, count: int = 2, interval: float = 2.0):
        """
        Simulates customer deletion events.
        Kept to a small default count — deletions in banking are rare.
        In production these almost always indicate test account cleanup.
        """
        log.info("=== Customer deletion stream: %d events ===", count)
        # Only delete from a copy — never mutate SEED_CUSTOMERS
        candidates = SEED_CUSTOMERS.copy()
        random.shuffle(candidates)

        for i in range(min(count, len(candidates))):
            customer = candidates[i]
            event = build_customer_deletion(customer)
            self.send(TOPICS["customers"], customer["id"], event)
            log.warn("[%d/%d] CUSTOMER DELETION | customer=%s %s %s",
                     i + 1, count, customer["id"],
                     customer["first"], customer["surname"])
            if interval > 0:
                time.sleep(interval)

    # ── Scenario: account deletions ───────────────────────────────────────────
    def run_account_deletions(self, count: int = 2, interval: float = 2.0):
        """
        Simulates account deletion events.
        Kept to a small default count — deletions in banking are rare.
        """
        log.info("=== Account deletion stream: %d events ===", count)
        sent = 0
        customers = SEED_CUSTOMERS.copy()
        random.shuffle(customers)

        for customer in customers:
            if sent >= count:
                break
            accounts = SEED_ACCOUNTS[customer["id"]].copy()
            for account_number in accounts:
                if sent >= count:
                    break
                event = build_account_deletion(customer["id"], account_number)
                self.send(TOPICS["accounts"], account_number, event)
                log.warn("[%d/%d] ACCOUNT DELETION | account=%s customer=%s",
                         sent + 1, count, account_number, customer["id"])
                sent += 1
                if interval > 0:
                    time.sleep(interval)

    # ── Scenario: full customer journey ──────────────────────────────────────
    def run_customer_journey(self, customer_id: Optional[str] = None):
        """
        Simulates a realistic customer journey:
        1. Customer created
        2. Account opened
        3. Initial deposit
        4. Multiple transactions over several days
        5. Contact info update
        6. High-value transaction (potential AML trigger)
        """
        customer = next(
            (c for c in SEED_CUSTOMERS if c["id"] == customer_id),
            random.choice(SEED_CUSTOMERS)
        )
        account_number = SEED_ACCOUNTS[customer["id"]][0]

        log.info("=== Customer journey for %s %s (%s) ===",
                 customer["first"], customer["surname"], customer["id"])

        steps = [
            ("Customer onboarding", 0.5, lambda: (
                self.send(TOPICS["customers"], customer["id"],
                          build_customer_create(customer)),
                self.send(TOPICS["accounts"], account_number,
                          build_account_create(customer["id"], account_number)),
                self.send(TOPICS["logs"], customer["id"],
                          build_audit_log(customer["id"], "CUSTOMER_CREATED"))
            )),
            ("Initial deposit (cash)", 1.0, lambda: (
                [self.send(TOPICS["transactions"], account_number, e)
                 for e in [build_transaction(account_number, customer["id"],
                            channel_id=9908,    # Branch
                            deposit_type=1,     # Cash
                            trn_type=1,         # Deposit
                            branch_id=customer["branch"],
                            days_ago=30)[0]]],
            )),
            ("Regular mobile transactions (5)", 0.3, lambda: (
                [self._send_transaction(account_number, customer["id"],
                                        channel_id=9907, days_ago=d)
                 for d in range(1, 6)]
            )),
            ("ATM withdrawals (3)", 0.3, lambda: (
                [self._send_transaction(account_number, customer["id"],
                                        channel_id=9909, trn_type=2, days_ago=d)
                 for d in range(7, 10)]
            )),
            ("Contact info update", 0.5, lambda: (
                self.send(TOPICS["customers"], customer["id"],
                          build_customer_update(customer["id"], {
                              "before": {"E_MAIL": "old@example.com"},
                              "after":  {"E_MAIL": fake.email()}
                          })),
            )),
            ("Large transaction (AML watchlist candidate)", 1.0, lambda: (
                self._send_transaction(account_number, customer["id"],
                                       channel_id=9908,
                                       amount_override=random.uniform(500000, 2000000))
            )),
        ]

        for step_name, delay, action in steps:
            log.info("  Journey step: %s", step_name)
            action()
            time.sleep(delay)

        log.info("Journey complete for customer %s", customer["id"])

    def _send_transaction(self, account_number: str, customer_id: str,
                          channel_id: Optional[int] = None,
                          trn_type: Optional[int] = None,
                          days_ago: int = 0,
                          amount_override: Optional[float] = None):
        tun = build_transaction(account_number, customer_id,
                                channel_id=channel_id,
                                trn_type=trn_type,
                                days_ago=days_ago)
        if amount_override:
            tun["payload"]["after"]["I_AMOUNT"] = f"{amount_override:.2f}"
        self.send(TOPICS["transactions"], account_number, tun)

    # ── Scenario: burst load (stress test) ───────────────────────────────────
    def run_burst(self, count: int = 500):
        log.info("=== Burst mode: %d transactions, no delay ===", count)
        start = time.time()
        for i in range(count):
            customer = random.choice(SEED_CUSTOMERS)
            account_number = random.choice(SEED_ACCOUNTS[customer["id"]])
            tun = build_transaction(account_number, customer["id"])
            self.send(TOPICS["transactions"], account_number, tun)
            if (i + 1) % 50 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                log.info("  [%d/%d] %.1f msg/sec", i + 1, count, rate)

        elapsed = time.time() - start
        log.info("Burst complete: %d messages in %.1fs (%.0f msg/sec)",
                 count, elapsed, count / elapsed)

    # ── Scenario: backfill historical data ───────────────────────────────────
    def run_historical_backfill(self, days: int = 7, txns_per_day: int = 50):
        """Generates historical transactions spread across past N days."""
        log.info("=== Historical backfill: %d days × %d txns/day ===",
                 days, txns_per_day)
        total = days * txns_per_day
        sent = 0
        for days_ago in range(days, 0, -1):
            date_str = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            log.info("  Backfilling %s (%d transactions)", date_str, txns_per_day)
            for _ in range(txns_per_day):
                customer = random.choice(SEED_CUSTOMERS)
                account_number = random.choice(SEED_ACCOUNTS[customer["id"]])
                tun = build_transaction(
                    account_number, customer["id"], days_ago=days_ago)
                self.send(TOPICS["transactions"], account_number, tun)
                sent += 1
            time.sleep(0.05)   # small pause between days

        log.info("Backfill complete: %d total transactions", sent)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="HF Group CDC Kafka Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scenarios:
  seed            Create all customers and accounts (run first)
  transactions    Continuous transaction stream
  updates         Customer field update events (status, email, AML etc.)
  account-updates Account field update events (status, ATM card etc.)
  deletions       Customer and account deletion events (small count by default)
  journey         End-to-end customer lifecycle (create → transact → update → delete)
  burst           High-volume stress test
  backfill        Historical data across past N days
  all             seed → backfill → updates → transactions
        """
    )
    parser.add_argument("--brokers",     default="localhost:9092")
    parser.add_argument("--scenario",    default="all",
                        choices=["seed", "transactions", "updates",
                                 "account-updates", "deletions",
                                 "journey", "burst", "backfill", "all"])
    parser.add_argument("--count",       type=int,   default=100)
    parser.add_argument("--interval",    type=float, default=1.0,
                        help="Seconds between messages (0 = burst)")
    parser.add_argument("--days",        type=int,   default=7,
                        help="Days of history for backfill")
    parser.add_argument("--txns-per-day",type=int,   default=50)
    parser.add_argument("--customer-id", default=None,
                        help="Specific customer ID for journey scenario")

    args = parser.parse_args()

    sim = CdcSimulator(args.brokers)

    try:
        if args.scenario == "seed":
            sim.seed_customers_and_accounts()

        elif args.scenario == "transactions":
            sim.run_transactions(args.count, args.interval)

        elif args.scenario == "updates":
            sim.run_customer_updates(args.count, args.interval)

        elif args.scenario == "account-updates":
            sim.run_account_updates(args.count, args.interval)

        elif args.scenario == "deletions":
            sim.run_customer_deletions(args.count, args.interval)
            sim.run_account_deletions(args.count, args.interval)

        elif args.scenario == "journey":
            sim.run_customer_journey(args.customer_id)

        elif args.scenario == "burst":
            sim.run_burst(args.count)

        elif args.scenario == "backfill":
            sim.run_historical_backfill(args.days, args.txns_per_day)

        elif args.scenario == "all":
            log.info("Running full pipeline simulation")

            # 1. Seed customers and accounts
            sim.seed_customers_and_accounts()
            time.sleep(1)

            # 2. Backfill historical transactions
            sim.run_historical_backfill(args.days, args.txns_per_day)
            time.sleep(1)

            # 3. Mix of customer and account updates
            sim.run_customer_updates(5, args.interval)
            time.sleep(0.5)
            sim.run_account_updates(5, args.interval)
            time.sleep(0.5)

            # 4. Live transaction stream
            log.info("Switching to live transaction stream (Ctrl+C to stop)")
            sim.run_transactions(args.count, args.interval)

    except KeyboardInterrupt:
        log.info("Simulation stopped by user")
    except Exception as e:
        log.error("Simulation failed: %s", e, exc_info=True)
    finally:
        sim.flush()
        sim.close()
        log.info("Producer closed")


if __name__ == "__main__":
    main()
