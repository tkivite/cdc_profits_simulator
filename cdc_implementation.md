# CDC Transaction Monitoring Pipeline

## Technical Design Document

**Version 2.0 | March 2026**

---

## 1. Executive Summary

This document describes the technical architecture, data design, and implementation
of the Change Data Capture (CDC) pipeline that monitors transactions, customer
activity, and account events from the core banking system (CBS). The
pipeline captures database change events in real time, classifies and persists
them across a tiered storage architecture, and serves three distinct workloads:
a live operational dashboard, an analytics dashboard, and a historical query
interface for compliance and reporting.

---

## 2. Architecture Overview

### 2.1 System Boundaries

The pipeline sits between the core banking system and downstream consumers. It
does not write back to the CBS under any circumstance. All data flows are
unidirectional: CBS → Kafka → Pipeline → PostgreSQL.

The CBS generates change events via Oracle Debezium, which captures every
INSERT, UPDATE, and DELETE at the database transaction log level and publishes
them as structured JSON messages to Apache Kafka. The pipeline consumes these
messages using Apache Camel running inside a Spring Boot application.

### 2.2 Component Inventory

**Apache Kafka** serves as the event backbone. It decouples the CBS from the
pipeline, provides durable message storage, and guarantees ordered delivery per
partition. The pipeline uses an existing Confluent Platform deployment
(cp-kafka:7.4.0) with Zookeeper coordination and a Control Center UI.

**Apache Camel** is the integration framework responsible for consuming Kafka
messages, deserialising them, routing them by event type, and orchestrating
writes to PostgreSQL. Camel runs inside a Spring Boot application and exposes
a declarative route DSL that makes the data flow explicit and testable.

**PostgreSQL 16** is the target database. It was selected for its native
declarative partitioning, the pg_partman extension for automated partition
lifecycle management, the pg_trgm extension for trigram-based fuzzy search,
and pg_cron for in-database scheduling.

**The CDC Simulator** is a Python tool that mimics Debezium event output across
all topics. It is used for development, integration testing, and load
simulation without requiring a live CBS connection.

### 2.3 Kafka Topic Design

Each CBS table is assigned a dedicated Kafka topic. This isolates concerns,
allows independent consumer group management per entity type, and prevents a
spike in transaction volume from affecting customer event processing latency.

| Topic                      | Source table      | Event types            | Consumer group           |
| -------------------------- | ----------------- | ---------------------- | ------------------------ |
| cdc.PROD.DEP_TRX_RECORDING | FST_DEMAND_TUN    | INSERT only            | hfgroup-cdc-consumer     |
| cdc.PROD.CUSTOMER          | CUSTOMER          | INSERT, UPDATE, DELETE | hfgroup-cdc-consumer     |
| cdc.PROD.PROFITS_ACCOUNT   | PROFITS_ACCOUNT   | INSERT, UPDATE, DELETE | hfgroup-cdc-consumer     |
| cdc.PROD.DLQ               | Dead Letter Queue | All failed events      | hfgroup-cdc-consumer.dlq |

Each topic is created with three partitions. Partitions allow parallel
consumption and distribute load across consumer instances when the application
is scaled horizontally.

---

## 3. Debezium Event Structure

Every Kafka message produced by Debezium follows a consistent envelope
structure regardless of which CBS table it originates from. The envelope
contains a schema descriptor and a payload. The payload carries four key
fields: `op` (the operation type), `before` (the row state before the
change), `after` (the row state after the change), and `source` (metadata
about the originating database and table).

The `op` field takes one of four values: `c` for create (INSERT), `u` for
update (UPDATE), `d` for delete (DELETE), and `r` for read (used during
initial snapshot). The pipeline handles `c`, `u`, and `d` explicitly.

For INSERT events, `before` is always null and `after` contains the full
row. For DELETE events, `before` contains the full row at time of deletion
and `after` is null. For UPDATE events, both `before` and `after` are
populated with the complete row state, enabling the pipeline to compute a
precise field-level diff.

### 3.1 CBS Date Sentinel Values

The CBS uses a sentinel value of `-62135596800000` (epoch milliseconds
corresponding to year 0001-01-01) to represent null dates. This is a
fixed-width database convention from the legacy system. All date fields
in the pipeline must be checked against this sentinel and treated as null
rather than stored as a date value. The `CdcFieldExtractor` utility class
handles this conversion consistently across all processors.

### 3.2 Fixed-Width String Padding

The CBS pads string fields to fixed widths using trailing spaces. For
example, `ACCOUNT_NUMBER` arrives as `9783751190` followed by 30 spaces.
All string extraction in the pipeline trims whitespace before persistence.
This is critical for the customer update diff logic, where a padded field
that appears changed but has not meaningfully changed would generate false
positive diff records.

---

## 4. Camel Route Architecture

### 4.1 Route Design Principles

The pipeline uses one Camel route per Kafka topic. This keeps each route
independently manageable, independently testable, and independently
scalable. Routes share a common error handler and a common Jackson
deserialisation configuration but are otherwise fully isolated.

A shared Kafka options string is applied to all consumer routes, ensuring
every route specifies a `groupId` and `autoOffsetReset`. Omitting `groupId`
causes Kafka to assign an anonymous consumer group, which does not persist
offset commits across restarts, resulting in full topic replay on every
application restart.

### 4.2 Operation-Based Routing

Within each route, a Camel `choice()` block branches on the `op` field of
the Debezium payload. CREATE operations are routed to creation processors,
UPDATE operations to update processors, and DELETE operations to deletion
processors. Each processor is a Spring-managed bean registered by name and
resolved by Camel at runtime via the Spring bean registry.

Transaction events (`DEP_TRX_RECORDING`) are INSERT-only by design. The
route logs a warning for any UPDATE or DELETE received on this topic, as
these represent anomalous CBS behaviour.

### 4.3 Jackson Deserialisation

All Kafka message bodies are deserialised into a typed `DebeziumEvent`
Java class using the Jackson library. The deserialiser is configured with
`FAIL_ON_UNKNOWN_PROPERTIES` disabled. This means that when Debezium adds
new fields to the event schema as part of a CBS upgrade, the pipeline
continues processing without throwing an exception. All model classes are
annotated with `@JsonIgnoreProperties(ignoreUnknown = true)` for the same
reason.

### 4.4 Original Message Preservation

The raw `DebeziumEvent` object is stored as an Exchange property
(`ORIGINAL_MESSAGE_PROPERTY`) before any processing begins. This preserves
the unmodified event for use in error handling and dead letter queue
routing. If a processor modifies the exchange body and subsequently fails,
the DLQ receives the original unmodified payload rather than a
partially-processed state.

### 4.5 Global Error Handler

A single `onException(Exception.class)` handler covers all routes. On
failure, it retries up to three times with exponential backoff (2s, 4s,
8s). After exhausting retries, it enriches the message with diagnostic
headers (`dlq.source.topic`, `dlq.source.table`, `dlq.exception.class`,
`dlq.exception.message`, `dlq.timestamp`) and routes the original message
to the DLQ Kafka topic. The exception is then marked as handled, allowing
the consumer to continue processing subsequent messages rather than
stalling on a single bad event.

---

## 5. Data Processing Layer

### 5.1 Field Extraction Utility

`CdcFieldExtractor` is a shared static utility class used by all
processors. It provides type-safe extraction methods for the raw
`Map<String, Object>` payload that Debezium delivers. Methods cover string
extraction with trim and null normalisation, integer and long parsing with
null safety, BigDecimal parsing for monetary amounts, LocalDate conversion
from epoch milliseconds with sentinel detection, and boolean extraction
from CBS indicator flags (which arrive as `"0"` and `"1"` strings).

Centralising extraction logic eliminates duplication across processors and
ensures consistent handling of CBS data quality issues across all entity
types.

### 5.2 Transaction Processing — Dual Write

The transaction processor performs two writes atomically for each CREATE
event received on the transaction topic.

The first write persists the full transaction detail to the hot tier
`raw_transactions` table. Because `raw_transactions` is a partitioned
table, the write uses `JdbcTemplate` directly rather than JPA's
`EntityManager`. This is necessary because Hibernate's `RETURNING id`
mechanism used by `GenerationType.IDENTITY` is incompatible with
PostgreSQL declarative partitioning in some driver configurations.

The second write performs four concurrent upserts to the summary tables
using the PostgreSQL `ON CONFLICT DO UPDATE` pattern. This atomic counter
increment pattern updates `summary_by_channel`, `summary_by_branch`,
`summary_by_deposit_type`, and `summary_by_channel_branch` in a single
database transaction. The upsert pattern eliminates the need for a
read-before-write and is race-condition-safe at the database level.

Both writes succeed or neither is committed — the `SummaryService` is
annotated `@Transactional` to enforce this atomicity.

### 5.3 Customer and Account Processing

Customer and account CREATE events are processed by dedicated processors
that map CBS fields to the curated `customers` and `accounts` tables. Both
processors implement an upsert pattern at the service layer: if the entity
already exists (identifiable by `customer_id` or `account_number`), the
existing record is updated and `updated_at` is refreshed; if it does not
exist, a new record is inserted. The original `created_at` timestamp is
always preserved on updates, maintaining the onboarding date integrity.

### 5.4 Customer and Account Update Tracking

UPDATE events for customers and accounts are processed by dedicated update
processors that compute a field-level diff between the `before` and `after`
snapshots. Rather than comparing all fields (which would generate noise
from internal CBS fields that change on every event, such as `TMSTAMP`),
each processor maintains an explicit `TRACKED_FIELDS` list containing only
fields with business significance.

For customers, tracked fields include status flags (`CUST_STATUS`,
`AML_STATUS`, `BLACKLISTED_IND`, `VIP_IND`), contact details
(`E_MAIL`, `MOBILE_TEL`), and assignment fields (`FK_BRANCH_PORTFBRA`,
`FK_BANKEMPLOYEEID`). For accounts, tracked fields include `ACC_STATUS`,
`PRODUCT_ID`, `ATM_CARD_FLAG`, and `DEP_OPEN_UNIT`.

The diff result (changed field names, old values, new values) is persisted
as structured text in `customer_changes` and `account_changes` respectively.
If no tracked fields differ between before and after, the processor exits
without writing, keeping the change tables free of noise.

### 5.5 Deletion Tracking

DELETE events carry the complete row state in `before` and null in `after`.
The deletion processors capture a full snapshot of the entity at time of
deletion and persist it to `customer_deletions` and `account_deletions`
tables. These tables serve as an immutable audit trail — once a record is
deleted from the CBS it can never be recovered from the source, making the
deletion snapshot the only reference.

Deletions in a banking core system are operationally rare. The pipeline
logs deletions at WARNING level to make them immediately visible in
monitoring. A spike in deletion events almost always indicates a bulk
data operation or test account cleanup rather than legitimate individual
account closures, and should be investigated.

---

## 6. Three-Tier Storage Architecture

### 6.1 Design Rationale

A single flat table cannot serve the three distinct query workloads of
the pipeline simultaneously. Live dashboard queries require sub-100ms
response on recent data. Analytics queries require pre-aggregated counts
and sums across dimensions. Historical queries involve large date-range
scans across years of data. These workloads have fundamentally conflicting
index and storage requirements.

The architecture separates storage into three tiers, each optimised for
its specific workload, each with a single designated writer.

### 6.2 Tier 1 — Hot Tier (raw_transactions)

The hot tier holds a 72-hour rolling window of raw transaction detail.
It is written by the Camel transaction processor and serves the live
operational dashboard. The table is partitioned by `ingested_at` using
6-hour intervals, resulting in 12 live partitions at any point in time
plus 4 pre-created future partitions maintained by pg_partman — 16
partitions in steady state.

Six-hour partitioning was chosen as the optimal balance between partition
count (which affects query planner overhead) and eviction precision. Daily
partitions would give up to 24 hours of overage; hourly partitions would
create 72+ tables. At 16 tables, PostgreSQL's partition pruning operates
with minimal overhead.

Data eviction is performed by dropping entire partitions rather than
deleting rows. A partition drop is a PostgreSQL metadata operation
completing in milliseconds, requiring no VACUUM and generating no table
bloat. Row-level deletes on a high-volume table would accumulate dead
tuples and degrade performance over time.

Indexes on the hot tier are deliberately minimal — only those needed for
live dashboard account lookups and the promotion job's unpromoted-row
scan. Write throughput is prioritised over read flexibility.

### 6.3 Tier 2 — Summary Tables

The summary tier holds pre-aggregated counters updated atomically on every
transaction event. Four tables cover the primary analytics dimensions:
by channel, by branch, by deposit type, and the cross-dimension
channel-by-branch. These tables serve the analytics dashboard.

The critical design decision is that summary tables are real tables with
`ON CONFLICT DO UPDATE` upserts — not materialized views with scheduled
refreshes. This means aggregation cost is paid once at insert time rather
than accumulated and paid at query time. Dashboard queries against the
summary tables return in microseconds regardless of transaction volume,
because they read from a small, pre-computed table rather than
scanning millions of raw rows.

The trade-off is that the summary tables reflect only what has been
processed by the pipeline, not the CBS directly. If the pipeline has lag,
the summaries will be slightly behind real time. This is acceptable for
analytics workloads.

### 6.4 Tier 3 — Cold Tier (transactions_history)

The cold tier holds all transactions older than 72 hours, partitioned by
`trx_date` on quarterly boundaries. It is written exclusively by the
hot-to-cold promotion job and serves historical queries and BI workloads.

Quarterly partitioning means four new partitions per year. Date-range
queries benefit from partition pruning — a query for a specific month
scans only one quarterly partition rather than the entire table. The cold
tier carries richer indexes than the hot tier, reflecting its read-heavy
analytical workload.

### 6.5 Hot-to-Cold Promotion

The `HotToColdPromotionJob` runs every 6 hours on a Spring `@Scheduled`
timer. It identifies rows in `raw_transactions` with `ingested_at` older
than 72 hours and `is_promoted = FALSE`, copies them to
`transactions_history`, marks them as promoted, then detaches and drops
the corresponding hot partition.

The copy and mark steps execute within a single database transaction,
ensuring atomicity. If the promotion job fails mid-run, rows remain with
`is_promoted = FALSE` and will be picked up on the next scheduled run.
No data is lost. The partition drop occurs outside the transaction because
DDL cannot be rolled back in PostgreSQL — a failed drop at worst leaves
an orphaned partition that will be detected by the monitoring job.

### 6.6 Partition Automation with pg_partman

pg_partman v4.7.4 is built from source inside the Docker PostgreSQL image
and manages the hot tier partition lifecycle. It pre-creates 4 future
6-hour partitions at all times, ensuring that a partition always exists
for incoming data even if the promotion job or a manual process falls
behind. The default partition serves as a final safety net — any row
arriving for a time range with no matching partition lands there rather
than causing an insert failure.

The pg_partman background worker (`pg_partman_bgw`) is intentionally not
used. Instead, maintenance is called explicitly from the Spring Boot
promotion job. This keeps scheduling control in the application layer
where it is observable, alertable, and testable.

---

## 7. Dead Letter Queue

### 7.1 Purpose and Design

The Dead Letter Queue (DLQ) is the mechanism by which the pipeline
ensures no event is silently lost. When a Camel processor fails after
exhausting retries, the original raw event is forwarded to the
`cdc.PROD.DLQ` Kafka topic with diagnostic headers attached. A dedicated
DLQ consumer route reads from this topic and persists each failed event
to the `dead_letter_events` PostgreSQL table.

The DLQ consumer group uses a distinct group ID (`hfgroup-cdc-consumer.dlq`)
to prevent offset interference with the main consumer.

### 7.2 Failure Classification

The DLQ consumer classifies failures by exception type and routes them to
specialised processors.

`DlqInvalidPayloadProcessor` handles schema or deserialisation failures.
These indicate a mismatch between the event structure and the Java model,
typically caused by a CBS schema change. They are automatically set to
`IN_REVIEW` status because they require a code change before replay is
possible.

`DlqDuplicateProcessor` handles database constraint violations, typically
caused by Kafka redelivery after a consumer restart. These are
automatically set to `DISCARDED` because the target record already exists
in the correct state.

`DlqGenericProcessor` handles all other failures with `PENDING` status,
requiring human triage.

### 7.3 Event Lifecycle

Every DLQ event progresses through a defined status lifecycle:
`PENDING` on arrival, `IN_REVIEW` during investigation, and either
`REPLAYED` or `DISCARDED` on resolution. The `dead_letter_events` table
stores the complete raw payload, the exception details, the source topic
and table, and timestamps for creation and resolution.

### 7.4 Replay Mechanism

Resolved events can be replayed via the `direct:dlq-replay` Camel route,
triggered by a REST endpoint (`POST /admin/dlq/{id}/replay`). The replay
route loads the raw payload from the database, deserialises it, and routes
it through the same processor chain as a normal event. On successful
replay, the event status is updated to `REPLAYED` by
`DlqMarkResolvedProcessor`. If replay fails, the status remains unchanged
and the event can be retried after further investigation.

### 7.5 Monitoring

The `PipelineMonitorJob` runs every 15 minutes and checks four health
indicators: DLQ pending count (alerts at > 10), default partition row
count (alerts if any rows exist, indicating a missing partition), stale
unpromoted rows in the hot tier (alerts if rows older than 78 hours have
not been promoted), and the `last_updated` freshness of each summary
table. These checks provide early warning of pipeline degradation before
it affects downstream consumers.

---

## 8. CDC Simulator

The CDC simulator is a Python 3.11 application that produces realistic
Debezium-format CDC events to all pipeline Kafka topics. It is packaged
as a Docker container and driven entirely by environment variables,
making it configurable without rebuilding the image.

### 8.1 Event Fidelity

The simulator produces events that are structurally identical to real
Debezium output, including the correct envelope schema, the `op` field,
sentinel date values for null dates, fixed-width string padding for CBS
string fields, and epoch millisecond timestamps. This fidelity is
necessary to exercise the pipeline's edge case handling — sentinel date
detection, whitespace trimming, and the before/after diff logic — under
conditions identical to production.

### 8.2 Scenarios

The simulator supports seven named scenarios:

`seed` creates all reference customers and accounts. This must be run
before any other scenario as subsequent scenarios reference the seeded
entity IDs.

`transactions` produces a continuous stream of transaction INSERT events
at a configurable rate.

`updates` produces customer field UPDATE events covering all tracked
field types: status changes, contact detail updates, AML flag changes,
VIP upgrades, blacklist flags, employer changes, and branch transfers.

`account-updates` produces account field UPDATE events covering status
changes (active, dormant, blocked), ATM card enablement and disablement,
product changes, and branch transfers.

`deletions` produces both customer and account DELETE events. The default
count is deliberately small (2 each) to reflect the operational rarity
of deletions in production banking systems.

`journey` simulates a complete customer lifecycle: onboarding, account
creation, initial deposit, regular mobile transactions, contact info
update, and one high-value transaction that may trigger AML review.

`backfill` generates historical transactions spread across a configurable
number of past days, useful for populating the cold tier during
development and testing.

`all` runs seed, backfill, a mix of updates and account-updates, then
switches to a continuous live transaction stream.

### 8.3 Kafka Connectivity

The simulator connects to the existing `aslead-kafka-queue` Kafka
deployment. Because the broker's `ADVERTISED_LISTENERS` is configured
to use the internal Docker container name (`broker:29092`), the simulator
container must join the same Docker network as the Kafka stack. The
simulator container is connected to `aslead-kafka-queue_default` (the
Kafka stack network) in addition to the `hfgroup-net` network, allowing
it to resolve the `broker` hostname and complete the post-bootstrap
metadata handshake.

The simulator implements a TCP-level wait loop before starting (`wait-for-kafka.sh`)
and an application-level retry loop inside the `KafkaProducer` initialiser.
Both layers are necessary because Docker healthchecks confirm the broker
process is alive but not that it has completed leader election and is
ready to accept producer connections.

---

## 9. Database Infrastructure

### 9.1 PostgreSQL Docker Image

The PostgreSQL instance runs from a custom Docker image built on top of
the official `postgres:16` base. pg_partman v4.7.4 is compiled from
source during the image build because it is not included in any official
PostgreSQL Docker image. pg_cron is installed via the PostgreSQL apt
repository. pg_trgm is available as a contrib module in the base image
and requires no compilation.

The `shared_preload_libraries` configuration is passed as a runtime
`postgres -c` argument in `docker-compose.yml` rather than written into
`postgresql.conf` during image build. Writing it into the image caused
PostgreSQL's `initdb` to fail on first container start because the library
load is attempted before the data directory exists.

### 9.2 Extension Initialisation

An `init-extensions.sql` script runs automatically on first container
start via the `docker-entrypoint-initdb.d` mechanism. It creates the
`partman` schema explicitly before installing pg_partman, ensuring the
extension installs into the correct schema. It also sets the database
`search_path` to include `public, partman`, allowing DDL scripts to call
`partman.create_parent()` without schema-qualifying every reference.

The schema DDL script (`schema.sql`) runs after extension initialisation,
creating all tables, indexes, partitions, and pg_partman configuration
in dependency order.

### 9.3 Transaction Annotation Strategy

All Spring service methods that perform writes are annotated
`@Transactional`. All service methods that only perform reads are annotated
`@Transactional(readOnly = true)`. This distinction is currently
informational but becomes operationally significant when a read replica is
added — the routing datasource (described in Section 10) uses this
annotation to direct read-only transactions to the replica and write
transactions to the primary.

---

## 10. Future Considerations at Scale

The following capabilities are recommended for consideration when the
pipeline reaches production scale. They are not required for initial
deployment but are architecturally compatible with the current design and
require no breaking changes to implement.

### 10.1 PostgreSQL Read Replica

At moderate to high transaction volumes, analytical queries against the
cold tier and summary table reads from the dashboards will begin to
compete with the Camel processor's write throughput. PostgreSQL streaming
replication provides a continuously-updated replica that receives all
committed changes from the primary within milliseconds.

The recommended implementation uses Spring Boot's `AbstractRoutingDataSource`
to maintain two connection pools — one to the primary for writes and one
to the replica for reads. Routing is determined automatically by the
presence or absence of `@Transactional(readOnly = true)` on the calling
service method. No processor or service code changes are required beyond
ensuring transaction annotations are correctly applied, which is already
the case in the current implementation.

The replica would be exposed on a different host port (e.g. `5433`) while
the primary remains on `5432`. Dashboard API endpoints would be configured
to use the read datasource, completely isolating analytical query load
from the ingestion write path.

### 10.2 PgBouncer Connection Pooling

At high Camel consumer concurrency (multiple parallel routes, high
partition counts), the number of database connections opened by HikariCP
connection pools can exceed PostgreSQL's `max_connections` limit. Each
PostgreSQL connection consumes approximately 5-10 MB of server memory,
and context switching between many connections degrades throughput.

PgBouncer is a lightweight connection pooler that sits between the
application and PostgreSQL. In transaction pooling mode, a database
connection is held only for the duration of a single transaction and
immediately returned to the pool, allowing thousands of application
connections to share a much smaller pool of actual PostgreSQL connections.

PgBouncer would be deployed as an additional Docker container, with the
Spring Boot application connecting to PgBouncer's port rather than
PostgreSQL directly. PostgreSQL's `max_connections` can then be reduced
to a value appropriate for the actual workload rather than sized for peak
concurrency. Typical production configurations use 20-50 PostgreSQL
connections behind PgBouncer serving hundreds of application-level
connections.

PgBouncer and the read replica are complementary: PgBouncer manages
connection count efficiency, the replica manages read/write isolation.
Both can be added independently without affecting the other.

### 10.3 Partition Management Automation

The cold tier quarterly partitions are currently created manually in the
schema DDL script. At scale, this should be automated using a scheduled
job (either via pg_cron or the Spring Boot scheduler) that detects the
current quarter and pre-creates the next quarter's partition 30 days in
advance. This eliminates the operational risk of a missing cold tier
partition causing promotion job failures at the turn of each quarter.

### 10.4 Dead Letter Queue Dashboard

The `dead_letter_events` table provides all the data needed for a
self-service DLQ management interface. A simple REST API exposing
`GET /admin/dlq?status=PENDING`, `POST /admin/dlq/{id}/replay`, and
`POST /admin/dlq/{id}/discard` would allow operations teams to triage
and resolve failed events without requiring direct database access. At
scale, DLQ event volume becomes a meaningful operational metric and
benefits from visual tooling.

---

## 11. Key Design Decisions

| Decision                         | Choice                           | Rationale                                                              |
| -------------------------------- | -------------------------------- | ---------------------------------------------------------------------- |
| Integration framework            | Apache Camel                     | Declarative DSL, native Kafka and bean registry integration            |
| One route per topic              | Yes                              | Independent lifecycle, testability, and scaling                        |
| Hot tier partition interval      | 6 hours                          | 16 tables steady state; optimal planner overhead vs eviction precision |
| Cold tier partition interval     | Quarterly                        | 4 partitions/year; efficient date-range pruning                        |
| Summary update strategy          | Upsert on every write            | No refresh lag; aggregation cost at insert not query time              |
| Partition drop strategy          | DROP partition (not DELETE rows) | Milliseconds; no VACUUM; no bloat                                      |
| FK constraints                   | None (soft references)           | CDC events arrive out of order; hard FKs reject valid events           |
| Jackson unknown fields           | Ignored                          | CBS schema evolution must not break consumers                          |
| pg_partman BGW                   | Not used                         | Spring @Scheduled provides observable, testable scheduling             |
| shared_preload_libraries         | Runtime -c argument              | Image-build-time config causes initdb failure                          |
| DLQ strategy                     | Kafka topic + PostgreSQL table   | Durable storage; queryable audit trail; replayable                     |
| JPA vs JdbcTemplate for hot tier | JdbcTemplate                     | JPA RETURNING id incompatible with partitioned tables                  |
| Transaction annotations          | @Transactional / readOnly        | Prepares for routing datasource without future code changes            |
| Simulator connectivity           | Join broker Docker network       | Required to resolve advertised listener hostname                       |

---

## 12. Operational Runbook Reference

### Running the schema

The complete schema is defined in `schema.sql`. It is idempotent (`CREATE
IF NOT EXISTS`) and safe to re-run. It must be run after
`init-extensions.sql` has executed (which happens automatically on first
Docker container start). For a fresh environment, both files run
automatically via the `docker-entrypoint-initdb.d` mechanism.

### Resetting the environment

Stopping containers with `docker compose down -v` wipes all volumes
including the PostgreSQL data directory. The next `docker compose up`
will re-run all init scripts and produce a clean database. This is the
recommended approach during development.

### Checking pipeline health

The `PipelineMonitorJob` logs health status every 15 minutes. The
following database queries provide immediate health visibility without
application logs: hot tier row count, default partition row count,
unpromoted rows older than 78 hours, summary table last-updated
timestamps, and DLQ pending count. These queries are documented in
the `DOCKER_README.md`.

### Adding a new quarter to the cold tier

Execute `CREATE TABLE IF NOT EXISTS transactions_history_{YEAR}_q{N}
PARTITION OF transactions_history FOR VALUES FROM ('{start}') TO ('{end}')`
before the start of the new quarter. This is a metadata operation and
completes in milliseconds with no impact on running queries.

---

_ Technology — Data Engineering_
_Document maintained by the CDC Platform team_
