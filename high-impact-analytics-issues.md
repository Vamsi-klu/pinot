# High-Impact Analytics and Data Engineering Issues

Shortlist of **5 open [Apache Pinot](https://github.com/apache/pinot) issues** that sit on the analytics / data-engineering path and would have **high impact if received and completed**.

Selected from ~1,100 open issues on [apache/pinot](https://github.com/apache/pinot) as of **2026-08-19**. Ranking used community signal (reactions, comments, in-progress work), production pain, and how much a landed fix would change query or ingestion behavior.

| # | Issue | Area | Why it is high impact | Signal |
|---|---|---|---|---|
| 1 | [#8618](https://github.com/apache/pinot/issues/8618) Query Processing Resiliency and Workload Isolation | Analytics / query runtime | Stops noisy-neighbor queries from degrading a shared cluster | 11 reactions, 15 comments, `in-progress` |
| 2 | [#12080](https://github.com/apache/pinot/issues/12080) Parallel combine and disk spill for GROUP BY | Analytics / aggregations | Makes large GROUP BY queries finish instead of OOM or trim | 11 comments, `performance` |
| 3 | [#12057](https://github.com/apache/pinot/issues/12057) Partitioned Aggregation Support | Analytics / aggregations | Turns partition-aware distinct-count into a cheap local sum | Design-review, recently refreshed |
| 4 | [#11954](https://github.com/apache/pinot/issues/11954) INSERT INTO API support | Data engineering / ingestion | Push-based ingest without Kafka; ClickHouse/ES parity | Open PR [#17939](https://github.com/apache/pinot/pull/17939) |
| 5 | [#17535](https://github.com/apache/pinot/issues/17535) UPSERT for OFFLINE dimension tables | Data engineering / modeling | Overwrite dim-table rows by PK instead of failing or duplicating | Opened 2026; still unsolved |

---

## 1. Query Processing Resiliency and Workload Isolation

- **Issue:** [apache/pinot#8618](https://github.com/apache/pinot/issues/8618)
- **Opened:** 2022-04-29 by [@vvivekiyer](https://github.com/vvivekiyer)
- **Updated:** 2026-03-20
- **Labels:** `feature`, `in-progress`
- **Reactions / comments:** 11 / 15
- **Design doc:** [Server selection](https://docs.google.com/document/d/1w8YVpKIj0S62NvwDpf1HgruwxJYJ6ODuKQLjGXupH8w/edit?usp=sharing)

### Problem

Pinot's broker currently picks one server per segment with a lightweight round-robin. There is no fair scheduling by query cost or priority, pre-emption is mostly timeout-based, and rate limiting is QPS-only. One expensive `TEXT_MATCH` or a slow/GC-heavy server can degrade every other query that shares those servers.

This is the **noisy-neighbor** problem in both shared-tenant and dedicated-tenant clusters.

### Scope called out in the issue

1. **Smarter server selection** — use latency, in-flight query count, query cost, server load, and heterogeneous server capability instead of round-robin.
2. **Fair query scheduling** — isolate high / medium / low cost classes at broker and server so expensive work cannot starve cheap interactive queries.
3. **Pre-emption** — cancel work on timeout **and** memory pressure, on both broker and server.
4. **Richer rate limiting** — QoS based on load and latency, not only QPS.
5. **Server circuit breaker** — take failing servers out of rotation and bring them back adaptively (builds on [#8490](https://github.com/apache/pinot/issues/8490)).
6. **Workload management** — protect high-sensitivity queries from occasional heavy scans on the same table.

### Why this is high impact

Every analytics platform that shares a cluster hits this. Stripe engineers commented they were already prototyping broker rate limiting (TCP Vegas–style) for the same reason. LinkedIn/community discussion is long-running and a prototype / PR path already exists. Landing even one slice (latency-aware routing or query-class isolation) is a production reliability win that users feel immediately.

### Suggested entry points

- Broker instance selectors and adaptive routing (related open work: [#18947](https://github.com/apache/pinot/pull/18947), [#18791](https://github.com/apache/pinot/pull/18791)).
- Server `PriorityScheduler` and query thread pools.
- Existing circuit-breaker / health-score hooks.

---

## 2. Parallel Combine and Disk Spill for GROUP BY

- **Issue:** [apache/pinot#12080](https://github.com/apache/pinot/issues/12080)
- **Opened:** 2023-12-01
- **Updated:** 2026-08-14
- **Labels:** `performance`, `feature`, `stale`
- **Comments:** 11
- **Related:** [apache/pinot#11706](https://github.com/apache/pinot/issues/11706) (non-deterministic GROUP BY without ORDER BY)

### Problem

`GroupByExecutor` is missing two capabilities that Druid's GroupBy V2 engine has:

1. **Spill to disk** for the merging buffer when the group key space does not fit in memory ([Druid `ParallelCombiner`](https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/ParallelCombiner.java)).
2. **Parallel combine** of sorted aggregation results via a combining thread tree ([Druid `SpillingGrouper`](https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/SpillingGrouper.java)).

Today Pinot:

- Builds a non-combine operator **per segment** and runs those on the query worker pool.
- Combines results on a **single thread** in the query runner pool (`BaseSingleBlockCombineOperator`).

That single-thread combine is a scalability cliff. Without spill, large-cardinality GROUP BY either OOMs, hits `safeTrim` / group limits, or returns incomplete / non-deterministic results when there is no `ORDER BY`.

### Why this is high impact

GROUP BY is the default analytics shape (funnels, unique users, rollups, cubes). Disk spill is the difference between "this dashboard query works at 10x data" and "we have to pre-aggregate offline." Parallel combine is a direct latency win on the leaf stage and can be reused by multi-stage engine (MSE) non-leaf aggregation.

### Suggested entry points

- `pinot-core` combine operators: `BaseCombineOperator`, `BaseSingleBlockCombineOperator`, `GroupByCombineOperator`.
- `InstancePlanMakerImplV2` per-segment operator construction.
- MSE leaf / non-leaf aggregation (issue explicitly calls this out).
- Compare with existing `safeTrim` work ([#16621](https://github.com/apache/pinot/pull/16621)) so spill and trim do not fight each other.

---

## 3. Partitioned Aggregation Support

- **Issue:** [apache/pinot#12057](https://github.com/apache/pinot/issues/12057)
- **Opened:** 2023-11-28
- **Updated:** 2026-08-18
- **Labels:** `enhancement`, `design-review`, `performance`, `feature`, `stale`
- **Reactions / comments:** 2 / 6

### Problem

If a column is partitioned, some aggregations can be computed **per partition** and then cheaply merged. Distinct count is the headline case: count uniques inside each partition, then sum those counts.

Pinot already has [`SEGMENTPARTITIONEDDISTINCTCOUNT`](https://docs.pinot.apache.org/configuration-reference/functions/segmentpartitioneddistinctcount), but it only works when **an entire partition fits in one segment**. That is rare for time-based tables, so most production tables cannot use it.

The issue proposes enabling the optimization under [partitioned replica-group segment assignment](https://docs.pinot.apache.org/operators/operating-pinot/segment-assignment#partitioned-replica-group-segment-assignment), where all segments for a partition are co-located on one server.

### Proposed approach (from the issue)

Keep per-segment aggregation unchanged. Change **inter-segment combine** so it first merges inside a partition, then merges across partitions.

Two API sketches are discussed. One adds a partitioned result type and new merge methods:

```text
boolean isPartitionedAggregation();
PartitionedResult extractAggregationResult(...);
PartitionedResult extractGroupByResult(...);
PartitionedResult mergeWithinPartition(...);
IntermediateResult extractPartitionResult(...);
```

Flow:

1. Aggregate blocks within a segment as today.
2. Extract a partitioned result.
3. Merge across segments **within the same partition**.
4. Extract the intermediate result per partition.
5. Merge across partitions with the existing `merge`.
6. Extract the final result.

Hot classes: `AggregationCombineOperator`, `GroupByCombineOperator`, `AggregationResultsBlockMerger`.

### Why this is high impact

Exact distinct count and similar aggregations are the most expensive dashboard queries. A correct partitioned path can drop them from "global sketch / full hash table" to "local count + sum" whenever the table is already partition-assigned — a common production layout. The design is already written; the remaining work is implementation and correctness tests (nulls, multi-value, hybrid tables, MSE).

### Suggested entry points

- Combine / results-block merger path above.
- Segment metadata that exposes partition id to the combine operator.
- Existing `SEGMENTPARTITIONEDDISTINCTCOUNT` as a reference, then generalize beyond the single-segment assumption.
- Related: [#19302](https://github.com/apache/pinot/pull/19302) (multi-value DISTINCTCOUNTULL / SEGMENTPARTITIONEDDISTINCTCOUNT).

---

## 4. INSERT INTO API Support

- **Issue:** [apache/pinot#11954](https://github.com/apache/pinot/issues/11954)
- **Opened:** 2023-11-06
- **Updated:** 2026-08-15
- **Labels:** `ingestion`, `rest-api`, `feature`, `stale`
- **Sibling issue:** [apache/pinot#11914](https://github.com/apache/pinot/issues/11914) — extend the file/URI ingest REST API to **realtime** tables (`help wanted`, `good first issue`).
- **Active PR:** [apache/pinot#17939](https://github.com/apache/pinot/pull/17939) — "Add push-based INSERT INTO support" (updated 2026-08-18).

### Problem

Realtime ingest today assumes a stream (Kafka / Kinesis / Pulsar). Offline ingest is segment push. There is no first-class **`INSERT INTO`** path that lets applications or pipelines push rows through SQL/REST the way ClickHouse and Elasticsearch do.

[#11914](https://github.com/apache/pinot/issues/11914) only covers ingesting a **file/URI into a realtime table**. [#11954](https://github.com/apache/pinot/issues/11954) is the broader product: push-based row ingest, including:

- Interface layer for messages arriving over an API.
- Partitioning incoming rows for upsert tables (or APIs that tell the client which server owns the key).
- Small-segment explosion (needs merge/rollup and upsert compaction to stay healthy).

### Why this is high impact

This is a **migration and platform** feature. Teams that do not want to stand up Kafka for modest or bursty write paths cannot use Pinot as a general analytics store. Closing the INSERT gap:

- Unlocks application-driven and batch-job-driven ingest.
- Reduces feature disparity vs ClickHouse / ES.
- Complements existing minion merge-rollup and upsert compaction.

There is already a substantial open PR ([#17939](https://github.com/apache/pinot/pull/17939)). Receiving this issue likely means **reviewing, finishing, and hardening that PR** rather than starting from zero.

### Suggested entry points

- Review and land [#17939](https://github.com/apache/pinot/pull/17939).
- Smaller on-ramp: [#11914](https://github.com/apache/pinot/issues/11914) realtime file/URI ingest API.
- Controller ingest REST endpoints and realtime segment assignment.
- Upsert routing: who owns the primary key when the writer is a push API, not a Kafka partition.

---

## 5. UPSERT Semantics for OFFLINE Dimension Tables

- **Issue:** [apache/pinot#17535](https://github.com/apache/pinot/issues/17535)
- **Opened:** 2026-01-20
- **Updated:** 2026-08-08
- **Labels:** `feature`, `stale`
- **Comments:** 4

### Problem

OFFLINE **dimension tables** (the lookup / broadcast side of star-schema analytics) do not support UPSERT.

Current options:

- `APPEND` is not supported for dimension tables.
- `REFRESH` re-reads the same input files and is a poor fit for incremental dim updates.
- Duplicate primary keys are either **silently kept** or **rejected** via:

```json
"dimensionTableConfig": {
  "errorOnDuplicatePrimaryKey": true
}
```

That flag (from [#12290](https://github.com/apache/pinot/pull/12290)) detects the problem. It does **not** overwrite the existing row.

### Expected behavior

Same mental model as realtime upsert tables:

- If an ingested row's primary key already exists, **overwrite** that row.
- Do not create a second row and do not fail the job.

### Why this is high impact

Dimension tables are how data engineers model users, accounts, products, and other slowly changing entities used in **lookup joins**. Without PK overwrite:

- Joins double-count or pick an arbitrary row.
- Pipelines must rebuild the entire dim table on every change.
- Teams either accept duplicates or fail ingest.

This is a classic warehouse UPSERT / MERGE gap. Closing it makes Pinot a realistic serving layer for SCD-style dims, not only append-only facts.

### Suggested entry points

- Dimension table config and `errorOnDuplicatePrimaryKey` path.
- Offline segment build / minion tasks that can replace a PK rather than append.
- Lookup-join and dimension-table serving on brokers/servers (must see one row per key after overwrite).
- Contrast with realtime upsert metadata managers — reuse the PK contract, not the stream-consumption machinery.

---

## How these five fit together

```text
                    ┌─────────────────────────────┐
                    │  Query / analytics runtime  │
                    │  #8618 isolation            │
                    │  #12080 GROUP BY spill      │
                    │  #12057 partitioned agg     │
                    └──────────────▲──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │  Tables used by those queries│
                    │  #17535 dim-table UPSERT     │
                    └──────────────▲──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │  How data gets in            │
                    │  #11954 INSERT INTO          │
                    │  (#11914 realtime file API)  │
                    └─────────────────────────────┘
```

Receiving **#8618 / #12080 / #12057** improves the **query engine** that analysts hit. Receiving **#11954 / #17535** improves the **data-engineering path** that fills the tables those queries read.

---

## Honorable mentions (not in the top 5)

| Issue | Why it was close |
|---|---|
| [#10712](https://github.com/apache/pinot/issues/10712) Logical tables | 30 comments and a design doc; **core logical-table support has already landed** in planner/runtime. Remaining value is atomic alias swap / table flip, not a green-field feature. |
| [#11914](https://github.com/apache/pinot/issues/11914) Realtime ingest REST API | Smaller, `help wanted` slice of #11954. Good first ingest PR if INSERT INTO is too large. |
| [#12448](https://github.com/apache/pinot/issues/12448) Flink connector enhancements | Real DE work (Flink upgrade, auth, types), but connector-scoped rather than cluster-wide. |
| [#12803](https://github.com/apache/pinot/issues/12803) Partial unnest of complex types | High leverage for nested Kafka/Avro pipelines; narrower than INSERT/UPSERT. |
| [#8837](https://github.com/apache/pinot/issues/8837) `ORDER BY sorted_col DESC LIMIT N` | Clear 3–4× per-segment win; more of a targeted performance bug than a platform feature. |

---

## Source

- Repository: [apache/pinot](https://github.com/apache/pinot)
- Snapshot date: 2026-08-19
- Method: GraphQL `issues(states: OPEN, orderBy: COMMENTS | UPDATED_AT)` plus label filters (`feature`, `query`, `ingestion`, `performance`, `multi-stage`)
