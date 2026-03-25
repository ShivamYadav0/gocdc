CREATE TABLE IF NOT EXISTS node_intervals (
    node_id String,
    down_at DateTime64(3),
    up_at DateTime64(3),
    duration_sec Float64
) ENGINE = MergeTree()
ORDER BY (node_id, down_at);


-- 1. Create a table to store daily aggregated uptime metrics
CREATE TABLE IF NOT EXISTS daily_node_stats (
    node_id String,
    day Date,
    total_down_sec AggregateFunction(sum, Float64),
    event_count AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (day, node_id);

-- 2. Create the Materialized View (The "Trigger")
-- This automatically updates 'daily_node_stats' whenever you insert into 'node_intervals'
CREATE MATERIALIZED VIEW IF NOT EXISTS v_daily_node_stats_mv
TO daily_node_stats
AS SELECT
    node_id,
    toDate(down_at) as day,
    sumState(duration_sec) as total_down_sec,
    countState() as event_count
FROM node_intervals
GROUP BY node_id, day;

-- 3. The SLA Query: Calculate Uptime % (e.g., for the last 24 hours)
-- Formula: (86400 - total_down_seconds) / 86400 * 100
CREATE VIEW IF NOT EXISTS node_sla_report AS
SELECT
    node_id,
    day,
    (1 - (sumMerge(total_down_sec) / 86400)) * 100 AS uptime_percentage,
    countMerge(event_count) AS total_incidents
FROM daily_node_stats
GROUP BY node_id, day;