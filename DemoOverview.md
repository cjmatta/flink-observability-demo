# Flink SQL Queries for Observability Demo

This document contains Flink SQL queries demonstrating Confluent's value for observability data integration and real-time analytics using Confluent Cloud.

## Overview

| Topic | Format | Purpose |
|-------|--------|---------|
| `logs-structured` | Clean JSON | Direct analytics, no parsing needed |
| `logs-syslog-raw` | RFC 3164 syslog strings | Parse with regex, extract facility/severity/host |
| `logs-nginx-raw` | Combined log format strings | Parse access logs, calculate latency percentiles |
| `logs-app-mixed` | Multiple inconsistent formats | Normalize heterogeneous logs into common schema |
| `telemetry-otel` | OTEL JSON | Join traces with logs, build service graphs |

---

## 1. Parse Raw Logs into Structured Data

### Parse Syslog (RFC 3164) → Structured Table

Input format: `<pri>timestamp hostname process[pid]: message`
Example: `<134>Dec 09 18:12:47 web-01 nginx[12345]: Connection from 192.168.1.1 port 443`

```sql
CREATE TABLE `logs-syslog-parsed` AS
WITH src AS (
  SELECT
    CAST(`val` AS STRING) AS s,
    `$rowtime` AS kafka_timestamp
  FROM `logs-syslog-raw`
)
SELECT
  kafka_timestamp AS event_time,
  CAST(REGEXP_EXTRACT(s, '<(\d+)>', 1) AS INT) AS priority,
  MOD(CAST(REGEXP_EXTRACT(s, '<(\d+)>', 1) AS INT), 8) AS severity,
  CAST(REGEXP_EXTRACT(s, '<(\d+)>', 1) AS INT) / 8 AS facility,
  REGEXP_EXTRACT(s, '>\s*(\w{3}\s+\d+\s+\d+:\d+:\d+)', 1) AS log_timestamp,
  REGEXP_EXTRACT(s, '\d+:\d+:\d+\s+(\S+)\s+', 1) AS hostname,
  REGEXP_EXTRACT(s, '\s+([\w-]+)\[', 1) AS process,
  CAST(REGEXP_EXTRACT(s, '\[(\d+)\]', 1) AS INT) AS pid,
  REGEXP_EXTRACT(s, '\]:\s+(.*)$', 1) AS message,
  'syslog' AS log_source
FROM src;
```

### Parse Nginx Combined Format → Structured Table

Input format: `ip - - [timestamp] "method path HTTP/1.1" status bytes "referer" "user-agent" response_time`
Example: `192.168.1.1 - - [09/Dec/2025:18:12:47 +0000] "GET /api/v1/users HTTP/1.1" 200 1234 "https://example.com/" "Mozilla/5.0..." 0.085`

```sql
CREATE TABLE `logs-nginx-parsed` AS
WITH src AS (
  SELECT
    CAST(`val` AS STRING) AS s,
    `$rowtime` AS kafka_timestamp
  FROM `logs-nginx-raw`
)
SELECT
  kafka_timestamp AS event_time,
  REGEXP_EXTRACT(s, '^(\S+)', 1) AS client_ip,
  REGEXP_EXTRACT(s, '\[([^\]]+)\]', 1) AS request_time,
  REGEXP_EXTRACT(s, '"(\w+)\s+', 1) AS http_method,
  REGEXP_EXTRACT(s, '"\w+\s+(\S+)', 1) AS request_path,
  CAST(REGEXP_EXTRACT(s, '"\s+(\d+)', 1) AS INT) AS status_code,
  CAST(REGEXP_EXTRACT(s, '"\s+\d+\s+(\d+)', 1) AS BIGINT) AS bytes_sent,
  REGEXP_EXTRACT(s, '"\s+\d+\s+\d+\s+"([^"]*)"', 1) AS referer,
  REGEXP_EXTRACT(s, '"([^"]+)"\s+[\d.]+$', 1) AS user_agent,
  CAST(REGEXP_EXTRACT(s, '\s+([\d.]+)$', 1) AS DOUBLE) AS response_time_sec,
  'nginx' AS log_source
FROM src;
```

### Parse Mixed App Logs (Multiple Formats)

Handles 3 different legacy log formats:
- Standard: `timestamp LEVEL [thread] class - message`
- Pipe-delimited: `LEVEL|timestamp|thread|class|message`
- Bracket: `[timestamp] LEVEL: class :: message`

```sql
CREATE TABLE `logs-app-mixed-parsed` AS
WITH src AS (
  SELECT
    CAST(`key` AS STRING) AS app,
    CAST(`val` AS STRING) AS s,
    `$rowtime` AS kafka_timestamp
  FROM `logs-app-mixed`
)
SELECT
  kafka_timestamp AS event_time,
  app AS application,
  -- Extract timestamp and convert
  TO_TIMESTAMP_LTZ(
    REGEXP_EXTRACT(s, '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', 1),
    'yyyy-MM-dd HH:mm:ss,SSS', 'UTC'
  ) AS ts,
  -- Handle different formats for level
  CASE 
    WHEN s LIKE '%|%|%|%' THEN REGEXP_EXTRACT(s, '^(\w+)\|', 1)
    WHEN s LIKE '[%' THEN REGEXP_EXTRACT(s, '\]\s+(\w+):', 1)
    ELSE REGEXP_EXTRACT(s, '^\S+\s+\S+\s+(\w+)\s+\[', 1)
  END AS level,
  -- Extract thread
  REGEXP_EXTRACT(s, '\[([^\]]+)\]', 1) AS thread,
  -- Extract class name
  CASE 
    WHEN s LIKE '%|%|%|%' THEN REGEXP_EXTRACT(s, '\|([^|]+)\|[^|]*$', 1)
    WHEN s LIKE '[%' THEN REGEXP_EXTRACT(s, ':\s+(\S+)\s+::', 1)
    ELSE REGEXP_EXTRACT(s, '\]\s+(\S+)\s+-', 1)
  END AS class_name,
  -- Extract message
  CASE 
    WHEN s LIKE '%|%|%|%' THEN REGEXP_EXTRACT(s, '\|([^|]*)$', 1)
    WHEN s LIKE '[%' THEN REGEXP_EXTRACT(s, '::\s+(.*)$', 1)
    ELSE REGEXP_EXTRACT(s, '-\s+(.*)$', 1)
  END AS message,
  -- Extract optional metrics from message
  CAST(REGEXP_EXTRACT(s, 'took\s+(\d+)ms', 1) AS BIGINT) AS duration_ms,
  CAST(REGEXP_EXTRACT(s, '\brows=(\d+)\b', 1) AS INT) AS `rows`,
  'app-legacy' AS log_source
FROM src;
```

---

## 2. Unified Log View (Schema Normalization)

Create a single unified view across all log sources with a common schema:

```sql
CREATE TABLE `logs-unified` AS

-- Structured app logs (already clean JSON)
SELECT
  TO_TIMESTAMP_LTZ(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ', 'UTC') AS event_time,
  `level` AS severity_label,
  service AS source_name,
  host AS hostname,
  message,
  http_status AS status_code,
  latency_ms,
  trace_id,
  'app-structured' AS log_type
FROM `logs-structured`

UNION ALL

-- Parsed syslog
SELECT
  event_time,
  CASE severity
    WHEN 0 THEN 'EMERGENCY'
    WHEN 1 THEN 'ALERT'
    WHEN 2 THEN 'CRITICAL'
    WHEN 3 THEN 'ERROR'
    WHEN 4 THEN 'WARN'
    WHEN 5 THEN 'NOTICE'
    WHEN 6 THEN 'INFO'
    WHEN 7 THEN 'DEBUG'
    ELSE 'UNKNOWN'
  END AS severity_label,
  process AS source_name,
  hostname,
  message,
  CAST(NULL AS INT) AS status_code,
  CAST(NULL AS INT) AS latency_ms,
  CAST(NULL AS STRING) AS trace_id,
  'syslog' AS log_type
FROM `logs-syslog-parsed`

UNION ALL

-- Parsed nginx access logs
SELECT
  event_time,
  CASE 
    WHEN status_code >= 500 THEN 'ERROR'
    WHEN status_code >= 400 THEN 'WARN'
    ELSE 'INFO'
  END AS severity_label,
  'nginx' AS source_name,
  CAST(NULL AS STRING) AS hostname,
  CONCAT(http_method, ' ', request_path) AS message,
  status_code,
  CAST(response_time_sec * 1000 AS INT) AS latency_ms,
  CAST(NULL AS STRING) AS trace_id,
  'nginx' AS log_type
FROM `logs-nginx-parsed`

UNION ALL

-- Parsed legacy app logs
SELECT
  event_time,
  level AS severity_label,
  application AS source_name,
  CAST(NULL AS STRING) AS hostname,
  message,
  CAST(NULL AS INT) AS status_code,
  CAST(duration_ms AS INT) AS latency_ms,
  CAST(NULL AS STRING) AS trace_id,
  'app-legacy' AS log_type
FROM `logs-app-mixed-parsed`;
```

---

## 3. Real-Time Analytics & Metrics

### Error Rate by Service (1-Minute Tumbling Window)

```sql
CREATE TABLE `metrics-error-rates` AS
SELECT
  window_start,
  window_end,
  service,
  COUNT(*) AS total_requests,
  SUM(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS server_errors,
  SUM(CASE WHEN http_status >= 400 AND http_status < 500 THEN 1 ELSE 0 END) AS client_errors,
  CAST(SUM(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS error_rate_pct
FROM TABLE(
  TUMBLE(
    TABLE `logs-structured`, 
    DESCRIPTOR(`$rowtime`), 
    INTERVAL '1' MINUTE
  )
)
GROUP BY window_start, window_end, service;
```

### Latency Percentiles by Service

```sql
CREATE TABLE `metrics-latency` AS
SELECT
  window_start,
  window_end,
  service,
  COUNT(*) AS request_count,
  CAST(AVG(latency_ms) AS INT) AS avg_latency_ms,
  MIN(latency_ms) AS min_latency_ms,
  MAX(latency_ms) AS max_latency_ms
FROM TABLE(
  TUMBLE(
    TABLE `logs-structured`, 
    DESCRIPTOR(`$rowtime`), 
    INTERVAL '1' MINUTE
  )
)
GROUP BY window_start, window_end, service;
```

### Request Volume by Endpoint (Nginx)

```sql
CREATE TABLE `metrics-endpoints` AS
SELECT
  window_start,
  window_end,
  request_path,
  http_method,
  COUNT(*) AS request_count,
  SUM(bytes_sent) AS total_bytes,
  CAST(AVG(response_time_sec * 1000) AS INT) AS avg_latency_ms,
  SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count
FROM TABLE(
  TUMBLE(TABLE `logs-nginx-parsed`, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, request_path, http_method;
```

### Log Volume by Source and Severity

```sql
SELECT
  window_start,
  log_type,
  severity_label,
  COUNT(*) AS log_count
FROM TABLE(
  TUMBLE(TABLE `logs-unified`, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, log_type, severity_label;
```

---

## 4. Alerting Streams

### High-Severity Alert Stream

```sql
CREATE TABLE `alerts-critical` AS
SELECT
  event_time,
  severity_label,
  source_name,
  hostname,
  message,
  status_code,
  trace_id,
  log_type,
  'CRITICAL_LOG' AS alert_type
FROM `logs-unified`
WHERE severity_label IN ('ERROR', 'CRITICAL', 'EMERGENCY', 'FATAL');
```

### High Error Rate Alert (Threshold-Based)

```sql
CREATE TABLE `alerts-error-rate` AS
SELECT
  window_end AS alert_time,
  service,
  error_rate_pct,
  total_requests,
  server_errors,
  CASE 
    WHEN error_rate_pct > 10 THEN 'CRITICAL'
    WHEN error_rate_pct > 5 THEN 'WARNING'
    ELSE 'INFO'
  END AS severity,
  'HIGH_ERROR_RATE' AS alert_type
FROM `metrics-error-rates`
WHERE error_rate_pct > 5;
```

### Latency SLA Breach Alert

```sql
CREATE TABLE `alerts-latency-sla` AS
SELECT
  window_end AS alert_time,
  service,
  max_latency_ms AS p99_latency_ms,
  avg_latency_ms,
  request_count,
  CASE 
    WHEN max_latency_ms > 5000 THEN 'CRITICAL'
    WHEN max_latency_ms > 2000 THEN 'WARNING'
    ELSE 'INFO'
  END AS severity,
  'LATENCY_SLA_BREACH' AS alert_type
FROM `metrics-latency`
WHERE max_latency_ms > 2000;  -- 2 second SLA threshold
```

---

## 5. Security & Anomaly Detection

### Failed SSH Attempts from Syslog

```sql
CREATE TABLE `security-ssh-failures` AS
SELECT
  window_start,
  window_end,
  hostname,
  COUNT(*) AS failure_count,
  CASE 
    WHEN COUNT(*) >= 10 THEN 'CRITICAL'
    WHEN COUNT(*) >= 5 THEN 'WARNING'
    ELSE 'INFO'
  END AS severity,
  'SSH_BRUTE_FORCE' AS alert_type
FROM TABLE(
  TUMBLE(TABLE `logs-syslog-parsed`, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
)
WHERE process = 'sshd' 
  AND message LIKE '%Failed password%'
GROUP BY window_start, window_end, hostname
HAVING COUNT(*) >= 3;
```

### HTTP Status Anomaly Detection

```sql
CREATE TABLE `security-http-anomalies` AS
SELECT
  window_start,
  window_end,
  status_code,
  COUNT(*) AS occurrences,
  CASE 
    WHEN COUNT(*) > 100 THEN 'CRITICAL'
    WHEN COUNT(*) > 50 THEN 'WARNING'
    ELSE 'INFO'
  END AS severity,
  CASE 
    WHEN status_code = 401 THEN 'UNAUTHORIZED_SPIKE'
    WHEN status_code = 403 THEN 'FORBIDDEN_SPIKE'
    WHEN status_code = 404 THEN 'NOT_FOUND_SPIKE'
    WHEN status_code >= 500 THEN 'SERVER_ERROR_SPIKE'
    ELSE 'CLIENT_ERROR_SPIKE'
  END AS alert_type
FROM TABLE(
  TUMBLE(TABLE `logs-nginx-parsed`, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE status_code >= 400
GROUP BY window_start, window_end, status_code
HAVING COUNT(*) > 10;
```

### Suspicious IP Activity (Multiple 4xx in Short Window)

```sql
CREATE TABLE `security-suspicious-ips` AS
SELECT
  window_start,
  window_end,
  client_ip,
  COUNT(*) AS total_requests,
  SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_requests,
  CAST(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS error_rate,
  'SUSPICIOUS_CLIENT' AS alert_type
FROM TABLE(
  TUMBLE(TABLE `logs-nginx-parsed`, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, client_ip
HAVING 
  COUNT(*) > 20 
  AND CAST(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) > 0.5;
```

---

## 6. OTEL Trace Analytics

### Extract Service Metrics from OTEL Spans

```sql
CREATE TABLE `telemetry-service-metrics` AS
WITH spans AS (
  SELECT
    `$rowtime` AS event_time,
    resourceSpans[1].resource.attributes[1].value.stringValue AS service_name,
    resourceSpans[1].scopeSpans[1].spans[1].name AS span_name,
    resourceSpans[1].scopeSpans[1].spans[1].status.code AS status_code,
    (resourceSpans[1].scopeSpans[1].spans[1].endTimeUnixNano - 
     resourceSpans[1].scopeSpans[1].spans[1].startTimeUnixNano) / 1000000 AS duration_ms
  FROM `telemetry-otel`
)
SELECT
  window_start,
  window_end,
  service_name,
  span_name,
  COUNT(*) AS span_count,
  SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS error_count,
  CAST(AVG(duration_ms) AS BIGINT) AS avg_duration_ms,
  MAX(duration_ms) AS max_duration_ms
FROM TABLE(
  TUMBLE(TABLE spans, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, service_name, span_name;
```

---

## 7. Demo Narrative

### Story Flow for Customer Presentation

1. **The Problem**: Show raw topics with heterogeneous formats
   - `logs-syslog-raw`: Syslog strings like `<134>Dec 09 18:12:47 web-01 nginx[12345]: ...`
   - `logs-nginx-raw`: Apache combined format
   - `logs-app-mixed`: Multiple legacy Java log formats
   - "Traditional approach: batch ETL, hours of delay, brittle regex"

2. **Flink Parsing**: Real-time transformation
   - Run the parsing queries in Confluent Cloud
   - Show structured output appearing in parsed topics within seconds
   - "Sub-second transformation, no batch jobs, no infrastructure to manage"

3. **Unified View**: Single schema across all sources
   - Query `logs-unified` table
   - "One query, all your logs, regardless of source format"

4. **Real-Time Analytics**: Instant insights
   - Show `metrics-error-rates` and `metrics-latency` topics
   - "No waiting for batch jobs, metrics update continuously"

5. **Alerting**: Proactive detection
   - Show alerts appearing in `alerts-critical`
   - "Detect issues in seconds, not hours"

6. **Downstream Integration**: Fan-out to any sink
   - Connect to Elasticsearch, Snowflake, S3 via managed connectors
   - No code required

### Key Value Props to Emphasize

| Traditional Approach | Confluent Cloud + Flink |
|---------------------|-------------------------|
| Batch ETL (hourly/daily) | Real-time streaming |
| Separate tools for each log type | Unified processing in SQL |
| Schema drift breaks pipelines | Schema Registry governance |
| Point-to-point integrations | Central streaming platform |
| Delayed alerting | Sub-second detection |
| Infrastructure to manage | Fully managed serverless |

---

## Quick Start

1. **Start data generation**:
   ```bash
   op run --env-file=.env -- ./run_shadowtraffic.sh config.json
   ```

2. **Create parsing tables** in Confluent Cloud Flink SQL workspace

3. **Run analytics queries** to see real-time metrics

4. **Connect downstream sinks** via Confluent Cloud connectors
