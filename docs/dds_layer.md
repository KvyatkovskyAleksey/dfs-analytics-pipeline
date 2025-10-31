# DDS Layer (Data Detail Storage)

## Overview

The DDS (Data Detail Storage) layer transforms raw staging data into normalized, structured tables optimized for analytics. This layer applies data quality rules, type conversions, and structural transformations while storing data in Parquet format for efficient querying.

## Daily DDS DAG

**Location**: `src/dags/daily_dds_dag.py`

### Purpose
Processes staging layer data into structured DDS tables using DuckDB for transformation and S3 for storage. The DAG runs daily with catchup enabled to process historical data.

### Workflow

```
For each sport (NFL):
  For each table (players, users_lineups, lineups):
    ├─ Check if date already processed
    │  ├─ If YES → Skip processing
    │  └─ If NO → Continue to processing
    ├─ Transform staging data to DDS format
    │  ├─ Extract and flatten nested JSON
    │  ├─ Apply type conversions (INTEGER, DOUBLE, etc.)
    │  ├─ Handle dynamic schemas (position columns)
    │  └─ Write to Parquet with Snappy compression
    └─ Mark date as processed
```

### Schedule Configuration
- **Schedule**: `@daily` (runs once per day)
- **Start Date**: September 1, 2025
- **Catchup**: Enabled (processes historical dates)
- **Max Active Runs**: 1 (prevents parallel execution)
- **Target Date Logic**: Processes data for `logical_date - 2 days` (allows staging data to settle)
- **Sequential Processing**: Tables processed one at a time to manage memory (24GB per task)

### Key Features
- **Idempotency**: Uses `DateTracker` to track processed dates and avoid reprocessing
- **Retry Logic**: 2 retries with 5-minute delay
- **Memory Management**: Sequential execution to stay within 48GB worker limit
- **Dynamic Schema**: Adapts to different position structures (Classic vs Single Game)

## DDS Processor

**Location**: `src/scripts/dds/dds_processor.py`

### Architecture

```
Staging Layer (JSONL gzip)
    ↓
DuckDB (SQL transformations)
    ↓
DDS Layer (Parquet Snappy)
```

### S3 Storage Structure

```
s3://{BUCKET_NAME}/dds/{SPORT}/
├── players/{GAME_TYPE}/{DATE}/data.parquet
├── users/{GAME_TYPE}/{DATE}/data.parquet
├── user_lineups/{GAME_TYPE}/{DATE}/data.parquet
└── lineups/{GAME_TYPE}/{DATE}/data.parquet
```

### File Format
- **Format**: Parquet (columnar storage)
- **Compression**: Snappy (fast compression/decompression)
- **Benefits**:
  - 10-100x smaller than JSON
  - Efficient columnar querying
  - Schema enforcement
  - Fast aggregations

## Data Transformations

### 1. Players Table

**Source**: `staging/{SPORT}/contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz`

**Transformation Logic**:
```sql
SELECT DISTINCT
    key as player_key,
    playerId as player_id,
    firstName, lastName, fullName,
    salary, position, rosterPosition,
    currentTeam, currentTeamId,
    eventId, eventTeamId,
    homeVisitor, favDog,
    projPoints, ownership, actualPoints,
    statDetails, madeCut
FROM staging_data
CROSS JOIN json_each(players)
```

**Purpose**: Normalize player performance across contests
- Player identification (ID, name, position)
- Salary and team context
- Projected vs actual performance
- Ownership percentages
- Game context (home/away, favorite/underdog)

**Key Fields**:
- `player_id`: Unique player identifier
- `salary`: DFS salary for this slate
- `proj_points`: Projected fantasy points
- `actual_points`: Actual fantasy points scored
- `ownership`: Percentage of lineups containing this player
- `stat_details`: Raw game statistics (JSON)

### 2. Users Table

**Source**: `staging/{SPORT}/contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz`

**Transformation Logic**:
```sql
SELECT DISTINCT
    contest_id, user_id,
    total_players, total_rosters, unique_rosters,
    max_exposure, lineups_cashing,
    lineups_in_percentile_1, ..., lineups_in_percentile_50,
    total_entry_cost, total_winning, roi
FROM staging_data
CROSS JOIN json_each(users)
```

**Purpose**: Analyze user performance and strategy
- User contest participation metrics
- Lineup diversity (unique vs total rosters)
- Player exposure management
- Success metrics (cashing lineups, percentile performance)
- ROI calculation

**Key Fields**:
- `user_id`: Unique user identifier
- `total_rosters`: Number of lineups entered
- `unique_rosters`: Number of distinct lineup constructions
- `max_exposure`: Highest player exposure percentage
- `lineups_cashing`: Number of profitable lineups
- `roi`: Return on investment percentage

### 3. User Lineups Table

**Source**: `staging/{SPORT}/contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz`

**Transformation Logic**:
```sql
SELECT
    contest_id, user_id, lineup_hash, lineup_ct
FROM staging_data
CROSS JOIN json_each(users)
CROSS JOIN json_each(user.lineups)
```

**Purpose**: Link users to their specific lineup constructions
- User-lineup relationships
- Lineup duplication tracking (lineup_ct)

**Key Fields**:
- `lineup_hash`: Unique identifier for lineup composition
- `lineup_ct`: Number of times user entered this lineup

### 4. Lineups Table

**Source**: `staging/{SPORT}/lineups/{GAME_TYPE}/{DATE}/data.json.gz`

**Transformation Logic**:
```sql
SELECT
    contest_id, lineup_hash, lineup_ct, lineup_user_ct,
    points, total_salary,
    total_own, min_own, max_own, avg_own,
    lineup_rank, is_cashing, payout, lineup_percentile,
    favorite_ct, underdog_ct, home_ct, visitor_ct,
    correlated_players,

    -- Dynamic position columns (varies by game type)
    pos_qb, pos_rb1, pos_rb2, pos_wr1, pos_wr2, pos_wr3,
    pos_te, pos_flex, pos_dst,  -- Classic

    -- OR

    pos_cpt, pos_flex1, pos_flex2, pos_flex3, pos_flex4, pos_flex5,  -- Single Game

    -- Complex JSON objects
    team_stacks, game_stacks, lineup_trends, entry_name_list
FROM staging_data
```

**Purpose**: Detailed lineup composition and performance analysis
- Roster construction (player IDs by position)
- Salary and ownership metrics
- Performance metrics (points, rank, payout)
- Stacking analysis (team/game correlations)
- Cashing status

**Key Fields**:
- `lineup_hash`: Unique lineup identifier
- `points`: Total fantasy points scored
- `total_salary`: Total salary used ($50,000 max typically)
- `avg_own`: Average ownership across all players
- `is_cashing`: Boolean indicating profitability
- `lineup_percentile`: Performance percentile in contest
- `pos_*`: Player ID for each roster position (dynamic columns)
- `team_stacks`/`game_stacks`: Correlation structures (JSON)

### Dynamic Position Handling

The processor automatically detects and creates position columns based on the data:

**Classic Slate Positions**:
- QB, RB1, RB2, WR1, WR2, WR3, TE, FLEX, DST

**Single Game Positions**:
- CPT (Captain - 1.5x points), FLEX1-5

This allows the same processor to handle multiple contest formats without schema changes.

## Technical Implementation

### DuckDB Configuration
```python
MAX_OBJECT_SIZE = 256 * 1024 * 1024  # 256MB for large JSON objects
```

Large JSON objects (lineups with hundreds of thousands of entries) require increased memory limits for parsing.

### S3 Integration
DuckDB directly reads from and writes to S3:
- **Read**: `read_json_auto('s3://...')`
- **Write**: `COPY (...) TO 's3://...' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`

No local staging required - data streams from S3 to S3.

### Processing Patterns

#### Nested JSON Extraction
```sql
json_each(players) as kv
kv.value->>'playerId' as player_id
```

#### Type Casting
```sql
(kv.value->>'salary')::INTEGER
(kv.value->>'ownership')::DOUBLE
```

#### Temporary Tables
Used for complex multi-step transformations:
```sql
CREATE OR REPLACE TABLE tmp_table AS ...
SELECT FROM tmp_table ...
```

## Data Quality

### Validation Checks
- **Existence Check**: Verify staging files exist before processing
- **DISTINCT**: Remove duplicate player records across contests
- **Type Safety**: Explicit type casting prevents silent errors

### Error Handling
- Logs warnings when staging files missing
- Skips processing for missing dates
- Continues to next table on failure (sequential processing)

## Performance Optimization

### Memory Management
- **Sequential Processing**: Process one table at a time
- **Streaming**: DuckDB streams data from S3 without full memory load
- **Parquet**: Columnar storage enables column-level reading

### Storage Efficiency
- **Compression**: Snappy provides ~5-10x compression with fast decompression
- **Partitioning**: Date-based partitions enable efficient time-range queries
- **Schema Optimization**: Proper types (INTEGER vs TEXT) reduce storage

## Configuration

Required environment variables:
```bash
WASABI_ENDPOINT=s3.us-east-2.wasabisys.com
WASABI_ACCESS_KEY=<your-access-key>
WASABI_SECRET_KEY=<your-secret-key>
WASABI_BUCKET_NAME=<your-bucket-name>
```

## Monitoring

Track processing success via:
- **Metadata Files**: `s3://{BUCKET_NAME}/dds/metadata/{SPORT}/{TABLE}/processed_dates.json`
- **Airflow Logs**: Task execution logs show SQL queries and row counts
- **Data Validation**: Check Parquet file sizes and record counts

## Next Steps

After DDS processing, data flows to:
1. **Data Marts** (aggregated analytics views)
2. **Superset** (visualization and dashboards)
3. **ML Models** (predictive analytics)

See respective documentation for downstream usage patterns.
