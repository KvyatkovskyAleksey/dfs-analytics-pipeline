# Staging Layer

## Overview

The staging layer is the entry point of the DFS analytics pipeline. It scrapes raw data from Rotogrinders/FantasyLabs and stores it in S3 in a structured format. This layer maintains data in its original form with minimal transformation, serving as a historical record of DFS contests, lineups, and player performance.

## Daily Scraping DAG

**Location**: `src/dags/daily_scraping_dag.py`

### Purpose
Orchestrates daily data collection from Rotogrinders for NFL contests. The DAG runs daily with catchup enabled to backfill historical data.

### Workflow

```
For each sport (NFL):
  ├─ Check if date already scraped
  │  ├─ If YES → Skip scraping
  │  └─ If NO → Continue to scraping
  ├─ Scrape data from Rotogrinders
  │  ├─ Draft groups (contest types and slates)
  │  ├─ Contests (tournament details)
  │  ├─ Events (NFL games/matchups)
  │  ├─ Contest analyze data (player stats, ownership, results)
  │  └─ Lineups (roster constructions)
  └─ Mark date as scraped
```

### Schedule Configuration
- **Schedule**: `@daily` (runs once per day)
- **Start Date**: September 1, 2025
- **Catchup**: Enabled (processes historical dates)
- **Max Active Runs**: 1 (prevents parallel execution)
- **Target Date Logic**: Scrapes data for `logical_date - 1 day` (previous day's contests)

### Key Features
- **Idempotency**: Uses `DateTracker` to track processed dates and avoid duplicate scraping
- **Retry Logic**: 2 retries with 5-minute delay between attempts
- **Proxy Support**: Optional proxy rotation to avoid rate limiting (via `PROXY_URL` env variable)

## Staging Processor

**Location**: `src/scripts/staging/rotogrinders_processor.py`

### Responsibilities
Transforms scraped data into JSONL format and uploads to S3 with gzip compression.

### Data Flow

```
RotogrindersScraper (raw data)
    ↓
RotogrindersStagingProcessor
    ↓
S3 Storage (JSONL + gzip)
```

### S3 Storage Structure

```
s3://{BUCKET_NAME}/staging/{SPORT}/
├── draft_groups/{DATE}/data.json.gz
├── contests/{GAME_TYPE}/{DATE}/data.json.gz
├── events/{GAME_TYPE}/{DATE}/data.json.gz
├── contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz
└── lineups/{GAME_TYPE}/{DATE}/data.json.gz
```

Where:
- `{SPORT}`: NFL, NBA, MLB, etc.
- `{GAME_TYPE}`: `dk_classic` (traditional DFS) or `dk_single_game` (showdown format)
- `{DATE}`: ISO format date (YYYY-MM-DD)

### File Format
- **Format**: JSONL (JSON Lines) - one JSON object per line
- **Compression**: gzip
- **Timestamp**: Each record includes `load_ts` timestamp for data lineage tracking

### Data Sources

#### 1. Draft Groups
**API**: `https://service.fantasylabs.com/contest-sources/?sport_id={SPORT_ID}&date={DATE}`

Contains slate and contest type metadata:
- Contest sources (DraftKings Classic, Single Game)
- Draft groups (Main, Afternoon, Evening slates)
- Contest start times

#### 2. Contests
**API**: `https://service.fantasylabs.com/live-contests/?sport={SPORT}&contest_group_id={SLATE_ID}`

Contest-level information:
- Contest ID, name, entry fees
- Prize pool structure
- Number of entries
- Contest status

#### 3. Events
**API**: `https://service.fantasylabs.com/live/events/?sport_id={SPORT_ID}&contest_group_id={SLATE_ID}`

NFL game information:
- Team matchups
- Home/away teams
- Game start times
- Vegas lines (spreads, totals)

#### 4. Contest Analyze Data
**API**: `https://dh5nxc6yx3kwy.cloudfront.net/contests/{sport}/{date}/{contest_id}/data/`

Player-level contest analytics:
- Player salaries and positions
- Projected points vs actual points
- Ownership percentages
- Team stacking information
- User performance metrics

#### 5. Lineups
**API**: `https://dh5nxc6yx3kwy.cloudfront.net/contests/{sport}/{date}/{contest_id}/lineups/`

Individual lineup data:
- Lineup composition (player IDs by position)
- Total salary used
- Points scored
- Ownership metrics (min, max, avg)
- Cashing status
- User information

## Processing Logic

### Scraping Flow (RotogrindersScraper)

1. **Draft Groups**: Fetch available slates for the target date
2. **Iterate Slates**: For each slate matching the date:
   - Scrape contests
   - Scrape events (game matchups)
   - For each contest:
     - Scrape contest analyze data (player stats)
     - Scrape lineups (roster constructions)

### Error Handling
- **Request Retries**: Up to 3 attempts per API call
- **Proxy Rotation**: Automatic proxy switching on connection failures
- **Rate Limiting**: Random 3-5 second delays when proxies not configured
- **Graceful Skipping**: Logs warnings and continues if data unavailable

### Configuration

Required environment variables:
```bash
WASABI_ENDPOINT=s3.us-east-2.wasabisys.com
WASABI_ACCESS_KEY=<your-access-key>
WASABI_SECRET_KEY=<your-secret-key>
WASABI_BUCKET_NAME=<your-bucket-name>
PROXY_URL=<optional-proxy-endpoint>  # Optional for rate limit avoidance
```

## Data Partitioning Strategy

Data is partitioned by:
1. **Sport** (NFL, NBA, etc.)
2. **Game Type** (dk_classic, dk_single_game)
3. **Date** (YYYY-MM-DD)

This structure enables:
- Efficient date-based queries
- Separate processing of different contest formats
- Easy data lifecycle management (archival/deletion)

## Monitoring

Track scraping success via:
- **Metadata Files**: `s3://{BUCKET_NAME}/staging/metadata/{SPORT}/scraped_dates.json`
- **Airflow Logs**: Task execution logs in Airflow UI
- **Data Completeness**: Check for presence of all 5 data types per slate

## Next Steps

After staging, data flows to the **DDS Layer** for transformation and normalization. See `dds_layer.md` for details.
