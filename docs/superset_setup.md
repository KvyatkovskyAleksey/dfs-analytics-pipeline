# Apache Superset Setup Guide

This guide explains how to use Apache Superset to visualize your DFS analytics marts.

**⚠️ Security Note:** Throughout this guide, replace placeholders with your actual credentials:
- `YOUR_WASABI_ACCESS_KEY` → Your actual Wasabi access key
- `YOUR_WASABI_SECRET_KEY` → Your actual Wasabi secret key
- `your-bucket-name` → Your actual S3 bucket name

## Overview

Superset is configured to:
- Run on port **8088** (http://localhost:8088)
- Use PostgreSQL for metadata storage (shared with Airflow)
- Query S3 marts directly via DuckDB
- Access your marts stored in Wasabi S3

## Quick Start

### 1. Start Superset

```bash
docker-compose up superset -d
```

Wait for initialization to complete (first run takes ~2 minutes).

### 2. Access Superset UI

- URL: http://localhost:8088
- Username: `admin`
- Password: `admin`

**Important:** Change the default password in production!

### 3. Generate Marts

Before creating dashboards, generate your mart data:

```bash
# Generate top users mart
python src/scripts/marts/top_users_mart.py

# Generate position correlations mart
python src/scripts/marts/position_correlations_mart.py
```

## Connecting to S3 Marts

### Configure DuckDB Database Connection

**Important:** DuckDB doesn't support concurrent connections well with file-based databases. We'll use **in-memory mode** for Superset.

1. In Superset, navigate to **Settings > Database Connections**
2. Click **+ Database**
3. Select **Other** (or search for DuckDB if available)
4. Configure the connection:

**Display Name:** `DuckDB S3`

**SQLAlchemy URI:**
```
duckdb:///:memory:
```

**Advanced Settings > SQL Lab:**
Enable:
- ✅ Expose database in SQL Lab
- ✅ Allow CREATE TABLE AS
- ✅ Allow CREATE VIEW AS
- ✅ Allow DML

**Advanced Settings > Other > Engine Parameters:**
```json
{
  "connect_args": {
    "config": {
      "s3_endpoint": "s3.us-east-2.wasabisys.com",
      "s3_access_key_id": "YOUR_WASABI_ACCESS_KEY",
      "s3_secret_access_key": "YOUR_WASABI_SECRET_KEY",
      "s3_url_style": "path"
    }
  }
}
```

5. Click **Test Connection**
6. If successful, click **Connect**

### Using SQL Lab to Query S3 Marts

After creating the DuckDB connection, use **SQL Lab** to query your marts.

**First, install the httpfs extension (run once):**

1. Navigate to **SQL > SQL Lab**
2. Run this query:

```sql
INSTALL httpfs;
LOAD httpfs;
```

**Then, create a SECRET for S3 access (run once):**

```sql
CREATE SECRET wasabi_secret (
    TYPE S3,
    KEY_ID 'YOUR_WASABI_ACCESS_KEY',
    SECRET 'YOUR_WASABI_SECRET_KEY',
    ENDPOINT 's3.us-east-2.wasabisys.com',
    URL_STYLE 'path'
);
```

**Now you can query your marts directly:**

```sql
SELECT * FROM read_parquet('s3://your-bucket-name/marts/top_users/data.parquet') LIMIT 100;
```

3. Click **Save** > **Save Dataset**
4. Name it (e.g., "Top Users Mart")

## Available Marts

### 1. Top Users Mart
**Path:** `s3://your-bucket-name/marts/top_users/data.parquet`

**Schema:**
- `user_id` (TEXT): User identifier
- `slate_type` (TEXT): Contest slate type (dk_classic, dk_single_game)
- `roi_sum` (DOUBLE): Total ROI for the user

**Example Query:**
```sql
SELECT
    slate_type,
    user_id,
    roi_sum,
    RANK() OVER (PARTITION BY slate_type ORDER BY roi_sum DESC) as user_rank
FROM read_parquet('s3://your-bucket-name/marts/top_users/data.parquet')
WHERE roi_sum > 0
LIMIT 100;
```

### 2. Position Correlations Mart
**Path:** `s3://your-bucket-name/marts/{sport}_position_correlations/data.parquet`

**Schema:**
- `position_1` (TEXT): First position
- `position_2` (TEXT): Second position
- `team_relationship` (TEXT): 'same_team' or 'opponent_team'
- `correlation` (DOUBLE): Correlation coefficient
- `sample_size` (INTEGER): Number of observations

**Example Query:**
```sql
SELECT *
FROM read_parquet('s3://your-bucket-name/marts/nfl_correlations/data.parquet')
WHERE team_relationship = 'same_team'
ORDER BY correlation DESC
LIMIT 20;
```

## Creating Charts

### Example: Top Users by ROI

1. Create a **Table Chart**:
   - Dataset: Top Users Mart
   - Columns: `user_id`, `slate_type`, `roi_sum`
   - Metrics: Sum of `roi_sum`
   - Filters: `roi_sum > 0`
   - Sort: `roi_sum DESC`

2. Create a **Bar Chart**:
   - Dataset: Top Users Mart
   - Dimension: `slate_type`
   - Metric: COUNT(DISTINCT `user_id`)

### Example: Position Correlations Heatmap

1. Create a **Heatmap**:
   - Dataset: Position Correlations
   - Rows: `position_1`
   - Columns: `position_2`
   - Metric: AVG(`correlation`)
   - Filter: `team_relationship = 'same_team'`

## Troubleshooting

### Issue: Cannot connect to S3

**Solution 1:** Use environment variables in init script
Edit `scripts/init_superset.sh` to add S3 config as environment variables.

**Solution 2:** Create a view in PostgreSQL
Load mart data into PostgreSQL tables for faster access:

```python
# In your mart processor
df = self.con.execute(query).df()
df.to_sql('top_users_mart', postgresql_connection, if_exists='replace')
```

### Issue: Slow queries

**Optimization:**
- Add filters to limit data scanned
- Use partitioned data if marts grow large
- Consider caching results in Superset
- Load frequently-used marts into PostgreSQL

### Issue: DuckDB version mismatch

Ensure DuckDB versions match:
```bash
docker-compose exec superset pip show duckdb
```

Expected version: `1.2.0`

## Security Notes

1. **Change default credentials:**
   - Update `SUPERSET_SECRET_KEY` in `.env`
   - Change admin password after first login

2. **Protect S3 credentials:**
   - Use IAM roles in production
   - Never commit `.env` to version control

3. **Enable SSL:**
   - Use reverse proxy (nginx) for HTTPS
   - Configure SSL certificates

## Next Steps

- Create dashboards combining multiple charts
- Set up row-level security for user filtering
- Schedule email reports
- Configure alerts for KPI thresholds
- Add custom CSS themes

## Resources

- [Superset Documentation](https://superset.apache.org/docs/intro)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [S3 Access in DuckDB](https://duckdb.org/docs/extensions/httpfs)