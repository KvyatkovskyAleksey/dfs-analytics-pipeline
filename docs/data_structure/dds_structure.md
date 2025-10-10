# DDS Layer Data Structures

This document describes the normalized data structures in the DDS (Data Detail Storage) layer. All data is stored in Parquet format with Snappy compression.

## Storage Paths

```
s3://{BUCKET_NAME}/dds/{SPORT}/
├── players/{GAME_TYPE}/{DATE}/data.parquet
├── users/{GAME_TYPE}/{DATE}/data.parquet
├── user_lineups/{GAME_TYPE}/{DATE}/data.parquet
└── lineups/{GAME_TYPE}/{DATE}/data.parquet
```

---

## 1. Players Table

**Path**: `dds/{SPORT}/players/{GAME_TYPE}/{DATE}/data.parquet`

Normalized player performance data for each contest. One row per player per contest.

### Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `player_key` | VARCHAR | No | Composite key: {playerId}-{eventId} |
| `player_id` | INTEGER | No | Unique player identifier |
| `first_name` | VARCHAR | Yes | Player first name |
| `last_name` | VARCHAR | Yes | Player last name |
| `full_name` | VARCHAR | Yes | Complete player name |
| `salary` | INTEGER | Yes | DFS salary for this slate |
| `position` | VARCHAR | Yes | NFL position (QB, RB, WR, TE, DST) |
| `roster_position` | VARCHAR | Yes | DFS roster slot (may include FLEX) |
| `current_team` | VARCHAR | Yes | Team abbreviation (e.g., KC, BAL) |
| `current_team_id` | INTEGER | Yes | Team identifier |
| `event_id` | INTEGER | Yes | Game identifier |
| `event_team_id` | INTEGER | Yes | Team ID within this specific event |
| `home_visitor` | VARCHAR | Yes | HOME or AWAY |
| `fav_dog` | VARCHAR | Yes | FAV (favorite) or DOG (underdog) |
| `proj_points` | DOUBLE | Yes | Projected fantasy points |
| `ownership` | DOUBLE | Yes | Percentage of lineups (0-100) |
| `actual_points` | DOUBLE | Yes | Actual fantasy points scored |
| `stat_details` | VARCHAR | Yes | JSON string of game statistics |
| `made_cut` | INTEGER | Yes | 1 if player met value threshold, 0 otherwise |

### Example Row

```
player_key: "12345-55123"
player_id: 12345
first_name: "Patrick"
last_name: "Mahomes"
full_name: "Patrick Mahomes"
salary: 8500
position: "QB"
roster_position: "QB"
current_team: "KC"
current_team_id: 1234
event_id: 55123
event_team_id: 1234
home_visitor: "HOME"
fav_dog: "FAV"
proj_points: 22.5
ownership: 35.8
actual_points: 28.4
stat_details: "{\"passYds\":320,\"passTD\":3,\"int\":0,\"rushYds\":15}"
made_cut: 1
```

### Usage

**Key Analytics**:
- Value analysis: actual_points / (salary / 1000) = points per $1K
- Projection accuracy: actual_points - proj_points
- Ownership leverage: Low ownership + high actual points = leverage opportunity
- Game script correlation: home_visitor, fav_dog vs actual_points

**Queries**:
```sql
-- Top value plays
SELECT player_id, full_name, salary, actual_points,
       actual_points / (salary / 1000.0) as value
FROM players
WHERE actual_points > 0
ORDER BY value DESC
LIMIT 20;

-- Ownership vs performance
SELECT
    CASE
        WHEN ownership < 10 THEN 'Low'
        WHEN ownership < 25 THEN 'Medium'
        ELSE 'High'
    END as ownership_tier,
    AVG(actual_points) as avg_points,
    COUNT(*) as player_count
FROM players
GROUP BY ownership_tier;
```

---

## 2. Users Table

**Path**: `dds/{SPORT}/users/{GAME_TYPE}/{DATE}/data.parquet`

User performance and strategy metrics for each contest. One row per user per contest.

### Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `contest_id` | VARCHAR | No | Contest identifier |
| `user_id` | VARCHAR | No | User identifier |
| `total_players` | INTEGER | Yes | Unique players used across all lineups |
| `total_rosters` | INTEGER | Yes | Total lineups entered |
| `unique_rosters` | INTEGER | Yes | Distinct lineup constructions |
| `max_exposure` | DOUBLE | Yes | Highest player exposure % (0-100) |
| `lineups_cashing` | INTEGER | Yes | Number of profitable lineups |
| `lineups_in_percentile_1` | INTEGER | Yes | Lineups finishing top 1% |
| `lineups_in_percentile_2` | INTEGER | Yes | Lineups finishing top 2% |
| `lineups_in_percentile_5` | INTEGER | Yes | Lineups finishing top 5% |
| `lineups_in_percentile_10` | INTEGER | Yes | Lineups finishing top 10% |
| `lineups_in_percentile_20` | INTEGER | Yes | Lineups finishing top 20% |
| `lineups_in_percentile_50` | INTEGER | Yes | Lineups finishing top 50% |
| `total_entry_cost` | DOUBLE | Yes | Total entry fees paid (USD) |
| `total_winning` | DOUBLE | Yes | Total prize money won (USD) |
| `roi` | DOUBLE | Yes | Return on investment percentage |

### Example Row

```
contest_id: "987654"
user_id: "user_abc123"
total_players: 45
total_rosters: 20
unique_rosters: 18
max_exposure: 75.0
lineups_cashing: 12
lineups_in_percentile_1: 2
lineups_in_percentile_2: 3
lineups_in_percentile_5: 5
lineups_in_percentile_10: 8
lineups_in_percentile_20: 12
lineups_in_percentile_50: 18
total_entry_cost: 100.0
total_winning: 145.0
roi: 45.0
```

### Usage

**Key Analytics**:
- Lineup diversity: unique_rosters / total_rosters (higher = more diverse)
- Player pool efficiency: total_players vs total_rosters
- Success rate: lineups_cashing / total_rosters
- Upside capture: lineups_in_percentile_1 / total_rosters

**Queries**:
```sql
-- Top performing users
SELECT user_id, total_rosters, lineups_cashing,
       ROUND(100.0 * lineups_cashing / total_rosters, 2) as cash_rate,
       roi
FROM users
WHERE total_rosters >= 10
ORDER BY roi DESC
LIMIT 50;

-- Lineup construction strategies
SELECT
    CASE
        WHEN unique_rosters = total_rosters THEN 'Full Diversity'
        WHEN unique_rosters / total_rosters::FLOAT > 0.8 THEN 'High Diversity'
        WHEN unique_rosters / total_rosters::FLOAT > 0.5 THEN 'Medium Diversity'
        ELSE 'Low Diversity'
    END as strategy,
    AVG(roi) as avg_roi,
    COUNT(*) as user_count
FROM users
WHERE total_rosters >= 20
GROUP BY strategy;
```

---

## 3. User Lineups Table

**Path**: `dds/{SPORT}/user_lineups/{GAME_TYPE}/{DATE}/data.parquet`

Junction table linking users to their specific lineup constructions. One row per user-lineup combination.

### Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `contest_id` | INTEGER | No | Contest identifier |
| `user_id` | VARCHAR | No | User identifier |
| `lineup_hash` | VARCHAR | No | Unique lineup identifier |
| `lineup_ct` | INTEGER | Yes | Times this user entered this lineup |

### Example Row

```
contest_id: 987654
user_id: "user_abc123"
lineup_hash: "abc123def456"
lineup_ct: 2
```

### Usage

**Key Analytics**:
- Lineup duplication patterns: lineup_ct > 1
- User-lineup relationships for detailed analysis
- Multi-entry strategy tracking

**Queries**:
```sql
-- Users with most lineup duplication
SELECT user_id, lineup_hash, lineup_ct
FROM user_lineups
WHERE lineup_ct > 1
ORDER BY lineup_ct DESC
LIMIT 100;

-- Join users with their lineup performance
SELECT u.user_id, u.roi, ul.lineup_hash, l.points, l.is_cashing
FROM users u
JOIN user_lineups ul ON u.user_id = ul.user_id AND u.contest_id = ul.contest_id
JOIN lineups l ON ul.lineup_hash = l.lineup_hash AND ul.contest_id = l.contest_id
WHERE u.roi > 50;
```

---

## 4. Lineups Table

**Path**: `dds/{SPORT}/lineups/{GAME_TYPE}/{DATE}/data.parquet`

Detailed lineup composition and performance. One row per unique lineup construction.

### Schema

#### Core Fields

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `contest_id` | VARCHAR | No | Contest identifier (from slate_id) |
| `lineup_hash` | VARCHAR | No | Unique lineup identifier |
| `lineup_ct` | INTEGER | Yes | Total entries of this lineup |
| `lineup_user_ct` | INTEGER | Yes | Unique users who entered this lineup |
| `points` | DOUBLE | Yes | Total fantasy points scored |
| `total_salary` | INTEGER | Yes | Total salary used (max ~50000) |
| `total_own` | DOUBLE | Yes | Sum of ownership percentages |
| `min_own` | DOUBLE | Yes | Lowest player ownership in lineup |
| `max_own` | DOUBLE | Yes | Highest player ownership in lineup |
| `avg_own` | DOUBLE | Yes | Average player ownership |
| `lineup_rank` | INTEGER | Yes | Final contest ranking |
| `is_cashing` | BOOLEAN | Yes | Whether lineup finished in the money |
| `favorite_ct` | INTEGER | Yes | Number of favorites rostered |
| `underdog_ct` | INTEGER | Yes | Number of underdogs rostered |
| `home_ct` | INTEGER | Yes | Number of home players |
| `visitor_ct` | INTEGER | Yes | Number of away players |
| `payout` | DOUBLE | Yes | Prize money won (USD) |
| `lineup_percentile` | DOUBLE | Yes | Performance percentile (0-100) |
| `correlated_players` | INTEGER | Yes | Number of correlated players (stacks) |

#### Position Fields (Dynamic - varies by game type)

**Classic (dk_classic)**:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `pos_qb` | INTEGER | Yes | Quarterback player ID |
| `pos_rb1` | INTEGER | Yes | Running back 1 player ID |
| `pos_rb2` | INTEGER | Yes | Running back 2 player ID |
| `pos_wr1` | INTEGER | Yes | Wide receiver 1 player ID |
| `pos_wr2` | INTEGER | Yes | Wide receiver 2 player ID |
| `pos_wr3` | INTEGER | Yes | Wide receiver 3 player ID |
| `pos_te` | INTEGER | Yes | Tight end player ID |
| `pos_flex` | INTEGER | Yes | Flex (RB/WR/TE) player ID |
| `pos_dst` | INTEGER | Yes | Defense/Special Teams player ID |

**Single Game (dk_single_game)**:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `pos_cpt` | INTEGER | Yes | Captain player ID (1.5x points) |
| `pos_flex1` | INTEGER | Yes | Flex position 1 player ID |
| `pos_flex2` | INTEGER | Yes | Flex position 2 player ID |
| `pos_flex3` | INTEGER | Yes | Flex position 3 player ID |
| `pos_flex4` | INTEGER | Yes | Flex position 4 player ID |
| `pos_flex5` | INTEGER | Yes | Flex position 5 player ID |

#### Complex JSON Fields

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `team_stacks` | VARCHAR | Yes | JSON array of team correlation data |
| `game_stacks` | VARCHAR | Yes | JSON array of game correlation data |
| `lineup_trends` | VARCHAR | Yes | JSON object with strategy flags |
| `entry_name_list` | VARCHAR | Yes | JSON array of user IDs |

### Example Row (Classic)

```
contest_id: "987654"
lineup_hash: "abc123def456"
lineup_ct: 15
lineup_user_ct: 8
points: 156.8
total_salary: 49800
total_own: 45.2
min_own: 2.1
max_own: 35.8
avg_own: 18.6
lineup_rank: 234
is_cashing: true
favorite_ct: 6
underdog_ct: 3
home_ct: 5
visitor_ct: 4
payout: 10.0
lineup_percentile: 85.5
correlated_players: 3

pos_qb: 12345
pos_rb1: 23456
pos_rb2: 34567
pos_wr1: 45678
pos_wr2: 56789
pos_wr3: 67890
pos_te: 78901
pos_flex: 89012
pos_dst: 90123

team_stacks: '[{"team":"KC","playerCount":3}]'
game_stacks: '[{"eventId":55123,"playerCount":4}]'
lineup_trends: '{"chalky":false,"contrarian":false,"balanced":true}'
entry_name_list: '["user_abc123","user_def456"]'
```

### Usage

**Key Analytics**:
- Optimal lineup construction: High points + low ownership
- Stacking effectiveness: correlated_players vs points
- Salary efficiency: points / (total_salary / 1000.0)
- Game theory: min_own vs is_cashing (contrarian plays)

**Queries**:
```sql
-- Best contrarian lineups
SELECT lineup_hash, points, avg_own, lineup_rank, payout
FROM lineups
WHERE is_cashing = true
  AND avg_own < 15
ORDER BY lineup_percentile DESC
LIMIT 50;

-- Stacking analysis
SELECT
    correlated_players as stack_size,
    AVG(points) as avg_points,
    SUM(CASE WHEN is_cashing THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as cash_rate,
    COUNT(*) as lineup_count
FROM lineups
GROUP BY correlated_players
ORDER BY stack_size;

-- Reconstruct lineup with player details
SELECT
    l.lineup_hash,
    l.points,
    l.total_salary,
    qb.full_name as qb_name, qb.actual_points as qb_points,
    rb1.full_name as rb1_name, rb1.actual_points as rb1_points,
    wr1.full_name as wr1_name, wr1.actual_points as wr1_points
    -- ... more positions
FROM lineups l
JOIN players qb ON l.pos_qb = qb.player_id
JOIN players rb1 ON l.pos_rb1 = rb1.player_id
JOIN players wr1 ON l.pos_wr1 = wr1.player_id
WHERE l.is_cashing = true
ORDER BY l.points DESC
LIMIT 10;

-- Ownership distribution by performance
SELECT
    CASE
        WHEN avg_own < 10 THEN 'Low Owned'
        WHEN avg_own < 20 THEN 'Medium Owned'
        ELSE 'High Owned'
    END as ownership_tier,
    AVG(points) as avg_points,
    AVG(lineup_percentile) as avg_percentile,
    SUM(CASE WHEN is_cashing THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as cash_rate
FROM lineups
GROUP BY ownership_tier;
```

---

## Data Relationships

```
lineups (lineup_hash, contest_id)
    ├─ user_lineups (lineup_hash, user_id, contest_id)
    │   └─ users (user_id, contest_id)
    └─ players (player_id) via pos_* columns
```

### Join Patterns

```sql
-- Full lineup analysis with user context
SELECT
    u.user_id,
    u.roi,
    l.lineup_hash,
    l.points,
    l.is_cashing,
    COUNT(*) OVER (PARTITION BY u.user_id) as user_total_lineups
FROM users u
JOIN user_lineups ul ON u.user_id = ul.user_id AND u.contest_id = ul.contest_id
JOIN lineups l ON ul.lineup_hash = l.lineup_hash AND ul.contest_id = l.contest_id;

-- Player performance across lineup constructions
SELECT
    p.full_name,
    p.salary,
    p.ownership,
    p.actual_points,
    COUNT(*) as times_rostered,
    AVG(l.points) as avg_lineup_points,
    SUM(CASE WHEN l.is_cashing THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as cash_rate
FROM players p
JOIN lineups l ON (
    p.player_id = l.pos_qb OR
    p.player_id = l.pos_rb1 OR
    p.player_id = l.pos_rb2 OR
    -- ... other positions
)
GROUP BY p.player_id, p.full_name, p.salary, p.ownership, p.actual_points;
```

---

## Performance Considerations

### Parquet Columnar Storage
- Query only needed columns (e.g., SELECT points, lineup_hash vs SELECT *)
- Filter early (WHERE clause) to leverage predicate pushdown
- Use partition pruning with date filters

### Indexes
Consider creating indexes on:
- `lineup_hash` (primary key, frequently joined)
- `contest_id` (partition key, frequently filtered)
- `player_id` (frequently joined to lineups)
- `user_id` (frequently filtered/grouped)

### Query Optimization
```sql
-- GOOD: Column pruning + early filtering
SELECT lineup_hash, points
FROM lineups
WHERE is_cashing = true AND contest_id = '987654';

-- BAD: Reading all columns
SELECT * FROM lineups WHERE is_cashing = true;
```

---

## Data Quality Notes

1. **DISTINCT Keyword**: Players table uses DISTINCT to deduplicate across contests
2. **Type Safety**: All numeric fields explicitly cast from JSON strings
3. **NULL Handling**: Optional fields marked as nullable
4. **Position Flexibility**: Dynamic position columns adapt to game type
5. **JSON Preservation**: Complex objects (stacks, trends) stored as JSON strings for flexibility
