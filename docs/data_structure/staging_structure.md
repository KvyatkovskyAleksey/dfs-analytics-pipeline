# Staging Layer Data Structures

This document describes the data structures stored in the staging layer. All data is stored in JSONL format with gzip compression.

## Storage Paths

```
s3://{BUCKET_NAME}/staging/{SPORT}/
├── draft_groups/{DATE}/data.json.gz
├── contests/{GAME_TYPE}/{DATE}/data.json.gz
├── events/{GAME_TYPE}/{DATE}/data.json.gz
├── contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz
└── lineups/{GAME_TYPE}/{DATE}/data.json.gz
```

## Common Fields

All records include:
- `load_ts` (timestamp): When the data was loaded to S3

---

## 1. Draft Groups

**Path**: `staging/{SPORT}/draft_groups/{DATE}/data.json.gz`

Contains metadata about contest slates and game types available on a given date.

### Structure

```json
{
  "contest-sources": [
    {
      "id": 4,  // Contest source ID (4 = DK Classic, 8 = DK Single Game)
      "name": "DraftKings Classic",
      "draft_groups": [
        {
          "id": 123456,
          "name": "NFL Main Slate",
          "contest_start_date": "2025-09-07T13:00:00",
          "sport_id": 1,
          "games_count": 13
        }
      ]
    }
  ],
  "load_ts": "2025-10-10T10:30:00"
}
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `contest-sources[].id` | int | Contest source identifier (4=Classic, 8=Single Game) |
| `contest-sources[].name` | string | Human-readable contest source name |
| `draft_groups[].id` | int | Unique slate identifier (used in subsequent API calls) |
| `draft_groups[].name` | string | Slate name (Main, Afternoon, Sunday Night, etc.) |
| `contest_start_date` | datetime | When contests begin |
| `sport_id` | int | Sport identifier (1=NFL) |
| `games_count` | int | Number of NFL games in this slate |

---

## 2. Contests

**Path**: `staging/{SPORT}/contests/{GAME_TYPE}/{DATE}/data.json.gz`

Contest-level information for each tournament in a slate.

### Structure

```json
{
  "live_contests": [
    {
      "contest_id": 987654,
      "contest_name": "NFL $5 Double Up",
      "entries": 2500,
      "entry_fee": 5.0,
      "prize_pool": 11250.0,
      "contest_type": "Double Up",
      "slate_id": 123456,
      "is_guaranteed": true,
      "max_entries": 2500
    }
  ],
  "load_ts": "2025-10-10T10:30:00"
}
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `contest_id` | int | Unique contest identifier |
| `contest_name` | string | Contest title shown to users |
| `entries` | int | Number of lineups entered |
| `entry_fee` | float | Cost per lineup entry (USD) |
| `prize_pool` | float | Total prize money (USD) |
| `contest_type` | string | Tournament type (GPP, Double Up, H2H, 50/50, etc.) |
| `slate_id` | int | Reference to draft_groups.id |
| `is_guaranteed` | bool | Whether prize pool is guaranteed regardless of entries |
| `max_entries` | int | Maximum allowed entries |

---

## 3. Events

**Path**: `staging/{SPORT}/events/{GAME_TYPE}/{DATE}/data.json.gz`

NFL game matchups and Vegas betting lines.

### Structure

```json
{
  "events": [
    {
      "event_id": 55123,
      "home_team": "Kansas City Chiefs",
      "home_team_id": 1234,
      "away_team": "Baltimore Ravens",
      "away_team_id": 5678,
      "start_time": "2025-09-07T20:20:00",
      "spread": -3.0,
      "over_under": 51.5,
      "weather": "Dome",
      "slate_id": 123456
    }
  ],
  "load_ts": "2025-10-10T10:30:00"
}
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | int | Unique game identifier |
| `home_team` | string | Home team name |
| `home_team_id` | int | Home team identifier |
| `away_team` | string | Away team name |
| `away_team_id` | int | Away team identifier |
| `start_time` | datetime | Game kickoff time |
| `spread` | float | Vegas point spread (negative = home favored) |
| `over_under` | float | Total points over/under line |
| `weather` | string | Weather conditions or "Dome" |
| `slate_id` | int | Reference to draft_groups.id |

---

## 4. Contest Analyze Data

**Path**: `staging/{SPORT}/contest_analyze/{GAME_TYPE}/{DATE}/data.json.gz`

Player performance and user analytics for a specific contest.

### Structure

```json
{
  "contest": {
    "contestId": "987654",
    "contestName": "NFL $5 Double Up",
    "totalEntries": 2500
  },
  "players": {
    "12345-55123": {  // Key format: playerId-eventId
      "playerId": 12345,
      "firstName": "Patrick",
      "lastName": "Mahomes",
      "fullName": "Patrick Mahomes",
      "position": "QB",
      "rosterPosition": "QB",
      "salary": 8500,
      "currentTeam": "KC",
      "currentTeamId": 1234,
      "eventId": 55123,
      "eventTeamId": 1234,
      "homeVisitor": "HOME",
      "favDog": "FAV",
      "projPoints": 22.5,
      "ownership": 35.8,
      "actualPoints": 28.4,
      "madeCut": 1,
      "statDetails": "{\"passYds\":320,\"passTD\":3,\"int\":0,\"rushYds\":15}"
    }
  },
  "users": {
    "user_abc123": {
      "userId": "user_abc123",
      "totalPlayers": 45,
      "totalRosters": 20,
      "uniqueRosters": 18,
      "maxExposure": 75.0,
      "lineupsCashing": 12,
      "lineupsInPercentile1": 2,
      "lineupsInPercentile2": 3,
      "lineupsInPercentile5": 5,
      "lineupsInPercentile10": 8,
      "lineupsInPercentile20": 12,
      "lineupsInPercentile50": 18,
      "totalEntryCost": 100.0,
      "totalWinning": 145.0,
      "roi": 45.0,
      "lineups": [
        {
          "lineupHash": "abc123def456",
          "lineupCt": 2
        }
      ]
    }
  },
  "load_ts": "2025-10-10T10:30:00"
}
```

### Players Fields

| Field | Type | Description |
|-------|------|-------------|
| `playerId` | int | Unique player identifier |
| `firstName` / `lastName` / `fullName` | string | Player name variations |
| `position` | string | NFL position (QB, RB, WR, TE, DST) |
| `rosterPosition` | string | DFS roster slot (QB, RB, WR, TE, FLEX, DST) |
| `salary` | int | DFS salary for this slate |
| `currentTeam` | string | Team abbreviation |
| `currentTeamId` | int | Team identifier |
| `eventId` | int | Game this player participated in |
| `eventTeamId` | int | Team identifier in this event |
| `homeVisitor` | string | HOME or AWAY |
| `favDog` | string | FAV (favorite) or DOG (underdog) |
| `projPoints` | float | Projected fantasy points |
| `ownership` | float | Percentage of lineups containing player (0-100) |
| `actualPoints` | float | Actual fantasy points scored |
| `madeCut` | int | 1 if player met value threshold, 0 otherwise |
| `statDetails` | string | JSON string of raw game statistics |

### Users Fields

| Field | Type | Description |
|-------|------|-------------|
| `userId` | string | Unique user identifier |
| `totalPlayers` | int | Total unique players used across all lineups |
| `totalRosters` | int | Total lineups entered |
| `uniqueRosters` | int | Number of distinct lineup constructions |
| `maxExposure` | float | Highest player exposure percentage (0-100) |
| `lineupsCashing` | int | Number of profitable lineups |
| `lineupsInPercentile{N}` | int | Lineups finishing in top N% |
| `totalEntryCost` | float | Total entry fees paid (USD) |
| `totalWinning` | float | Total prize money won (USD) |
| `roi` | float | Return on investment percentage |
| `lineups[].lineupHash` | string | Unique lineup identifier |
| `lineups[].lineupCt` | int | Times this lineup was entered |

---

## 5. Lineups

**Path**: `staging/{SPORT}/lineups/{GAME_TYPE}/{DATE}/data.json.gz`

Detailed lineup construction and performance data.

### Structure

```json
{
  "slate_id": 987654,
  "lineups": {
    "lineupHash": "abc123def456",
    "lineupCt": 15,
    "lineupUserCt": 8,
    "points": 156.8,
    "totalSalary": 49800,
    "totalOwn": 45.2,
    "minOwn": 2.1,
    "maxOwn": 35.8,
    "avgOwn": 18.6,
    "lineupRank": 234,
    "isCashing": true,
    "payout": 10.0,
    "lineupPercentile": 85.5,
    "favoriteCt": 6,
    "underdogCt": 3,
    "homeCt": 5,
    "visitorCt": 4,
    "correlatedPlayers": 3,

    "lineupPlayers": {
      "QB": 12345,
      "RB1": 23456,
      "RB2": 34567,
      "WR1": 45678,
      "WR2": 56789,
      "WR3": 67890,
      "TE": 78901,
      "FLEX": 89012,
      "DST": 90123
    },

    "teamStacks": [
      {"team": "KC", "playerCount": 3}
    ],

    "gameStacks": [
      {"eventId": 55123, "playerCount": 4}
    ],

    "lineupTrends": {
      "chalky": false,
      "contrarian": false,
      "balanced": true
    },

    "entryNameList": ["user_abc123", "user_def456"]
  },
  "load_ts": "2025-10-10T10:30:00"
}
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `slate_id` | int | Contest identifier (added by processor) |
| `lineupHash` | string | Unique identifier for this roster construction |
| `lineupCt` | int | Total times this lineup was entered across all users |
| `lineupUserCt` | int | Number of unique users who entered this lineup |
| `points` | float | Total fantasy points scored |
| `totalSalary` | int | Total salary used (max typically 50000) |
| `totalOwn` | float | Sum of ownership percentages |
| `minOwn` / `maxOwn` / `avgOwn` | float | Ownership statistics across lineup |
| `lineupRank` | int | Final ranking in contest |
| `isCashing` | bool | Whether lineup finished in the money |
| `payout` | float | Prize money won (USD) |
| `lineupPercentile` | float | Performance percentile (0-100) |
| `favoriteCt` / `underdogCt` | int | Number of favorites/underdogs rostered |
| `homeCt` / `visitorCt` | int | Number of home/away players |
| `correlatedPlayers` | int | Players from same team/game |
| `lineupPlayers` | object | Player IDs by position (schema varies by game type) |
| `teamStacks` | array | Team correlation analysis |
| `gameStacks` | array | Game correlation analysis |
| `lineupTrends` | object | Strategic classification flags |
| `entryNameList` | array | User IDs who entered this lineup |

### Position Schemas

**Classic (dk_classic)**:
- QB, RB1, RB2, WR1, WR2, WR3, TE, FLEX, DST

**Single Game (dk_single_game)**:
- CPT (Captain - 1.5x points multiplier)
- FLEX1, FLEX2, FLEX3, FLEX4, FLEX5

---

## Data Relationships

```
draft_groups (slate_id)
    ↓
contests (slate_id → contest_id)
    ↓
contest_analyze (contest_id)
    ├── players (player_id, event_id)
    └── users (user_id)
        └── lineups (lineup_hash)

events (slate_id, event_id)
    ↓
players (event_id# DFS analytics
)

lineups (lineup_hash)
    ↓
user lineups (lineup_hash)
```

## Usage Notes

1. **JSON Nesting**: contest_analyze and lineups contain deeply nested JSON requiring large object size limits when parsing
2. **Dynamic Schemas**: Position structures vary by game type (Classic vs Single Game)
3. **Timestamps**: All records include `load_ts` for data lineage tracking
4. **Compression**: JSONL with gzip provides ~5-10x compression vs raw JSON
5. **Partitioning**: Data partitioned by sport, game_type, and date for efficient querying
