# ESPN Odds Data Structure (v1)

This document describes the v1 structure for ESPN odds data stored in S3 staging.

## Overview

The v1 implementation fetches ESPN odds data through a two-step process:
1. **Scoreboard API** - Get all games for a specific date
2. **Odds API** - Get detailed odds for each game

All data is stored raw (unparsed) in S3 for later processing in the DDS stage.

## Storage Location

```
s3://{bucket}/staging/{sport}/espn_odds/v1/{date}/data.json.gz
```

**Examples:**
- `s3://bucket/staging/NBA/espn_odds/v1/2025-11-20/data.json.gz`
- `s3://bucket/staging/NFL/espn_odds/v1/2025-11-19/data.json.gz`

## Data Structure

### Top Level (v1)

```json
{
  "version": "v1",
  "date": "2025-11-20",
  "sport": "NBA",
  "fetch_timestamp": "2025-11-20T15:30:45Z",
  "scoreboard_response": {...},
  "odds_responses": [...]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Data structure version ("v1") |
| `date` | string | Game date in YYYY-MM-DD format |
| `sport` | string | Sport: "NBA", "NFL", "NHL", or "MLB" |
| `fetch_timestamp` | string | When data was fetched (ISO 8601 UTC) |
| `scoreboard_response` | object | Complete scoreboard API response |
| `odds_responses` | array | Detailed odds for each game |

---

## Scoreboard Response

Complete response from:
```
https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard?dates={YYYYMMDD}
```

### Key Fields

```json
{
  "leagues": [...],
  "events": [
    {
      "id": "401810104",
      "uid": "s:40~l:46~e:401810104",
      "date": "2025-11-21T00:00Z",
      "name": "Warriors at Grizzlies",
      "shortName": "GS @ MEM",
      "season": {
        "year": 2026,
        "type": 2
      },
      "competitions": [
        {
          "id": "401810104",
          "competitors": [
            {
              "id": "9",
              "homeAway": "away",
              "team": {
                "id": "9",
                "abbreviation": "GS",
                "displayName": "Golden State Warriors"
              }
            },
            {
              "id": "29",
              "homeAway": "home",
              "team": {
                "id": "29",
                "abbreviation": "MEM",
                "displayName": "Memphis Grizzlies"
              }
            }
          ]
        }
      ]
    }
  ]
}
```

**Use scoreboard for:**
- Game IDs
- Team information
- Game schedule/timing
- Basic game metadata

---

## Odds Responses

Array of detailed odds, one entry per game.

### Structure

```json
{
  "game_id": "401810104",
  "game_name": "Warriors at Grizzlies",
  "odds_data": {...}
}
```

### Odds Data

Complete response from:
```
https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events/{game_id}/competitions/{game_id}/odds
```

#### Example (Detailed)

```json
{
  "count": 2,
  "pageIndex": 1,
  "pageSize": 25,
  "pageCount": 1,
  "items": [
    {
      "provider": {
        "id": "58",
        "name": "ESPN BET",
        "priority": 1
      },
      "details": "GS -2.5",
      "overUnder": 223.5,
      "spread": 2.5,
      "overOdds": -115.0,
      "underOdds": -105.0,
      "awayTeamOdds": {
        "favorite": true,
        "underdog": false,
        "moneyLine": -135,
        "spreadOdds": -105.0,
        "open": {
          "favorite": true,
          "pointSpread": {
            "american": "-2.5"
          },
          "spread": {
            "american": "-115",
            "decimal": 1.87
          },
          "moneyLine": {
            "american": "-145",
            "decimal": 1.69
          }
        },
        "close": {
          "pointSpread": {
            "american": "-2.5"
          },
          "spread": {
            "american": "-105",
            "decimal": 1.952
          },
          "moneyLine": {
            "american": "-135",
            "decimal": 1.741
          }
        },
        "current": {
          "pointSpread": {
            "american": "-2.5"
          },
          "spread": {
            "american": "-105",
            "decimal": 1.952,
            "outcome": {
              "type": "loss"
            }
          },
          "moneyLine": {
            "american": "-135",
            "decimal": 1.741,
            "outcome": {
              "type": "loss"
            }
          }
        }
      },
      "homeTeamOdds": {
        "favorite": false,
        "underdog": true,
        "moneyLine": 115,
        "spreadOdds": -115.0,
        "open": {...},
        "close": {...},
        "current": {...}
      }
    },
    {
      "provider": {
        "id": "59",
        "name": "ESPN Bet - Live Odds"
      },
      ...
    }
  ]
}
```

### Key Odds Fields

| Field | Description |
|-------|-------------|
| `count` | Number of odds providers |
| `items` | Array of odds from different providers |
| `provider.id` | "58" = ESPN BET (pregame), "59" = ESPN BET Live |
| `details` | Summary string (e.g., "GS -2.5") |
| `spread` | Point spread value |
| `overUnder` | Total points over/under |
| `awayTeamOdds.open` | Opening line |
| `awayTeamOdds.close` | Closing line |
| `awayTeamOdds.current` | Current line (includes outcome after game) |
| `moneyLine.american` | American odds format (e.g., "-135") |
| `moneyLine.decimal` | Decimal odds format (e.g., "1.741") |
| `outcome.type` | "win" or "loss" (available after game completion) |

---

## Odds Availability

### When Odds Are Available

- **Pregame**: A few days to hours before game time
- **Live**: During the game (provider ID 59)
- **Postgame**: May include outcomes but lines are removed

### When Odds Are NOT Available

- Games too far in future (>1 week typically)
- Games that have finished (lines removed)
- Some early season or special games

### Missing Odds Handling

If `odds_data` is `null` for a game, it means:
- Odds API returned empty/error
- No odds available yet
- Game too far out or already finished

**In DDS processing:** Check `odds_data` is not null before parsing odds fields.

---

## Multiple Odds Formats

ESPN provides odds in multiple formats for each line:

### American Format
```json
"american": "-135"  // Negative = favorite, Positive = underdog
```

### Decimal Format
```json
"decimal": 1.741    // European odds format
```

### Fraction Format
```json
"fraction": "20/27",
"displayValue": "20/27"
```

**Recommendation:** Use American format for DFS/sports betting analysis in US markets.

---

## Line Movement Analysis

Track changes from open → close → current:

```python
# Example: Spread movement
open_spread = odds["awayTeamOdds"]["open"]["pointSpread"]["american"]
close_spread = odds["awayTeamOdds"]["close"]["pointSpread"]["american"]

# Did the line move?
if open_spread != close_spread:
    print(f"Line moved from {open_spread} to {close_spread}")
```

### Useful for:
- Public betting trends
- Sharp money detection
- Value identification
- Reverse line movement

---

## Parsing in DDS Stage

### Recommended Approach

1. **Load v1 data from S3**
2. **Extract scoreboard events**
   - Game IDs
   - Teams
   - Schedule
3. **Join with odds_responses**
4. **Parse relevant odds fields**
   - Provider 58 for pregame analysis
   - Open/close lines for movement
   - American odds for betting
5. **Create dimensional tables**
   - Games
   - Teams
   - Odds (by provider)
   - Line movements

### Example DDS Tables

```sql
-- dim_games
game_id, date, sport, home_team, away_team, game_time

-- fact_odds_pregame
game_id, provider_id, fetch_time,
spread_open, spread_close,
moneyline_home_open, moneyline_home_close,
total_open, total_close

-- fact_line_movement
game_id, movement_type (spread/total/moneyline),
open_value, close_value, movement_amount
```

---

## API Endpoints Reference

### Scoreboard
```
GET https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard?dates={YYYYMMDD}
```

### Detailed Odds
```
GET https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events/{game_id}/competitions/{game_id}/odds
```

### Sport/League Mappings

| Sport | Sport Path | League Path |
|-------|------------|-------------|
| NFL   | football   | nfl         |
| NBA   | basketball | nba         |
| NHL   | hockey     | nhl         |
| MLB   | baseball   | mlb         |

---

## Version History

### v1 (Current)
- Two-step fetch (scoreboard + odds)
- Complete raw API responses
- Multiple odds providers
- Open/close/current lines
- Multiple odds formats

### Future Versions

Consider v2 for:
- Additional odds endpoints (props, futures)
- Historical odds tracking
- More providers
- Processed/normalized data

---

## Notes

1. **Data is raw** - No parsing or transformation applied in staging
2. **Complete responses** - All API fields preserved for flexibility
3. **Versioned** - Easy to iterate without breaking existing data
4. **Gzip compressed** - Efficient storage
5. **One file per date/sport** - Simple partitioning strategy

---

## Testing

Test with today's date to see actual odds:

```bash
# Test NBA (usually has games)
python test_espn_fetch.py

# Check S3 storage
aws s3 ls s3://bucket/staging/NBA/espn_odds/v1/2025-11-20/ --endpoint-url=https://...
```

---

## References

- [ESPN API Community Docs](https://gist.github.com/nntrn/ee26cb2a0716de0947a0a4e9a157bc1c)
- [ESPN Wiki GitHub](https://github.com/nntrn/espn-wiki)
- [ESPN API Response Structure](../ESPN%20API%20Response%20Structure.md) (Original reference doc)
