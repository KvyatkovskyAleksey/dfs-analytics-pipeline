# Top Lineups Mart

Universal, configurable top lineups mart system for DFS analytics across all sports and game types.

## Overview

This module provides a flexible framework for generating top lineups analytics marts. It enriches lineup data with player details (position, team, salary, player_id) for each roster slot.

## Architecture

```
top_lineups/
├── base_top_lineups_mart.py          # Base class with dynamic SQL generation
├── configs.py                         # Pre-defined configurations
└── README.md                          # This file
```

## Quick Start

### Using Pre-defined Configs (Recommended)

```python
from scripts.marts.top_lineups import (
    BaseTopLineupsMart,
    DK_SINGLE_GAME_NFL_CONFIG,
    DK_SINGLE_GAME_NBA_CONFIG,
    DK_SINGLE_GAME_NHL_CONFIG,
)

# NFL DK Single Game
with BaseTopLineupsMart(config=DK_SINGLE_GAME_NFL_CONFIG) as processor:
    processor.process()
# Output: s3://bucket/marts/top_lineups/NFL/dk_single_game/data.parquet

# NBA DK Single Game
with BaseTopLineupsMart(config=DK_SINGLE_GAME_NBA_CONFIG) as processor:
    processor.process()
# Output: s3://bucket/marts/top_lineups/NBA/dk_single_game/data.parquet

# NHL DK Single Game
with BaseTopLineupsMart(config=DK_SINGLE_GAME_NHL_CONFIG) as processor:
    processor.process()
# Output: s3://bucket/marts/top_lineups/NHL/dk_single_game/data.parquet
```

### Creating Custom Configs

For custom use cases, create a config directly:

```python
from scripts.marts.top_lineups import BaseTopLineupsMart, TopLineupsMartConfig

# Example: FanDuel Single Game with MVP/Star positions
config = TopLineupsMartConfig(
    sport="MLB",
    game_type="fd_single_game",
    positions=["mvp", "star", "pro", "util1", "util2"],
    percentile_threshold=0.02,
    output_path="top_lineups/MLB/fd_single_game/data.parquet",
    roster_position_mapping={"mvp": "MVP", "star": "STAR"}  # Special position handling
)

with BaseTopLineupsMart(config=config) as processor:
    processor.process()

# Example: Top 5% instead of top 1%
config = TopLineupsMartConfig(
    sport="NFL",
    game_type="dk_single_game",
    positions=["cpt1", "flex1", "flex2", "flex3", "flex4", "flex5"],
    percentile_threshold=0.05,  # Top 5%
    output_path="top_lineups/NFL/dk_single_game_top5pct/data.parquet",
    roster_position_mapping={"cpt1": "CPT"}
)

with BaseTopLineupsMart(config=config) as processor:
    processor.process()
```

## Configuration Reference

### TopLineupsMartConfig

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sport` | str | Yes | Sport name (e.g., "NFL", "NBA", "NHL", "MLB") |
| `game_type` | str | Yes | Game type identifier (e.g., "dk_single_game", "dk_classic") |
| `positions` | list[str] | Yes | List of position slot names (e.g., ["cpt1", "flex1", ...]) |
| `percentile_threshold` | float | Yes | Percentile for filtering (0.01 = top 1%, 0.05 = top 5%) |
| `output_path` | str | Yes | S3 path for output parquet (relative to bucket/marts/) |
| `roster_position_mapping` | dict[str, str] | No | Maps slots to roster_position values for special handling |

### roster_position_mapping

This parameter handles cases where the same player appears multiple times in the player pool with different salaries/multipliers.

**Example:** DraftKings Single Game has "Captain" slots with 1.5x points and higher salary:

```python
roster_position_mapping={"cpt1": "CPT"}
```

This ensures:
- When joining data for the `cpt1` slot, we match players with `roster_position = 'CPT'`
- For all other slots (`flex1-5`), we match players with `roster_position != 'CPT'`

**Why?** The same player (e.g., Patrick Mahomes, player_id=12345) appears twice:
- As CPT: $15,000 salary, 1.5x points
- As FLEX: $10,000 salary, 1x points

Without this mapping, we might get the wrong salary/multiplier.

## Output Schema

The mart generates one row per lineup with these columns:

### Core Lineup Info
- `contest_id`: Contest identifier
- `lineup_rank`: Lineup rank within the contest
- `total_own`: Total ownership percentage
- `total_salary`: Total salary used

### Per-Position Enrichment

For each position in `positions` list, four columns are generated:

- `pos_{position}_player_id`: Player identifier
- `pos_{position}_real`: Actual player position (QB, RB, WR, etc.)
- `pos_{position}_team`: Team abbreviation
- `pos_{position}_salary`: Player salary/cost

**Example for DK Single Game:**
```
contest_id | lineup_rank | total_own | total_salary | pos_cpt1_player_id | pos_cpt1_real | pos_cpt1_team | pos_cpt1_salary | pos_flex1_player_id | ...
12345      | 1           | 15.2      | 49800        | 67890              | QB            | KC            | 15000           | 11223               | ...
```

## Implementation Details

### Dynamic SQL Generation

The base class generates SQL dynamically based on configuration:

1. **Unnesting positions:** Converts wide format (pos_cpt1, pos_flex1, ...) to long format
2. **Pivoting results:** Aggregates back to wide format with enriched columns
3. **Roster position filtering:** Optional filtering based on roster_position_mapping

### Data Sources

- **Lineups:** `s3://bucket/dds/{sport}/lineups/{game_type}/*/data.parquet`
- **Players:** `s3://bucket/dds/{sport}/players/{game_type}/*/data.parquet`

### Performance Optimizations

- Pre-filters players to only those appearing in top lineups
- Uses date_id for efficient joins
- Parquet format with Snappy compression

## Adding New Game Types

To add support for a new game type, create a new config in `configs.py`:

```python
# In configs.py
DK_CLASSIC_NFL_CONFIG = TopLineupsMartConfig(
    sport="NFL",
    game_type="dk_classic",
    positions=["qb", "rb1", "rb2", "wr1", "wr2", "wr3", "te", "flex", "dst"],
    percentile_threshold=0.01,
    output_path="top_lineups/NFL/dk_classic/data.parquet",
    roster_position_mapping=None,  # No special positions for classic
)
```

Then export it in `__init__.py` and use it:

```python
from scripts.marts.top_lineups import BaseTopLineupsMart, DK_CLASSIC_NFL_CONFIG

with BaseTopLineupsMart(config=DK_CLASSIC_NFL_CONFIG) as processor:
    processor.process()
```

## Migration from Old Implementation

The old `single_game_dk_top_lineups_mart.py` has been replaced. To migrate:

**Before:**
```python
from scripts.marts.single_game_dk_top_lineups_mart import SingleGameDKTopLineupsMart

with SingleGameDKTopLineupsMart(sport="NFL") as processor:
    processor.process()
```

**After:**
```python
from scripts.marts.top_lineups import BaseTopLineupsMart, DK_SINGLE_GAME_NFL_CONFIG

with BaseTopLineupsMart(config=DK_SINGLE_GAME_NFL_CONFIG) as processor:
    processor.process()
```

**Note:** The output path has changed from `marts/single_game_dk_top_lineups/data.parquet` to `marts/top_lineups/NFL/dk_single_game/data.parquet` for better organization.

## Troubleshooting

### "No module named 'scripts'" error
Run from the `src/` directory:
```bash
cd src
python -c "from scripts.marts.top_lineups import *"
```

### Empty output
- Check that source data exists in DDS layer
- Verify percentile_threshold isn't too restrictive
- Check sport/game_type paths match your data structure

### Wrong player salary
- Verify roster_position_mapping is configured correctly
- Check that players table has roster_position column populated
- Ensure captain/special positions are mapped properly
