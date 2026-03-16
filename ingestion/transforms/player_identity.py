"""
player_identity.py — Build master_players curated table.

One row per player, anchored on gsis_id (the NFL's Game Statistics &
Information System ID). This is the canonical player_id used to join
all other datasets in the platform.

Source: nflreadpy load_rosters() — one row per player-season (e.g. a player
active for 10 seasons produces 10 rows). This builder deduplicates to one
row per unique gsis_id, computing career span from the full history and
taking most-recent-season values for attributes that can change over time
(position, team).
"""

import pandas as pd

# Cross-platform ID columns carried forward for PlayerIdResolver (Milestone 4).
# These let the resolver link PFR / ESPN / Yahoo rows to a gsis_id without
# falling back to fuzzy name matching.
_PLATFORM_ID_COLS = [
    "espn_id",  # ESPN player ID
    "sportradar_id",  # Sportradar player ID
    "yahoo_id",  # Yahoo Sports player ID
    "rotowire_id",  # Rotowire fantasy player ID
    "pff_id",  # Pro Football Focus player ID
    "pfr_id",  # Pro Football Reference player slug (e.g. "MahoPatr01")
    "fantasy_data_id",  # FantasyData player ID
    "sleeper_id",  # Sleeper fantasy app player ID
]

# Final column order for the output table
_OUTPUT_COLS = [
    "player_id",  # canonical ID (= gsis_id)
    "player_name",  # full name from most recent roster entry
    "position",  # most recent listed position
    "team",  # most recent team (current or last active)
    "college",  # college attended
    "draft_club",  # team that drafted the player (null = undrafted)
    "draft_number",  # overall pick number (null = undrafted)
    "entry_year",  # first NFL season per roster record
    "rookie_year",  # official rookie year designation
    "birth_date",  # date of birth
    "height",  # height in inches
    "weight",  # weight in lbs
    "first_season",  # earliest season this player appears in rosters data
    "last_season",  # most recent season this player appears in rosters data
    *_PLATFORM_ID_COLS,
]


def build_master_players(rosters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build canonical master players table from nflreadpy load_rosters() output.

    Input : rosters_df — multi-season roster DataFrame (one row per player-season)
    Output: master_players — one row per unique player (keyed on player_id)

    Career span logic:
      first_season = min(season) across all roster appearances
      last_season  = max(season) across all roster appearances

    Attribute staleness:
      All non-span columns (position, team, college, draft info, IDs) are
      taken from the player's most recent season entry, since things like
      position can legitimately change during a career.
    """
    df = rosters_df.copy()

    # Drop rows with no gsis_id — identity cannot be established without it
    df = df.dropna(subset=["gsis_id"])

    # Compute career span per player across all seasons in the dataset
    career_span = (
        df.groupby("gsis_id")["season"]
        .agg(first_season="min", last_season="max")
        .reset_index()
    )

    # Take the most recent season's row for all per-player attributes
    latest = (
        df.sort_values("season", ascending=False)
        .groupby("gsis_id", as_index=False)
        .first()
    )

    # Merge career span back onto the latest-row attributes
    result = latest.merge(career_span, on="gsis_id", how="left")

    # Rename to canonical platform schema
    result = result.rename(
        columns={
            "gsis_id": "player_id",
            "full_name": "player_name",
        }
    )

    # Select final columns (skip any that this roster pull doesn't include)
    output_cols = [c for c in _OUTPUT_COLS if c in result.columns]
    result = result[output_cols].copy()

    return result.reset_index(drop=True)
