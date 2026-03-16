"""
game_builder.py — Build master_games curated table.

One row per game keyed on game_id from nflreadpy load_schedules().
Team abbreviations are normalized through TEAM_ALIASES so all downstream
joins use the current canonical abbreviation regardless of when the game
was played (e.g. an OAK game from 2018 is stored as team_id="LV").
"""

import pandas as pd

from ingestion.transforms.team_normalizer import normalize_team_abbr

# Columns to carry through from schedules (in priority order)
_KEEP = [
    "game_id",  # unique key — e.g. "2024_01_KC_BAL"
    "season",  # NFL season year
    "week",  # week number within season
    "game_type",  # "REG" (regular season) | "POST" (playoffs) | "PRE" (preseason) | "SB" (Super Bowl)
    "gameday",  # date of game — YYYY-MM-DD (not needed)
    "gametime",  # kickoff time in local timezone — HH:MM (not needed)
    "home_team",  # canonical team abbreviation
    "away_team",  # canonical team abbreviation
    "home_score",  # final score
    "away_score",  # final score
    "result",  # home_score - away_score (positive = home win)
    "total",  # combined final score (home + away)
    "overtime",  # 1 if game went to OT, else 0
    "stadium",  # stadium name (Not needed)
    "roof",  # "dome" | "outdoors" | "retractable" | "open"
    "surface",  # "grass" | "astroturf" | "fieldturf" etc.
    "temp",  # temperature at kickoff (°F); null for dome/indoor games (not needed)
    "wind",  # wind speed at kickoff (mph); null for dome/indoor games (not needed)
    "away_coach",  # head coach of away team
    "home_coach",  # head coach of home team
    "referee",  # crew chief / referee name
    "div_game",  # 1 if both teams are in the same division, else 0
    "spread_line",  # Vegas spread (negative = home favored)
    "total_line",  # Vegas over/under total
]


def build_master_games(schedules_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build canonical master games table from nflreadpy load_schedules() output.

    Input : schedules_df — one row per game from nfl.load_schedules()
    Output: master_games — cleaned, normalized, one row per game

    Key design decisions:
      - home_team / away_team are normalized through TEAM_ALIASES so a 2018
        Oakland Raiders game is stored with team_id "LV", not "OAK"
      - result is computed as home_score - away_score if not already present
      - rows with a null game_id are dropped (bye weeks / placeholder rows)
    """
    df = schedules_df.copy()

    # Normalize team abbreviations
    for col in ("home_team", "away_team"):
        if col in df.columns:
            df[col] = df[col].map(normalize_team_abbr)

    # Compute result
    if "result" not in df.columns:
        if "home_score" in df.columns and "away_score" in df.columns:
            df["result"] = df["home_score"] - df["away_score"]

    # Select only the columns we need
    df = df[[c for c in _KEEP if c in df.columns]].copy()

    # Drop rows with no game_id (bye-week placeholders, future unplayed games)
    df = df.dropna(subset=["game_id"])

    return df.reset_index(drop=True)
