"""
team_normalizer.py — Build master_teams curated table.

nflreadpy load_teams() returns 36 rows: 32 currently active franchises
plus 4 historical entries for relocated teams that appear in legacy data:

  LAR  Los Angeles Rams  — early post-move abbreviation, now canonical "LA"
  OAK  Oakland Raiders   — moved to Las Vegas in 2020, now "LV"
  SD   San Diego Chargers — moved to Los Angeles in 2017, now "LAC"
  STL  St. Louis Rams    — moved to Los Angeles in 2016, now "LA"

All four historical abbreviations are kept as rows (with is_active=False)
so foreign-key joins on team_abbr never silently drop historical data.
The TEAM_ALIASES dict is the single source of truth for normalizing any
abbreviation — from any data source — to its current canonical form.
"""

import pandas as pd

# ---------------------------------------------------------------------------
# Alias map: any known abbreviation → current canonical team abbreviation
# ---------------------------------------------------------------------------

# Franchise relocations (confirmed from nflreadpy load_teams() output)
_RELOCATIONS: dict[str, str] = {
    "LAR": "LA",  # Los Angeles Rams early post-move abbr → current "LA"
    "OAK": "LV",  # Oakland Raiders → Las Vegas Raiders (2020)
    "SD": "LAC",  # San Diego Chargers → Los Angeles Chargers (2017)
    "STL": "LA",  # St. Louis Rams → Los Angeles Rams (2016)
}

# Source-specific quirks (same franchise, different abbreviation by data vendor)
_SOURCE_QUIRKS: dict[str, str] = {
    "JAC": "JAX",  # Jaguars — NFL.com uses JAC, nflverse uses JAX
    "ARZ": "ARI",  # Cardinals — used by some legacy sources
    "BLT": "BAL",  # Ravens — used by some legacy sources
    "CLV": "CLE",  # Browns — used by some legacy sources
    "HST": "HOU",  # Texans — used by some legacy sources
    "SL": "LA",  # abbreviated St. Louis form in old box scores
}

# Combined map used by normalize_team_abbr()
TEAM_ALIASES: dict[str, str] = {**_RELOCATIONS, **_SOURCE_QUIRKS}

# The 4 rows in load_teams() that represent historical (relocated) franchises
HISTORICAL_ABBRS: frozenset[str] = frozenset(_RELOCATIONS.keys())


def normalize_team_abbr(abbr: str) -> str:
    """
    Map any known historical or source-specific abbreviation to its current
    canonical form. Returns the input unchanged for already-canonical values.
    """
    if not isinstance(abbr, str):
        return abbr
    return TEAM_ALIASES.get(abbr.upper(), abbr.upper())


def build_master_teams(teams_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build canonical master teams table from nflreadpy load_teams() output.

    Keeps all 36 rows (32 active + 4 historical) so joins on historical
    abbreviations resolve cleanly. Use is_active to filter to current teams.

    Output columns:
      team_id          — primary key; current canonical abbreviation for active
                         teams, historical abbreviation for inactive rows
      team_name        — full franchise name (e.g. "Kansas City Chiefs")
      team_nick        — nickname only (e.g. "Chiefs")
      conference       — "AFC" | "NFC"
      division         — e.g. "AFC West"
      primary_color    — hex brand color
      secondary_color  — hex brand color
      is_active        — True for the 32 current franchises
      canonical_abbr   — for historical rows: the current abbreviation this
                         team_id maps to (e.g. LAR → "LA"); same as team_id
                         for active rows — use this for all downstream joins
    """
    df = teams_df.copy()

    # nflreadpy includes a numeric 'team_id' column alongside 'team_abbr'.
    # We use team_abbr as our stable string primary key, so drop the numeric one first.
    df = df.drop(columns=["team_id"], errors="ignore")

    rename = {
        "team_abbr": "team_id",
        "team_name": "team_name",
        "team_nick": "team_nick",
        "team_conf": "conference",
        "team_division": "division",
        "team_color": "primary_color",
        "team_color2": "secondary_color",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = [
        "team_id",
        "team_name",
        "team_nick",
        "conference",
        "division",
        "primary_color",
        "secondary_color",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Flag active vs. historical franchises
    df["is_active"] = ~df["team_id"].isin(HISTORICAL_ABBRS)

    # canonical_abbr: resolve historical rows to their current abbreviation
    df["canonical_abbr"] = df["team_id"].map(lambda t: TEAM_ALIASES.get(t, t))

    return df.reset_index(drop=True)
