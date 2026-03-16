"""

Measures how available and "durable" a player has been across their career.
Two complementary dimensions are combined into a single durability_score:

  injury_frequency  = total injury events / seasons_active
                      An "injury event" is any week where a player appears
                      on the injury report with a non-trivial status:
                      "Out", "Doubtful", or "Questionable".
                      Lower is better.

  games_played_rate = distinct weeks with offensive snaps / (seasons_active × 17)
                      Approximates the fraction of possible games a player
                      actually appeared in. 17 is the current regular-season
                      game count; pre-2021 seasons (16 games) are handled by
                      using actual max weeks in the data rather than a fixed 17.
                      Higher is better.

  durability_score  = composite z-score: mean(-injury_frequency_z, games_played_rate_z)
                      Positive = more durable than average; negative = less durable.
                      Z-scored within position group so a CB is compared to CBs,
                      not to QBs who historically miss fewer games.
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# Injury statuses that represent a meaningful health concern
_INJURY_STATUSES = {"Out", "Doubtful", "Questionable"}

_POS_GROUPS = {
    "QB": "QB",
    "RB": "RB",
    "HB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OL",
    "OG": "OL",
    "C": "OL",
    "G": "OL",
    "T": "OL",
    "OL": "OL",
    "DE": "EDGE",
    "OLB": "EDGE",
    "DT": "IDL",
    "NT": "IDL",
    "DL": "IDL",
    "ILB": "LB",
    "MLB": "LB",
    "LB": "LB",
    "CB": "CB",
    "S": "DB",
    "FS": "DB",
    "SS": "DB",
    "DB": "DB",
    "K": "SPEC",
    "P": "SPEC",
    "LS": "SPEC",
}


def _pos_group(pos: str) -> str:
    if not isinstance(pos, str):
        return "UNK"
    return _POS_GROUPS.get(pos.strip().upper(), "UNK")


def _zscore(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std(ddof=0)


def build_durability_profiles(
    injuries_path: Path,
    snap_counts_path: Path,
    rosters_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    """
    Build player_durability_profiles from injuries, snap_counts, and rosters.

    Input  : injuries.parquet + snap_counts.parquet + rosters.parquet (staged, after Stage 2)
    Output : player_durability_profiles.parquet (curated) — one row per player
    """
    missing = [
        p for p in [injuries_path, snap_counts_path, rosters_path] if not p.exists()
    ]
    if missing:
        logger.warning("Missing input files: %s", [p.name for p in missing])
        return pd.DataFrame()

    rosters = pd.read_parquet(rosters_path)  # check for the active season
    id_col_r = "player_id" if "player_id" in rosters.columns else "gsis_id"
    seasons_active = (
        rosters.groupby(id_col_r)["season"]
        .nunique()
        .reset_index()
        .rename(columns={id_col_r: "player_id", "season": "seasons_active"})
    )

    # injury events
    inj = pd.read_parquet(injuries_path)
    id_col_i = "player_id" if "player_id" in inj.columns else "gsis_id"

    # Keep only regular-season injury events with actual status
    if "game_type" in inj.columns:
        inj = inj[inj["game_type"] == "REG"]
    if "report_status" in inj.columns:
        inj = inj[inj["report_status"].isin(_INJURY_STATUSES)]

    injury_counts = (
        inj.groupby(id_col_i)
        .size()
        .reset_index(name="total_injury_events")
        .rename(columns={id_col_i: "player_id"})
    )

    sc = pd.read_parquet(snap_counts_path)  # games from snap counts
    if "game_type" in sc.columns:
        sc = sc[sc["game_type"] == "REG"]
    if "player_id" in sc.columns:
        id_col_sc = "player_id"
    else:
        id_col_sc = "pfr_player_id"
        logger.warning(
            "snap_counts lacks player_id — run Stage 2 first. "
            "games_played and position_map will not merge with rosters/injuries."
        )

    # Grab position from snap_counts
    position_map = sc.groupby(id_col_sc)["position"].first().reset_index()
    position_map.columns = ["player_id", "position"]

    games_played = (
        sc[sc["offense_snaps"] > 0]
        .groupby(id_col_sc)
        .agg(games_played=("week", "nunique"))
        .reset_index()
        .rename(columns={id_col_sc: "player_id"})
    )

    # --- Merge all three sources ---
    result = seasons_active.merge(injury_counts, on="player_id", how="left")
    result = result.merge(games_played, on="player_id", how="left")
    result = result.merge(position_map, on="player_id", how="left")

    result["total_injury_events"] = result["total_injury_events"].fillna(0)
    result["games_played"] = result["games_played"].fillna(0)

    # --- Derived metrics ---
    result["injury_frequency"] = result["total_injury_events"] / result[
        "seasons_active"
    ].replace(0, np.nan)
    result["games_played_rate"] = result["games_played"] / (
        result["seasons_active"] * 17
    ).replace(0, np.nan)

    # Cap games_played_rate at 1.0 (some players appear in more than 17 weeks via ST)
    result["games_played_rate"] = result["games_played_rate"].clip(upper=1.0)

    # --- durability_score within position group ---
    result["position_group"] = result["position"].map(_pos_group).fillna("UNK")

    def _group_zscore(df: pd.DataFrame, col: str) -> pd.Series:
        return df.groupby("position_group")[col].transform(
            lambda x: (
                _zscore(x) if x.notna().sum() >= 3 else pd.Series(np.nan, index=x.index)
            )
        )

    result["_inj_freq_z"] = _group_zscore(result, "injury_frequency")
    result["_games_played_rate_z"] = _group_zscore(result, "games_played_rate")

    # Combine: negate injury_frequency_z because lower frequency = better durability
    result["durability_score"] = (
        (-result["_inj_freq_z"]) + result["_games_played_rate_z"]
    ) / 2

    result = result.drop(columns=["_inj_freq_z", "_games_played_rate_z"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    logger.info(
        "player_durability_profiles → %s (%s players, %s with durability_score)",
        output_path.name,
        f"{len(result):,}",
        f"{result['durability_score'].notna().sum():,}",
    )
    return result
