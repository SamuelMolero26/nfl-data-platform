"""
production_scores.py — Build player_production_profiles curated table.

Computes per-season NFL production metrics per player, aggregated from
weekly_stats and snap_counts. All z-scores are within position group and
season so that a 2015 WR is compared to other 2015 WRs, not to modern ones.

Metrics:
  snap_share        — mean offense_pct across all regular-season weeks played
                      (directly from snap_counts; already normalized 0–1)

  epa_per_game      — sum of all EPA columns / games played; measures how much
                      value a player added per game across all play types
                      (passing_epa + rushing_epa + receiving_epa from weekly_stats)

  passing_cpoe      — mean completion % over expected (QBs only); from
                      weekly_stats.passing_cpoe; accounts for target difficulty

  target_share      — mean target_share from weekly_stats (WR/TE/RB);
                      fraction of team targets captured each week

  nfl_production_score — composite z-score:
                         mean(snap_share_z, epa_per_game_z) within position × season
                         Summarizes "how much did this player contribute relative
                         to peers at the same position in the same year?"
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

_OFFENSE_POSITIONS = {"QB", "RB", "HB", "FB", "WR", "TE"}


def _zscore_within(df: pd.DataFrame, col: str, groups: list[str]) -> pd.Series:
    """Z-score of col within each combination of group columns."""

    def _z(x):
        if x.notna().sum() < 3:
            return pd.Series(np.nan, index=x.index)
        return (x - x.mean()) / x.std(ddof=0)

    return df.groupby(groups)[col].transform(_z)


def build_production_profiles(
    snap_counts_path: Path,
    weekly_stats_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    """
    Build player_production_profiles from staged snap_counts and weekly_stats.

    Input  : snap_counts.parquet + weekly_stats.parquet (staged, after Stage 2)
    Output : player_production_profiles.parquet (curated) — one row per
             (player_id, season)

    Only regular-season weeks (game_type == 'REG') are included in all
    aggregations so postseason performances don't inflate season totals.
    """
    missing = [p for p in [snap_counts_path, weekly_stats_path] if not p.exists()]
    if missing:
        logger.warning("Missing input files: %s", [p.name for p in missing])
        return pd.DataFrame()

    # --- Snap counts: season-level snap_share ---
    # Requires Stage 2 to have run so snap_counts has player_id (gsis_id).
    # If only pfr_player_id is available, snap_share won't join to weekly_stats
    # and will be logged as a warning.

    # TODO: testing and validation
    sc = pd.read_parquet(snap_counts_path)
    if "game_type" in sc.columns:
        sc = sc[sc["game_type"] == "REG"]

    if "player_id" in sc.columns:
        id_col_sc = "player_id"
    else:
        id_col_sc = "pfr_player_id"
        logger.warning(
            "snap_counts lacks player_id — run Stage 2 first. "
            "snap_share will not merge with weekly_stats EPA metrics."
        )

    sc_season = (
        sc.groupby([id_col_sc, "season", "position"])
        .agg(
            snap_share=("offense_pct", "mean"),
            games_with_snaps=("offense_snaps", lambda x: (x > 0).sum()),
        )
        .reset_index()
        .rename(columns={id_col_sc: "player_id"})
    )

    # weekly stats
    ws = pd.read_parquet(weekly_stats_path)
    if "season_type" in ws.columns:
        ws = ws[ws["season_type"] == "REG"]
    elif "game_type" in ws.columns:
        ws = ws[ws["game_type"] == "REG"]

    epa_cols = [
        c for c in ["passing_epa", "rushing_epa", "receiving_epa"] if c in ws.columns
    ]

    agg_dict: dict = {"player_name": "first"}
    if epa_cols:
        ws["total_epa"] = ws[epa_cols].fillna(0).sum(axis=1)
        agg_dict["total_epa"] = "sum"
    if "passing_cpoe" in ws.columns:
        agg_dict["passing_cpoe"] = "mean"
    if "target_share" in ws.columns:
        agg_dict["target_share"] = "mean"

    ws["games_played"] = 1
    agg_dict["games_played"] = "sum"

    ws_season = ws.groupby(["player_id", "season"]).agg(agg_dict).reset_index()

    if "total_epa" in ws_season.columns and "games_played" in ws_season.columns:
        ws_season["epa_per_game"] = ws_season["total_epa"] / ws_season[
            "games_played"
        ].replace(0, np.nan)

    result = sc_season.merge(
        ws_season.drop(columns=["player_name"], errors="ignore"),
        on=["player_id", "season"],
        how="outer",
    )  # snaps + stats

    if "position" not in result.columns and "position" in ws.columns:
        pos_map = ws.groupby("player_id")["position"].first()
        result["position"] = result["player_id"].map(pos_map)

    result["position"] = result["position"].fillna("UNK")

    # z-score within (position, season)
    z_cols = [c for c in ["snap_share", "epa_per_game"] if c in result.columns]
    if z_cols:
        z_parts = [_zscore_within(result, c, ["position", "season"]) for c in z_cols]
        result["nfl_production_score"] = pd.concat(z_parts, axis=1).mean(axis=1)

    # Drop rows with no player_id
    before = len(result)
    result = result.dropna(subset=["player_id"])
    dropped = before - len(result)
    if dropped:
        logger.warning("Dropped %s rows with no player_id", f"{dropped:,}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    logger.info(
        "player_production_profiles → %s (%s player-seasons)",
        output_path.name,
        f"{len(result):,}",
    )
    return result
