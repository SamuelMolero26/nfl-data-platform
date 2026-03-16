"""
draft_value.py — Build draft_value_history curated table.

Measures whether each drafted player over- or under-performed the historical
expectations for their draft slot, using Career Approximate Value (car_av)
from Pro Football Reference as the career production metric.

  car_av        — Pro Football Reference's Career Approximate Value. A single
                  number capturing overall career contribution across all
                  positions. Calculated by PFR and already present in
                  load_draft_picks() output; no external lookup required.

  draft_value_score — z-score of car_av within draft round.
                      "Did this player produce more or less than other players
                      taken in the same round?"
                      Positive = outperformed round expectations.
                      Negative = underperformed round expectations.

  draft_value_percentile — percentile rank of car_av within round (0–100).
                           More interpretable than raw z-score for non-technical
                           stakeholders: a 90 means the player produced more
                           than 90% of players drafted in the same round.

Why round-based rather than pick-based:
  Pick-by-pick z-scores would have very few observations per bucket, making
  individual comparisons noisy. Grouping by round (7 groups of ~32 picks)
  gives stable distributions while preserving the most meaningful distinction
  in draft capital (1st vs 2nd vs late rounds).
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def build_draft_value_history(
    draft_history_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    """
    Build draft_value_history from staged draft_history Parquet.

    Input  : draft_history.parquet (staged, after Stage 2) — one row per draft pick
    Output : draft_value_history.parquet (curated) — one row per drafted player

    Only players with car_av > 0 are scored (players who never played have
    car_av = 0 and are kept in the table but receive NaN scores so they don't
    distort the round-level distributions).
    """
    if not draft_history_path.exists():
        logger.warning("draft_history_path not found: %s", draft_history_path)
        return pd.DataFrame()

    df = pd.read_parquet(draft_history_path).copy()

    # Resolve player_id: prefer Stage-2-resolved player_id, fall back to gsis_id
    if "player_id" not in df.columns:
        if "gsis_id" in df.columns:
            df = df.rename(columns={"gsis_id": "player_id"})
            logger.warning(
                "player_id not found — using gsis_id directly. Run Stage 2 for full resolution."
            )
        else:
            logger.error("No player identity column found in draft_history")
            return pd.DataFrame()

    # Standardize player name column
    if "player_name" not in df.columns and "pfr_player_name" in df.columns:
        df = df.rename(columns={"pfr_player_name": "player_name"})

    # nflreadpy returns car_av as None; fall back to w_av (Weighted Career AV),
    # which is populated and slightly prefers peak/recent seasons over raw volume.
    if "car_av" not in df.columns or df["car_av"].isna().all():
        if "w_av" in df.columns:
            logger.info(
                "car_av unavailable — using w_av (Weighted Career Approximate Value)"
            )
            df["car_av"] = pd.to_numeric(df["w_av"], errors="coerce")
        else:
            logger.warning(
                "Neither car_av nor w_av found — draft_value_score unavailable"
            )
            df["car_av"] = np.nan
    else:
        df["car_av"] = pd.to_numeric(df["car_av"], errors="coerce")

    # Only score players who actually have career data
    scoreable = df["car_av"].notna() & (df["car_av"] > 0)

    # --- draft_value_score: z-score within round ---
    def _round_zscore(x: pd.Series) -> pd.Series:
        if x.notna().sum() < 3:
            return pd.Series(np.nan, index=x.index)
        return (x - x.mean()) / x.std(ddof=0)

    df["draft_value_score"] = np.where(
        scoreable,
        df.groupby("round")["car_av"].transform(_round_zscore),
        np.nan,
    )

    # --- draft_value_percentile: percentile rank within round (0–100) ---
    def _round_percentile(x: pd.Series) -> pd.Series:
        return x.rank(pct=True) * 100

    df["draft_value_percentile"] = np.where(
        scoreable,
        df.groupby("round")["car_av"].transform(_round_percentile),
        np.nan,
    )

    # Select output columns
    keep_cols = [
        "player_id",
        "player_name",
        "season",
        "team",
        "round",
        "pick",
        "position",
        "category",
        "college",
        "age",
        "car_av",
        "w_av",
        "games",
        "seasons_started",
        "allpro",
        "probowls",
        "draft_value_score",
        "draft_value_percentile",
    ]
    result = df[[c for c in keep_cols if c in df.columns]].copy()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    logger.info(
        "draft_value_history → %s (%s picks, %s scored)",
        output_path.name,
        f"{len(result):,}",
        f"{result['draft_value_score'].notna().sum():,}",
    )
    return result
