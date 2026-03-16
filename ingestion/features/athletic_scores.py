"""
Explanation of this approach:

Computes five combine-derived athletic scores per player. All scores except
speed_score are z-scored within position group so a WR's agility is compared
to other WRs, not to offensive linemen.

Formulas:
  speed_score   = (weight_lbs * 200) / (forty_yard ** 4)
                  Bill Barnwell's formula — rewards heavy players who run fast.
                  A 230-lb RB running a 4.4 is more impressive than a 175-lb
                  WR running the same time.

  agility_score = mean(-three_cone_z, -shuttle_z)  [within position group]
                  Negative because lower time = better agility. Measures
                  change-of-direction ability.

  burst_score   = mean(vertical_z, broad_jump_z)   [within position group]
                  Explosive lower-body power. Strong predictor of separation
                  ability for skill positions.

  strength_score = bench_reps_z                    [within position group]
                  Upper-body strength relative to peers. Most relevant for
                  OL, DL, and power backs.

  size_score    = (height_in * weight_lbs) / position_group_avg_size
                  Raw size relative to the position average. A value > 1.0
                  means the player is larger than the typical prospect at
                  their position.
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


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
    "S": "S",
    "FS": "S",
    "SS": "S",
    "DB": "S",
    "K": "SPEC",
    "P": "SPEC",
    "LS": "SPEC",
}


def _pos_group(pos: str) -> str:
    if not isinstance(pos, str):
        return "UNK"
    return _POS_GROUPS.get(pos.strip().upper(), "UNK")


def _zscore_within_group(df: pd.DataFrame, col: str, group_col: str) -> pd.Series:
    """Z-score of col within each group. Groups with < 3 observations get NaN."""

    def _z(x):
        if x.notna().sum() < 3:
            return pd.Series(np.nan, index=x.index)
        return (x - x.mean()) / x.std(ddof=0)

    return df.groupby(group_col)[col].transform(_z)


def build_athletic_profiles(combine_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Build player_athletic_profiles from staged combine Parquet.

    Input  : combine.parquet (staged) — one row per combine participant
    Output : player_athletic_profiles.parquet (curated) — one row per player

    Rows without player_id (unresolved combine prospects) are retained so
    scores are still available for scouting, but they won't join to
    master_players until those players are drafted and rostered.
    """
    if not combine_path.exists():
        logger.warning("combine_path not found: %s", combine_path)
        return pd.DataFrame()

    df = pd.read_parquet(combine_path).copy()

    if "player_id" not in df.columns:
        logger.warning(
            "player_id column missing from combine — run Stage 2 first. "
            "Athletic scores will be computed but won't join to master_players."
        )

    # Assign position group
    df["position_group"] = df["position"].map(_pos_group).fillna("UNK")

    # --- speed_score (raw formula, not z-scored — already size-adjusted) ---
    mask_speed = (
        df["weight_lbs"].notna() & df["forty_yard"].notna() & (df["forty_yard"] > 0)
    )
    df["speed_score"] = np.where(
        mask_speed,
        (df["weight_lbs"] * 200) / (df["forty_yard"] ** 4),
        np.nan,
    )

    # --- agility_score: mean(-three_cone_z, -shuttle_z) within position group ---
    df["_three_cone_z"] = _zscore_within_group(df, "three_cone", "position_group")
    df["_shuttle_z"] = _zscore_within_group(df, "shuttle", "position_group")
    df["agility_score"] = ((-df["_three_cone_z"]) + (-df["_shuttle_z"])) / 2

    # --- burst_score: mean(vertical_z, broad_jump_z) within position group ---
    df["_vertical_z"] = _zscore_within_group(df, "vertical_in", "position_group")
    df["_broad_jump_z"] = _zscore_within_group(df, "broad_jump_in", "position_group")
    df["burst_score"] = (df["_vertical_z"] + df["_broad_jump_z"]) / 2

    # --- strength_score: bench_reps z-score within position group ---
    df["strength_score"] = _zscore_within_group(df, "bench_reps", "position_group")

    # --- size_score: player size / position-group average size ---
    df["_size_raw"] = df["height_in"] * df["weight_lbs"]
    group_avg_size = df.groupby("position_group")["_size_raw"].transform("mean")
    df["size_score"] = np.where(
        group_avg_size > 0,
        df["_size_raw"] / group_avg_size,
        np.nan,
    )

    # Drop temp columns
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")])

    # Select correct output columns
    id_cols = [c for c in ["player_id", "player_name"] if c in df.columns]
    meta_cols = [
        "position",
        "position_group",
        "school",
        "height_in",
        "weight_lbs",
        "forty_yard",
        "vertical_in",
        "broad_jump_in",
        "three_cone",
        "shuttle",
        "bench_reps",
        "draft_year",
        "draft_round",
        "draft_pick",
    ]
    score_cols = [
        "speed_score",
        "agility_score",
        "burst_score",
        "strength_score",
        "size_score",
    ]

    keep = id_cols + [c for c in meta_cols + score_cols if c in df.columns]
    result = df[keep].copy()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    logger.info(
        "player_athletic_profiles → %s (%s rows, %s with speed_score)",
        output_path.name,
        f"{len(result):,}",
        f"{result['speed_score'].notna().sum():,}",
    )
    return result
