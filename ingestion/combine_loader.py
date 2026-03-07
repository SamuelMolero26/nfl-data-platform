import re
import pandas as pd
from pathlib import Path

from ingestion.base import SourceLoader


def _parse_height(ht: str) -> float | None:
    """Convert '6-2' → 74.0 inches."""
    if not isinstance(ht, str):
        return None
    m = re.match(r"(\d+)-(\d+)", ht.strip())
    return int(m.group(1)) * 12 + int(m.group(2)) if m else None


def _parse_drafted(drafted: str) -> dict:
    """
    Parse 'Dallas Cowboys / 7th / 247th pick / 2025'
    → {draft_team, draft_round, draft_pick, draft_year}
    """
    empty = {"draft_team": None, "draft_round": None, "draft_pick": None, "draft_year": None}
    if not isinstance(drafted, str) or not drafted.strip():
        return empty
    parts = [p.strip() for p in drafted.split("/")]
    if len(parts) < 4:
        return empty
    pick_m = re.search(r"(\d+)", parts[2])
    year_m = re.search(r"(\d{4})", parts[3])
    return {
        "draft_team": parts[0],
        "draft_round": parts[1],
        "draft_pick": int(pick_m.group(1)) if pick_m else None,
        "draft_year": int(year_m.group(1)) if year_m else None,
    }


class CombineLoader(SourceLoader):
    def extract(self) -> pd.DataFrame:
        # File is HTML-disguised XLS — pd.read_html is the correct parser
        tables = pd.read_html(str(self.source_path))
        return tables[0]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Drop the "College Stats" link-text column
        if "College" in df.columns:
            df = df.drop(columns=["College"])

        df = df.rename(columns={
            "Player": "player_name",
            "Pos": "position",
            "School": "school",
            "Ht": "height_str",
            "Wt": "weight_lbs",
            "40yd": "forty_yard",
            "Vertical": "vertical_in",
            "Bench": "bench_reps",
            "Broad Jump": "broad_jump_in",
            "3Cone": "three_cone",
            "Shuttle": "shuttle",
            "Drafted (tm/rnd/yr)": "drafted_raw",
        })

        # Height string → total inches
        df["height_in"] = df["height_str"].apply(_parse_height)
        df = df.drop(columns=["height_str"])

        # Split draft string into structured columns
        draft_cols = df["drafted_raw"].apply(_parse_drafted).apply(pd.Series)
        df = pd.concat([df.drop(columns=["drafted_raw"]), draft_cols], axis=1)

        # Cast all numeric columns (coerce bad values to NaN)
        for col in ["weight_lbs", "forty_yard", "vertical_in", "bench_reps",
                    "broad_jump_in", "three_cone", "shuttle"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df.reset_index(drop=True)
