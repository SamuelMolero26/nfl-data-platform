import pandas as pd
from pathlib import Path

from ingestion.base import SourceLoader


class TeamStatsLoader(SourceLoader):
    def extract(self) -> pd.DataFrame:
        return pd.read_csv(self.source_path)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if len(df.columns) < 56:
            raise ValueError(f"Expected 56+ columns, got {len(df.columns)}")

        # Derived columns for curated convenience
        #Subject to change
        total_games = df["wins"] + df["losses"] + df["ties"]
        safe_games = total_games.replace(0, pd.NA)
        df["win_pct"] = (df["wins"] + 0.5 * df["ties"]) / safe_games
        df["point_diff_per_game"] = df["score_differential"] / safe_games

        # Nulls in ave_air_yards / ave_yac for early seasons are expected — keep as NaN

        return df.reset_index(drop=True)
