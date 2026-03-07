import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import config
from ingestion.combine_loader import CombineLoader
from ingestion.team_stats_loader import TeamStatsLoader


def run_ingestion() -> dict[str, pd.DataFrame]:
    print("=== Ingestion Pipeline ===\n")

    results = {}

    # --- Sources ---
    sources = [
        (CombineLoader(config.RAW_COMBINE), config.STAGED_COMBINE, "combine"),
        (TeamStatsLoader(config.RAW_TEAM_STATS), config.STAGED_TEAM_STATS, "team_stats"),
    ]

    for loader, output_path, key in sources:
        results[key] = loader.run(output_path)
        print()

    # --- Curated layer ---
    print("=== Building Curated Layer ===\n")
    _build_player_profiles(results["combine"])
    _build_team_performance(results["team_stats"])

    print("\nPipeline complete.")
    return results


def _build_player_profiles(combine: pd.DataFrame) -> None:
    """Curated player profiles — combine stats with parsed draft info."""
    out = config.CURATED_PLAYER_PROFILES
    out.parent.mkdir(parents=True, exist_ok=True)
    combine.to_parquet(out, index=False)
    print(f"  [curated] player_profiles → {out} ({len(combine):,} rows)")


def _build_team_performance(team_stats: pd.DataFrame) -> None:
    """Curated team performance — full stats with derived win_pct and per-game metrics."""
    out = config.CURATED_TEAM_PERFORMANCE
    out.parent.mkdir(parents=True, exist_ok=True)
    team_stats.to_parquet(out, index=False)
    print(f"  [curated] team_performance → {out} ({len(team_stats):,} rows)")


if __name__ == "__main__":
    run_ingestion()
