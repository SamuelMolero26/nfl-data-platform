"""
Ingestion pipeline — medallion architecture (raw → staged → curated).

Stages:
  0 — Raw ingestion   : all sources → staged Parquet (parallel-safe)
  1 — Master tables   : team/game/player anchors
  2 — ID resolution   : attach canonical player_id to all staged tables
  3 — Gold features   : athletic / production / durability / draft-value profiles
  4 — Backward compat : legacy player_profiles + team_performance curated files

Run everything:
    python ingestion/pipeline.py

Run only legacy sources (no nflreadpy required):
    python ingestion/pipeline.py --legacy
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import logging

import pandas as pd

import config
from ingestion.loaders.combine_loader import CombineLoader
from ingestion.loaders.nflreadpy_loader import NflreadpyLoader
from ingestion.loaders.team_stats_loader import TeamStatsLoader
from ingestion.player_id_resolver import PlayerIdResolver
from ingestion.transforms.game_builder import build_master_games
from ingestion.transforms.player_identity import build_master_players
from ingestion.transforms.team_normalizer import build_master_teams

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 0 — Raw ingestion
# ---------------------------------------------------------------------------


def run_stage0(skip_nflreadpy: bool = False) -> dict[str, pd.DataFrame]:
    """Ingest all raw sources and write staged Parquet files."""
    logger.info("=== Stage 0: Raw Ingestion ===")
    results: dict[str, pd.DataFrame] = {}

    file_sources = [
        (CombineLoader(config.RAW_COMBINE), config.STAGED_COMBINE, "combine"),
        (
            TeamStatsLoader(config.RAW_TEAM_STATS),
            config.STAGED_TEAM_STATS,
            "team_stats",
        ),
    ]
    for loader, output_path, key in file_sources:
        results[key] = loader.run(output_path)

    if skip_nflreadpy:
        logger.info("nflreadpy ingestion skipped (--legacy mode)")
        return results

    loader = NflreadpyLoader(
        start_year=config.NFLVERSE_START_YEAR,
        end_year=config.NFLVERSE_END_YEAR,
    )

    # Merge XLS combine (current class) with nflreadpy historical combine (2000–present).
    # XLS rows take precedence for any overlapping draft_year.
    results["combine"] = loader.load_combine(
        config.STAGED_COMBINE,
        xls_df=results.get("combine"),
    )

    nfl_paths = {
        "rosters": config.STAGED_ROSTERS,
        "weekly_stats": config.STAGED_WEEKLY_STATS,
        "snap_counts": config.STAGED_SNAP_COUNTS,
        "depth_charts": config.STAGED_DEPTH_CHARTS,
        "injuries": config.STAGED_INJURIES,
        "draft_picks": config.STAGED_DRAFT_HISTORY,
        "ngs_data": config.STAGED_NGS_DATA,
        "pfr_advstats": config.STAGED_PFR_ADVSTATS,
        "ftn_data": config.STAGED_FTN_DATA,
        "contracts": config.STAGED_CONTRACTS,
        "schedules": config.STAGED_SCHEDULES,
        "teams": config.LAKE_STAGED_DIR / "teams" / "nfl_teams.parquet",
        "ff_playerids": config.LAKE_STAGED_DIR / "players" / "ff_playerids.parquet",
    }
    results.update(loader.run_all(nfl_paths))
    return results


# ---------------------------------------------------------------------------
# Stage 1 — Master tables (canonical team/game/player anchors)
# ---------------------------------------------------------------------------


def run_stage1(stage0: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Build canonical master_teams, master_games, master_players tables.

    Reads from stage0 results when available; falls back to staged Parquet
    files on disk so this stage can run independently after Stage 0.
    """
    logger.info("=== Stage 1: Master Tables ===")
    results: dict[str, pd.DataFrame] = {}

    def _get(key: str, path: Path) -> pd.DataFrame | None:
        if key in stage0:
            return stage0[key]
        if path.exists():
            return pd.read_parquet(path)
        logger.warning("%s not found — skipping dependent tables", path.name)
        return None

    teams_raw = _get("teams", config.LAKE_STAGED_DIR / "teams" / "nfl_teams.parquet")
    if teams_raw is not None:
        master_teams = build_master_teams(teams_raw)
        out = config.CURATED_MASTER_TEAMS
        out.parent.mkdir(parents=True, exist_ok=True)
        master_teams.to_parquet(out, index=False)
        logger.info("master_teams → %s (%s rows)", out.name, f"{len(master_teams):,}")
        results["master_teams"] = master_teams

    schedules_raw = _get("schedules", config.STAGED_SCHEDULES)
    if schedules_raw is not None:
        master_games = build_master_games(schedules_raw)
        out = config.CURATED_MASTER_GAMES
        out.parent.mkdir(parents=True, exist_ok=True)
        master_games.to_parquet(out, index=False)
        logger.info("master_games → %s (%s rows)", out.name, f"{len(master_games):,}")
        results["master_games"] = master_games

    rosters_raw = _get("rosters", config.STAGED_ROSTERS)
    if rosters_raw is not None:
        master_players = build_master_players(rosters_raw)
        out = config.CURATED_MASTER_PLAYERS
        out.parent.mkdir(parents=True, exist_ok=True)
        master_players.to_parquet(out, index=False)
        logger.info(
            "master_players → %s (%s rows)", out.name, f"{len(master_players):,}"
        )
        results["master_players"] = master_players

    return results


# ---------------------------------------------------------------------------
# Stage 2 — Player ID resolution
# ---------------------------------------------------------------------------


def run_stage2(stage1: dict[str, pd.DataFrame]) -> None:
    """
    Attach canonical player_id (gsis_id) to all staged tables that need it.

    Builds a PlayerIdResolver from master_players + ff_playerids, then runs
    three-pass resolution (direct gsis_id → pfr_id lookup → name fuzzy) on
    each staged table. Overwrites staged Parquet files in-place.
    """
    logger.info("=== Stage 2: Player ID Resolution ===")

    master_players = stage1.get("master_players")
    if master_players is None:
        if config.CURATED_MASTER_PLAYERS.exists():
            master_players = pd.read_parquet(config.CURATED_MASTER_PLAYERS)
        else:
            logger.error("master_players not found — Stage 2 skipped")
            return

    ff_ids_path = config.LAKE_STAGED_DIR / "players" / "ff_playerids.parquet"
    ff_playerids = pd.read_parquet(ff_ids_path) if ff_ids_path.exists() else None
    if ff_playerids is None:
        logger.warning("ff_playerids not found — Pass 2 (pfr_id lookup) disabled")

    resolver = PlayerIdResolver(master_players, ff_playerids)
    logger.info(
        "Resolver built: %s players indexed, %s pfr_id mappings",
        f"{len(resolver._fuzzy_names):,}",
        f"{len(resolver._pfr_to_gsis):,}",
    )

    specs = [
        (
            config.STAGED_INJURIES,
            dict(gsis_id_col="gsis_id", name_col="full_name", position_col="position"),
            "injuries",
        ),
        (
            config.STAGED_DEPTH_CHARTS,
            dict(gsis_id_col="gsis_id", name_col="full_name", position_col="position"),
            "depth_charts",
        ),
        (config.STAGED_CONTRACTS, dict(gsis_id_col="gsis_id"), "contracts"),
        (config.STAGED_WEEKLY_STATS, dict(gsis_id_col="player_id"), "weekly_stats"),
        (
            config.STAGED_DRAFT_HISTORY,
            dict(
                gsis_id_col="gsis_id",
                pfr_id_col="pfr_player_id",
                name_col="pfr_player_name",
                position_col="position",
            ),
            "draft_history",
        ),
        (
            config.STAGED_SNAP_COUNTS,
            dict(
                pfr_id_col="pfr_player_id", name_col="player", position_col="position"
            ),
            "snap_counts",
        ),
        (
            config.STAGED_PFR_ADVSTATS,
            dict(pfr_id_col="pfr_player_id", name_col="pfr_player_name"),
            "pfr_advstats",
        ),
        (
            config.STAGED_COMBINE,
            dict(name_col="player_name", position_col="position"),
            "combine",
        ),
    ]

    for path, kwargs, label in specs:
        if not path.exists():
            logger.warning("[%s] skipped — file not found", label)
            continue
        df = pd.read_parquet(path)
        resolved = resolver.resolve_dataframe(df, **kwargs)
        logger.info("[%s] ", label)
        resolver.resolution_summary(resolved)
        resolved.to_parquet(path, index=False)


# feature engineering
def run_stage3() -> None:
    """Build all gold-layer feature profiles."""
    logger.info("=== Stage 3: Gold Feature Engineering ===")

    from ingestion.features.athletic_scores import build_athletic_profiles
    from ingestion.features.draft_value import build_draft_value_history
    from ingestion.features.durability_scores import build_durability_profiles
    from ingestion.features.production_scores import build_production_profiles

    build_athletic_profiles(
        combine_path=config.STAGED_COMBINE,
        output_path=config.CURATED_ATHLETIC_PROFILES,
    )
    build_production_profiles(
        snap_counts_path=config.STAGED_SNAP_COUNTS,
        weekly_stats_path=config.STAGED_WEEKLY_STATS,
        output_path=config.CURATED_PRODUCTION_PROFILES,
    )
    build_durability_profiles(
        injuries_path=config.STAGED_INJURIES,
        snap_counts_path=config.STAGED_SNAP_COUNTS,
        rosters_path=config.STAGED_ROSTERS,
        output_path=config.CURATED_DURABILITY_PROFILES,
    )
    build_draft_value_history(
        draft_history_path=config.STAGED_DRAFT_HISTORY,
        output_path=config.CURATED_DRAFT_VALUE_HISTORY,
    )


# Backward-compat curated files


def run_stage4(stage0: dict[str, pd.DataFrame]) -> None:
    """Write legacy player_profiles and team_performance curated Parquets."""
    logger.info("=== Stage 4: Backward-Compat Curated Layer ===")
    if "combine" in stage0:
        _build_player_profiles(stage0["combine"])
    if "team_stats" in stage0:
        _build_team_performance(stage0["team_stats"])


def _build_player_profiles(combine: pd.DataFrame) -> None:
    out = config.CURATED_PLAYER_PROFILES
    out.parent.mkdir(parents=True, exist_ok=True)
    combine.to_parquet(out, index=False)
    logger.info("player_profiles → %s (%s rows)", out.name, f"{len(combine):,}")


def _build_team_performance(team_stats: pd.DataFrame) -> None:
    out = config.CURATED_TEAM_PERFORMANCE
    out.parent.mkdir(parents=True, exist_ok=True)
    team_stats.to_parquet(out, index=False)
    logger.info("team_performance → %s (%s rows)", out.name, f"{len(team_stats):,}")


# ---------------------------------------------------------------------------
# PBP ingestion (opt-in — ~35 MB/season)
# ---------------------------------------------------------------------------


def run_pbp_ingestion(years: list[int]) -> None:
    """
    Download play-by-play data for specific seasons into a Hive-partitioned dir.
    Opt-in due to large file sizes (~35 MB/season).
    """
    from ingestion.loaders.nflreadpy_loader import _require_nflreadpy, _save, _to_pandas
    import nflreadpy as nfl

    _require_nflreadpy()
    for year in years:
        logger.info("[PBP] Loading %s", year)
        df = _to_pandas(nfl.load_pbp([year]))
        out = config.STAGED_PBP_DIR / f"season={year}" / "data.parquet"
        _save(df, out, f"pbp_{year}")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def run_ingestion() -> dict[str, pd.DataFrame]:
    """Legacy entry point — runs Stage 0 + Stage 4."""
    stage0 = run_stage0()
    run_stage4(stage0)
    logger.info("Pipeline complete")
    return stage0


def run_full_pipeline(skip_nflreadpy: bool = False) -> None:
    """Full pipeline: Stages 0 → 1 → 2 → 3 → 4."""
    stage0 = run_stage0(skip_nflreadpy=skip_nflreadpy)
    stage1 = run_stage1(stage0)
    run_stage2(stage1)
    run_stage3()
    run_stage4(stage0)
    logger.info("Full pipeline complete")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s [%(name)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="NFL Data Platform ingestion pipeline")
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Run only the legacy combine/team_stats loaders (no nflreadpy required)",
    )
    parser.add_argument(
        "--pbp",
        nargs="+",
        type=int,
        metavar="YEAR",
        help="Download play-by-play for specific seasons (opt-in, ~35 MB/season)",
    )
    args = parser.parse_args()

    if args.pbp:
        run_pbp_ingestion(args.pbp)
    else:
        run_full_pipeline(skip_nflreadpy=args.legacy)
