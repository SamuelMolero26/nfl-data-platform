from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import nflreadpy as nfl

    _NFLREADPY_AVAILABLE = True
except ImportError:
    _NFLREADPY_AVAILABLE = False


def _require_nflreadpy() -> None:
    if not _NFLREADPY_AVAILABLE:
        raise ImportError(
            "nflreadpy is not installed. Run: pip install nflreadpy>=0.1.0"
        )


def _to_pandas(obj) -> pd.DataFrame:
    """Accept Polars or pandas DataFrame and return pandas."""
    if hasattr(obj, "to_pandas"):
        return obj.to_pandas()
    return obj  # already pandas


def _save(df: pd.DataFrame, path: Path, label: str) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("%s: %s rows → %s", label, f"{len(df):,}", path.name)
    return df


class NflreadpyLoader:
    """
    Loads NFL data via nflreadpy for a given season range.

    Usage:
        loader = NflreadpyLoader(start_year=2010, end_year=2024)
        rosters_df = loader.load_rosters(config.STAGED_ROSTERS)
    """

    def __init__(self, start_year: int, end_year: int):
        self.seasons = list(range(start_year, end_year + 1))

    def load_rosters(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        df = _to_pandas(nfl.load_rosters(self.seasons))
        return _save(df, output_path, "rosters")

    def load_weekly_stats(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        df = _to_pandas(nfl.load_player_stats(self.seasons))
        return _save(df, output_path, "weekly_stats")

    def load_snap_counts(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        # FYI: snap counts available from 2012 onwards
        seasons = [s for s in self.seasons if s >= 2012]
        df = _to_pandas(nfl.load_snap_counts(seasons))
        return _save(df, output_path, "snap_counts")

    def load_depth_charts(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        seasons = [s for s in self.seasons if s >= 2001]
        df = _to_pandas(nfl.load_depth_charts(seasons))
        return _save(df, output_path, "depth_charts")

    def load_injuries(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        seasons = [s for s in self.seasons if s >= 2009]
        df = _to_pandas(nfl.load_injuries(seasons))
        return _save(df, output_path, "injuries")

    def load_draft_picks(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        # draft picks go back to 1980; use all available by default
        seasons = list(range(1980, max(self.seasons) + 1))
        df = _to_pandas(nfl.load_draft_picks(seasons))
        return _save(df, output_path, "draft_history")

    def load_ngs_data(self, output_path: Path) -> pd.DataFrame:
        """
        NGS — NFL Next Gen Stats (2016+).

        Proprietary tracking data collected from chips embedded in every player's
        shoulder pads. Captures precise on-field positioning at 10 frames/second.

        Loaded for all three offensive stat types and stacked into one table
        with a `stat_type` column ('passing' | 'receiving' | 'rushing'):

          passing  — QB metrics: completion % over expected (CPOE), air yards,
                     time to throw, aggressiveness %, yards above expectation
          receiving — WR/TE/RB metrics: average separation at catch point,
                      average intended air yards, yards after catch above expected
          rushing  — RB metrics: rush yards over expected (RYOE), efficiency,
                     % of rushes 8+ yards, avg time to line of scrimmage

        Key columns used downstream:
          cpoe                 → QB production score
          avg_separation       → WR production score
          rush_yards_over_expected → RB production score
        """
        _require_nflreadpy()
        seasons = [s for s in self.seasons if s >= 2016]
        frames = []
        for stat_type in ("passing", "receiving", "rushing"):
            part = _to_pandas(
                nfl.load_nextgen_stats(stat_type=stat_type, seasons=seasons)
            )
            part["stat_type"] = stat_type
            frames.append(part)
        df = pd.concat(frames, ignore_index=True)
        return _save(df, output_path, "ngs_data")

    def load_pfr_advstats(self, output_path: Path) -> pd.DataFrame:
        """
        PFR Advanced Stats — Pro Football Reference (2018+).

        Hand-charted data published weekly by Pro Football Reference covering
        scheme and situation context that standard box scores omit.

        Loaded for all four stat types and stacked with a `stat_type` column
        ('pass' | 'rush' | 'rec' | 'def'):

          pass — QB pocket stats: times sacked, pressures, hurries, knockdowns,
                 blitzes faced, dropped passes by receivers, on-target throw %,
                 bad throw %, time to throw
          rush — RB/QB rushing: yards before contact, yards after contact,
                 broken tackles, stuffed %, yards per attempt vs. stacked box
          rec  — Receiver route data: targets, yards per route run (YPRR),
                 drop rate, contested catch %, yards after catch (YAC),
                 average depth of target (aDOT)
          def  — Defender coverage/pass rush: pressures, sacks, stops,
                 missed tackles, targets allowed, passer rating allowed,
                 average yards allowed per coverage snap

        Key columns used downstream:
          pocket_time, times_pressured → QB durability / offensive line quality
          yprr, adot                   → WR production score
          broken_tackles               → RB elusiveness
        """
        _require_nflreadpy()
        seasons = [s for s in self.seasons if s >= 2018]
        frames = []
        for stat_type in ("pass", "rush", "rec", "def"):
            part = _to_pandas(
                nfl.load_pfr_advstats(seasons=seasons, stat_type=stat_type)
            )
            part["stat_type"] = stat_type
            frames.append(part)
        df = pd.concat(frames, ignore_index=True)
        return _save(df, output_path, "pfr_advstats")

    def load_ftn_data(self, output_path: Path) -> pd.DataFrame:
        """
        FTN — FTN Data charting (2022+).

        Play-level route and coverage charting sourced from FTN Data, a
        boutique analytics company that manually charts every offensive snap.
        This fills the gap between NGS (tracking) and PFR (box-score derived)
        by providing route-tree and personnel context at the play level.

        Key columns:
          routes_run          — exact count of routes run per target opportunity;
                                used to compute YPRR (yards per route run), the
                                premier WR efficiency metric
          is_motioned         — whether the player was in pre-snap motion
          is_play_action      — play-action pass indicator (affects EPA context)
          n_pass_rushers      — number of defenders rushing the passer
          n_defense_box       — defenders in the box at snap (run defense context)
          offense_personnel   — personnel grouping string (e.g. '11', '12', '21')
          defense_coverage    — coverage shell called (Cover 0/1/2/3/4/6)

        Key column used downstream:
          routes_run → yprr = receiving_yards / routes_run (receiver efficiency)
        """
        _require_nflreadpy()
        seasons = [s for s in self.seasons if s >= 2022]
        df = _to_pandas(nfl.load_ftn_charting(seasons))
        return _save(df, output_path, "ftn_data")

    def load_combine(self, output_path: Path, xls_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Load historical NFL combine data from nflreadpy (2000–present).

        Normalizes nflverse column names to the platform schema so downstream
        consumers (build_athletic_profiles, PlayerIdResolver) see the same
        shape regardless of whether a row came from the XLS or nflreadpy.

        If xls_df is provided (e.g. the 2025 XLS class), its draft years are
        dropped from the nflreadpy pull to avoid duplicates, then the two
        sources are concatenated with XLS rows appended last.

        nflverse column → platform column mapping:
          player_name  → player_name
          pos          → position
          school       → school
          ht           → height_in   (stored as numeric inches in nflverse)
          wt           → weight_lbs
          forty        → forty_yard
          vertical     → vertical_in
          bench_reps   → bench_reps
          broad_jump   → broad_jump_in
          cone         → three_cone
          shuttle      → shuttle
          draft_team   → draft_team
          draft_round  → draft_round
          draft_pick   → draft_pick
          season       → draft_year  (combine season = draft year in nflverse)
          gsis_id, pfr_id kept as-is
        """
        _require_nflreadpy()
        seasons = [s for s in range(2000, max(self.seasons) + 1)]
        df = _to_pandas(nfl.load_combine(seasons))

        col_map = {
            "player_name": "player_name",
            "pos": "position",
            "school": "school",
            "ht": "height_in",
            "wt": "weight_lbs",
            "forty": "forty_yard",
            "vertical": "vertical_in",
            "bench_reps": "bench_reps",
            "broad_jump": "broad_jump_in",
            "cone": "three_cone",
            "shuttle": "shuttle",
            "draft_team": "draft_team",
            "draft_round": "draft_round",
            "draft_pick": "draft_pick",
            "season": "draft_year",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        keep = list(col_map.values()) + ["gsis_id", "pfr_id"]
        df = df[[c for c in keep if c in df.columns]]

        for col in ["weight_lbs", "forty_yard", "vertical_in", "bench_reps",
                    "broad_jump_in", "three_cone", "shuttle", "height_in",
                    "draft_pick", "draft_year"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if xls_df is not None and not xls_df.empty:
            if "draft_year" in xls_df.columns and "draft_year" in df.columns:
                xls_years = set(xls_df["draft_year"].dropna().unique())
                df = df[~df["draft_year"].isin(xls_years)]
            df = pd.concat([df, xls_df], ignore_index=True)

        return _save(df, output_path, "combine_historical")

    def load_contracts(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        df = _to_pandas(nfl.load_contracts())
        return _save(df, output_path, "contracts")

    def load_schedules(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        df = _to_pandas(nfl.load_schedules(self.seasons))
        return _save(df, output_path, "schedules")

    def load_teams(self, output_path: Path) -> pd.DataFrame:
        _require_nflreadpy()
        df = _to_pandas(nfl.load_teams())
        return _save(df, output_path, "teams")

    def load_ff_playerids(self, output_path: Path) -> pd.DataFrame:
        """
        FF Player IDs — Fantasy Football cross-platform ID map (static).

        A static lookup table maintained by the nflverse community that maps
        every known NFL player to their unique ID across all major platforms.
        Essential for joining data from sources that don't share a common key.

        Key columns:
          gsis_id    — the NFL's own Game Statistics & Information System ID;
                       this is the canonical player_id used throughout this
                       platform (sourced from nflreadpy rosters)
          espn_id    — ESPN player ID (used by ESPN APIs and fantasy platforms)
          yahoo_id   — Yahoo Sports player ID
          sleeper_id — Sleeper fantasy app player ID
          pfr_id     — Pro Football Reference player slug (e.g. 'MahoPatr01');
                       needed to join PFR advanced stats back to gsis_id
          pff_id     — Pro Football Focus player ID
          rotowire_id, sportradar_id, stats_id — additional platform IDs

        Usage in this platform:
          PlayerIdResolver uses this table as a first-pass lookup before
          falling back to fuzzy name matching, dramatically improving the
          resolution rate for PFR advstats rows that only carry a pfr_id.
        """
        _require_nflreadpy()
        df = _to_pandas(nfl.load_ff_playerids())
        return _save(df, output_path, "ff_playerids")

    def run_all(self, paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
        """
        Load every dataset and return a {key: DataFrame} dict.

        `paths` must map dataset keys to their staged output paths.
        Any key absent from `paths` is skipped.
        """
        results: dict[str, pd.DataFrame] = {}
        dispatch = {
            "rosters": self.load_rosters,
            "weekly_stats": self.load_weekly_stats,
            "snap_counts": self.load_snap_counts,
            "depth_charts": self.load_depth_charts,
            "injuries": self.load_injuries,
            "draft_picks": self.load_draft_picks,
            "ngs_data": self.load_ngs_data,
            "pfr_advstats": self.load_pfr_advstats,
            "ftn_data": self.load_ftn_data,
            "contracts": self.load_contracts,
            "schedules": self.load_schedules,
            "teams": self.load_teams,
            "ff_playerids": self.load_ff_playerids,
        }
        for key, fn in dispatch.items():
            if key in paths:
                try:
                    results[key] = fn(paths[key])
                except Exception as exc:
                    logger.warning("%s failed: %s", key, exc)
        return results
