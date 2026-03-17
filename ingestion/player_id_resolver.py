from __future__ import annotations

import logging
import re

import pandas as pd
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)

_POS_GROUPS: dict[str, str] = {
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
    "DE": "DL",
    "DT": "DL",
    "NT": "DL",
    "DL": "DL",
    "LB": "LB",
    "ILB": "LB",
    "OLB": "LB",
    "MLB": "LB",
    "CB": "DB",
    "S": "DB",
    "FS": "DB",
    "SS": "DB",
    "DB": "DB",
    "K": "K",
    "P": "P",
    "LS": "LS",
}


def _pos_group(pos: str) -> str:
    """Normalize a position string to its broader group (e.g. 'OLB' → 'LB')."""
    if not isinstance(pos, str):
        return ""
    return _POS_GROUPS.get(pos.strip().upper(), pos.strip().upper())


def _normalize_name(name: str) -> str:
    """
    Lowercase, strip generational suffixes and punctuation for consistent matching.
    'Patrick Mahomes II' → 'patrick mahomes'
    'Charles Leno Jr.'   → 'charles leno'
    """
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", name)
    name = re.sub(r"[^a-z\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


class PlayerIdResolver:
    """
    Build once from master_players (and optionally ff_playerids), then call
    resolve_dataframe() on any staged table that needs player_id attached.

    Parameters
    ----------
    master_players : pd.DataFrame
        Output of build_master_players() — one row per player with player_id
        (gsis_id), player_name, position, and platform ID columns.
    ff_playerids : pd.DataFrame, optional
        Output of load_ff_playerids() — used for pfr_id → gsis_id lookup.
        If not provided, Pass 2 is skipped and only name-based resolution runs.
    fuzzy_threshold : int
        Minimum token_sort_ratio score (0–100) for a fuzzy match to be accepted.
        Default 90 is strict enough to avoid most false positives.
    """

    def __init__(
        self,
        master_players: pd.DataFrame,
        ff_playerids: pd.DataFrame | None = None,
        fuzzy_threshold: int = 90,
    ):
        self.fuzzy_threshold = fuzzy_threshold

        # --- Pass 2 index: pfr_id → gsis_id ---
        self._pfr_to_gsis: dict[str, str] = {}
        if ff_playerids is not None:
            valid = ff_playerids.dropna(subset=["pfr_id", "gsis_id"])
            self._pfr_to_gsis = dict(
                zip(
                    valid["pfr_id"].astype(str),
                    valid["gsis_id"].astype(str),
                )
            )

        # --- Pass 3 indexes: name-based ---
        # exact: normalized_name → list of (gsis_id, pos_group)
        self._exact: dict[str, list[tuple[str, str]]] = {}

        # fuzzy: parallel lists for rapidfuzz (might need to be validated/updated later)
        self._fuzzy_names: list[str] = []
        self._fuzzy_ids: list[str] = []
        self._fuzzy_pos: list[str] = []

        for _, row in master_players.iterrows():
            pid = str(row["player_id"]) if pd.notna(row.get("player_id")) else None
            norm = _normalize_name(row.get("player_name", ""))
            pos = _pos_group(row.get("position", ""))

            if not pid or not norm:
                continue

            self._exact.setdefault(norm, []).append((pid, pos))
            self._fuzzy_names.append(norm)
            self._fuzzy_ids.append(pid)
            self._fuzzy_pos.append(pos)

        # Warn about names that map to multiple players — these will require
        # position disambiguation at resolve time and may still be unresolvable.
        ambiguous = {name: entries for name, entries in self._exact.items() if len(entries) > 1}
        if ambiguous:
            logger.warning(
                "PlayerIdResolver: %d ambiguous name(s) in master_players "
                "(same normalized name, different player_ids). "
                "Resolution will require position context.",
                len(ambiguous),
            )
            logger.debug("Ambiguous names: %s", list(ambiguous.keys())[:20])

    def resolve_by_pfr_id(self, pfr_id: str) -> str | None:
        """Pass 2: direct pfr_id → gsis_id lookup."""
        if not isinstance(pfr_id, str) or not pfr_id:
            return None
        return self._pfr_to_gsis.get(pfr_id)

    def resolve(
        self,
        player_name: str,
        position: str | None = None,
    ) -> tuple[str | None, str | None]:
        """
        Pass 3 name-based resolution.

        Returns (gsis_id, confidence) where confidence is:
          "exact" — normalized name + position group matched uniquely
          "fuzzy" — fuzzy name score >= threshold + position group agreed
          None    — unresolved
        """

        norm = _normalize_name(player_name)
        pos = _pos_group(position) if position else ""

        if not norm:
            return None, None

        # --- Exact match ---
        candidates = self._exact.get(norm, [])
        if candidates:
            if pos:
                pos_match = [pid for pid, p in candidates if p == pos]
                if len(pos_match) == 1:
                    logger.debug(
                        "resolve: exact+pos match '%s' (%s) → %s",
                        player_name, pos, pos_match[0],
                    )
                    return pos_match[0], "exact"
                if len(pos_match) > 1:
                    # Ambiguous: multiple players with same name + position group
                    logger.debug(
                        "resolve: ambiguous exact+pos match for '%s' (%s) — %d candidates",
                        player_name, pos, len(pos_match),
                    )
                    return None, None
            if len(candidates) == 1:
                logger.debug(
                    "resolve: exact match '%s' → %s", player_name, candidates[0][0]
                )
                return candidates[0][0], "exact"

        # --- Fuzzy match ---
        if not self._fuzzy_names:
            return None, None

        results = process.extract(
            norm,
            self._fuzzy_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=self.fuzzy_threshold,
            limit=5,
        )
        if not results:
            logger.debug("resolve: no match for '%s' (norm='%s')", player_name, norm)
            return None, None

        # Filter by position group if available
        if pos:
            pos_results = [r for r in results if self._fuzzy_pos[r[2]] == pos]
            results = pos_results if pos_results else results

        best_match, best_score, best_idx = max(results, key=lambda x: x[1])
        logger.debug(
            "resolve: fuzzy match '%s' → '%s' (score=%.0f, id=%s)",
            player_name, best_match, best_score, self._fuzzy_ids[best_idx],
        )
        return self._fuzzy_ids[best_idx], "fuzzy"

    def resolve_dataframe(
        self,
        df: pd.DataFrame,
        *,
        gsis_id_col: str | None = None,
        pfr_id_col: str | None = None,
        name_col: str | None = None,
        position_col: str | None = None,
    ) -> pd.DataFrame:
        """

        Dataframe resolution

        Add `player_id` and `id_confidence` columns to df.

        Runs three passes in priority order; already-resolved rows are never
        overwritten.

        Parameters
        ----------
        gsis_id_col  : column already containing gsis_id    (Pass 1)
        pfr_id_col   : column containing pfr_player_id      (Pass 2)
        name_col     : column containing player name        (Pass 3)
        position_col : column containing position string    (Pass 3)
        """
        df = df.copy()
        df["player_id"] = pd.NA
        df["id_confidence"] = pd.NA

        total = len(df)
        logger.debug("resolve_dataframe: starting resolution for %d rows", total)

        # --- Pass 1: direct gsis_id ---
        if gsis_id_col and gsis_id_col in df.columns:
            mask = df[gsis_id_col].notna()
            df.loc[mask, "player_id"] = df.loc[mask, gsis_id_col].astype(str)
            df.loc[mask, "id_confidence"] = "gsis"
            logger.debug("resolve_dataframe: Pass 1 (gsis) resolved %d rows", int(mask.sum()))

        # --- Pass 2: pfr_id cross-reference ---
        unresolved = df["player_id"].isna()
        if pfr_id_col and pfr_id_col in df.columns and unresolved.any():
            mapped = df.loc[unresolved, pfr_id_col].map(self._pfr_to_gsis)
            resolved_idx = mapped[mapped.notna()].index
            df.loc[resolved_idx, "player_id"] = mapped[resolved_idx]
            df.loc[resolved_idx, "id_confidence"] = "pfr_id"
            logger.debug(
                "resolve_dataframe: Pass 2 (pfr_id) resolved %d rows", len(resolved_idx)
            )

        # --- Pass 3: name-based (exact → fuzzy) ---
        unresolved = df["player_id"].isna()
        if name_col and name_col in df.columns and unresolved.any():
            unresolved_idx = df.index[unresolved]
            names = df.loc[unresolved_idx, name_col]
            positions = (
                df.loc[unresolved_idx, position_col]
                if position_col and position_col in df.columns
                else pd.Series("", index=unresolved_idx)
            )
            pass3_resolved = 0
            for idx in unresolved_idx:
                pid, conf = self.resolve(names[idx], positions[idx])
                if pid:
                    df.at[idx, "player_id"] = pid
                    df.at[idx, "id_confidence"] = conf
                    pass3_resolved += 1
            logger.debug(
                "resolve_dataframe: Pass 3 (name) resolved %d / %d rows",
                pass3_resolved,
                len(unresolved_idx),
            )

        final_resolved = int(df["player_id"].notna().sum())
        logger.debug(
            "resolve_dataframe: complete — %d/%d rows resolved (%.1f%%)",
            final_resolved,
            total,
            (final_resolved / total * 100) if total else 0.0,
        )
        return df

    def resolution_summary(self, df: pd.DataFrame) -> dict:
        """
        Diagnostics
        Return and print a resolution summary after resolve_dataframe() has run.
        """
        total = len(df)
        resolved = df["player_id"].notna().sum()
        by_confidence = df["id_confidence"].value_counts().to_dict()
        rate = resolved / total if total else 0.0

        summary = {
            "total": total,
            "resolved": int(resolved),
            "unresolved": int(total - resolved),
            "resolution_rate": round(rate, 4),
            "by_confidence": by_confidence,
        }

        logger.info(
            "Resolution: %s/%s (%.1f%%)", f"{resolved:,}", f"{total:,}", rate * 100
        )
        for conf, count in sorted(by_confidence.items(), key=lambda x: -x[1]):
            logger.info("  %s: %s", conf, f"{count:,}")

        return summary
