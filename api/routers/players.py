from fastapi import APIRouter, HTTPException, Query
import db.duckdb_client as duckdb_client

router = APIRouter(prefix="/players", tags=["players"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gold_table_exists(table: str) -> bool:
    return table in duckdb_client.list_tables()


# ---------------------------------------------------------------------------
# Legacy endpoints (combine-based — backward compat)
# ---------------------------------------------------------------------------


@router.get("", summary="List players (combine data)")
def list_players(
    position: str | None = Query(None),
    school: str | None = Query(None),
    draft_team: str | None = Query(None),
    drafted_only: bool = Query(False),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
):
    """List players from the combine dataset with optional filters."""
    filters = []
    if position:
        filters.append(f"position = '{position}'")
    if school:
        filters.append(f"school ILIKE '%{school}%'")
    if draft_team:
        filters.append(f"draft_team ILIKE '%{draft_team}%'")
    if drafted_only:
        filters.append("draft_team IS NOT NULL")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT player_name, position, school, height_in, weight_lbs,
               forty_yard, vertical_in, bench_reps, broad_jump_in,
               three_cone, shuttle, draft_team, draft_round, draft_pick, draft_year
        FROM players
        {where}
        ORDER BY player_name
        LIMIT {limit} OFFSET {offset}
    """
    df = duckdb_client.execute(sql)
    return {"players": duckdb_client.df_to_records(df), "count": len(df)}


@router.get("/search", summary="Search players by name")
def search_players(
    name: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
):
    """
    Search master_players by name (case-insensitive CONTAINS).
    Falls back to combine table if master_players is not yet built.
    """
    if _gold_table_exists("master_players"):
        sql = """
            SELECT player_id, player_name, position, team,
                   first_season, last_season, college
            FROM master_players
            WHERE player_name ILIKE ?
            ORDER BY last_season DESC, player_name
            LIMIT ?
        """
        params = [f"%{name}%", limit]
    else:
        sql = """
            SELECT player_name, position, school AS college
            FROM players
            WHERE player_name ILIKE ?
            ORDER BY player_name
            LIMIT ?
        """
        params = [f"%{name}%", limit]
    df = duckdb_client.execute(sql, params)
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No players found matching '{name}'"
        )
    return {"players": duckdb_client.df_to_records(df), "count": len(df)}


@router.get("/{name}", summary="Get player by name (combine)")
def get_player(name: str):
    """Get a single player profile by name from the combine dataset."""
    df = duckdb_client.execute(
        "SELECT * FROM players WHERE player_name ILIKE ? LIMIT 10", [f"%{name}%"]
    )
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No player found matching '{name}'"
        )
    return {"players": duckdb_client.df_to_records(df)}


# ---------------------------------------------------------------------------
# Gold profile endpoints (keyed on player_id / gsis_id)
# ---------------------------------------------------------------------------


@router.get("/id/{player_id}/profile", summary="Full enriched player profile")
def get_player_full_profile(player_id: str):
    """
    Join master_players with all four gold tables to return the complete
    analytical profile for a player: identity, athletic scores, career
    production, durability, and draft value.
    """
    tables = {
        "master_players": "master_players",
        "player_athletic_profiles": "player_athletic_profiles",
        "player_durability_profiles": "player_durability_profiles",
        "draft_value_history": "draft_value_history",
    }
    missing = [t for t in tables if not _gold_table_exists(t)]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"Gold tables not yet built: {missing}. Run the ingestion pipeline first.",
        )

    # Identity
    identity_df = duckdb_client.execute(
        "SELECT * FROM master_players WHERE player_id = ? LIMIT 1", [player_id]
    )
    if identity_df.empty:
        raise HTTPException(status_code=404, detail=f"Player '{player_id}' not found.")

    result = duckdb_client.df_to_records(identity_df)[0]

    # Athletic scores
    if _gold_table_exists("player_athletic_profiles"):
        ath_df = duckdb_client.execute(
            "SELECT * FROM player_athletic_profiles WHERE player_id = ? LIMIT 1",
            [player_id],
        )
        result["athletic"] = (
            duckdb_client.df_to_records(ath_df)[0] if not ath_df.empty else None
        )

    # Production (all seasons)
    if _gold_table_exists("player_production_profiles"):
        prod_df = duckdb_client.execute(f"""
            SELECT season, snap_share, epa_per_game, passing_cpoe,
                   target_share, nfl_production_score, games_played
            FROM player_production_profiles
            WHERE player_id = '{player_id}'
            ORDER BY season DESC
            """)
        result["production"] = duckdb_client.df_to_records(prod_df)

    # Durability
    if _gold_table_exists("player_durability_profiles"):
        dur_df = duckdb_client.execute(
            "SELECT * FROM player_durability_profiles WHERE player_id = ? LIMIT 1",
            [player_id],
        )
        result["durability"] = (
            duckdb_client.df_to_records(dur_df)[0] if not dur_df.empty else None
        )

    # Draft value
    if _gold_table_exists("draft_value_history"):
        dv_df = duckdb_client.execute(f"""
            SELECT season AS draft_year, round, pick, car_av,
                   draft_value_score, draft_value_percentile
            FROM draft_value_history
            WHERE player_id = '{player_id}'
            LIMIT 1
            """)
        result["draft_value"] = (
            duckdb_client.df_to_records(dv_df)[0] if not dv_df.empty else None
        )

    return result


@router.get("/id/{player_id}/athletic", summary="Athletic combine scores")
def get_player_athletic(player_id: str):
    """Return combine-derived athletic scores for a player."""
    if not _gold_table_exists("player_athletic_profiles"):
        raise HTTPException(
            status_code=503, detail="player_athletic_profiles not yet built."
        )

    df = duckdb_client.execute(
        "SELECT 1 FROM master_players WHERE player_id = ? LIMIT 1", [player_id]
    )

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No athletic profile for '{player_id}'."
        )
    return duckdb_client.df_to_records(df)[0]


@router.get("/id/{player_id}/production", summary="Season-by-season production")
def get_player_production(player_id: str):
    """
    Return per-season production metrics: snap_share, epa_per_game,
    passing_cpoe (QBs), target_share (receivers), nfl_production_score.
    """
    if not _gold_table_exists("player_production_profiles"):
        raise HTTPException(
            status_code=503, detail="player_production_profiles not yet built."
        )

    df = duckdb_client.execute(f"""
        SELECT season, position, snap_share, epa_per_game, passing_cpoe,
               target_share, nfl_production_score, games_played, games_with_snaps
        FROM player_production_profiles
        WHERE player_id = '{player_id}'
        ORDER BY season DESC
        """)
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No production data for '{player_id}'."
        )
    return {"player_id": player_id, "seasons": duckdb_client.df_to_records(df)}


@router.get("/id/{player_id}/durability", summary="Career durability profile")
def get_player_durability(player_id: str):
    """
    Return career durability metrics: injury_frequency, games_played_rate,
    and the composite durability_score relative to position group peers.
    """
    if not _gold_table_exists("player_durability_profiles"):
        raise HTTPException(
            status_code=503, detail="player_durability_profiles not yet built."
        )

    df = duckdb_client.execute(
        "SELECT * FROM player_athletic_profiles WHERE player_id = ? LIMIT 1",
        [player_id],
    )

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No durability data for '{player_id}'."
        )
    return duckdb_client.df_to_records(df)[0]


@router.get("/id/{player_id}/draft-value", summary="Draft value relative to pick")
def get_player_draft_value(player_id: str):
    """
    Return draft value metrics: car_av (or w_av), draft_value_score (z-score
    within round), and draft_value_percentile (0–100 within round).
    """
    if not _gold_table_exists("draft_value_history"):
        raise HTTPException(
            status_code=503, detail="draft_value_history not yet built."
        )

    df = duckdb_client.execute(f"""
        SELECT player_name, season AS draft_year, team, round, pick, position,
               car_av, draft_value_score, draft_value_percentile,
               allpro, probowls, games, seasons_started
        FROM draft_value_history
        WHERE player_id = '{player_id}'
        LIMIT 1
        """)
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No draft value data for '{player_id}'."
        )
    return duckdb_client.df_to_records(df)[0]


# ---------------------------------------------------------------------------
# Leaderboard endpoints (gold tables, position-filtered)
# ---------------------------------------------------------------------------


@router.get("/leaderboard/athletic", summary="Athletic score leaderboard")
def athletic_leaderboard(
    position: str | None = Query(None, description="Filter by position (e.g. WR, QB)"),
    metric: str = Query("speed_score", description="Score column to rank by"),
    limit: int = Query(25, le=100),
):
    """Rank players by a combine-derived athletic score within an optional position filter."""
    if not _gold_table_exists("player_athletic_profiles"):
        raise HTTPException(
            status_code=503, detail="player_athletic_profiles not yet built."
        )

    allowed = {
        "speed_score",
        "agility_score",
        "burst_score",
        "strength_score",
        "size_score",
    }
    if metric not in allowed:
        raise HTTPException(
            status_code=400, detail=f"metric must be one of {sorted(allowed)}"
        )

    where = (
        f"WHERE position = '{position}'" if position else f"WHERE {metric} IS NOT NULL"
    )
    sql = f"""
        SELECT player_name, position, {metric},
               height_in, weight_lbs, forty_yard, draft_year
        FROM player_athletic_profiles
        {where}
          AND {metric} IS NOT NULL
        ORDER BY {metric} DESC
        LIMIT {limit}
    """
    df = duckdb_client.execute(sql)
    return {
        "metric": metric,
        "position": position,
        "players": duckdb_client.df_to_records(df),
    }


@router.get("/leaderboard/production", summary="Production score leaderboard")
def production_leaderboard(
    position: str | None = Query(None),
    season: int | None = Query(None),
    limit: int = Query(25, le=100),
):
    """Rank players by nfl_production_score within an optional position and season filter."""
    if not _gold_table_exists("player_production_profiles"):
        raise HTTPException(
            status_code=503, detail="player_production_profiles not yet built."
        )

    filters = ["nfl_production_score IS NOT NULL"]
    if position:
        filters.append(f"position = '{position}'")
    if season:
        filters.append(f"season = {season}")

    where = "WHERE " + " AND ".join(filters)
    sql = f"""
        SELECT pp.player_id, mp.player_name, pp.position, pp.season,
               pp.snap_share, pp.epa_per_game, pp.nfl_production_score, pp.games_played
        FROM player_production_profiles pp
        LEFT JOIN master_players mp USING (player_id)
        {where}
        ORDER BY nfl_production_score DESC
        LIMIT {limit}
    """
    df = duckdb_client.execute(sql)
    return {
        "position": position,
        "season": season,
        "players": duckdb_client.df_to_records(df),
    }


@router.get("/leaderboard/draft-value", summary="Draft value leaderboard by round")
def draft_value_leaderboard(
    round: int | None = Query(None, ge=1, le=7),
    season: int | None = Query(None),
    limit: int = Query(25, le=100),
):
    """Rank drafted players by draft_value_score (outperformance vs round peers)."""
    if not _gold_table_exists("draft_value_history"):
        raise HTTPException(
            status_code=503, detail="draft_value_history not yet built."
        )

    filters = ["draft_value_score IS NOT NULL"]
    if round:
        filters.append(f'"round" = {round}')
    if season:
        filters.append(f"season = {season}")

    where = "WHERE " + " AND ".join(filters)
    sql = f"""
        SELECT player_name, position, season AS draft_year, team,
               "round", pick, car_av, draft_value_score, draft_value_percentile,
               allpro, probowls
        FROM draft_value_history
        {where}
        ORDER BY draft_value_score DESC
        LIMIT {limit}
    """
    df = duckdb_client.execute(sql)
    return {
        "round": round,
        "season": season,
        "players": duckdb_client.df_to_records(df),
    }
