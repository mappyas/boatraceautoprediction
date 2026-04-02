"""
特徴量エンジニアリング
DBから学習用DataFrameを構築する。
"""

import math
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, text
from loguru import logger

from src.db.models import Race, RaceEntry, RaceResult, Racer, Odds

GRADE_MAP = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}

STADIUM_LOCATIONS = {
    "01": (36.414, 139.232), "02": (35.843, 139.666), "03": (35.672, 139.877),
    "04": (35.582, 139.746), "05": (35.589, 139.488), "06": (34.732, 137.717),
    "07": (34.832, 137.216), "08": (35.003, 136.864), "09": (34.714, 136.513),
    "10": (36.186, 136.140), "11": (35.125, 135.996), "12": (34.650, 135.507),
    "13": (34.723, 135.409), "14": (34.165, 134.516), "15": (34.304, 133.836),
    "16": (34.498, 133.995), "17": (34.344, 132.459), "18": (33.960, 131.865),
    "19": (33.946, 130.956), "20": (33.903, 130.783), "21": (33.897, 130.673),
    "22": (33.606, 130.401), "23": (33.440, 129.967), "24": (32.915, 129.869),
}


def build_feature_df(session: Session, start_date=None, end_date=None) -> pd.DataFrame:
    """
    学習・推論用の特徴量DataFrameを構築する。

    Returns:
        DataFrame with columns: race_id, boat_number, + features..., target(着順)
    """

    # ベースクエリ：レース × エントリ × 結果 × 選手
    query = """
    SELECT
        r.race_id,
        r.stadium_code,
        r.race_date,
        r.race_number,
        r.weather,
        r.temperature,
        r.water_temperature,
        r.wind_speed,
        r.wind_direction,
        r.wave_height,
        e.boat_number,
        e.racer_id,
        e.course,
        e.motor_number,
        e.motor_rate,
        e.boat_number_motor,
        e.boat_rate,
        e.exhibition_time,
        e.start_exhibition_time,
        ra.grade,
        ra.national_win_rate,
        ra.national_place2_rate,
        ra.national_place3_rate,
        ra.local_win_rate,
        ra.local_place2_rate,
        ra.fly_count,
        ra.late_count,
        rr.arrival,
        rr.start_timing
    FROM races r
    JOIN race_entries e ON r.race_id = e.race_id
    LEFT JOIN racers ra ON e.racer_id = ra.racer_id
    LEFT JOIN race_results rr ON r.race_id = rr.race_id AND e.boat_number = rr.boat_number
    """

    conditions = []
    if start_date:
        conditions.append(f"r.race_date >= '{start_date}'")
    if end_date:
        conditions.append(f"r.race_date <= '{end_date}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY r.race_date, r.stadium_code, r.race_number, e.boat_number"

    df = pd.read_sql(text(query), session.bind)
    logger.info(f"Loaded {len(df)} rows for feature building")

    if df.empty:
        return df

    df = _add_wind_features(df)
    df = _add_grade_numeric(df)
    df = _add_course_features(df)
    df = _add_rolling_stats(df, session)
    df = _add_odds_features(df, session)
    df = _add_exhibition_features(df)

    return df


def _add_wind_features(df: pd.DataFrame) -> pd.DataFrame:
    """風向をcos/sin変換"""
    df = df.copy()
    # wind_direction: 1=北, 2=北東, ... 8=北西 → 角度（度）
    dir_to_deg = {1: 0, 2: 45, 3: 90, 4: 135, 5: 180, 6: 225, 7: 270, 8: 315}
    df["wind_deg"] = df["wind_direction"].map(dir_to_deg).fillna(0)
    df["wind_cos"] = np.cos(np.radians(df["wind_deg"]))
    df["wind_sin"] = np.sin(np.radians(df["wind_deg"]))
    df["wind_speed"] = df["wind_speed"].fillna(0)
    return df


def _add_grade_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """級別を数値化"""
    df = df.copy()
    df["grade_num"] = df["grade"].map(GRADE_MAP).fillna(1).astype(int)
    return df


def _add_course_features(df: pd.DataFrame) -> pd.DataFrame:
    """コース関連特徴量"""
    df = df.copy()
    df["course"] = pd.to_numeric(df["course"], errors="coerce").fillna(
        pd.to_numeric(df["boat_number"], errors="coerce")
    ).astype(float)
    df["is_1course"] = (df["course"] == 1).astype(int)
    df["is_inner"] = (df["course"] <= 3).astype(int)
    return df


def _add_rolling_stats(df: pd.DataFrame, session: Session) -> pd.DataFrame:
    """
    選手の直近6走の成績を集計してマージ。
    過去レースのみ対象（当日以前）とする。
    """
    # 全選手の全結果を取得
    result_query = """
    SELECT
        e.racer_id,
        r.race_date,
        rr.arrival,
        rr.start_timing
    FROM race_results rr
    JOIN race_entries e ON rr.race_id = e.race_id AND rr.boat_number = e.boat_number
    JOIN races r ON rr.race_id = r.race_id
    ORDER BY e.racer_id, r.race_date
    """
    hist = pd.read_sql(text(result_query), session.bind)
    hist["race_date"] = pd.to_datetime(hist["race_date"])

    rolling_records = []
    for racer_id, group in hist.groupby("racer_id"):
        group = group.sort_values("race_date").reset_index(drop=True)
        arrivals = group["arrival"].tolist()
        sts = group["start_timing"].tolist()
        for idx in range(len(arrivals)):
            past = arrivals[max(0, idx - 6):idx]
            past_st = [s for s in sts[max(0, idx - 6):idx] if s is not None]
            rolling_records.append({
                "racer_id": racer_id,
                "race_date": group["race_date"].iloc[idx].date(),
                "_row_idx": idx,
                "recent_avg_arrival": np.mean(past) if past else 3.5,
                "recent_win_count": sum(1 for a in past if a == 1),
                "recent_avg_st": np.mean(past_st) if past_st else 0.18,
            })

    if not rolling_records:
        df["recent_avg_arrival"] = 3.5
        df["recent_win_count"] = 0
        df["recent_avg_st"] = 0.18
        return df

    rolling_df = pd.DataFrame(rolling_records)
    # 同日複数レースによる重複を避けるため racer_id+race_date で集約（先頭値を使用）
    rolling_df = rolling_df.groupby(["racer_id", "race_date"], as_index=False).first()
    rolling_df["race_date"] = pd.to_datetime(rolling_df["race_date"])
    df["race_date"] = pd.to_datetime(df["race_date"])

    df = df.merge(rolling_df[["racer_id", "race_date", "recent_avg_arrival", "recent_win_count", "recent_avg_st"]],
                  on=["racer_id", "race_date"], how="left")
    df["recent_avg_arrival"] = df["recent_avg_arrival"].fillna(3.5)
    df["recent_win_count"] = df["recent_win_count"].fillna(0)
    df["recent_avg_st"] = df["recent_avg_st"].fillna(0.18)
    return df


def _add_odds_features(df: pd.DataFrame, session: Session) -> pd.DataFrame:
    """単勝オッズを特徴量に追加"""
    odds_query = """
    SELECT
        o.race_id,
        CAST(o.combination AS INTEGER) AS boat_number,
        o.odds AS win_odds
    FROM odds o
    WHERE o.bet_type = 'win'
    """
    odds_df = pd.read_sql(text(odds_query), session.bind)
    if odds_df.empty:
        df["win_odds"] = None
        return df

    df = df.merge(odds_df, on=["race_id", "boat_number"], how="left")
    df["win_odds"] = df["win_odds"].fillna(df["win_odds"].median())
    return df


def _add_exhibition_features(df: pd.DataFrame) -> pd.DataFrame:
    """展示タイム関連の特徴量"""
    df = df.copy()
    df["exhibition_time"] = pd.to_numeric(df["exhibition_time"], errors="coerce")
    df["start_exhibition_time"] = pd.to_numeric(df["start_exhibition_time"], errors="coerce")

    median_ex = df["exhibition_time"].median()
    df["exhibition_time"] = df["exhibition_time"].fillna(median_ex if pd.notna(median_ex) else 6.7)

    # レース内での展示タイム順位（速い=1）
    df["exhibition_rank"] = df.groupby("race_id")["exhibition_time"].rank(ascending=True)

    df["start_exhibition_time"] = df["start_exhibition_time"].fillna(0.18)

    return df


FEATURE_COLS = [
    "boat_number",
    "course",
    "is_1course",
    "is_inner",
    "grade_num",
    "national_win_rate",
    "national_place2_rate",
    "local_win_rate",
    "local_place2_rate",
    "motor_rate",
    "boat_rate",
    "exhibition_time",
    "exhibition_rank",
    "start_exhibition_time",
    "recent_avg_arrival",
    "recent_win_count",
    "recent_avg_st",
    "temperature",
    "water_temperature",
    "wind_speed",
    "wind_cos",
    "wind_sin",
    "wave_height",
    "race_number",
    "win_odds",
]

TARGET_COL = "arrival"
