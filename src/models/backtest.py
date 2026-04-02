"""
バックテスト
過去データでの回収率・的中率を検証する。

使い方:
    python -m src.models.backtest --start 2025-01-01 --end 2025-12-31
"""

import argparse
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from src.db.models import get_engine
from src.db.repository import get_session_factory, session_scope
from src.features.builder import build_feature_df, FEATURE_COLS, TARGET_COL
from src.models.predictor import load_model, predict_race
from src.models.trainer import prepare_data

DB_PATH = "data/db/boatrace.db"


def run_backtest(
    start_date: str,
    end_date: str,
    ev_threshold: float = 1.0,
    bet_unit: int = 100,
    model_path: str = None,
):
    """
    バックテストを実行し、結果サマリを返す。

    Args:
        start_date: バックテスト開始日
        end_date: バックテスト終了日
        ev_threshold: 期待値がこの値以上の舟券のみ投票（単勝）
        bet_unit: 1回の賭け金（円）
        model_path: モデルファイルパス
    """
    model_meta = load_model(model_path)
    session_factory = get_session_factory(DB_PATH)

    with session_scope(session_factory) as session:
        df = build_feature_df(session, start_date=start_date, end_date=end_date)

    if df.empty:
        logger.error("バックテストデータが空です")
        return

    # 結果がある行のみ
    df_with_result = df[df[TARGET_COL].notna() & df[TARGET_COL].between(1, 6)]
    races = df_with_result.groupby("race_id")

    total_bet = 0
    total_payout = 0
    hit_count = 0
    bet_count = 0
    skipped_races = 0

    records = []

    for race_id, group in races:
        if len(group) < 2:
            skipped_races += 1
            continue

        pred = predict_race(group.copy(), model_meta=model_meta)
        if pred.empty or pred["expected_value"].isna().all():
            skipped_races += 1
            continue

        # 期待値閾値を超える艇に賭ける
        candidates = pred[pred["expected_value"] >= ev_threshold]

        for _, row in candidates.iterrows():
            boat = int(row["boat_number"])
            ev = row["expected_value"]
            odds = row.get("win_odds")
            if odds is None:
                continue

            # 実際の結果を確認
            actual = group[group["boat_number"] == boat][TARGET_COL].values
            if len(actual) == 0:
                continue
            is_hit = int(actual[0]) == 1

            payout = int(odds * bet_unit) if is_hit else 0
            total_bet += bet_unit
            total_payout += payout
            bet_count += 1
            if is_hit:
                hit_count += 1

            records.append({
                "race_id": race_id,
                "boat_number": boat,
                "prob_1st": row["prob_1st"],
                "expected_value": ev,
                "odds": odds,
                "is_hit": is_hit,
                "bet": bet_unit,
                "payout": payout,
            })

    # サマリ
    recovery_rate = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hit_count / bet_count * 100 if bet_count > 0 else 0

    logger.info("=" * 50)
    logger.info(f"バックテスト結果 ({start_date} 〜 {end_date})")
    logger.info(f"期待値閾値: {ev_threshold}")
    logger.info(f"投票回数: {bet_count}")
    logger.info(f"総投資額: {total_bet:,}円")
    logger.info(f"総払戻額: {total_payout:,}円")
    logger.info(f"回収率: {recovery_rate:.1f}%")
    logger.info(f"的中率: {hit_rate:.1f}% ({hit_count}/{bet_count})")
    logger.info(f"スキップレース: {skipped_races}")
    logger.info("=" * 50)

    # 期待値別の回収率分析
    if records:
        rec_df = pd.DataFrame(records)
        for threshold in [1.0, 1.1, 1.2, 1.3, 1.5]:
            sub = rec_df[rec_df["expected_value"] >= threshold]
            if len(sub) == 0:
                continue
            rr = sub["payout"].sum() / sub["bet"].sum() * 100
            hr = sub["is_hit"].mean() * 100
            logger.info(
                f"EV>={threshold}: 投票{len(sub)}回 回収率{rr:.1f}% 的中率{hr:.1f}%"
            )

    return {
        "bet_count": bet_count,
        "total_bet": total_bet,
        "total_payout": total_payout,
        "recovery_rate": recovery_rate,
        "hit_rate": hit_rate,
        "records": records,
    }


def main():
    parser = argparse.ArgumentParser(description="バックテスト")
    parser.add_argument("--start", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--ev", type=float, default=1.0, help="期待値閾値（デフォルト1.0）")
    parser.add_argument("--bet", type=int, default=100, help="1回の賭け金（円）")
    parser.add_argument("--model", default=None, help="モデルファイルパス")
    args = parser.parse_args()

    run_backtest(
        start_date=args.start,
        end_date=args.end,
        ev_threshold=args.ev,
        bet_unit=args.bet,
        model_path=args.model,
    )


if __name__ == "__main__":
    main()
