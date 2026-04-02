"""
推論モジュール
学習済みLightGBMモデルで指定レースの着順確率・期待値を予測する。
"""

import pickle
from pathlib import Path
from itertools import permutations
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from src.features.builder import FEATURE_COLS

MODEL_DIR = Path("data/models")
DEFAULT_MODEL_PATH = MODEL_DIR / "lgbm_latest.pkl"


def load_model(model_path: str = None) -> dict:
    path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
    if not path.exists():
        raise FileNotFoundError(f"Model not found: {path}. 先にtrainer.pyを実行してください。")
    with open(path, "rb") as f:
        return pickle.load(f)


def predict_race(entry_df: pd.DataFrame, model_meta: dict = None) -> pd.DataFrame:
    """
    1レース分のエントリDataFrameを受け取り、予測結果を返す。

    Args:
        entry_df: 1レース分の特徴量DataFrame（boat_number列を含む）
        model_meta: load_model()の返り値。Noneの場合はlatest modelを使用。

    Returns:
        DataFrame with columns:
            boat_number, prob_1st, prob_2nd, prob_3rd, expected_value
    """
    if model_meta is None:
        model_meta = load_model()

    model = model_meta["model"]
    feature_cols = model_meta["feature_cols"]

    # 特徴量を揃える（不足列は0埋め）
    for col in feature_cols:
        if col not in entry_df.columns:
            entry_df[col] = 0

    X = entry_df[feature_cols].copy()
    for col in X.columns:
        if X[col].dtype in [float, np.float64]:
            X[col] = X[col].fillna(X[col].median() if not X[col].isna().all() else 0)
        else:
            X[col] = X[col].fillna(0)

    # 予測 → shape: (n_boats, 6) = 各艇の各着順の確率
    probs = model.predict(X)  # multiclass: P(着順=k) for k=0..5

    result = entry_df[["boat_number"]].copy()
    result["prob_1st"] = probs[:, 0]  # 1着確率

    # 2着・3着確率は簡易計算（条件付き確率の近似）
    # P(2着) ≈ 1着以外の各艇が「残り着順でトップ」になる確率
    result["prob_2nd"] = probs[:, 1]
    result["prob_3rd"] = probs[:, 2]

    # 正規化（確率の合計を1に）
    result["prob_1st"] = result["prob_1st"] / result["prob_1st"].sum()
    result["prob_2nd"] = result["prob_2nd"] / result["prob_2nd"].sum()
    result["prob_3rd"] = result["prob_3rd"] / result["prob_3rd"].sum()

    # 期待値（単勝）
    if "win_odds" in entry_df.columns:
        result["win_odds"] = entry_df["win_odds"].values
        result["expected_value"] = result["prob_1st"] * result["win_odds"]
    else:
        result["win_odds"] = None
        result["expected_value"] = None

    return result.sort_values("prob_1st", ascending=False).reset_index(drop=True)


def predict_trifecta(result_df: pd.DataFrame, trifecta_odds: dict = None) -> pd.DataFrame:
    """
    3連単の期待値TOP候補を計算する。

    Args:
        result_df: predict_race()の返り値
        trifecta_odds: {"1-2-3": 100.0, ...} 形式の3連単オッズ辞書

    Returns:
        DataFrame: combination, prob, odds, expected_value
    """
    boats = result_df["boat_number"].tolist()
    prob_1st = dict(zip(result_df["boat_number"], result_df["prob_1st"]))
    prob_2nd = dict(zip(result_df["boat_number"], result_df["prob_2nd"]))
    prob_3rd = dict(zip(result_df["boat_number"], result_df["prob_3rd"]))

    rows = []
    for perm in permutations(boats, 3):
        b1, b2, b3 = perm
        # 条件付き確率の近似
        p = prob_1st[b1] * prob_2nd[b2] * prob_3rd[b3]
        combo = f"{b1}-{b2}-{b3}"
        odds = trifecta_odds.get(combo) if trifecta_odds else None
        ev = p * odds if odds else None
        rows.append({
            "combination": combo,
            "prob": p,
            "odds": odds,
            "expected_value": ev,
        })

    df = pd.DataFrame(rows)
    if "expected_value" in df.columns and df["expected_value"].notna().any():
        df = df.sort_values("expected_value", ascending=False)
    else:
        df = df.sort_values("prob", ascending=False)

    return df.reset_index(drop=True)
