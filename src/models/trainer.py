"""
LightGBM モデル学習スクリプト

使い方:
    python -m src.models.trainer --start 2023-01-01 --end 2025-12-31
"""

import argparse
import pickle
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import shap
from loguru import logger
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import log_loss
from sqlalchemy.orm import Session

from src.db.models import get_engine
from src.db.repository import get_session_factory, session_scope
from src.features.builder import build_feature_df, FEATURE_COLS, TARGET_COL

DB_PATH = "data/db/boatrace.db"
MODEL_DIR = Path("data/models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

LGB_PARAMS = {
    "objective": "multiclass",
    "num_class": 6,          # 着順1〜6
    "metric": "multi_logloss",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 30,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 0.1,
    "verbose": -1,
    "n_jobs": -1,
}


def prepare_data(df: pd.DataFrame):
    """DataFrameを学習用X, yに変換"""
    available_cols = [c for c in FEATURE_COLS if c in df.columns]
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        logger.warning(f"Missing feature columns: {missing}")

    df_clean = df.dropna(subset=[TARGET_COL])
    df_clean = df_clean[df_clean[TARGET_COL].between(1, 6)]

    X = df_clean[available_cols].copy()
    # object型を数値に変換してからNaN補完
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")
    for col in X.columns:
        median = X[col].median()
        X[col] = X[col].fillna(median if pd.notna(median) else 0)

    # 着順を0始まりのラベルに変換（LightGBMのmulticlassは0-indexed）
    y = df_clean[TARGET_COL].astype(int) - 1  # 1着→0, 2着→1, ...

    return X, y, df_clean[["race_id", "boat_number", "race_date"]]


def train(start_date: str = None, end_date: str = None) -> str:
    """モデルを学習して保存する。モデルファイルパスを返す。"""
    session_factory = get_session_factory(DB_PATH)

    with session_scope(session_factory) as session:
        df = build_feature_df(session, start_date=start_date, end_date=end_date)

    if df.empty:
        raise ValueError("学習データが空です。先にデータ投入を実行してください。")

    X, y, meta = prepare_data(df)
    logger.info(f"Training data: {len(X)} rows, {len(X.columns)} features")

    # 時系列分割でバリデーション
    meta_race_date = pd.to_datetime(meta["race_date"])
    split_date = meta_race_date.quantile(0.8)
    train_mask = meta_race_date <= split_date
    val_mask = meta_race_date > split_date

    X_train, y_train = X[train_mask], y[train_mask]
    X_val, y_val = X[val_mask], y[val_mask]

    logger.info(f"Train: {len(X_train)}, Val: {len(X_val)}")

    train_ds = lgb.Dataset(X_train, label=y_train)
    val_ds = lgb.Dataset(X_val, label=y_val, reference=train_ds)

    callbacks = [
        lgb.early_stopping(stopping_rounds=50, verbose=True),
        lgb.log_evaluation(period=100),
    ]

    model = lgb.train(
        LGB_PARAMS,
        train_ds,
        num_boost_round=1000,
        valid_sets=[val_ds],
        callbacks=callbacks,
    )

    # バリデーション評価
    val_pred = model.predict(X_val)
    val_logloss = log_loss(y_val, val_pred)
    logger.info(f"Validation LogLoss: {val_logloss:.4f}")

    # 1着的中率
    pred_winner = np.argmax(val_pred, axis=1)
    actual_winner = y_val.values
    accuracy = (pred_winner == actual_winner).mean()
    logger.info(f"1着的中率: {accuracy:.4f}")

    # モデル保存
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = MODEL_DIR / f"lgbm_{version}.pkl"
    metadata = {
        "model": model,
        "feature_cols": X.columns.tolist(),
        "version": version,
        "val_logloss": val_logloss,
        "accuracy_1st": accuracy,
        "train_rows": len(X_train),
        "val_rows": len(X_val),
    }
    with open(model_path, "wb") as f:
        pickle.dump(metadata, f)

    # 最新モデルへのシンボリックリンク（Windows非対応のためコピー）
    latest_path = MODEL_DIR / "lgbm_latest.pkl"
    with open(latest_path, "wb") as f:
        pickle.dump(metadata, f)

    logger.info(f"Model saved: {model_path}")
    logger.info(f"Features: {X.columns.tolist()}")

    # SHAP 特徴量重要度
    _log_feature_importance(model, X.columns.tolist())

    return str(model_path)


def _log_feature_importance(model: lgb.Booster, feature_names: list):
    importance = model.feature_importance(importance_type="gain")
    pairs = sorted(zip(feature_names, importance), key=lambda x: -x[1])
    logger.info("Feature importances (gain):")
    for name, imp in pairs[:20]:
        logger.info(f"  {name}: {imp:.1f}")


def main():
    parser = argparse.ArgumentParser(description="LightGBM モデル学習")
    parser.add_argument("--start", default="2023-01-01", help="学習データ開始日")
    parser.add_argument("--end", default=None, help="学習データ終了日")
    args = parser.parse_args()

    model_path = train(start_date=args.start, end_date=args.end)
    print(f"Model saved to: {model_path}")


if __name__ == "__main__":
    main()
