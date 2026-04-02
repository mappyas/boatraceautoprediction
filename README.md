# ボートレース AI 自動予測システム

LightGBMを使ったボートレース予測・投資支援システム。

## セットアップ

```bash
pip install -r requirements.txt
playwright install chromium
```

## 使い方

### 1. 過去データ投入（初回のみ）
```bash
python -m src.collectors.ingest --start 2023-01-01 --end 2025-12-31
```

### 2. モデル学習
```bash
python -m src.models.trainer --start 2023-01-01
```

### 3. バックテスト
```bash
python -m src.models.backtest --start 2025-01-01 --end 2025-12-31 --ev 1.2
```

### 4. 日次スケジューラ起動
```bash
python -m src.scheduler.daily_job
```

### 5. ダッシュボード起動
```bash
streamlit run ui/app.py
```

## 通知設定（任意）

`.env.example` を `.env` にコピーして設定：
```
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
LINE_NOTIFY_TOKEN=xxxxx
```

## システム構成

```
src/
├── collectors/     # データ収集
│   ├── open_api.py     # Boatrace Open API
│   ├── official_dl.py  # 公式ダウンロードデータ
│   ├── scraper.py      # boatrace.jp スクレイパー
│   ├── weather.py      # Open-Meteo 気象API
│   └── ingest.py       # 過去データ投入
├── db/             # データベース
│   ├── models.py       # SQLAlchemyモデル
│   └── repository.py   # CRUD
├── features/       # 特徴量
│   └── builder.py
├── models/         # 機械学習
│   ├── trainer.py      # 学習
│   ├── predictor.py    # 推論
│   └── backtest.py     # バックテスト
└── scheduler/      # スケジューラ
    ├── daily_job.py
    └── notify.py       # Discord/LINE通知
ui/
└── app.py          # Streamlit UI
```
