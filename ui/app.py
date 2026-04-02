"""
Streamlit ダッシュボード
使い方: streamlit run ui/app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from src.db.models import init_db, get_engine
from src.db.repository import get_session_factory, session_scope, RaceRepository, OddsRepository
from src.features.builder import build_feature_df, FEATURE_COLS
from src.models.predictor import load_model, predict_race, predict_trifecta

DB_PATH = "data/db/boatrace.db"

st.set_page_config(
    page_title="ボートレース AI予測",
    page_icon="🚤",
    layout="wide",
    initial_sidebar_state="expanded",
)

STADIUM_NAMES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡", "08": "常滑",
    "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
    "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島",
    "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村",
}


@st.cache_resource
def get_db():
    init_db(DB_PATH)
    return get_session_factory(DB_PATH)


@st.cache_resource
def get_model():
    try:
        return load_model()
    except FileNotFoundError:
        return None


def page_prediction():
    st.title("🚤 レース予測")

    col1, col2, col3 = st.columns(3)
    with col1:
        race_date = st.date_input("開催日", value=date.today())
    with col2:
        stadium_options = {f"{k}: {v}": k for k, v in STADIUM_NAMES.items()}
        stadium_label = st.selectbox("競艇場", list(stadium_options.keys()))
        stadium_code = stadium_options[stadium_label]
    with col3:
        race_number = st.number_input("レース番号", min_value=1, max_value=12, value=1)

    if st.button("予測実行", type="primary"):
        model_meta = get_model()
        if model_meta is None:
            st.error("モデルが見つかりません。先に `python -m src.models.trainer` を実行してください。")
            return

        session_factory = get_db()
        race_id = f"{stadium_code}{race_date.strftime('%Y%m%d')}{str(race_number).zfill(2)}"

        with session_scope(session_factory) as session:
            df = build_feature_df(
                session,
                start_date=str(race_date - timedelta(days=180)),
                end_date=str(race_date),
            )

        race_df = df[df["race_id"] == race_id]
        if race_df.empty:
            st.warning("このレースのデータが見つかりません。データ取得を確認してください。")
            return

        pred = predict_race(race_df.copy(), model_meta=model_meta)

        # 予測結果表示
        st.subheader(f"予測結果: {STADIUM_NAMES.get(stadium_code, stadium_code)} R{race_number}")

        col_chart, col_table = st.columns([3, 2])

        with col_chart:
            fig = px.bar(
                pred,
                x="boat_number",
                y="prob_1st",
                color="prob_1st",
                color_continuous_scale="RdYlGn",
                labels={"boat_number": "艇番", "prob_1st": "1着確率"},
                title="各艇の1着確率",
            )
            fig.update_layout(xaxis=dict(tickmode="array", tickvals=list(range(1, 7))))
            st.plotly_chart(fig, use_container_width=True)

        with col_table:
            display_df = pred[["boat_number", "prob_1st", "win_odds", "expected_value"]].copy()
            display_df.columns = ["艇番", "1着確率", "単勝オッズ", "期待値"]
            display_df["1着確率"] = display_df["1着確率"].map("{:.1%}".format)
            display_df["単勝オッズ"] = display_df["単勝オッズ"].map(
                lambda x: f"{x:.1f}" if pd.notna(x) else "-"
            )
            display_df["期待値"] = display_df["期待値"].map(
                lambda x: f"{x:.2f}" if pd.notna(x) else "-"
            )

            def highlight_ev(row):
                try:
                    ev = float(row["期待値"])
                    if ev >= 1.2:
                        return ["background-color: #90EE90"] * len(row)
                    elif ev >= 1.0:
                        return ["background-color: #FFFFE0"] * len(row)
                except (ValueError, TypeError):
                    pass
                return [""] * len(row)

            st.dataframe(display_df.style.apply(highlight_ev, axis=1), use_container_width=True)

        # 3連単TOP10
        st.subheader("3連単 期待値ランキング")
        with session_scope(session_factory) as session:
            odds_repo = OddsRepository(session)
            trifecta_odds_raw = odds_repo.get_latest_by_race(race_id, "trifecta")

        trifecta_odds_dict = {o.combination: o.odds for o in trifecta_odds_raw}

        tri_df = predict_trifecta(pred, trifecta_odds=trifecta_odds_dict if trifecta_odds_dict else None)

        display_tri = tri_df.head(10).copy()
        display_tri["prob"] = display_tri["prob"].map("{:.4%}".format)
        display_tri["odds"] = display_tri["odds"].map(
            lambda x: f"{x:.1f}" if pd.notna(x) else "-"
        )
        display_tri["expected_value"] = display_tri["expected_value"].map(
            lambda x: f"{x:.2f}" if pd.notna(x) else "-"
        )
        display_tri.columns = ["組み合わせ", "確率", "オッズ", "期待値"]
        st.dataframe(display_tri, use_container_width=True)

        # 推奨買い目
        if trifecta_odds_dict:
            recommended = tri_df[tri_df["expected_value"].notna() & (tri_df["expected_value"] >= 1.2)]
            if not recommended.empty:
                st.success(f"推奨買い目（期待値1.2以上）: {', '.join(recommended['combination'].head(5).tolist())}")
            else:
                st.info("期待値1.2以上の3連単は見つかりませんでした。")


def page_betting_records():
    st.title("📊 収益管理")

    session_factory = get_db()

    query = """
    SELECT
        br.record_id,
        br.race_id,
        r.race_date,
        r.stadium_code,
        r.race_number,
        br.bet_type,
        br.combination,
        br.amount,
        br.odds_at_bet,
        br.is_hit,
        br.payout,
        br.bet_at,
        br.note
    FROM betting_records br
    JOIN races r ON br.race_id = r.race_id
    ORDER BY br.bet_at DESC
    """

    engine = get_engine(DB_PATH)
    df = pd.read_sql(query, engine)

    if df.empty:
        st.info("投票記録がまだありません。")
        return

    df["race_date"] = pd.to_datetime(df["race_date"])
    df["stadium_name"] = df["stadium_code"].map(STADIUM_NAMES)

    # サマリ
    total_bet = df["amount"].sum()
    total_payout = df["payout"].sum()
    recovery = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_count = df["is_hit"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("総投資額", f"¥{total_bet:,}")
    col2.metric("総払戻額", f"¥{total_payout:,}")
    col3.metric("回収率", f"{recovery:.1f}%", f"{total_payout - total_bet:+,}円")
    col4.metric("的中回数", f"{hit_count}回 / {len(df)}回")

    # 月次推移
    df["month"] = df["race_date"].dt.to_period("M").astype(str)
    monthly = df.groupby("month").agg(
        bet=("amount", "sum"),
        payout=("payout", "sum"),
    ).reset_index()
    monthly["recovery_rate"] = monthly["payout"] / monthly["bet"] * 100

    fig = go.Figure()
    fig.add_bar(x=monthly["month"], y=monthly["bet"], name="投資額", marker_color="lightblue")
    fig.add_bar(x=monthly["month"], y=monthly["payout"], name="払戻額", marker_color="orange")
    fig.add_scatter(
        x=monthly["month"], y=monthly["recovery_rate"],
        name="回収率(%)", yaxis="y2", line=dict(color="red")
    )
    fig.update_layout(
        title="月次 投資額・払戻額・回収率",
        yaxis=dict(title="金額（円）"),
        yaxis2=dict(title="回収率(%)", overlaying="y", side="right"),
        barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)

    # 投票記録一覧
    st.subheader("投票履歴")
    display_df = df[["race_date", "stadium_name", "race_number", "bet_type",
                      "combination", "amount", "odds_at_bet", "is_hit", "payout", "note"]].copy()
    display_df.columns = ["日付", "競艇場", "R", "券種", "組み合わせ", "賭け金", "オッズ", "的中", "払戻", "メモ"]
    display_df["的中"] = display_df["的中"].map({1: "✓", 0: ""})
    st.dataframe(display_df, use_container_width=True)

    # 投票記録追加
    st.subheader("投票記録を追加")
    with st.form("add_bet"):
        c1, c2, c3 = st.columns(3)
        with c1:
            bet_date = st.date_input("日付", value=date.today())
            stadium_label = st.selectbox(
                "競艇場",
                [f"{k}: {v}" for k, v in STADIUM_NAMES.items()]
            )
            bet_race_number = st.number_input("レース番号", 1, 12, 1)
        with c2:
            bet_type = st.selectbox("券種", ["win", "trifecta", "exacta", "quinella", "trio"])
            combination = st.text_input("組み合わせ（例: 1-2-3）")
            amount = st.number_input("賭け金（円）", min_value=100, step=100, value=100)
        with c3:
            odds_at_bet = st.number_input("オッズ", min_value=1.0, step=0.1, value=1.0)
            is_hit = st.checkbox("的中")
            payout = st.number_input("払戻金額", min_value=0, step=100, value=0)
            note = st.text_input("メモ")

        if st.form_submit_button("記録を追加"):
            from src.db.models import BettingRecord
            stadium_code_bet = stadium_label.split(":")[0].strip()
            race_id = f"{stadium_code_bet}{bet_date.strftime('%Y%m%d')}{str(bet_race_number).zfill(2)}"
            with session_scope(session_factory) as session:
                record = BettingRecord(
                    race_id=race_id,
                    bet_type=bet_type,
                    combination=combination,
                    amount=int(amount),
                    odds_at_bet=float(odds_at_bet),
                    is_hit=int(is_hit),
                    payout=int(payout),
                    note=note,
                )
                session.add(record)
            st.success("記録を追加しました。")
            st.rerun()


def page_data_status():
    st.title("🗄️ データ状況")

    engine = get_engine(DB_PATH)

    # レース数
    race_count = pd.read_sql("SELECT COUNT(*) as cnt FROM races", engine).iloc[0, 0]
    result_count = pd.read_sql("SELECT COUNT(*) as cnt FROM race_results", engine).iloc[0, 0]
    racer_count = pd.read_sql("SELECT COUNT(*) as cnt FROM racers", engine).iloc[0, 0]

    col1, col2, col3 = st.columns(3)
    col1.metric("総レース数", f"{race_count:,}")
    col2.metric("結果記録数", f"{result_count:,}")
    col3.metric("選手数", f"{racer_count:,}")

    # 直近30日の取得状況
    st.subheader("直近30日の取得状況")
    daily_df = pd.read_sql(
        """
        SELECT race_date, COUNT(*) as race_count
        FROM races
        WHERE race_date >= date('now', '-30 days')
        GROUP BY race_date
        ORDER BY race_date
        """,
        engine,
    )
    if not daily_df.empty:
        fig = px.bar(daily_df, x="race_date", y="race_count", title="日別レース数")
        st.plotly_chart(fig, use_container_width=True)

    # モデル情報
    st.subheader("モデル情報")
    model_meta = get_model()
    if model_meta:
        st.success(f"モデルバージョン: {model_meta.get('version', 'unknown')}")
        col1, col2 = st.columns(2)
        col1.metric("Validation LogLoss", f"{model_meta.get('val_logloss', '-'):.4f}" if model_meta.get('val_logloss') else "-")
        col2.metric("1着的中率", f"{model_meta.get('accuracy_1st', 0):.1%}" if model_meta.get('accuracy_1st') else "-")
    else:
        st.warning("モデルが見つかりません。`python -m src.models.trainer` を実行してください。")

    # 選手検索
    st.subheader("選手検索")
    search_name = st.text_input("選手名で検索")
    if search_name:
        racer_df = pd.read_sql(
            f"SELECT racer_id, name, branch, grade, national_win_rate, national_place2_rate "
            f"FROM racers WHERE name LIKE '%{search_name}%' LIMIT 20",
            engine,
        )
        if not racer_df.empty:
            racer_df.columns = ["登録番号", "名前", "支部", "級別", "全国勝率", "全国2連率"]
            st.dataframe(racer_df, use_container_width=True)
        else:
            st.info("該当する選手が見つかりません。")


def main():
    st.sidebar.title("🚤 ボートレース AI")
    page = st.sidebar.radio(
        "メニュー",
        ["予測", "収益管理", "データ状況"],
        index=0,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("**使い方**")
    st.sidebar.markdown(
        "1. データ投入: `python -m src.collectors.ingest --start 2023-01-01`\n"
        "2. モデル学習: `python -m src.models.trainer`\n"
        "3. スケジューラ: `python -m src.scheduler.daily_job`\n"
        "4. このUI: `streamlit run ui/app.py`"
    )

    if page == "予測":
        page_prediction()
    elif page == "収益管理":
        page_betting_records()
    elif page == "データ状況":
        page_data_status()


if __name__ == "__main__":
    main()
