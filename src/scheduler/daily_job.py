"""
日次スケジューラ
毎朝の出走表取得、レース直前の情報・オッズ取得、レース後の結果取得を管理する。

使い方:
    python -m src.scheduler.daily_job        # デーモン起動
    python -m src.scheduler.daily_job --once # 1回だけ実行
"""

import argparse
import time
from datetime import date, datetime, timedelta

import schedule
from loguru import logger

from src.collectors.ingest import ingest_date, DB_PATH
from src.collectors.scraper import get_today_race_list, scrape_before_info_sync, scrape_odds_sync
from src.collectors.weather import fetch_weather
from src.db.models import Race, RaceEntry, Odds, init_db
from src.db.repository import (
    get_session_factory, session_scope,
    RaceRepository, RaceEntryRepository, OddsRepository,
)

logger.add("logs/daily_job_{time:YYYY-MM-DD}.log", rotation="1 day", retention="30 days")


def job_fetch_programs():
    """毎朝8:00 - 本日の出走表データを取得"""
    today = date.today()
    logger.info(f"[JOB] fetch_programs: {today}")

    try:
        ingest_date(get_session_factory(DB_PATH), today, skip_existing=False)
        logger.info(f"[JOB] fetch_programs: done for {today}")
    except Exception as e:
        logger.error(f"[JOB] fetch_programs failed: {e}")


def job_fetch_results():
    """毎晩21:00 - 昨日・今日のレース結果を取得"""
    today = date.today()
    yesterday = today - timedelta(days=1)
    logger.info(f"[JOB] fetch_results: {yesterday} 〜 {today}")

    session_factory = get_session_factory(DB_PATH)
    for d in [yesterday, today]:
        try:
            ingest_date(session_factory, d, skip_existing=False)
        except Exception as e:
            logger.error(f"[JOB] fetch_results failed for {d}: {e}")


def job_fetch_before_info():
    """
    レース直前情報取得（毎時15分に実行し、現在時刻から2時間後のレースを対象）
    本日の開催レース一覧を取得し、直前情報・オッズをDBに保存する。
    """
    today = date.today()
    now = datetime.now()
    logger.info(f"[JOB] fetch_before_info: {now.strftime('%H:%M')}")

    session_factory = get_session_factory(DB_PATH)

    with session_scope(session_factory) as session:
        race_repo = RaceRepository(session)
        races = race_repo.get_by_date(today)

    if not races:
        logger.info("[JOB] No races today")
        return

    for race in races:
        try:
            # 展示・オッズ取得
            before = scrape_before_info_sync(race.stadium_code, today, race.race_number)
            if before:
                with session_scope(session_factory) as session:
                    entry_repo = RaceEntryRepository(session)
                    entries = entry_repo.get_by_race(race.race_id)
                    entry_map = {e.boat_number: e for e in entries}

                    for item in before.get("exhibition_times", []):
                        bn = item.get("boat_number")
                        if bn in entry_map:
                            entry_map[bn].exhibition_time = item.get("exhibition_time")

                    for item in before.get("start_times", []):
                        bn = item.get("boat_number")
                        if bn in entry_map:
                            entry_map[bn].start_exhibition_time = item.get("start_exhibition_time")

            # オッズ取得
            odds_data = scrape_odds_sync(race.stadium_code, today, race.race_number)
            if odds_data:
                now_dt = datetime.now()
                with session_scope(session_factory) as session:
                    odds_repo = OddsRepository(session)
                    odds_list = []

                    for boat_num, odds_val in odds_data.get("win", {}).items():
                        odds_list.append(Odds(
                            race_id=race.race_id,
                            bet_type="win",
                            combination=str(boat_num),
                            odds=odds_val,
                            recorded_at=now_dt,
                        ))

                    for combo, odds_val in odds_data.get("trifecta", {}).items():
                        odds_list.append(Odds(
                            race_id=race.race_id,
                            bet_type="trifecta",
                            combination=combo,
                            odds=odds_val,
                            recorded_at=now_dt,
                        ))

                    if odds_list:
                        odds_repo.bulk_insert(odds_list)

        except Exception as e:
            logger.error(f"[JOB] before_info failed for {race.race_id}: {e}")

    logger.info(f"[JOB] fetch_before_info: done ({len(races)} races)")


def job_fetch_weather():
    """毎朝9:00 - 本日開催場の気象データ取得"""
    today = date.today()
    logger.info("[JOB] fetch_weather")

    session_factory = get_session_factory(DB_PATH)
    with session_scope(session_factory) as session:
        race_repo = RaceRepository(session)
        races = race_repo.get_by_date(today)
        stadium_codes = list(set(r.stadium_code for r in races))

    for stadium_code in stadium_codes:
        try:
            weather = fetch_weather(stadium_code, today)
            if weather:
                with session_scope(session_factory) as session:
                    race_repo = RaceRepository(session)
                    for race in race_repo.get_by_date(today):
                        if race.stadium_code == stadium_code:
                            race.temperature = race.temperature or weather.get("temperature")
                            race.wind_speed = race.wind_speed or weather.get("wind_speed")
                            race.wind_direction = race.wind_direction or weather.get("wind_direction")
        except Exception as e:
            logger.error(f"[JOB] weather failed for {stadium_code}: {e}")

    logger.info(f"[JOB] fetch_weather: done ({len(stadium_codes)} stadiums)")


def setup_schedule():
    schedule.every().day.at("08:00").do(job_fetch_programs)
    schedule.every().day.at("09:00").do(job_fetch_weather)
    schedule.every().hour.at(":15").do(job_fetch_before_info)
    schedule.every().day.at("21:00").do(job_fetch_results)

    logger.info("Schedule configured:")
    logger.info("  08:00 - 出走表取得")
    logger.info("  09:00 - 気象データ取得")
    logger.info("  毎時15分 - 直前情報・オッズ取得")
    logger.info("  21:00 - レース結果取得")


def run_once():
    """全ジョブを1回実行（テスト用）"""
    job_fetch_programs()
    job_fetch_weather()
    job_fetch_before_info()


def main():
    parser = argparse.ArgumentParser(description="日次スケジューラ")
    parser.add_argument("--once", action="store_true", help="1回だけ実行して終了")
    args = parser.parse_args()

    init_db(DB_PATH)

    if args.once:
        run_once()
        return

    setup_schedule()
    logger.info("Scheduler started. Press Ctrl+C to stop.")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
