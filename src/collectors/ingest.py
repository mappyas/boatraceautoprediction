"""
過去データ投入スクリプト
Open API から指定期間のデータを取得してDBに格納する。

使い方:
    python -m src.collectors.ingest --start 2023-01-01 --end 2025-12-31
"""

import argparse
import time
from datetime import date, datetime
from loguru import logger

from src.collectors.open_api import (
    fetch_results_by_date,
    fetch_programs_by_date,
    parse_results,
    parse_programs,
    date_range,
)
from src.db.models import (
    Race, Racer, RaceEntry, RaceResult, Odds, init_db
)
from src.db.repository import (
    get_session_factory, session_scope,
    RaceRepository, RacerRepository, RaceEntryRepository,
    RaceResultRepository, OddsRepository,
)

DB_PATH = "data/db/boatrace.db"
REQUEST_INTERVAL = 3.0  # 秒


def ingest_date(session_factory, target_date: date, skip_existing: bool = True):
    """1日分のデータを取得してDBに保存"""

    # --- 結果 ---
    result_data = fetch_results_by_date(target_date)
    time.sleep(REQUEST_INTERVAL)

    if result_data:
        records = parse_results(result_data)
        with session_scope(session_factory) as session:
            race_repo = RaceRepository(session)
            result_repo = RaceResultRepository(session)
            odds_repo = OddsRepository(session)

            for rec in records:
                if skip_existing and race_repo.get_by_id(rec["race_id"]):
                    logger.debug(f"Skip existing race: {rec['race_id']}")
                    continue

                race = Race(
                    race_id=rec["race_id"],
                    stadium_code=rec["stadium_code"],
                    race_date=rec["race_date"],
                    race_number=rec["race_number"],
                    weather=rec.get("weather"),
                    temperature=rec.get("temperature"),
                    water_temperature=rec.get("water_temperature"),
                    wind_speed=rec.get("wind_speed"),
                    wind_direction=rec.get("wind_direction"),
                    wave_height=rec.get("wave_height"),
                )
                race_repo.upsert(race)

                results = [
                    RaceResult(
                        race_id=rec["race_id"],
                        boat_number=r["boat_number"],
                        arrival=r.get("arrival"),
                        start_timing=r.get("start_timing"),
                        race_time=r.get("race_time"),
                        winning_trick=r.get("winning_trick"),
                    )
                    for r in rec.get("results", [])
                    if r.get("boat_number")
                ]
                result_repo.bulk_upsert(results)

                odds_list = [
                    Odds(
                        race_id=rec["race_id"],
                        bet_type=p["bet_type"],
                        combination=p["combination"],
                        odds=p["payout"] / 100.0 if p.get("payout") else None,
                    )
                    for p in rec.get("payouts", [])
                    if p.get("combination") and p.get("payout")
                ]
                if odds_list:
                    odds_repo.bulk_insert(odds_list)

        logger.info(f"Results saved: {target_date} ({len(records)} races)")

    # --- 出走表 ---
    program_data = fetch_programs_by_date(target_date)
    time.sleep(REQUEST_INTERVAL)

    if program_data:
        records = parse_programs(program_data)
        with session_scope(session_factory) as session:
            race_repo = RaceRepository(session)
            racer_repo = RacerRepository(session)
            entry_repo = RaceEntryRepository(session)

            for rec in records:
                # レースが未登録なら作成
                if not race_repo.get_by_id(rec["race_id"]):
                    race = Race(
                        race_id=rec["race_id"],
                        stadium_code=rec["stadium_code"],
                        race_date=rec["race_date"],
                        race_number=rec["race_number"],
                    )
                    race_repo.upsert(race)

                entries = []
                for e in rec.get("entries", []):
                    if not e.get("racer_id") or not e.get("boat_number"):
                        continue

                    # 選手マスタ更新
                    racer = Racer(
                        racer_id=e["racer_id"],
                        name=e.get("racer_name", ""),
                        branch=e.get("branch"),
                        grade=e.get("grade"),
                        weight=e.get("weight"),
                        national_win_rate=e.get("national_win_rate"),
                        national_place2_rate=e.get("national_place2_rate"),
                        local_win_rate=e.get("local_win_rate"),
                        local_place2_rate=e.get("local_place2_rate"),
                    )
                    racer_repo.upsert(racer)

                    entries.append(
                        RaceEntry(
                            race_id=rec["race_id"],
                            boat_number=e["boat_number"],
                            racer_id=e["racer_id"],
                            motor_number=e.get("motor_number"),
                            motor_rate=e.get("motor_rate"),
                            boat_number_motor=e.get("boat_number_motor"),
                            boat_rate=e.get("boat_rate"),
                        )
                    )

                entry_repo.bulk_upsert(entries)

        logger.info(f"Programs saved: {target_date} ({len(records)} races)")


def run(start: date, end: date, skip_existing: bool = True):
    logger.info(f"Ingesting data from {start} to {end}")
    init_db(DB_PATH)
    session_factory = get_session_factory(DB_PATH)

    total = 0
    for d in date_range(start, end):
        logger.info(f"Processing {d}...")
        try:
            ingest_date(session_factory, d, skip_existing=skip_existing)
            total += 1
        except Exception as e:
            logger.error(f"Error processing {d}: {e}")

    logger.info(f"Done. Processed {total} days.")


def main():
    parser = argparse.ArgumentParser(description="Boatrace 過去データ投入")
    parser.add_argument("--start", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="終了日 (YYYY-MM-DD)、省略時は今日")
    parser.add_argument("--no-skip", action="store_true", help="既存データも上書き")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else date.today()

    run(start, end, skip_existing=not args.no_skip)


if __name__ == "__main__":
    main()
