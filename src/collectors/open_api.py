"""
Boatrace Open API クライアント
https://github.com/BoatraceOpenAPI

JSON schema (v3):
- results: { "results": [ { date, stadium_number, number, wind_speed, ... , boats, payouts } ] }
- programs: { "programs": [ { date, stadium_number, number, ..., boats } ] }

利用可能期間: おおよそ直近2〜3週間分のみ（過去分は404）
"""

import time
from datetime import date, timedelta
from typing import Optional

import requests
from loguru import logger

BASE_URLS = {
    "results": "https://boatraceopenapi.github.io/results/v3",
    "programs": "https://boatraceopenapi.github.io/programs/v3",
}

GRADE_CLASS_MAP = {1: "A1", 2: "A2", 3: "B1", 4: "B2"}


def _get_json(url: str, retries: int = 3, wait: float = 3.0) -> Optional[dict]:
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"GET {url} failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(wait)
    return None


def fetch_results_by_date(target_date: date) -> Optional[dict]:
    """指定日のレース結果を取得"""
    year = target_date.strftime("%Y")
    date_str = target_date.strftime("%Y%m%d")
    url = f"{BASE_URLS['results']}/{year}/{date_str}.json"
    logger.info(f"Fetching results: {url}")
    data = _get_json(url)
    if data is None:
        logger.warning(f"No results data for {date_str}")
    return data


def fetch_programs_by_date(target_date: date) -> Optional[dict]:
    """指定日の出走表を取得"""
    year = target_date.strftime("%Y")
    date_str = target_date.strftime("%Y%m%d")
    url = f"{BASE_URLS['programs']}/{year}/{date_str}.json"
    logger.info(f"Fetching programs: {url}")
    data = _get_json(url)
    if data is None:
        logger.warning(f"No programs data for {date_str}")
    return data


def parse_results(data: dict) -> list[dict]:
    """
    results JSON を正規化されたレコードリストに変換。
    """
    records = []
    for race_data in data.get("results", []):
        try:
            stadium_code = str(race_data["stadium_number"]).zfill(2)
            race_date_str = race_data["date"].replace("-", "")
            race_number = int(race_data["number"])
            race_id = f"{stadium_code}{race_date_str}{str(race_number).zfill(2)}"
            parts = race_date_str
            race_date = date(int(parts[:4]), int(parts[4:6]), int(parts[6:8]))

            rec = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": race_date,
                "race_number": race_number,
                "weather": str(race_data.get("weather_number", "")),
                "temperature": race_data.get("air_temperature"),
                "water_temperature": race_data.get("water_temperature"),
                "wind_speed": race_data.get("wind_speed"),
                "wind_direction": race_data.get("wind_direction_number"),
                "wave_height": race_data.get("wave_height"),
                "results": [],
                "payouts": [],
            }

            for boat in race_data.get("boats", []):
                rec["results"].append({
                    "boat_number": boat.get("racer_boat_number"),
                    "arrival": boat.get("racer_place_number"),
                    "start_timing": boat.get("racer_start_timing"),
                    "race_time": None,
                    "winning_trick": None,
                    "course": boat.get("racer_course_number"),
                    "racer_id": str(boat.get("racer_number", "")),
                })

            payouts = race_data.get("payouts", {})
            if isinstance(payouts, dict):
                for bet_type, items in payouts.items():
                    for item in (items or []):
                        rec["payouts"].append({
                            "bet_type": bet_type,
                            "combination": item.get("combination", "").replace("=", "-"),
                            "payout": item.get("amount"),
                        })

            records.append(rec)
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse race record: {e} | data={race_data}")

    return records


def parse_programs(data: dict) -> list[dict]:
    """
    programs JSON を正規化されたレコードリストに変換。
    """
    records = []
    for race_data in data.get("programs", []):
        try:
            stadium_code = str(race_data["stadium_number"]).zfill(2)
            race_date_str = race_data["date"].replace("-", "")
            race_number = int(race_data["number"])
            race_id = f"{stadium_code}{race_date_str}{str(race_number).zfill(2)}"
            parts = race_date_str
            race_date = date(int(parts[:4]), int(parts[4:6]), int(parts[6:8]))

            rec = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": race_date,
                "race_number": race_number,
                "entries": [],
            }

            for boat in race_data.get("boats", []):
                racer_id = str(boat.get("racer_number", ""))
                grade_num = boat.get("racer_class_number")
                rec["entries"].append({
                    "boat_number": boat.get("racer_boat_number"),
                    "racer_id": racer_id,
                    "racer_name": boat.get("racer_name", ""),
                    "grade": GRADE_CLASS_MAP.get(grade_num, "B1"),
                    "weight": boat.get("racer_weight"),
                    "national_win_rate": boat.get("racer_national_top_1_percent"),
                    "national_place2_rate": boat.get("racer_national_top_2_percent"),
                    "national_place3_rate": boat.get("racer_national_top_3_percent"),
                    "local_win_rate": boat.get("racer_local_top_1_percent"),
                    "local_place2_rate": boat.get("racer_local_top_2_percent"),
                    "fly_count": boat.get("racer_flying_count", 0),
                    "late_count": boat.get("racer_late_count", 0),
                    "motor_number": boat.get("racer_assigned_motor_number"),
                    "motor_rate": boat.get("racer_assigned_motor_top_2_percent"),
                    "boat_number_motor": boat.get("racer_assigned_boat_number"),
                    "boat_rate": boat.get("racer_assigned_boat_top_2_percent"),
                })

            records.append(rec)
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse program record: {e} | data={race_data}")

    return records


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
