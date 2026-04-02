"""
Boatrace Open API クライアント
https://github.com/BoatraceOpenAPI

データはGitHub Pages上にJSONで公開されている。
- results: レース結果・配当
- programs: 出走表
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

STADIUMS = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡", "08": "常滑",
    "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
    "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島",
    "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村",
}


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
    """指定日のレース結果をすべて取得"""
    year = target_date.strftime("%Y")
    date_str = target_date.strftime("%Y%m%d")
    url = f"{BASE_URLS['results']}/{year}/{date_str}.json"
    logger.info(f"Fetching results: {url}")
    data = _get_json(url)
    if data is None:
        logger.warning(f"No results data for {date_str}")
    return data


def fetch_programs_by_date(target_date: date) -> Optional[dict]:
    """指定日の出走表をすべて取得"""
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
    Open API の results JSONを正規化されたレコードリストに変換。

    Returns:
        list of {
            race_id, stadium_code, race_date, race_number,
            weather, temperature, water_temperature, wind_speed, wind_direction, wave_height,
            results: [{boat_number, arrival, start_timing, race_time, winning_trick}],
            payouts: [{bet_type, combination, payout}],
        }
    """
    records = []
    for race_data in data.get("races", []):
        try:
            stadium_code = str(race_data["jcd"]).zfill(2)
            race_date_str = str(race_data["hd"])
            race_number = int(race_data["rno"])
            race_id = f"{stadium_code}{race_date_str}{str(race_number).zfill(2)}"

            rec = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": date(
                    int(race_date_str[:4]),
                    int(race_date_str[4:6]),
                    int(race_date_str[6:8]),
                ),
                "race_number": race_number,
                "weather": race_data.get("weather"),
                "temperature": race_data.get("temp"),
                "water_temperature": race_data.get("wtemp"),
                "wind_speed": race_data.get("wspd"),
                "wind_direction": race_data.get("wdir"),
                "wave_height": race_data.get("wave"),
                "results": [],
                "payouts": [],
            }

            for r in race_data.get("result", []):
                rec["results"].append({
                    "boat_number": r.get("no"),
                    "arrival": r.get("rank"),
                    "start_timing": r.get("st"),
                    "race_time": r.get("time"),
                    "winning_trick": r.get("trick"),
                })

            for payout in race_data.get("payout", []):
                bet_type_raw = payout.get("betType", "")
                bet_type = _normalize_bet_type(bet_type_raw)
                for item in payout.get("payouts", []):
                    combo = "-".join(str(n) for n in item.get("nums", []))
                    rec["payouts"].append({
                        "bet_type": bet_type,
                        "combination": combo,
                        "payout": item.get("payout"),
                    })

            records.append(rec)
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse race record: {e} | data={race_data}")

    return records


def parse_programs(data: dict) -> list[dict]:
    """
    Open API の programs JSONを正規化されたレコードリストに変換。

    Returns:
        list of {
            race_id, stadium_code, race_date, race_number,
            entries: [{boat_number, racer_id, racer_name, ...}]
        }
    """
    records = []
    for race_data in data.get("races", []):
        try:
            stadium_code = str(race_data["jcd"]).zfill(2)
            race_date_str = str(race_data["hd"])
            race_number = int(race_data["rno"])
            race_id = f"{stadium_code}{race_date_str}{str(race_number).zfill(2)}"

            rec = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": date(
                    int(race_date_str[:4]),
                    int(race_date_str[4:6]),
                    int(race_date_str[6:8]),
                ),
                "race_number": race_number,
                "entries": [],
            }

            for entry in race_data.get("entries", []):
                racer = entry.get("racer", {})
                rec["entries"].append({
                    "boat_number": entry.get("no"),
                    "racer_id": str(racer.get("regNo", "")),
                    "racer_name": racer.get("name"),
                    "branch": racer.get("branch"),
                    "grade": racer.get("class"),
                    "weight": racer.get("weight"),
                    "national_win_rate": racer.get("nRateWin"),
                    "national_place2_rate": racer.get("nRate2"),
                    "local_win_rate": racer.get("lRateWin"),
                    "local_place2_rate": racer.get("lRate2"),
                    "motor_number": entry.get("motorNo"),
                    "motor_rate": entry.get("motorRate2"),
                    "boat_number_motor": entry.get("boatNo"),
                    "boat_rate": entry.get("boatRate2"),
                })

            records.append(rec)
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse program record: {e} | data={race_data}")

    return records


def _normalize_bet_type(raw: str) -> str:
    mapping = {
        "単勝": "win",
        "複勝": "place",
        "2連単": "exacta",
        "2連複": "quinella",
        "拡連複": "quinella_place",
        "3連単": "trifecta",
        "3連複": "trio",
    }
    return mapping.get(raw, raw)


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
