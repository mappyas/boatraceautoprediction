"""
Open-Meteo API クライアント
各競艇場の気象データ（気温・風速・風向・波高）を取得する。
https://open-meteo.com/
"""

import requests
from datetime import date, datetime
from loguru import logger

# 各競艇場の緯度経度
STADIUM_LOCATIONS = {
    "01": (36.414, 139.232, "桐生"),
    "02": (35.843, 139.666, "戸田"),
    "03": (35.672, 139.877, "江戸川"),
    "04": (35.582, 139.746, "平和島"),
    "05": (35.589, 139.488, "多摩川"),
    "06": (34.732, 137.717, "浜名湖"),
    "07": (34.832, 137.216, "蒲郡"),
    "08": (35.003, 136.864, "常滑"),
    "09": (34.714, 136.513, "津"),
    "10": (36.186, 136.140, "三国"),
    "11": (35.125, 135.996, "びわこ"),
    "12": (34.650, 135.507, "住之江"),
    "13": (34.723, 135.409, "尼崎"),
    "14": (34.165, 134.516, "鳴門"),
    "15": (34.304, 133.836, "丸亀"),
    "16": (34.498, 133.995, "児島"),
    "17": (34.344, 132.459, "宮島"),
    "18": (33.960, 131.865, "徳山"),
    "19": (33.946, 130.956, "下関"),
    "20": (33.903, 130.783, "若松"),
    "21": (33.897, 130.673, "芦屋"),
    "22": (33.606, 130.401, "福岡"),
    "23": (33.440, 129.967, "唐津"),
    "24": (32.915, 129.869, "大村"),
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(stadium_code: str, target_date: date = None) -> dict:
    """
    指定競艇場の気象データを取得する。

    Returns:
        {
            "temperature": float,   # 気温（°C）
            "wind_speed": float,    # 風速（m/s）
            "wind_direction": int,  # 風向（度）
            "precipitation": float, # 降水量（mm）
        }
    """
    if stadium_code not in STADIUM_LOCATIONS:
        logger.warning(f"Unknown stadium code: {stadium_code}")
        return {}

    lat, lon, name = STADIUM_LOCATIONS[stadium_code]
    if target_date is None:
        target_date = date.today()

    date_str = target_date.isoformat()

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation",
        "start_date": date_str,
        "end_date": date_str,
        "wind_speed_unit": "ms",
        "timezone": "Asia/Tokyo",
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        # 12時前後（レース開催時間帯）の平均を使う
        noon_indices = [i for i, t in enumerate(times) if "12:" in t or "13:" in t or "14:" in t]
        if not noon_indices:
            noon_indices = list(range(len(times)))

        def avg(key):
            vals = [hourly[key][i] for i in noon_indices if hourly.get(key) and i < len(hourly[key])]
            return sum(vals) / len(vals) if vals else None

        result = {
            "temperature": avg("temperature_2m"),
            "wind_speed": avg("wind_speed_10m"),
            "wind_direction_deg": avg("wind_direction_10m"),
            "precipitation": avg("precipitation"),
        }

        # 風向を8方向コードに変換
        if result["wind_direction_deg"] is not None:
            result["wind_direction"] = _deg_to_8dir(result["wind_direction_deg"])

        logger.info(f"Weather for {name}({stadium_code}): {result}")
        return result

    except requests.RequestException as e:
        logger.error(f"Failed to fetch weather for {stadium_code}: {e}")
        return {}


def _deg_to_8dir(deg: float) -> int:
    """角度（0-360）を8方向コード（1=北〜8=北西）に変換"""
    dirs = [1, 2, 3, 4, 5, 6, 7, 8]
    boundaries = [22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5]
    for i, boundary in enumerate(boundaries):
        if deg < boundary:
            return dirs[i]
    return 1  # 337.5〜360 → 北
