"""
ボートレース公式ダウンロードデータのパーサ
https://www.boatrace.jp/owpc/pc/extra/data/download.html

ファイル形式：
- K{YYYYMMDD}.TXT  → 番組表（出走表）
- B{YYYYMMDD}.TXT  → 競走成績
"""

import re
from datetime import date
from pathlib import Path
from typing import Optional
from loguru import logger


# 競艇場コードマッピング（名前→2桁コード）
STADIUM_NAME_TO_CODE = {
    "桐生": "01", "戸田": "02", "江戸川": "03", "平和島": "04",
    "多摩川": "05", "浜名湖": "06", "蒲郡": "07", "常滑": "08",
    "津": "09", "三国": "10", "びわこ": "11", "住之江": "12",
    "尼崎": "13", "鳴門": "14", "丸亀": "15", "児島": "16",
    "宮島": "17", "徳山": "18", "下関": "19", "若松": "20",
    "芦屋": "21", "福岡": "22", "唐津": "23", "大村": "24",
}

WEATHER_MAP = {
    "1": "晴", "2": "曇", "3": "雨", "4": "霧", "5": "雪",
}

WIND_DIR_MAP = {
    "1": "北", "2": "北東", "3": "東", "4": "南東",
    "5": "南", "6": "南西", "7": "西", "8": "北西",
}

GRADE_MAP = {
    "A1": "A1", "A2": "A2", "B1": "B1", "B2": "B2",
    "1": "A1", "2": "A2", "3": "B1", "4": "B2",
}


def _safe_float(s: str) -> Optional[float]:
    s = s.strip()
    if not s or s in (".", "―", "-", "F", "L", "S", "0.00"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _safe_int(s: str) -> Optional[int]:
    s = s.strip()
    if not s or s in (".", "―", "-"):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_program_file(filepath: str | Path) -> list[dict]:
    """
    番組表ファイル (K{YYYYMMDD}.TXT) を解析し、レース・選手エントリ情報を返す。
    """
    filepath = Path(filepath)
    records = []

    try:
        with open(filepath, encoding="cp932", errors="replace") as f:
            lines = f.readlines()
    except OSError as e:
        logger.error(f"Failed to open {filepath}: {e}")
        return []

    i = 0
    current_race = None

    while i < len(lines):
        line = lines[i].rstrip("\n")

        # レースヘッダ行（場コード、日付、レース番号が含まれる行を検出）
        if line.startswith("BBGN"):
            # 新しいレースブロック開始
            current_race = None
            i += 1
            continue

        # 場・日付・レース番ヘッダを解析（固定幅フォーマット）
        # 先頭に 場コード2桁 がある行
        m = re.match(r"^(\d{2})(\d{8})(\d{2})", line)
        if m and current_race is None:
            stadium_code = m.group(1)
            date_str = m.group(2)
            race_number = int(m.group(3))
            race_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            race_id = f"{stadium_code}{date_str}{str(race_number).zfill(2)}"
            current_race = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": race_date,
                "race_number": race_number,
                "entries": [],
            }
            records.append(current_race)
            i += 1
            continue

        # 選手エントリ行（艇番1〜6）
        if current_race is not None:
            m_entry = re.match(r"^([1-6])\s+(\d{4})\s+(\S+)", line)
            if m_entry:
                boat_number = int(m_entry.group(1))
                racer_id = m_entry.group(2)
                racer_name = m_entry.group(3)
                # 残りフィールドは位置依存で取得（簡易実装）
                parts = line.split()
                entry = {
                    "boat_number": boat_number,
                    "racer_id": racer_id,
                    "racer_name": racer_name,
                    "motor_number": _safe_int(parts[6]) if len(parts) > 6 else None,
                    "motor_rate": _safe_float(parts[7]) if len(parts) > 7 else None,
                    "boat_number_motor": _safe_int(parts[8]) if len(parts) > 8 else None,
                    "boat_rate": _safe_float(parts[9]) if len(parts) > 9 else None,
                }
                current_race["entries"].append(entry)

        i += 1

    logger.info(f"Parsed {len(records)} races from {filepath}")
    return records


def parse_result_file(filepath: str | Path) -> list[dict]:
    """
    競走成績ファイル (B{YYYYMMDD}.TXT) を解析し、レース結果を返す。
    """
    filepath = Path(filepath)
    records = []

    try:
        with open(filepath, encoding="cp932", errors="replace") as f:
            content = f.read()
    except OSError as e:
        logger.error(f"Failed to open {filepath}: {e}")
        return []

    # レースブロックをBBGNで分割
    blocks = re.split(r"BBGN", content)

    for block in blocks:
        if not block.strip():
            continue

        lines = block.strip().split("\n")
        if len(lines) < 5:
            continue

        try:
            # 1行目：場コード・日付・レース番
            header = lines[0].strip()
            m = re.match(r"(\d{2})(\d{8})(\d{2})", header)
            if not m:
                continue

            stadium_code = m.group(1)
            date_str = m.group(2)
            race_number = int(m.group(3))
            race_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            race_id = f"{stadium_code}{date_str}{str(race_number).zfill(2)}"

            rec = {
                "race_id": race_id,
                "stadium_code": stadium_code,
                "race_date": race_date,
                "race_number": race_number,
                "weather": None,
                "temperature": None,
                "water_temperature": None,
                "wind_speed": None,
                "wind_direction": None,
                "wave_height": None,
                "results": [],
                "payouts": [],
            }

            # 天候・気温・水温・風向風速・波高を探す
            for line in lines:
                m_env = re.search(
                    r"天候(\d)\s*気温([\d.]+)\s*水温([\d.]+)\s*風(\d+)\s*([\d.]+)\s*波(\d+)",
                    line
                )
                if m_env:
                    rec["weather"] = WEATHER_MAP.get(m_env.group(1), m_env.group(1))
                    rec["temperature"] = _safe_float(m_env.group(2))
                    rec["water_temperature"] = _safe_float(m_env.group(3))
                    rec["wind_direction"] = _safe_int(m_env.group(4))
                    rec["wind_speed"] = _safe_float(m_env.group(5))
                    rec["wave_height"] = _safe_int(m_env.group(6))
                    break

            # 着順・STタイム
            for line in lines:
                m_result = re.match(
                    r"\s*([1-6])\s+([1-6])\s+(\d{4})\s+(\S+)\s+([-F\d.]+)\s+([\d:'.]+)?",
                    line
                )
                if m_result:
                    arrival = int(m_result.group(1))
                    boat_number = int(m_result.group(2))
                    st_raw = m_result.group(5)
                    # STの正規化
                    if st_raw.startswith("F"):
                        st = -float(st_raw[1:]) if len(st_raw) > 1 else -0.001
                    elif st_raw.startswith("L"):
                        st = None
                    else:
                        st = _safe_float(st_raw)

                    rec["results"].append({
                        "boat_number": boat_number,
                        "arrival": arrival,
                        "start_timing": st,
                        "race_time": None,
                        "winning_trick": None,
                    })

            # 払戻
            payout_patterns = [
                ("trifecta", r"3連単\s+([\d-]+)\s+([\d,]+)"),
                ("trio", r"3連複\s+([\d-]+)\s+([\d,]+)"),
                ("exacta", r"2連単\s+([\d-]+)\s+([\d,]+)"),
                ("quinella", r"2連複\s+([\d-]+)\s+([\d,]+)"),
                ("win", r"単勝\s+(\d+)\s+([\d,]+)"),
            ]
            for line in lines:
                for bet_type, pattern in payout_patterns:
                    m_pay = re.search(pattern, line)
                    if m_pay:
                        combo = m_pay.group(1).replace("=", "-")
                        payout = int(m_pay.group(2).replace(",", ""))
                        rec["payouts"].append({
                            "bet_type": bet_type,
                            "combination": combo,
                            "payout": payout,
                        })

            records.append(rec)

        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse block: {e}")
            continue

    logger.info(f"Parsed {len(records)} results from {filepath}")
    return records
