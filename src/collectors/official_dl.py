"""
ボートレース公式ダウンロードデータのパーサ
https://www.boatrace.jp/owpc/pc/extra/data/download.html

対応ファイル形式:
  fan*.txt  ... モーターボートファン手帳（選手マスタ・期別成績）
  K*.txt    ... 番組表（出走表）
  B*.txt    ... 競走成績

ファン手帳 固定幅フォーマット（バイト位置・1始まり）:
  1- 4: 登番(4)
  5-20: 名前漢字(16) ※全角含むためバイト数に注意
 21-35: 名前カナ(15)
 36-39: 支部(4)
 40-41: 級(2)
 42   : 年号(1) S=昭和 H=平成 R=令和
 43-48: 生年月日(6) YYMMDD
 49   : 性別(1) 1=男 2=女
 50-51: 年齢(2)
 52-54: 身長(3)
 55-56: 体重(2)  ※kg整数値
 57-58: 血液型(2)
 59-62: 勝率(4)  ※100倍整数 例:0587→5.87
 63-66: 複勝率(4) ※100倍整数 例:0333→33.3
 67-69: 1着回数(3)
 70-72: 2着回数(3)
 73-75: 出走回数(3)
 76-77: 優出回数(2)
 78-79: 優勝回数(2)
 80-82: 平均ST(3)  ※1000倍整数 例:017→0.17
"""

from pathlib import Path
from typing import Optional
from loguru import logger


def _get_bytes(data: bytes, start: int, length: int) -> bytes:
    """1-indexed バイト位置からスライス"""
    return data[start - 1: start - 1 + length]


def _decode(data: bytes, start: int, length: int) -> str:
    return _get_bytes(data, start, length).decode("cp932", errors="replace").strip()


def _int(data: bytes, start: int, length: int) -> Optional[int]:
    s = _decode(data, start, length)
    try:
        return int(s)
    except ValueError:
        return None


def _float_div(data: bytes, start: int, length: int, divisor: float) -> Optional[float]:
    v = _int(data, start, length)
    return round(v / divisor, 4) if v is not None else None


def parse_fan_file(filepath: str | Path) -> list[dict]:
    """
    ファン手帳ファイル (fan*.txt) を解析し、選手情報リストを返す。

    Returns:
        list of {
            racer_id, name, branch, grade,
            weight, height, national_win_rate,
            national_place2_rate, national_1st_count,
            national_2nd_count, national_race_count,
            fly_count(優出回数), late_count(優勝回数), avg_st
        }
    """
    filepath = Path(filepath)
    records = []

    try:
        with open(filepath, "rb") as f:
            raw_lines = f.readlines()
    except OSError as e:
        logger.error(f"Failed to open {filepath}: {e}")
        return []

    for i, raw in enumerate(raw_lines):
        raw = raw.rstrip(b"\r\n")
        if len(raw) < 82:
            continue

        try:
            racer_id = _decode(raw, 1, 4)
            if not racer_id.isdigit():
                continue  # ヘッダ行等をスキップ

            name = _decode(raw, 5, 16)
            branch = _decode(raw, 36, 4)
            grade = _decode(raw, 40, 2)
            age = _int(raw, 50, 2)
            height = _int(raw, 52, 3)
            weight = _int(raw, 55, 2)
            # 勝率・複勝率: 100倍整数
            win_rate = _float_div(raw, 59, 4, 100)
            place2_rate = _float_div(raw, 63, 4, 100)
            count_1st = _int(raw, 67, 3)
            count_2nd = _int(raw, 70, 3)
            race_count = _int(raw, 73, 3)
            yushutsu = _int(raw, 76, 2)   # 優出回数
            yusho = _int(raw, 78, 2)      # 優勝回数
            avg_st = _float_div(raw, 80, 3, 100)

            records.append({
                "racer_id": racer_id,
                "name": name,
                "branch": branch,
                "grade": grade if grade in ("A1", "A2", "B1", "B2") else "B1",
                "height": height,
                "weight": weight,
                "national_win_rate": win_rate,
                "national_place2_rate": place2_rate,
                "national_place3_rate": None,
                "national_1st_count": count_1st,
                "national_2nd_count": count_2nd,
                "national_race_count": race_count,
                "fly_count": yushutsu or 0,
                "late_count": yusho or 0,
                "avg_st": avg_st,
            })

        except Exception as e:
            logger.debug(f"Skip line {i+1}: {e}")
            continue

    logger.info(f"Parsed {len(records)} racers from {filepath.name}")
    return records


def ingest_fan_files(raw_dir: str = "data/raw", db_path: str = "data/db/boatrace.db"):
    """
    data/raw/ 以下の fan*.txt を全て読み込み、racersテーブルに投入する。
    同一登番は最新期のデータで上書きする。
    """
    from src.db.models import Racer, init_db
    from src.db.repository import get_session_factory, session_scope, RacerRepository

    raw_dir = Path(raw_dir)
    fan_files = sorted(raw_dir.glob("fan*.txt"))

    if not fan_files:
        logger.warning(f"No fan*.txt files found in {raw_dir}")
        return 0

    logger.info(f"Found {len(fan_files)} fan files: {[f.name for f in fan_files]}")

    init_db(db_path)
    session_factory = get_session_factory(db_path)

    total = 0
    for fan_file in fan_files:
        records = parse_fan_file(fan_file)

        with session_scope(session_factory) as session:
            repo = RacerRepository(session)
            for rec in records:
                racer = Racer(
                    racer_id=rec["racer_id"],
                    name=rec["name"],
                    branch=rec["branch"],
                    grade=rec["grade"],
                    weight=float(rec["weight"]) if rec["weight"] else None,
                    national_win_rate=rec["national_win_rate"],
                    national_place2_rate=rec["national_place2_rate"],
                    national_place3_rate=rec["national_place3_rate"],
                    fly_count=rec["fly_count"],
                    late_count=rec["late_count"],
                )
                repo.upsert(racer)
            total += len(records)

        logger.info(f"Ingested {len(records)} racers from {fan_file.name}")

    logger.info(f"Total racers ingested: {total}")
    return total
