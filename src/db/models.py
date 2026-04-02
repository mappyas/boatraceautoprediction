"""
SQLAlchemy ORM モデル定義
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Text,
    ForeignKey, UniqueConstraint, create_engine
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Race(Base):
    """レース基本情報"""
    __tablename__ = "races"

    race_id = Column(String, primary_key=True)  # {場コード2桁}{日付8桁}{レース番号2桁}
    stadium_code = Column(String(2), nullable=False)
    race_date = Column(Date, nullable=False)
    race_number = Column(Integer, nullable=False)
    weather = Column(String(20))
    temperature = Column(Float)
    water_temperature = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(Integer)
    wave_height = Column(Integer)

    entries = relationship("RaceEntry", back_populates="race", cascade="all, delete-orphan")
    results = relationship("RaceResult", back_populates="race", cascade="all, delete-orphan")
    odds = relationship("Odds", back_populates="race", cascade="all, delete-orphan")
    predictions = relationship("Prediction", back_populates="race", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("stadium_code", "race_date", "race_number", name="uq_race"),
    )


class Racer(Base):
    """選手マスタ"""
    __tablename__ = "racers"

    racer_id = Column(String(10), primary_key=True)
    name = Column(String(20), nullable=False)
    branch = Column(String(10))
    birth_date = Column(Date)
    weight = Column(Float)
    height = Column(Float)
    grade = Column(String(5))  # A1/A2/B1/B2
    national_win_rate = Column(Float)
    national_place2_rate = Column(Float)
    national_place3_rate = Column(Float)
    local_win_rate = Column(Float)
    local_place2_rate = Column(Float)
    fly_count = Column(Integer, default=0)
    late_count = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    entries = relationship("RaceEntry", back_populates="racer")


class RaceEntry(Base):
    """出走情報（1レース×6艇分）"""
    __tablename__ = "race_entries"

    entry_id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey("races.race_id"), nullable=False)
    boat_number = Column(Integer, nullable=False)  # 艇番 1〜6
    racer_id = Column(String(10), ForeignKey("racers.racer_id"), nullable=False)
    course = Column(Integer)  # 進入コース
    motor_number = Column(Integer)
    motor_rate = Column(Float)   # モーター2連率
    boat_number_motor = Column(Integer)
    boat_rate = Column(Float)    # ボート2連率
    exhibition_time = Column(Float)  # 展示タイム
    start_exhibition_time = Column(Float)  # スタート展示タイム

    race = relationship("Race", back_populates="entries")
    racer = relationship("Racer", back_populates="entries")

    __table_args__ = (
        UniqueConstraint("race_id", "boat_number", name="uq_entry"),
    )


class RaceResult(Base):
    """レース結果"""
    __tablename__ = "race_results"

    result_id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey("races.race_id"), nullable=False)
    boat_number = Column(Integer, nullable=False)
    arrival = Column(Integer)       # 着順（失格等はNone）
    start_timing = Column(Float)    # ST（マイナス=フライング）
    race_time = Column(Float)       # レースタイム（秒）
    winning_trick = Column(String(20))  # 決まり手

    race = relationship("Race", back_populates="results")

    __table_args__ = (
        UniqueConstraint("race_id", "boat_number", name="uq_result"),
    )


class Odds(Base):
    """オッズ"""
    __tablename__ = "odds"

    odds_id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey("races.race_id"), nullable=False)
    bet_type = Column(String(20), nullable=False)  # win/quinella/exacta/trifecta/trio
    combination = Column(String(20), nullable=False)  # "1-2-3" 形式
    odds = Column(Float, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    race = relationship("Race", back_populates="odds")

    __table_args__ = (
        UniqueConstraint("race_id", "bet_type", "combination", "recorded_at", name="uq_odds"),
    )


class Prediction(Base):
    """予測ログ"""
    __tablename__ = "predictions"

    prediction_id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey("races.race_id"), nullable=False)
    boat_number = Column(Integer, nullable=False)
    predicted_prob = Column(Float)   # 予測1着確率
    expected_value = Column(Float)   # 期待値
    model_version = Column(String(50))
    predicted_at = Column(DateTime, default=datetime.utcnow)

    race = relationship("Race", back_populates="predictions")


class BettingRecord(Base):
    """投票・収益記録"""
    __tablename__ = "betting_records"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey("races.race_id"), nullable=False)
    bet_type = Column(String(20), nullable=False)
    combination = Column(String(20), nullable=False)
    amount = Column(Integer, nullable=False)   # 投票金額（円）
    odds_at_bet = Column(Float)               # 投票時オッズ
    is_hit = Column(Integer, default=0)        # 的中フラグ（0/1）
    payout = Column(Integer, default=0)        # 払戻金額
    bet_at = Column(DateTime, default=datetime.utcnow)
    note = Column(Text)


def get_engine(db_path: str = "data/db/boatrace.db"):
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(db_path: str = "data/db/boatrace.db"):
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    return engine
