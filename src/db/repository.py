"""
DB CRUD操作
"""

from contextlib import contextmanager
from datetime import date
from typing import Optional
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import select

from src.db.models import (
    Base, Race, Racer, RaceEntry, RaceResult, Odds, Prediction,
    BettingRecord, get_engine
)


def get_session_factory(db_path: str = "data/db/boatrace.db"):
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@contextmanager
def session_scope(session_factory):
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class RaceRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert(self, race: Race) -> Race:
        existing = self.session.get(Race, race.race_id)
        if existing:
            for col in Race.__table__.columns:
                if col.name != "race_id":
                    val = getattr(race, col.name)
                    if val is not None:
                        setattr(existing, col.name, val)
            return existing
        self.session.add(race)
        return race

    def get_by_date(self, race_date: date) -> list[Race]:
        return self.session.execute(
            select(Race).where(Race.race_date == race_date)
        ).scalars().all()

    def get_by_id(self, race_id: str) -> Optional[Race]:
        return self.session.get(Race, race_id)


class RacerRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert(self, racer: Racer) -> Racer:
        existing = self.session.get(Racer, racer.racer_id)
        if existing:
            for col in Racer.__table__.columns:
                if col.name != "racer_id":
                    val = getattr(racer, col.name)
                    if val is not None:
                        setattr(existing, col.name, val)
            return existing
        self.session.add(racer)
        return racer

    def get_by_id(self, racer_id: str) -> Optional[Racer]:
        return self.session.get(Racer, racer_id)


class RaceEntryRepository:
    def __init__(self, session: Session):
        self.session = session

    def bulk_upsert(self, entries: list[RaceEntry]):
        for entry in entries:
            existing = self.session.execute(
                select(RaceEntry).where(
                    RaceEntry.race_id == entry.race_id,
                    RaceEntry.boat_number == entry.boat_number,
                )
            ).scalar_one_or_none()
            if existing:
                for col in RaceEntry.__table__.columns:
                    if col.name not in ("entry_id", "race_id", "boat_number"):
                        val = getattr(entry, col.name)
                        if val is not None:
                            setattr(existing, col.name, val)
            else:
                self.session.add(entry)

    def get_by_race(self, race_id: str) -> list[RaceEntry]:
        return self.session.execute(
            select(RaceEntry).where(RaceEntry.race_id == race_id)
        ).scalars().all()


class RaceResultRepository:
    def __init__(self, session: Session):
        self.session = session

    def bulk_upsert(self, results: list[RaceResult]):
        for result in results:
            existing = self.session.execute(
                select(RaceResult).where(
                    RaceResult.race_id == result.race_id,
                    RaceResult.boat_number == result.boat_number,
                )
            ).scalar_one_or_none()
            if existing:
                for col in RaceResult.__table__.columns:
                    if col.name not in ("result_id", "race_id", "boat_number"):
                        val = getattr(result, col.name)
                        if val is not None:
                            setattr(existing, col.name, val)
            else:
                self.session.add(result)

    def get_by_race(self, race_id: str) -> list[RaceResult]:
        return self.session.execute(
            select(RaceResult).where(RaceResult.race_id == race_id)
        ).scalars().all()


class OddsRepository:
    def __init__(self, session: Session):
        self.session = session

    def bulk_insert(self, odds_list: list[Odds]):
        self.session.add_all(odds_list)

    def get_latest_by_race(self, race_id: str, bet_type: str) -> list[Odds]:
        from sqlalchemy import func
        subq = (
            select(func.max(Odds.recorded_at))
            .where(Odds.race_id == race_id, Odds.bet_type == bet_type)
            .scalar_subquery()
        )
        return self.session.execute(
            select(Odds).where(
                Odds.race_id == race_id,
                Odds.bet_type == bet_type,
                Odds.recorded_at == subq,
            )
        ).scalars().all()


class PredictionRepository:
    def __init__(self, session: Session):
        self.session = session

    def bulk_insert(self, predictions: list[Prediction]):
        self.session.add_all(predictions)

    def get_by_race(self, race_id: str) -> list[Prediction]:
        return self.session.execute(
            select(Prediction).where(Prediction.race_id == race_id)
        ).scalars().all()


class BettingRecordRepository:
    def __init__(self, session: Session):
        self.session = session

    def insert(self, record: BettingRecord) -> BettingRecord:
        self.session.add(record)
        return record

    def get_all(self) -> list[BettingRecord]:
        return self.session.execute(select(BettingRecord)).scalars().all()
