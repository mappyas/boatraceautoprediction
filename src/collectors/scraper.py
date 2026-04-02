"""
boatrace.jp スクレイパー
出走表・直前情報・オッズを取得する。

Playwright を使用（動的ページ対応）。
pip install playwright && playwright install chromium
"""

import asyncio
import time
from datetime import date
from typing import Optional

from loguru import logger

try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("playwright がインストールされていません: pip install playwright && playwright install chromium")


BASE_URL = "https://www.boatrace.jp"
REQUEST_INTERVAL = 3.0


async def _fetch_page(page: Page, url: str, wait_selector: str = None):
    await page.goto(url)
    await asyncio.sleep(REQUEST_INTERVAL)
    if wait_selector:
        await page.wait_for_selector(wait_selector, timeout=15000)
    return await page.content()


async def scrape_race_program(stadium_code: str, race_date: date, race_number: int) -> Optional[dict]:
    """
    出走表を取得する。

    Returns:
        dict with entries list or None on failure
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright が使用できません")
        return None

    date_str = race_date.strftime("%Y%m%d")
    url = (
        f"{BASE_URL}/owpc/pc/race/racelist"
        f"?jcd={stadium_code}&hd={date_str}&rno={race_number}"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(url, timeout=20000)
            await asyncio.sleep(REQUEST_INTERVAL)

            entries = []
            rows = await page.query_selector_all(".is-p3-0")

            for i, row in enumerate(rows[:6], start=1):
                entry = {"boat_number": i}

                # 選手登録番号
                reg_el = await row.query_selector(".is-fs12")
                if reg_el:
                    entry["racer_id"] = (await reg_el.inner_text()).strip()

                # 選手名
                name_el = await row.query_selector(".is-fs18")
                if name_el:
                    entry["racer_name"] = (await name_el.inner_text()).strip()

                # 級別
                grade_el = await row.query_selector(".is-fs14")
                if grade_el:
                    entry["grade"] = (await grade_el.inner_text()).strip()

                entries.append(entry)

            logger.info(f"Scraped program: {stadium_code} {date_str} R{race_number} ({len(entries)} entries)")
            return {"entries": entries}

        except Exception as e:
            logger.error(f"Failed to scrape program {stadium_code} {date_str} R{race_number}: {e}")
            return None
        finally:
            await browser.close()


async def scrape_before_info(stadium_code: str, race_date: date, race_number: int) -> Optional[dict]:
    """
    直前情報（展示タイム・スタート展示）を取得する。
    レース2時間前〜直前に更新される。
    """
    if not PLAYWRIGHT_AVAILABLE:
        return None

    date_str = race_date.strftime("%Y%m%d")
    url = (
        f"{BASE_URL}/owpc/pc/race/beforeinfo"
        f"?jcd={stadium_code}&hd={date_str}&rno={race_number}"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(url, timeout=20000)
            await asyncio.sleep(REQUEST_INTERVAL)

            before_data = {}
            exhibition_times = []
            start_times = []

            # 展示タイム
            ex_rows = await page.query_selector_all(".table1 tbody tr")
            for row in ex_rows[:6]:
                cells = await row.query_selector_all("td")
                if len(cells) >= 3:
                    try:
                        boat_num = int((await cells[0].inner_text()).strip())
                        ex_time_text = (await cells[2].inner_text()).strip()
                        ex_time = float(ex_time_text) if ex_time_text else None
                        exhibition_times.append({"boat_number": boat_num, "exhibition_time": ex_time})
                    except (ValueError, IndexError):
                        pass

            # スタート展示タイム
            st_rows = await page.query_selector_all(".table1-1 tbody tr")
            for row in st_rows[:6]:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    try:
                        boat_num = int((await cells[0].inner_text()).strip())
                        st_text = (await cells[1].inner_text()).strip()
                        st_time = float(st_text) if st_text and st_text not in ("F", "L", "S") else None
                        start_times.append({"boat_number": boat_num, "start_exhibition_time": st_time})
                    except (ValueError, IndexError):
                        pass

            before_data["exhibition_times"] = exhibition_times
            before_data["start_times"] = start_times

            logger.info(f"Scraped before info: {stadium_code} {date_str} R{race_number}")
            return before_data

        except Exception as e:
            logger.error(f"Failed to scrape before info: {e}")
            return None
        finally:
            await browser.close()


async def scrape_odds(stadium_code: str, race_date: date, race_number: int) -> Optional[dict]:
    """
    オッズ（単勝・3連単）を取得する。
    """
    if not PLAYWRIGHT_AVAILABLE:
        return None

    date_str = race_date.strftime("%Y%m%d")
    odds_data = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            # 単勝オッズ
            win_url = (
                f"{BASE_URL}/owpc/pc/race/odds1t"
                f"?jcd={stadium_code}&hd={date_str}&rno={race_number}"
            )
            await page.goto(win_url, timeout=20000)
            await asyncio.sleep(REQUEST_INTERVAL)

            win_odds = {}
            win_rows = await page.query_selector_all(".table1 tbody tr")
            for row in win_rows[:6]:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    try:
                        boat_num = int((await cells[0].inner_text()).strip())
                        odds_text = (await cells[-1].inner_text()).strip()
                        odds_val = float(odds_text) if odds_text else None
                        if odds_val:
                            win_odds[str(boat_num)] = odds_val
                    except (ValueError, IndexError):
                        pass

            odds_data["win"] = win_odds

            # 3連単オッズ
            trifecta_url = (
                f"{BASE_URL}/owpc/pc/race/odds3t"
                f"?jcd={stadium_code}&hd={date_str}&rno={race_number}"
            )
            await page.goto(trifecta_url, timeout=20000)
            await asyncio.sleep(REQUEST_INTERVAL)

            trifecta_odds = {}
            tri_rows = await page.query_selector_all(".table1 tbody tr")
            for row in tri_rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    try:
                        combo_text = (await cells[0].inner_text()).strip().replace("=", "-")
                        odds_text = (await cells[1].inner_text()).strip()
                        odds_val = float(odds_text) if odds_text else None
                        if combo_text and odds_val:
                            trifecta_odds[combo_text] = odds_val
                    except (ValueError, IndexError):
                        pass

            odds_data["trifecta"] = trifecta_odds

            logger.info(f"Scraped odds: {stadium_code} {date_str} R{race_number} "
                        f"(win={len(win_odds)}, trifecta={len(trifecta_odds)})")
            return odds_data

        except Exception as e:
            logger.error(f"Failed to scrape odds: {e}")
            return None
        finally:
            await browser.close()


def get_today_race_list(race_date: date) -> list[dict]:
    """
    本日の開催レース一覧を取得する（同期ラッパー）。
    Returns: [{"stadium_code": "01", "race_count": 12}, ...]
    """
    import requests
    from bs4 import BeautifulSoup

    date_str = race_date.strftime("%Y%m%d")
    url = f"{BASE_URL}/owpc/pc/race/index?hd={date_str}"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        races = []
        stadium_links = soup.select("a[href*='racelist']")
        seen = set()
        for link in stadium_links:
            href = link.get("href", "")
            import re
            m = re.search(r"jcd=(\d{2})", href)
            if m:
                jcd = m.group(1)
                if jcd not in seen:
                    seen.add(jcd)
                    races.append({"stadium_code": jcd, "race_count": 12})

        logger.info(f"Today's races: {len(races)} stadiums")
        return races

    except Exception as e:
        logger.error(f"Failed to get today's race list: {e}")
        return []


# 同期ラッパー
def scrape_before_info_sync(stadium_code: str, race_date: date, race_number: int) -> Optional[dict]:
    return asyncio.run(scrape_before_info(stadium_code, race_date, race_number))


def scrape_odds_sync(stadium_code: str, race_date: date, race_number: int) -> Optional[dict]:
    return asyncio.run(scrape_odds(stadium_code, race_date, race_number))
