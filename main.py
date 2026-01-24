import asyncio
import logging
import os
import re
import sqlite3
import random
from datetime import datetime
from typing import Optional, List, Dict, Tuple

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from playwright.async_api import async_playwright, Error as PlaywrightError
from bs4 import BeautifulSoup

# ================= CONFIG =================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

EXCEL_PATH = os.getenv("EXCEL_PATH")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 600))

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free")

QUERY = os.getenv("AVITO_QUERY", "iphone")
SCAN_PAGES = int(os.getenv("SCAN_PAGES", 3))

PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "0").strip() in ("1", "true", "True")
AVITO_SESSION_DIR = os.getenv("AVITO_SESSION_DIR", "avito_session_final")
DB_PATH = os.getenv("DB_PATH", "sent.db")

# ✅ Жёстко только Москва и МО
AVITO_REGION = "moskva_i_mo"

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("AVITO")


def escape_html(s: Optional[str]) -> str:
    """Экранирует HTML для Telegram parse_mode=HTML."""
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ================= DATABASE =================
class Storage:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS sent (id TEXT PRIMARY KEY)")

    def is_sent(self, item_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM sent WHERE id=?", (item_id,))
        return cur.fetchone() is not None

    def mark_sent(self, item_id: str):
        self.conn.execute("INSERT OR IGNORE INTO sent VALUES (?)", (item_id,))
        self.conn.commit()


# ================= PRICE REF =================
class PriceReference:
    def __init__(self, path: str):
        self.rows = []
        if not path or not os.path.exists(path):
            logger.error(f"❌ Excel не найден: {path}")
            return

        try:
            df = pd.read_excel(path, sheet_name="Сводный отчет", header=1)
            df.columns = [str(c).lower().strip() for c in df.columns]

            for _, r in df.iterrows():
                mean = r.get("mean")
                model = str(r.get("модель", "")).lower().strip()
                memory = str(r.get("память", "")).lower().strip()
                if model and memory and mean:
                    self.rows.append({"model": model, "memory": memory, "mean": float(mean)})

            logger.info(f"📊 Загружено цен: {len(self.rows)}")
        except Exception as e:
            logger.error(f"❌ Ошибка Excel: {e}")
            self.rows = []

    def extract_memory(self, text: str) -> Optional[str]:
        t = text.lower()
        m = re.search(r"(\d{2,4})\s*(gb|гб|tb|тб)\b", t)
        if m:
            val, unit = m.group(1), m.group(2)
            if val == "1024" or unit in ("tb", "тб"):
                return "1tb"
            return f"{val}gb"

        if re.search(r"\b1\s*(tb|тб)\b", t):
            return "1tb"

        m2 = re.search(r"(64|128|256|512|1024|1\s?тб|1tb)", t)
        if not m2:
            return None

        v = m2.group(1).replace(" ", "")
        return "1tb" if v in ("1024", "1тб", "1tb") else f"{v}gb"

    def find_price(self, title: str, description: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        text = f"{title} {description}".lower()
        mem = self.extract_memory(text)
        if not mem:
            return None, None, None

        for r in sorted(self.rows, key=lambda x: len(x["model"]), reverse=True):
            if r["model"] in text and r["memory"] == mem:
                return r["mean"], r["model"], mem

        return None, None, mem


# ================= AI =================
class AIAnalyzer:
    async def analyze(self, item: Dict, avg_price: Optional[float]) -> str:
        if not OPENROUTER_API_KEY:
            return "🤖 AI: OPENROUTER_API_KEY не задан"

        # Мы передаём цену и рынок, но запрещаем "рубить" вердикт только по цене.
        avg_text = f"{int(avg_price)} ₽" if avg_price else "н/д"
        diff_text = "н/д"
        if avg_price and avg_price > 0:
            diff = round((item["price"] - avg_price) / avg_price * 100, 1)  # + если выше рынка
            diff_text = f"{diff:+.1f}%"

        prompt = f"""
Ты — эксперт по проверке объявлений Avito про iPhone.
Твоя задача — дать решение о покупке, учитывая состояние, риски и цену относительно рынка.

ОГРАНИЧЕНИЯ:
- Никакой воды и общих советов.
- Только факты, которые прямо указаны в объявлении.
- Нельзя писать "возможно/скорее всего" и придумывать факты.
- Нельзя делать вердикт ТОЛЬКО на основании цены.
  Если состояние подтверждено (без ремонта/вскрытия, без дефектов, АКБ, комплект),
  допустимо рекомендовать покупку даже если цена немного выше рынка.

Дано:
Название: {item['title']}
Цена: {item['price']} ₽
Средняя цена по рынку (Excel): {avg_text}
Отклонение от рынка: {diff_text}
Текст объявления: {item.get('description','')}

Ответь СТРОГО в 4 строках:

✅ Плюсы: <факты>
⚠️ Минусы: <факты/риски>
🏁 Вердикт: <ПОКУПАТЬ или НЕ ПОКУПАТЬ> — <1 короткая причина на основе плюсов/минусов и цены>
📌 Комментарий по цене: <1 короткая строка: "ниже рынка/в рынке/чуть выше рынка оправдано состоянием/выше рынка не оправдано">

ПРАВИЛА:
- "НЕ ПОКУПАТЬ" при критических рисках: iCloud/залочен/нет доступа/вскрывался/ремонт/восстановлен/неоригинал/подмены/нет информации.
- "ПОКУПАТЬ" если нет критики и в тексте есть сильные признаки хорошего состояния (без дефектов, без ремонта/вскрытия, АКБ указано, комплект).
- По цене:
  - если цена немного выше рынка (до ~5%) и состояние подтверждено — это может быть ОК.
  - если цена выше рынка заметно и нет сильных плюсов — не рекомендовать.
"""

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": OPENROUTER_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=25,
                ) as r:
                    data = await r.json()
                    txt = data["choices"][0]["message"]["content"]
                    return escape_html(txt)
        except Exception as e:
            logger.warning(f"[AI] Ошибка: {e}")
            return "⚠️ AI временно недоступен"


# ================= PARSER =================
class AvitoParser:
    def __init__(self):
        self.session_path = os.path.abspath(AVITO_SESSION_DIR)

    def _extract_real_item_id(self, card: BeautifulSoup, url_item: str) -> Optional[str]:
        did = card.get("data-item-id")
        if did and re.fullmatch(r"\d+", str(did)):
            return str(did)

        m = re.search(r"_(\d{6,})", url_item)
        if m:
            return m.group(1)

        m2 = re.search(r"(\d{6,})", url_item)
        if m2:
            return m2.group(1)

        return None

    def _extract_location(self, card: BeautifulSoup) -> str:
        addr = card.select_one('[data-marker="item-address"]')
        if addr:
            return addr.get_text(" ", strip=True)
        return ""

    def _is_moscow_mo(self, location: str) -> bool:
        # регион уже moskva_i_mo, но оставим страховку на случай странностей
        if not location:
            return True
        l = location.lower()
        return ("москва" in l) or ("москов" in l) or (re.search(r"\bмо\b", l) is not None)

    async def fetch(self, pages: int, bot: Bot) -> List[Dict]:
        items: List[Dict] = []
        async with async_playwright() as p:
            context = None
            try:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=self.session_path,
                    headless=PLAYWRIGHT_HEADLESS,
                    args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                )
                page = context.pages[0] if context.pages else await context.new_page()
                page.set_default_timeout(60000)

                for p_num in range(1, pages + 1):
                    # ✅ сортировка по дате размещения
                    url = f"https://www.avito.ru/{AVITO_REGION}?q={QUERY}&s=104&p={p_num}"
                    logger.info(f"[PARSER] Сканирую страницу {p_num}/{pages} ...")

                    if p_num > 1:
                        await asyncio.sleep(random.uniform(2, 4))

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(2.5)

                    if await page.query_selector('div[id*="captcha"], #firewall'):
                        logger.warning("🚨 Avito показал проверку/капчу. Реши её в открытом окне браузера...")
                        try:
                            await bot.send_message(
                                ADMIN_ID,
                                "🚨 Капча/проверка Avito. Реши её в браузере, я жду карточки (до 5 минут).",
                            )
                        except:
                            pass
                        await page.wait_for_selector('[data-marker="item"]', timeout=300000)

                    try:
                        await page.wait_for_selector('[data-marker="item"]', timeout=15000)
                    except:
                        logger.warning("[PARSER] items selector not found (timeout)")
                        continue

                    soup = BeautifulSoup(await page.content(), "html.parser")
                    cards = soup.select('[data-marker="item"]')
                    if not cards:
                        logger.warning("[PARSER] Карточек не найдено на странице")
                        continue

                    for c in cards:
                        try:
                            title_el = c.select_one('[itemprop="name"]') or c.select_one("h3")
                            price_el = c.select_one('meta[itemprop="price"]') or c.select_one('[itemprop="price"]')
                            link_el = c.select_one('a[itemprop="url"]') or c.select_one("a[href]")

                            if not title_el or not link_el:
                                continue

                            title = title_el.get_text(strip=True)
                            href = link_el.get("href", "")
                            if not href:
                                continue
                            url_item = href if href.startswith("http") else ("https://www.avito.ru" + href)

                            item_id = self._extract_real_item_id(c, url_item)
                            if not item_id:
                                continue

                            price = 0
                            if price_el and price_el.get("content"):
                                price = int(price_el["content"])
                            if not (5000 < price < 600000):
                                continue

                            location = self._extract_location(c)
                            if not self._is_moscow_mo(location):
                                continue

                            items.append(
                                {
                                    "id": item_id,
                                    "title": title,
                                    "price": price,
                                    "url": url_item,
                                    "description": c.get_text(" ", strip=True),
                                    "location": location,
                                }
                            )
                        except:
                            continue

            except PlaywrightError as e:
                logger.error(f"🛑 Playwright error: {e}")
            finally:
                if context:
                    await context.close()

        return items


# ================= BOT =================
class MonitorBot:
    def __init__(self):
        self.bot = Bot(BOT_TOKEN)
        self.dp = Dispatcher()

        self.parser = AvitoParser()
        self.prices = PriceReference(EXCEL_PATH)
        self.ai = AIAnalyzer()
        self.db = Storage(DB_PATH)

        self.is_paused = False
        self.mode_all = False  # False=выгодные, True=все новые

        self.force_search_event: Optional[asyncio.Event] = None
        self.last_manual = False

        self._register_handlers()

    def keyboard(self) -> ReplyKeyboardMarkup:
        mode = "📦 ВСЕ НОВЫЕ" if self.mode_all else "🔥 ВЫГОДНЫЕ"
        pause = "▶ Активен" if not self.is_paused else "💤 Пауза"
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔎 Найти сейчас"), KeyboardButton(text="⚙️ Настройки")],
                [KeyboardButton(text=mode)],
                [KeyboardButton(text=pause)],
            ],
            resize_keyboard=True,
        )

    def _register_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(m: Message):
            await m.answer("📱 Мониторинг запущен!", reply_markup=self.keyboard())

        @self.dp.message(F.text == "⚙️ Настройки")
        async def settings(m: Message):
            mode_str = "ВСЕ НОВЫЕ" if self.mode_all else "ВЫГОДНЫЕ (price <= avg*1.2)"
            status_str = "Пауза 💤" if self.is_paused else "Работает ▶"
            now = datetime.now()

            msg = (
                "⚙️ <b>Текущие настройки</b>\n\n"
                f"Статус: <code>{escape_html(status_str)}</code>\n"
                f"Режим: <code>{escape_html(mode_str)}</code>\n"
                f"Регион Avito: <code>Москва и МО</code>\n"
                f"Сортировка Avito: <code>s=104 (по дате)</code>\n"
                f"Скан страниц: <code>{SCAN_PAGES}</code>\n"
                f"Интервал проверки: <code>{CHECK_INTERVAL} сек</code>\n"
                f"Headless: <code>{'ON' if PLAYWRIGHT_HEADLESS else 'OFF'}</code>\n"
                f"Время сервера: <code>{now.strftime('%H:%M:%S')}</code>\n"
            )
            await m.answer(msg, parse_mode="HTML", reply_markup=self.keyboard())

        @self.dp.message(F.text == "🔎 Найти сейчас")
        async def manual(m: Message):
            if not self.force_search_event:
                await m.answer("⚠️ Бот ещё загружается...")
                return
            self.last_manual = True
            self.force_search_event.set()
            await m.answer("⏳ Запускаю внеочередную проверку...", reply_markup=self.keyboard())

        @self.dp.message(F.text.in_(["📦 ВСЕ НОВЫЕ", "🔥 ВЫГОДНЫЕ"]))
        async def toggle_mode(m: Message):
            self.mode_all = not self.mode_all
            await m.answer("🔄 Режим переключён", reply_markup=self.keyboard())

        @self.dp.message(F.text.in_(["▶ Активен", "💤 Пауза"]))
        async def toggle_pause(m: Message):
            self.is_paused = not self.is_paused
            await m.answer("⏯ Статус переключён", reply_markup=self.keyboard())

    async def _send_item(
        self,
        item: Dict,
        avg: Optional[float],
        model: Optional[str],
        mem: Optional[str],
        ai_text_safe: str,
    ):
        title = escape_html(item["title"])
        item_id = escape_html(item["id"])
        model_s = escape_html(model or "?")
        mem_s = escape_html(mem or "?")
        location_s = escape_html(item.get("location") or "н/д")

        diff = None
        if avg and avg > 0:
            diff = round((item["price"] - avg) / avg * 100, 1)  # + если выше рынка

        tag = "📦" if self.mode_all else "🔥"

        price_line = f"💰 {item['price']} ₽"
        if avg:
            price_line += f" | Рынок: {int(avg)} ₽"
            if diff is not None:
                price_line += f" | Отклонение: {diff:+.1f}%"

        msg = (
            f"{tag} <b>{title}</b>\n"
            f"🆔 <code>{item_id}</code>\n"
            f"📍 {location_s}\n"
            f"🤖 Модель: {model_s} | {mem_s}\n"
            f"{price_line}\n\n"
            f"{ai_text_safe}"
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔗 Открыть", url=item["url"])]]
        )
        await self.bot.send_message(ADMIN_ID, msg, parse_mode="HTML", reply_markup=kb)

    async def monitor_loop(self):
        logger.info("🚀 Мониторинг работает (Москва и МО, s=104)")
        while True:
            try:
                if self.is_paused:
                    await asyncio.sleep(1)
                    continue

                try:
                    await asyncio.wait_for(self.force_search_event.wait(), timeout=CHECK_INTERVAL)
                    logger.info("[LOOP] Принудительный запуск")
                except asyncio.TimeoutError:
                    logger.info("[LOOP] Автоматический запуск")

                self.force_search_event.clear()

                items = await self.parser.fetch(SCAN_PAGES, self.bot)
                logger.info(f"[LOOP] Получено объявлений: {len(items)}")

                stats = {
                    "scanned": len(items),
                    "new": 0,
                    "sent": 0,
                    "no_avg": 0,
                    "too_expensive": 0,
                }

                for item in items:
                    if self.db.is_sent(item["id"]):
                        continue
                    stats["new"] += 1

                    avg, model, mem = self.prices.find_price(item["title"], item["description"])

                    # ✅ Фильтр отправки (не ИИ!):
                    # в режиме выгодных отправляем только если price <= avg*1.2
                    if not self.mode_all:
                        if not avg:
                            stats["no_avg"] += 1
                            continue
                        if item["price"] > avg * 1.2:
                            stats["too_expensive"] += 1
                            continue

                    # ✅ ИИ влияет только на текст, не на отправку
                    ai_text_safe = await self.ai.analyze(item, avg)

                    await self._send_item(item, avg, model, mem, ai_text_safe)

                    self.db.mark_sent(item["id"])
                    stats["sent"] += 1

                    await asyncio.sleep(0.8)

                if self.last_manual:
                    self.last_manual = False
                    msg = (
                        f"📊 <b>Итог поиска</b>\n"
                        f"Сканировано: <b>{stats['scanned']}</b>\n"
                        f"Новых (не в БД): <b>{stats['new']}</b>\n"
                        f"Отправлено: <b>{stats['sent']}</b>\n"
                    )
                    if not self.mode_all:
                        msg += (
                            f"\nПропуски (в выгодных):\n"
                            f"— нет средней цены: <b>{stats['no_avg']}</b>\n"
                            f"— дороже avg*1.2: <b>{stats['too_expensive']}</b>\n"
                        )
                    await self.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")

            except Exception as e:
                logger.error(f"[LOOP ERROR] {e}")
                await asyncio.sleep(5)

    async def run(self):
        self.force_search_event = asyncio.Event()
        asyncio.create_task(self.monitor_loop())
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    try:
        asyncio.run(MonitorBot().run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот выключен")
