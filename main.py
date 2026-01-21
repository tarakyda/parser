import asyncio
import logging
import os
import re
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from typing import Optional, List, Dict
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ================= CONFIG & LOGGING =================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
EXCEL_PATH = os.getenv("EXCEL_PATH", "prices.xlsx")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 60))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("AVITO")


# ================= DATABASE =================
class Storage:
    def __init__(self):
        self.conn = sqlite3.connect("sent.db")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sent (id TEXT PRIMARY KEY)")

    def is_sent(self, item_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM sent WHERE id=?", (item_id,))
        return cur.fetchone() is not None

    def mark_sent(self, item_id: str):
        self.conn.execute("INSERT OR IGNORE INTO sent VALUES (?)", (item_id,))
        self.conn.commit()


# ================= PRICE ANALYSIS =================
class PriceReference:
    def __init__(self, path: str):
        self.rows = []
        try:
            # Читаем Excel. Убедитесь, что файл prices.xlsx лежит в той же папке
            df = pd.read_excel(path, sheet_name="Сводный отчет", header=1)
            df.columns = [str(c).lower().strip() for c in df.columns]
            for _, r in df.iterrows():
                if r.get("mean"):
                    self.rows.append({
                        "model": str(r["модель"]).lower(),
                        "memory": str(r["память"]).lower(),
                        "mean": float(r["mean"])
                    })
            logger.info(f"[PRICE] База загружена: {len(self.rows)} позиций")
        except Exception as e:
            logger.error(f"[PRICE] Ошибка файла цен: {e}. Проверьте наличие prices.xlsx")

    def extract_memory(self, text: str) -> Optional[str]:
        m = re.search(r"(64|128|256|512|1024|1\s?тб|1tb)", text.lower())
        if not m: return None
        val = m.group(1).replace(" ", "")
        return "1tb" if val in ["1024", "1тб", "1tb"] else f"{val}gb"

    def find_price(self, title: str, description: str):
        text = f"{title} {description}".lower()
        mem = self.extract_memory(text)
        if not mem: return None, None, None
        for r in self.rows:
            if r["model"] in text and r["memory"] == mem:
                return r["mean"], r["model"], mem
        return None, None, mem


# ================= PARSER =================
class AvitoParser:
    def parse_time(self, time_str: str) -> int:
        time_str = time_str.lower()
        num = re.search(r'(\d+)', time_str)
        if "секунд" in time_str: return 0
        if not num: return 999
        val = int(num.group(1))
        if "минут" in time_str: return val
        if "час" in time_str: return val * 60
        return 1440

    async def fetch(self) -> List[Dict]:
        items = []
        async with async_playwright() as p:
            # headless=True ОБЯЗАТЕЛЬНО для Raspberry Pi и серверов без монитора
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
            try:
                page = await context.new_page()
                for p_num in range(1, 3):
                    url = f"https://www.avito.ru/moskva_i_mo?q=iphone&s=104&p={p_num}"
                    logger.info(f"[PARSER] Сканирую страницу {p_num}...")
                    await page.goto(url, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_selector('[data-marker="item"]', timeout=10000)
                    except:
                        continue

                    soup = BeautifulSoup(await page.content(), "html.parser")
                    cards = soup.select('[data-marker="item"]')
                    for c in cards:
                        try:
                            title_el = c.select_one('[itemprop="name"]') or c.select_one("h3")
                            price_el = c.select_one('[itemprop="price"]')
                            link_el = c.select_one('a[itemprop="url"]')
                            date_el = c.select_one('[data-marker="item-date"]')
                            if not all([title_el, price_el, link_el]): continue

                            items.append({
                                "id": link_el["href"].split("_")[-1],
                                "title": title_el.get_text(strip=True),
                                "price": int(price_el["content"]),
                                "url": "https://www.avito.ru" + link_el["href"],
                                "description": c.get_text(" ", strip=True),
                                "minutes_ago": self.parse_time(date_el.get_text(strip=True) if date_el else "")
                            })
                        except:
                            continue
                    await asyncio.sleep(2)  # Защита от бана
            finally:
                await browser.close()
        return items


# ================= MONITOR BOT =================
class MonitorBot:
    def __init__(self):
        self.bot = Bot(BOT_TOKEN)
        self.dp = Dispatcher()
        self.parser = AvitoParser()
        self.prices = PriceReference(EXCEL_PATH)
        self.db = Storage()

        self.is_paused = False
        self.mode_all = False
        self.time_limit = 5
        self.force_search = None  # Инициализируем в run()

        self._register_handlers()

    def keyboard(self) -> ReplyKeyboardMarkup:
        mode = "📦 ВСЕ НОВЫЕ" if self.mode_all else "🔥 ВЫГОДНЫЕ"
        pause = "▶ Активен" if not self.is_paused else "💤 Пауза"
        return ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🔎 Найти сейчас")],
            [KeyboardButton(text=mode), KeyboardButton(text=f"⏳ Время: {self.time_limit}м")],
            [KeyboardButton(text=pause)]
        ], resize_keyboard=True)

    def _register_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(m: Message):
            await m.answer("📱 Мониторинг запущен!", reply_markup=self.keyboard())

        @self.dp.message(Command("settings"))
        async def cmd_settings(m: Message):
            mode_str = "Все новые объявления" if self.mode_all else "Только выгодные (>5%)"
            status_str = "Пауза 💤" if self.is_paused else "Работает ▶"
            msg = (
                "⚙️ <b>Текущие настройки:</b>\n\n"
                f"Статус: <code>{status_str}</code>\n"
                f"Режим: <code>{mode_str}</code>\n"
                f"Фильтр времени: <code>{self.time_limit} мин.</code>\n"
                f"Интервал проверки: <code>{CHECK_INTERVAL} сек.</code>\n"
            )
            await m.answer(msg, parse_mode="HTML")

        @self.dp.message(F.text == "🔎 Найти сейчас")
        async def manual(m: Message):
            if self.force_search:
                self.force_search.set()
                await m.answer("⏳ Запускаю проверку 100 объявлений...")
            else:
                await m.answer("⚠️ Бот еще загружается...")

        @self.dp.message(F.text.in_(["📦 ВСЕ НОВЫЕ", "🔥 ВЫГОДНЫЕ"]))
        async def toggle_mode(m: Message):
            self.mode_all = not self.mode_all
            await m.answer(f"Режим изменен на: {'ВСЕ' if self.mode_all else 'ВЫГОДНЫЕ'}", reply_markup=self.keyboard())

        @self.dp.message(F.text.startswith("⏳ Время:"))
        async def toggle_time(m: Message):
            intervals = {5: 10, 10: 30, 30: 60, 60: 5}
            self.time_limit = intervals.get(self.time_limit, 30)
            await m.answer(f"Лимит времени: {self.time_limit}м", reply_markup=self.keyboard())

        @self.dp.message(F.text.in_(["▶ Активен", "💤 Пауза"]))
        async def toggle_pause(m: Message):
            self.is_paused = not self.is_paused
            await m.answer(f"Статус: {'ПАУЗА' if self.is_paused else 'РАБОТАЕТ'}", reply_markup=self.keyboard())

    async def monitor_loop(self):
        logger.info(f"[SYSTEM] Цикл запущен. Интервал: {CHECK_INTERVAL}с")
        while True:
            if self.is_paused:
                await asyncio.sleep(1)
                continue

            try:
                # Ожидание следующей итерации или нажатия кнопки
                try:
                    await asyncio.wait_for(self.force_search.wait(), timeout=CHECK_INTERVAL)
                    logger.info("[LOOP] Поиск запущен принудительно")
                except asyncio.TimeoutError:
                    logger.info("[LOOP] Автоматический поиск")

                self.force_search.clear()
                items = await self.parser.fetch()
                sent_count = 0

                for item in items:
                    if self.db.is_sent(item["id"]): continue
                    if item["minutes_ago"] > self.time_limit: continue

                    avg, model, mem = self.prices.find_price(item["title"], item["description"])
                    diff = round((1 - item["price"] / avg) * 100, 1) if avg else None

                    if not self.mode_all:
                        if not avg or not diff or diff < 5: continue

                    tag = "🔥" if diff and diff >= 20 else "✅" if diff and diff >= 5 else "📦"
                    msg = (f"{tag} <b>{item['title']}</b>\n"
                           f"🤖 Модель: {model or '?'}, {mem or '?'}\n"
                           f"💰 {item['price']} ₽ | Рынок: {int(avg) if avg else 'н/д'}\n"
                           f"📉 Выгода: {diff if diff else '0'}%\n"
                           f"🕒 Опубликовано: {item['minutes_ago']} мин. назад")

                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🔗 Открыть", url=item["url"])]])
                    await self.bot.send_message(ADMIN_ID, msg, parse_mode="HTML", reply_markup=kb)
                    self.db.mark_sent(item["id"])
                    sent_count += 1
                    await asyncio.sleep(1.2)  # Небольшой отдых для API

                if sent_count > 0:
                    await self.bot.send_message(ADMIN_ID, f"🏁 Поиск завершен. Найдено новых: <b>{sent_count}</b>",
                                                parse_mode="HTML")
                else:
                    await self.bot.send_message(ADMIN_ID, "🔎 Новых объявлений не найдено.")

            except Exception as e:
                logger.error(f"[LOOP ERROR] {e}")
                await asyncio.sleep(10)

    async def run(self):
        # ВАЖНО: Создаем Event прямо здесь, внутри работающего цикла
        self.force_search = asyncio.Event()
        asyncio.create_task(self.monitor_loop())
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    try:
        asyncio.run(MonitorBot().run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот выключен")
