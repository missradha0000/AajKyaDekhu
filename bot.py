"""AajKyaDekhu_Bot — Movie discovery bot backed by Google Sheets."""

from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import random
import re
import shlex
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import gspread
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    WebAppInfo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("AajKyaDekhu_Bot")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CATALOG_TAB = "Bot_Lookup_Rows"
REQUESTS_TAB = "Requests"
QUOTA_TAB = "UserQuota"
FREE_DAILY_LIMIT = 2
DEFAULT_FORCE_CHANNEL_URL = "https://t.me/aajkyadekhu"

YEAR_RE = re.compile(r"^(19\d{2}|20\d{2})$")
UID_RE = re.compile(r"uid:(\d+)")
STATUS_CODE_MAP = {"p": "Pending", "a": "Added", "u": "Unavailable"}


class UserForm(StatesGroup):
    waiting_request_movie = State()
    waiting_complaint = State()
    waiting_report_link = State()


@dataclass(frozen=True)
class Settings:
    bot_token: str
    spreadsheet_id: str
    google_credentials_file: str
    admin_chat_ids: tuple[int, ...]
    log_channel_id: Optional[int] = None
    force_channel_id: Optional[str] = None
    force_channel_url: Optional[str] = None
    webapp_url: Optional[str] = None
    port: int = 10000


def _parse_admin_ids(raw: Optional[str]) -> tuple[int, ...]:
    if not raw:
        return ()
    out: list[int] = []
    for p in raw.split(","):
        c = p.strip()
        if c and c.lstrip("-").isdigit():
            out.append(int(c))
    return tuple(out)


def _resolve_credentials_path(raw_path: str) -> str:
    if raw_path and os.path.exists(raw_path):
        return raw_path
    if os.path.exists("/etc/secrets/credentials.json"):
        return "/etc/secrets/credentials.json"
    return raw_path


def load_settings() -> Settings:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    creds = _resolve_credentials_path(
        os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json").strip()
    )
    admin_ids = _parse_admin_ids(os.getenv("ADMIN_CHAT_ID"))
    log_raw = os.getenv("LOG_CHANNEL_ID", "").strip()
    log_id = int(log_raw) if log_raw and log_raw.lstrip("-").isdigit() else None
    force_id = os.getenv("FORCE_CHANNEL_ID", "").strip() or None
    force_url = os.getenv("FORCE_CHANNEL_URL", "").strip() or None
    webapp_url = os.getenv("WEBAPP_URL", "").strip() or None
    port = int(os.getenv("PORT", "10000").strip() or "10000")

    missing = [
        k
        for k, v in [
            ("BOT_TOKEN", bot_token),
            ("SPREADSHEET_ID", spreadsheet_id),
            ("GOOGLE_CREDENTIALS_FILE", creds),
        ]
        if not v
    ]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return Settings(
        bot_token=bot_token,
        spreadsheet_id=spreadsheet_id,
        google_credentials_file=creds,
        admin_chat_ids=admin_ids,
        log_channel_id=log_id,
        force_channel_id=force_id,
        force_channel_url=force_url,
        webapp_url=webapp_url,
        port=port,
    )


def short_hash(value: str, length: int = 10) -> str:
    return hashlib.md5(value.strip().lower().encode("utf-8")).hexdigest()[:length]


@dataclass(frozen=True)
class CatalogRow:
    movie_id: str
    movie_name: str
    year: str
    language: str
    platform: str
    watch_link: str
    channel_source: str

    @property
    def movie_key(self) -> str:
        base = self.movie_id.strip()
        return base if base else f"{self.movie_name.strip().lower()}::{self.year.strip()}"

    @property
    def movie_token(self) -> str:
        return short_hash(self.movie_key, 10)

    @property
    def language_token(self) -> str:
        return short_hash(self.language, 10)

    @property
    def platform_token(self) -> str:
        return short_hash(f"{self.language}::{self.platform}", 10)


@dataclass(frozen=True)
class MovieSummary:
    movie_key: str
    movie_token: str
    movie_id: str
    movie_name: str
    year: str
    rows: tuple[CatalogRow, ...]


@dataclass(frozen=True)
class SearchFilters:
    movie_query: str = ""
    year: Optional[str] = None
    language: Optional[str] = None
    platform: Optional[str] = None

    @property
    def has_facets(self) -> bool:
        return bool(self.year or self.language or self.platform)

    def describe(self) -> str:
        parts: list[str] = []
        if self.movie_query:
            parts.append(f'Movie="{self.movie_query}"')
        if self.year:
            parts.append(f"Year={self.year}")
        if self.language:
            parts.append(f"Language={self.language}")
        if self.platform:
            parts.append(f"Platform={self.platform}")
        return ", ".join(parts)


@dataclass(frozen=True)
class SearchResult:
    filters: SearchFilters
    movies: tuple[MovieSummary, ...]
    matching_rows: tuple[CatalogRow, ...]
    exact_match: bool


@dataclass(frozen=True)
class RequestRecord:
    request_id: str
    requested_movie: str
    preferred_language: str
    requested_by: str
    request_date: str
    status: str
    notes: str

    @property
    def requester_user_id(self) -> Optional[int]:
        m = UID_RE.search(self.requested_by)
        return int(m.group(1)) if m else None


class SheetsError(Exception):
    pass


class SheetsService:
    def __init__(self, spreadsheet_id: str, credentials_file: str) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._credentials_file = credentials_file
        self._client: Optional[gspread.Client] = None

    def _ws(self, title: str) -> gspread.Worksheet:
        if self._client is None:
            creds = Credentials.from_service_account_file(
                self._credentials_file, scopes=SCOPES
            )
            self._client = gspread.authorize(creds)
        return self._client.open_by_key(self._spreadsheet_id).worksheet(title)

    @staticmethod
    def _clean(raw: dict[str, object]) -> dict[str, str]:
        return {str(k).strip(): str(v).strip() for k, v in raw.items()}

    def _fetch_catalog_sync(self) -> list[CatalogRow]:
        records = self._ws(CATALOG_TAB).get_all_records(default_blank="")
        rows: list[CatalogRow] = []
        for raw in records:
            r = self._clean(raw)
            name = r.get("Movie_Name", "")
            if not name:
                continue
            rows.append(
                CatalogRow(
                    movie_id=r.get("Movie_ID", ""),
                    movie_name=name,
                    year=r.get("Year", ""),
                    language=r.get("Language", "") or "Default",
                    platform=r.get("Platform", "") or "Unknown Platform",
                    watch_link=r.get("Watch_Link", ""),
                    channel_source=r.get("Channel_Source", ""),
                )
            )
        return rows

    async def fetch_catalog(self) -> list[CatalogRow]:
        try:
            return await asyncio.to_thread(self._fetch_catalog_sync)
        except Exception as exc:
            logger.exception("Error reading %s", CATALOG_TAB)
            raise SheetsError(str(exc)) from exc

    @staticmethod
    def group_movies(rows: list[CatalogRow]) -> list[MovieSummary]:
        grouped: dict[str, list[CatalogRow]] = {}
        for row in rows:
            grouped.setdefault(row.movie_key, []).append(row)
        summaries = [
            MovieSummary(
                movie_key=k,
                movie_token=short_hash(k, 10),
                movie_id=g[0].movie_id,
                movie_name=g[0].movie_name,
                year=g[0].year,
                rows=tuple(g),
            )
            for k, g in grouped.items()
        ]
        summaries.sort(key=lambda m: (m.movie_name.lower(), m.year))
        return summaries

    @staticmethod
    def parse_query(raw_query: str, catalog: list[CatalogRow]) -> SearchFilters:
        cleaned = raw_query.strip()
        if not cleaned:
            return SearchFilters()
        try:
            tokens = shlex.split(cleaned)
        except ValueError:
            tokens = cleaned.split()

        explicit_movie: list[str] = []
        free_tokens: list[str] = []
        year = language = platform = None
        has_kv = False

        for token in tokens:
            if "=" in token:
                key, val = token.split("=", 1)
                k, v = key.strip().lower(), val.strip()
                if v:
                    if k in ("year", "y"):
                        year, has_kv = v, True
                        continue
                    if k in ("language", "lang", "l"):
                        language, has_kv = v, True
                        continue
                    if k in ("platform", "plat", "p"):
                        platform, has_kv = v, True
                        continue
                    if k in ("movie", "title", "name", "m"):
                        explicit_movie.append(v)
                        has_kv = True
                        continue
            free_tokens.append(token)

        if has_kv:
            return SearchFilters(
                movie_query=" ".join(explicit_movie + free_tokens).strip(),
                year=year,
                language=language,
                platform=platform,
            )

        full_lower = cleaned.lower()
        if any(
            r.movie_name.lower() == full_lower or full_lower in r.movie_name.lower()
            for r in catalog
        ):
            return SearchFilters(movie_query=cleaned)

        known_langs = {r.language.lower(): r.language for r in catalog if r.language}
        known_plats = {r.platform.lower(): r.platform for r in catalog if r.platform}
        title_tokens: list[str] = []

        for token in free_tokens:
            low = token.lower()
            if year is None and YEAR_RE.match(token):
                year = token
            elif language is None and low in known_langs:
                language = known_langs[low]
            elif platform is None and low in known_plats:
                platform = known_plats[low]
            else:
                title_tokens.append(token)

        movie_query = " ".join(title_tokens).strip()
        if not movie_query and not (year or language or platform):
            movie_query = cleaned

        return SearchFilters(
            movie_query=movie_query, year=year, language=language, platform=platform
        )

    async def search_movies(self, raw_query: str) -> SearchResult:
        catalog = await self.fetch_catalog()
        filters = self.parse_query(raw_query, catalog)
        filtered = catalog
        if filters.year:
            filtered = [r for r in filtered if r.year.lower() == filters.year.lower()]
        if filters.language:
            lq = filters.language.lower()
            filtered = [r for r in filtered if lq in r.language.lower()]
        if filters.platform:
            pq = filters.platform.lower()
            filtered = [r for r in filtered if pq in r.platform.lower()]

        exact_match = False
        if filters.movie_query:
            q = filters.movie_query.lower()
            exact_rows = [r for r in filtered if r.movie_name.lower() == q]
            if exact_rows:
                matched_rows, exact_match = exact_rows, True
            else:
                matched_rows = [r for r in filtered if q in r.movie_name.lower()]
        else:
            matched_rows = filtered

        movies = self.group_movies(matched_rows)
        return SearchResult(
            filters=filters,
            movies=tuple(movies),
            matching_rows=tuple(matched_rows),
            exact_match=exact_match,
        )

    async def get_movie_by_token(self, movie_token: str) -> Optional[MovieSummary]:
        catalog = await self.fetch_catalog()
        return next(
            (m for m in self.group_movies(catalog) if m.movie_token == movie_token),
            None,
        )

    @staticmethod
    def get_unique_languages(movie: MovieSummary) -> list[str]:
        seen: set[str] = set()
        langs: list[str] = []
        for r in movie.rows:
            if r.language.lower() not in seen:
                seen.add(r.language.lower())
                langs.append(r.language)
        return langs

    @staticmethod
    def get_platforms_for_language(
        movie: MovieSummary, language_token: str
    ) -> tuple[Optional[str], list[CatalogRow]]:
        rows = [r for r in movie.rows if r.language_token == language_token]
        return (rows[0].language, rows) if rows else (None, [])

    def _get_today_quota_sync(self, user_id: int) -> int:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        uid_str = str(user_id)
        for row in self._ws(QUOTA_TAB).get_all_values()[1:]:
            if len(row) >= 3 and row[0].strip() == uid_str and row[1].strip() == today:
                try:
                    return int(row[2].strip())
                except ValueError:
                    return 0
        return 0

    async def get_today_quota(self, user_id: int) -> int:
        try:
            return await asyncio.to_thread(self._get_today_quota_sync, user_id)
        except Exception:
            logger.exception("Error reading %s", QUOTA_TAB)
            return 0

    def _increment_today_quota_sync(self, user_id: int) -> int:
        ws = self._ws(QUOTA_TAB)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        uid_str = str(user_id)
        rows = ws.get_all_values()
        for idx, row in enumerate(rows[1:], start=2):
            if len(row) >= 3 and row[0].strip() == uid_str and row[1].strip() == today:
                try:
                    cur = int(row[2].strip())
                except ValueError:
                    cur = 0
                ws.update_cell(idx, 3, cur + 1)
                return cur + 1
        ws.append_row([uid_str, today, 1], value_input_option="USER_ENTERED")
        return 1

    async def increment_today_quota(self, user_id: int) -> int:
        try:
            return await asyncio.to_thread(self._increment_today_quota_sync, user_id)
        except Exception:
            logger.exception("Error updating %s", QUOTA_TAB)
            return 0

    def _create_request_sync(
        self,
        requested_movie: str,
        preferred_language: str,
        user_id: int,
        username: Optional[str],
        full_name: str,
        notes: str = "",
    ) -> RequestRecord:
        ws = self._ws(REQUESTS_TAB)
        existing: set[str] = set()
        try:
            for rec in ws.get_all_records(default_blank=""):
                rid = str(rec.get("Request_ID", "")).strip().upper()
                if rid:
                    existing.add(rid)
        except Exception:
            existing = set()

        req_id = ""
        for _ in range(30):
            cand = f"REQ-{random.randint(1000, 9999)}"
            if cand not in existing:
                req_id = cand
                break
        if not req_id:
            req_id = f"REQ-{random.randint(10000, 99999)}"

        identity = f"@{username}" if username else full_name
        req_by = f"uid:{user_id} | {identity}"
        req_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        ws.append_row(
            [req_id, requested_movie, preferred_language or "Any", req_by, req_date, "Pending", notes],
            value_input_option="USER_ENTERED",
        )
        return RequestRecord(
            req_id, requested_movie, preferred_language or "Any", req_by, req_date, "Pending", notes
        )

    async def create_request(
        self,
        requested_movie: str,
        preferred_language: str,
        user_id: int,
        username: Optional[str],
        full_name: str,
        notes: str = "",
    ) -> RequestRecord:
        try:
            return await asyncio.to_thread(
                self._create_request_sync,
                requested_movie,
                preferred_language,
                user_id,
                username,
                full_name,
                notes,
            )
        except Exception as exc:
            logger.exception("Error creating request in %s", REQUESTS_TAB)
            raise SheetsError(str(exc)) from exc

    def _fetch_requests_sync(self) -> list[RequestRecord]:
        records = self._ws(REQUESTS_TAB).get_all_records(default_blank="")
        out: list[RequestRecord] = []
        for raw in records:
            r = self._clean(raw)
            if r.get("Request_ID"):
                out.append(
                    RequestRecord(
                        r["Request_ID"],
                        r.get("Requested_Movie", ""),
                        r.get("Preferred_Language", "") or "Any",
                        r.get("Requested_By", ""),
                        r.get("Request_Date", ""),
                        r.get("Status", "") or "Pending",
                        r.get("Notes", ""),
                    )
                )
        return out

    async def get_user_requests(self, user_id: int, username: Optional[str]) -> list[RequestRecord]:
        try:
            all_reqs = await asyncio.to_thread(self._fetch_requests_sync)
        except Exception as exc:
            raise SheetsError(str(exc)) from exc
        uid_m = f"uid:{user_id}"
        un_m = f"@{username.lower()}" if username else None
        matched = [
            rec for rec in all_reqs if uid_m in rec.requested_by or (un_m and un_m in rec.requested_by.lower())
        ]
        matched.reverse()
        return matched

    async def get_pending_requests(self) -> list[RequestRecord]:
        try:
            all_reqs = await asyncio.to_thread(self._fetch_requests_sync)
        except Exception as exc:
            raise SheetsError(str(exc)) from exc
        return [r for r in all_reqs if r.status.strip().lower() in ("pending", "")]

    def _update_request_status_sync(self, request_id: str, new_status: str) -> Optional[RequestRecord]:
        ws = self._ws(REQUESTS_TAB)
        all_vals = ws.get_all_values()
        if not all_vals:
            return None
        headers = [h.strip() for h in all_vals[0]]
        id_col = headers.index("Request_ID") + 1
        status_col = headers.index("Status") + 1
        target = request_id.strip().upper()
        for row_num, row in enumerate(all_vals[1:], start=2):
            if (row[id_col - 1].strip().upper() if len(row) >= id_col else "") == target:
                ws.update_cell(row_num, status_col, new_status.strip())
                return RequestRecord(
                    target,
                    row[headers.index("Requested_Movie")].strip() if "Requested_Movie" in headers else "",
                    row[headers.index("Preferred_Language")].strip() if "Preferred_Language" in headers else "Any",
                    row[headers.index("Requested_By")].strip() if "Requested_By" in headers else "",
                    row[headers.index("Request_Date")].strip() if "Request_Date" in headers else "",
                    new_status.strip(),
                    row[headers.index("Notes")].strip() if "Notes" in headers else "",
                )
        return None

    async def update_request_status(self, request_id: str, new_status: str) -> Optional[RequestRecord]:
        try:
            return await asyncio.to_thread(self._update_request_status_sync, request_id, new_status)
        except Exception as exc:
            raise SheetsError(str(exc)) from exc


class RequestTokenCache:
    def __init__(self, max_size: int = 2000) -> None:
        self._max_size = max_size
        self._store: OrderedDict[str, tuple[str, str]] = OrderedDict()

    def put(self, movie_name: str, preferred_language: str = "Any") -> str:
        tok = short_hash(f"{movie_name.strip().lower()}::{preferred_language.strip().lower()}", 12)
        self._store[tok] = (movie_name.strip(), preferred_language.strip() or "Any")
        self._store.move_to_end(tok)
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)
        return tok

    def get(self, token: str) -> Optional[tuple[str, str]]:
        return self._store.get(token)


request_cache = RequestTokenCache()
router = Router()


def _is_valid_http_url(url: str) -> bool:
    try:
        p = urlparse(url.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _format_movie_header(movie: MovieSummary) -> str:
    yr = f" ({html.escape(movie.year)})" if movie.year else ""
    return f"🎬 <b>{html.escape(movie.movie_name)}{yr}</b>"


def _main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎬 Movie Search"), KeyboardButton(text="📩 Request Movie")],
            [KeyboardButton(text="⚠️ Complaint"), KeyboardButton(text="🔗 Broken Link")],
            [KeyboardButton(text="📢 Join Channel")],
        ],
        resize_keyboard=True,
    )


async def _send_to_admin_log(
    bot: Bot, settings: Settings, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None
) -> None:
    if settings.log_channel_id:
        try:
            await bot.send_message(
                settings.log_channel_id, text, reply_markup=reply_markup, disable_web_page_preview=True
            )
            return
        except Exception as exc:
            logger.warning("LOG_CHANNEL_ID send failed: %s", exc)
    for aid in settings.admin_chat_ids:
        try:
            await bot.send_message(aid, text, reply_markup=reply_markup, disable_web_page_preview=True)
        except Exception as exc:
            logger.warning("Admin %s send failed: %s", aid, exc)


async def _is_joined_force_channel(bot: Bot, settings: Settings, user_id: int) -> bool:
    if not settings.force_channel_id:
        return True
    try:
        member = await bot.get_chat_member(chat_id=settings.force_channel_id, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as exc:
        logger.warning("get_chat_member failed: %s", exc)
        return True


def _force_join_kb(settings: Settings, movie_token: str, lang_token: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    channel_url = (
        settings.force_channel_url
        if settings.force_channel_url and _is_valid_http_url(settings.force_channel_url)
        else DEFAULT_FORCE_CHANNEL_URL
    )
    b.row(InlineKeyboardButton(text="📢 Join Channel", url=channel_url))
    b.row(InlineKeyboardButton(text="✅ Check Again", callback_data=f"chk:{movie_token}:{lang_token}"))
    return b.as_markup()


def _request_kb(movie_name: str, pref_lang: str = "Any") -> InlineKeyboardMarkup:
    tok = request_cache.put(movie_name, pref_lang)
    b = InlineKeyboardBuilder()
    b.button(text="📩 Request This Movie", callback_data=f"req:{tok}")
    return b.as_markup()


def _movie_picker_kb(movies: tuple[MovieSummary, ...]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for m in movies[:25]:
        yr = f" ({m.year})" if m.year else ""
        b.button(text=f"🎬 {m.movie_name}{yr}"[:64], callback_data=f"mv:{m.movie_token}")
    b.adjust(1)
    return b.as_markup()


def _languages_kb(movie: MovieSummary) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for lang in SheetsService.get_unique_languages(movie):
        b.button(text=f"🌐 {lang}"[:64], callback_data=f"lg:{movie.movie_token}:{short_hash(lang, 10)}")
    b.adjust(2)
    return b.as_markup()


def _platforms_view(
    movie: MovieSummary, language_name: str, rows: list[CatalogRow]
) -> tuple[str, InlineKeyboardMarkup]:
    b = InlineKeyboardBuilder()
    blocks: list[str] = []
    for r in rows:
        plat = html.escape(r.platform)
        link = r.watch_link.strip()
        if _is_valid_http_url(link):
            blocks.append(f"<b>{plat}</b>\n{html.escape(link)}")
            b.row(InlineKeyboardButton(text=f"▶️ {r.platform}"[:64], url=link))
        else:
            src = f" (Source: {html.escape(r.channel_source)})" if r.channel_source else ""
            blocks.append(f"<b>{plat}</b>\n<i>Link unavailable{src}</i>")

        b.row(
            InlineKeyboardButton(
                text=f"❌ {r.platform} Not Available"[:64],
                callback_data=f"na:{movie.movie_token}:{r.language_token}:{r.platform_token}",
            )
        )

    b.row(InlineKeyboardButton(text="⬅️ Back to Languages", callback_data=f"mv:{movie.movie_token}"))
    text = f"{_format_movie_header(movie)}\n🌐 Language: <b>{html.escape(language_name)}</b>\n\n" + "\n\n".join(blocks)
    return text, b.as_markup()


def _admin_requests_view(pending: list[RequestRecord]) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    if not pending:
        return "✅ No pending movie requests right now.", None
    b = InlineKeyboardBuilder()
    lines = [f"🛠 <b>Pending Movie Requests ({len(pending)})</b>\n"]
    for rec in pending[:8]:
        lines.append(
            f"• <code>{html.escape(rec.request_id)}</code> — <b>{html.escape(rec.requested_movie)}</b> "
            f"({html.escape(rec.preferred_language)})\n  By: {html.escape(rec.requested_by)}"
        )
        b.row(
            InlineKeyboardButton(text=f"⏳ {rec.request_id}", callback_data=f"st:{rec.request_id}:p"),
            InlineKeyboardButton(text="✅ Added", callback_data=f"st:{rec.request_id}:a"),
            InlineKeyboardButton(text="❌ Unavailable", callback_data=f"st:{rec.request_id}:u"),
        )
    return "\n\n".join(lines), b.as_markup()


async def _safe_edit(cb: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    if not cb.message:
        return
    try:
        await cb.message.edit_text(text=text, reply_markup=kb, disable_web_page_preview=True)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            await cb.message.answer(text=text, reply_markup=kb, disable_web_page_preview=True)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    name = html.escape(message.from_user.first_name if message.from_user else "there")
    text = (
        f"👋 Hey <b>{name}</b>, welcome to <b>AajKyaDekhu</b>!\n\n"
        "किसी भी <b>मूवी का नाम</b> लिखकर भेजें या नीचे दिए गए बटन चुनें।\n"
        "• /request — नई मूवी की रिक्वेस्ट\n"
        "• /complaint — कोई भी कंप्लेंट लिखें\n"
        "• /reportlink — गलत या बंद लिंक की रिपोर्ट करें\n"
        "• /myrequests — अपनी रिक्वेस्ट देखें\n"
        "• /help — मदद"
    )
    if settings.webapp_url and _is_valid_http_url(settings.webapp_url):
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="Open WebApp", web_app=WebAppInfo(url=settings.webapp_url)))
        await message.answer(text, reply_markup=b.as_markup())
        await message.answer("👇 Quick Menu:", reply_markup=_main_menu_kb())
        return
    await message.answer(text, reply_markup=_main_menu_kb())


@router.message(Command("help"))
async def cmd_help(message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    is_admin = bool(message.from_user and message.from_user.id in settings.admin_chat_ids)
    text = (
        "📖 <b>AajKyaDekhu_Bot Help</b>\n\n"
        "• मूवी का नाम लिखें (जैसे <code>3 Idiots</code>)\n"
        "• /request — मूवी रिक्वेस्ट भेजें\n"
        "• /complaint — शिकायत/सुझाव भेजें\n"
        "• /reportlink — लिंक गलत या ब्लॉक करवाना हो तो बताएं\n"
        "• /myrequests — अपनी रिक्वेस्ट का स्टेटस"
    )
    if is_admin:
        text += "\n\n<b>Admin:</b> /requests"
    await message.answer(text, reply_markup=_main_menu_kb())


@router.message(Command("request"))
async def cmd_req_start(message: Message, state: FSMContext) -> None:
    await state.set_state(UserForm.waiting_request_movie)
    await message.answer("🎬 कृपया उस <b>मूवी का नाम</b> लिखकर भेजें जिसकी रिक्वेस्ट करनी है:")


@router.message(UserForm.waiting_request_movie, F.text & ~F.text.startswith("/"))
async def proc_req_movie(
    message: Message, state: FSMContext, bot: Bot, settings: Settings, sheets: SheetsService
) -> None:
    await state.clear()
    if not message.from_user or not message.text:
        return
    movie_name = message.text.strip()
    try:
        rec = await sheets.create_request(
            movie_name, "Any", message.from_user.id, message.from_user.username, message.from_user.full_name
        )
        req_id = rec.request_id
    except SheetsError:
        req_id = "LOG-ONLY"
    await message.answer(
        f"✅ आपकी रिक्वेस्ट (<b>{html.escape(movie_name)}</b>) भेज दी गई है!",
        reply_markup=_main_menu_kb(),
    )
    uname = f"@{message.from_user.username}" if message.from_user.username else "No Username"
    await _send_to_admin_log(
        bot,
        settings,
        f"🆕 <b>Movie Request</b>\n• ID: <code>{html.escape(req_id)}</code>\n"
        f"• Movie: <b>{html.escape(movie_name)}</b>\n"
        f"• User: {html.escape(message.from_user.full_name)} ({html.escape(uname)}) | <code>{message.from_user.id}</code>",
    )


@router.message(Command("complaint"))
async def cmd_comp_start(message: Message, state: FSMContext) -> None:
    await state.set_state(UserForm.waiting_complaint)
    await message.answer("📝 कृपया अपनी <b>कंप्लेंट</b> लिखकर भेजें:")


@router.message(UserForm.waiting_complaint, F.text & ~F.text.startswith("/"))
async def proc_comp(message: Message, state: FSMContext, bot: Bot, settings: Settings) -> None:
    await state.clear()
    if not message.from_user or not message.text:
        return
    msg_text = message.text.strip()
    await message.answer("✅ आपकी कंप्लेंट एडमिन तक भेज दी गई है।", reply_markup=_main_menu_kb())
    uname = f"@{message.from_user.username}" if message.from_user.username else "No Username"
    await _send_to_admin_log(
        bot,
        settings,
        f"📢 <b>Complaint</b>\n• User: {html.escape(message.from_user.full_name)} ({html.escape(uname)}) | "
        f"<code>{message.from_user.id}</code>\n• Message:\n{html.escape(msg_text)}",
    )


@router.message(Command("reportlink"))
async def cmd_replink_start(message: Message, state: FSMContext) -> None:
    await state.set_state(UserForm.waiting_report_link)
    await message.answer("⚠️ बताएं <b>किस मूवी का लिंक</b> काम नहीं कर रहा या ब्लॉक करवाना है:")


@router.message(UserForm.waiting_report_link, F.text & ~F.text.startswith("/"))
async def proc_replink(message: Message, state: FSMContext, bot: Bot, settings: Settings) -> None:
    await state.clear()
    if not message.from_user or not message.text:
        return
    msg_text = message.text.strip()
    await message.answer("✅ आपकी लिंक रिपोर्ट एडमिन तक भेज दी गई है।", reply_markup=_main_menu_kb())
    uname = f"@{message.from_user.username}" if message.from_user.username else "No Username"
    await _send_to_admin_log(
        bot,
        settings,
        f"🚫 <b>Link Report</b>\n• User: {html.escape(message.from_user.full_name)} ({html.escape(uname)}) | "
        f"<code>{message.from_user.id}</code>\n• Details:\n{html.escape(msg_text)}",
    )


@router.message(F.text == "🎬 Movie Search")
async def btn_movie_search(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        "🔎 कृपया उस <b>मूवी का नाम</b> लिखकर भेजें जिसे आप देखना चाहते हैं:",
        reply_markup=_main_menu_kb(),
    )


@router.message(F.text == "📩 Request Movie")
async def btn_request_movie(message: Message, state: FSMContext) -> None:
    await cmd_req_start(message, state)


@router.message(F.text == "⚠️ Complaint")
async def btn_complaint(message: Message, state: FSMContext) -> None:
    await cmd_comp_start(message, state)


@router.message(F.text == "🔗 Broken Link")
async def btn_broken_link(message: Message, state: FSMContext) -> None:
    await cmd_replink_start(message, state)


@router.message(F.text == "📢 Join Channel")
async def btn_join_channel(message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    url = (
        settings.force_channel_url
        if settings.force_channel_url and _is_valid_http_url(settings.force_channel_url)
        else DEFAULT_FORCE_CHANNEL_URL
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📢 Join Channel", url=url)]]
    )
    await message.answer(
        "📢 हमारे ऑफ़िशियल चैनल से जुड़ें:",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


async def _run_search(
    message: Message, raw_query: str, bot: Bot, settings: Settings, sheets: SheetsService
) -> None:
    q = raw_query.strip()
    if not q:
        await message.answer("कृपया मूवी का नाम लिखें।")
        return
    try:
        result = await sheets.search_movies(q)
    except SheetsError:
        await message.answer("⚠️ Catalog temporarily unavailable. Try again.")
        return

    if not result.movies:
        await message.answer(
            "यह मूवी अभी लिस्ट में नहीं है। नीचे दबाकर रिक्वेस्ट भेजें:",
            reply_markup=_request_kb(result.filters.movie_query or q, result.filters.language or "Any"),
        )
        return

    if len(result.movies) > 1:
        await message.answer(
            f"🔎 <b>{len(result.movies)}</b> मूवीज़ मिलीं, एक चुनें:",
            reply_markup=_movie_picker_kb(result.movies),
        )
        return

    movie = result.movies[0]
    await message.answer(
        f"{_format_movie_header(movie)}\n\nSelect a language:",
        reply_markup=_languages_kb(movie),
    )


@router.message(Command("search"))
async def cmd_search(
    message: Message, command: CommandObject, bot: Bot, settings: Settings, sheets: SheetsService
) -> None:
    await _run_search(message, command.args or "", bot, settings, sheets)


@router.message(Command("myrequests"))
async def cmd_myrequests(message: Message, sheets: SheetsService) -> None:
    if not message.from_user:
        return
    try:
        recs = await sheets.get_user_requests(message.from_user.id, message.from_user.username)
    except SheetsError:
        await message.answer("⚠️ Could not fetch requests.")
        return
    if not recs:
        await message.answer("📭 आपने अभी कोई रिक्वेस्ट नहीं भेजी है।")
        return
    lines = ["📋 <b>Your Requests</b>\n"]
    for r in recs[:20]:
        lines.append(f"• <b>{html.escape(r.requested_movie)}</b> (<code>{html.escape(r.request_id)}</code>) — <b>{html.escape(r.status)}</b>")
    await message.answer("\n".join(lines))


@router.message(Command("requests"))
async def cmd_admin_requests(message: Message, settings: Settings, sheets: SheetsService) -> None:
    if not message.from_user or message.from_user.id not in settings.admin_chat_ids:
        await message.answer("⛔ Admin only.")
        return
    text, kb = _admin_requests_view(await sheets.get_pending_requests())
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("mv:"))
async def cb_movie(cb: CallbackQuery, sheets: SheetsService) -> None:
    await cb.answer()
    if not cb.data:
        return
    movie = await sheets.get_movie_by_token(cb.data.split(":", 1)[1])
    if not movie:
        await _safe_edit(cb, "Movie not found. Search again.")
        return
    await _safe_edit(cb, f"{_format_movie_header(movie)}\n\nSelect a language:", _languages_kb(movie))


async def _deliver_with_quota(
    cb: CallbackQuery, movie_token: str, lang_token: str, bot: Bot, settings: Settings, sheets: SheetsService
) -> None:
    if not cb.from_user:
        return
    movie = await sheets.get_movie_by_token(movie_token)
    if not movie:
        await _safe_edit(cb, "Movie not found. Search again.")
        return
    lang_name, rows = SheetsService.get_platforms_for_language(movie, lang_token)
    if not lang_name or not rows:
        await _safe_edit(cb, f"{_format_movie_header(movie)}\n\nNo platforms found.", _languages_kb(movie))
        return

    count = await sheets.get_today_quota(cb.from_user.id)
    if count >= FREE_DAILY_LIMIT and not await _is_joined_force_channel(bot, settings, cb.from_user.id):
        await _safe_edit(
            cb,
            "🔒 आज की <b>2 फ्री मूवीज़</b> पूरी हो गई हैं!\nअनलिमिटेड के लिए चैनल जॉइन करें और <b>Check Again</b> दबाएं:",
            _force_join_kb(settings, movie_token, lang_token),
        )
        return

    await sheets.increment_today_quota(cb.from_user.id)
    text, kb = _platforms_view(movie, lang_name, rows)
    await _safe_edit(cb, text, kb)


@router.callback_query(F.data.startswith("lg:"))
async def cb_lang(cb: CallbackQuery, bot: Bot, settings: Settings, sheets: SheetsService) -> None:
    await cb.answer()
    if not cb.data:
        return
    _, m_tok, l_tok = cb.data.split(":", 2)
    await _deliver_with_quota(cb, m_tok, l_tok, bot, settings, sheets)


@router.callback_query(F.data.startswith("chk:"))
async def cb_check(cb: CallbackQuery, bot: Bot, settings: Settings, sheets: SheetsService) -> None:
    if not cb.data or not cb.from_user:
        await cb.answer()
        return
    _, m_tok, l_tok = cb.data.split(":", 2)
    if not await _is_joined_force_channel(bot, settings, cb.from_user.id):
        await cb.answer("❌ पहले चैनल जॉइन करें!", show_alert=True)
        return
    await cb.answer("✅ चैनल जॉइन कन्फर्म!")
    await _deliver_with_quota(cb, m_tok, l_tok, bot, settings, sheets)


@router.callback_query(F.data.startswith("na:"))
async def cb_platform_not_available(
    cb: CallbackQuery, bot: Bot, settings: Settings, sheets: SheetsService
) -> None:
    if not cb.data or not cb.from_user:
        await cb.answer()
        return
    parts = cb.data.split(":", 3)
    if len(parts) != 4:
        await cb.answer()
        return
    _, m_tok, l_tok, p_tok = parts

    movie = await sheets.get_movie_by_token(m_tok)
    title = movie.movie_name if movie else m_tok
    yr_str = f" ({movie.year})" if movie and movie.year else ""
    lang_name, rows = (
        SheetsService.get_platforms_for_language(movie, l_tok) if movie else (None, [])
    )
    target_row = next((r for r in rows if r.platform_token == p_tok), None)
    plat_name = target_row.platform if target_row else p_tok
    link_str = target_row.watch_link if target_row and target_row.watch_link else "N/A"

    await cb.answer(
        f"✅ {plat_name} की रिपोर्ट एडमिन को भेज दी गई है!", show_alert=True
    )

    uname = f"@{cb.from_user.username}" if cb.from_user.username else "No Username"
    await _send_to_admin_log(
        bot,
        settings,
        f"❌ <b>Platform Link Not Available</b>\n\n"
        f"• Movie: <b>{html.escape(title)}{html.escape(yr_str)}</b>\n"
        f"• Language: <b>{html.escape(lang_name or 'N/A')}</b>\n"
        f"• Platform: <b>{html.escape(plat_name)}</b>\n"
        f"• Link: {html.escape(link_str)}\n"
        f"• User: {html.escape(cb.from_user.full_name)} ({html.escape(uname)}) | <code>{cb.from_user.id}</code>",
    )


@router.callback_query(F.data.startswith("req:"))
async def cb_req(cb: CallbackQuery, bot: Bot, settings: Settings, sheets: SheetsService) -> None:
    if not cb.data or not cb.from_user:
        await cb.answer()
        return
    payload = request_cache.get(cb.data.split(":", 1)[1])
    if not payload:
        await cb.answer("Expired. Type movie name again.", show_alert=True)
        return
    movie_name, pref_lang = payload
    await cb.answer("Saving...")
    rec = await sheets.create_request(
        movie_name, pref_lang, cb.from_user.id, cb.from_user.username, cb.from_user.full_name
    )
    await _safe_edit(cb, f"✅ Request Saved: <b>{html.escape(rec.requested_movie)}</b> (<code>{html.escape(rec.request_id)}</code>)")
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Added", callback_data=f"st:{rec.request_id}:a"),
        InlineKeyboardButton(text="❌ Unavailable", callback_data=f"st:{rec.request_id}:u"),
    )
    await _send_to_admin_log(
        bot,
        settings,
        f"🆕 <b>Movie Request</b>\n• ID: <code>{html.escape(rec.request_id)}</code>\n"
        f"• Movie: <b>{html.escape(rec.requested_movie)}</b>\n• By: {html.escape(rec.requested_by)}",
        b.as_markup(),
    )


@router.callback_query(F.data.startswith("st:"))
async def cb_status(cb: CallbackQuery, bot: Bot, settings: Settings, sheets: SheetsService) -> None:
    if not cb.from_user or cb.from_user.id not in settings.admin_chat_ids:
        await cb.answer("⛔ Admin only.", show_alert=True)
        return
    if not cb.data:
        await cb.answer()
        return
    _, req_id, code = cb.data.split(":", 2)
    new_st = STATUS_CODE_MAP.get(code)
    if not new_st:
        await cb.answer("Invalid status", show_alert=True)
        return
    await cb.answer("Updating...")
    updated = await sheets.update_request_status(req_id, new_st)
    if not updated:
        await _safe_edit(cb, f"❌ {html.escape(req_id)} not found.")
        return
    if updated.requester_user_id and new_st != "Pending":
        try:
            await bot.send_message(
                updated.requester_user_id,
                f"🔔 Request <b>{html.escape(updated.requested_movie)}</b> (<code>{html.escape(updated.request_id)}</code>) → <b>{html.escape(updated.status)}</b>",
            )
        except Exception:
            pass
    await _safe_edit(cb, f"✅ <code>{html.escape(updated.request_id)}</code> → <b>{html.escape(updated.status)}</b>")


@router.message(F.text & ~F.text.startswith("/"))
async def handle_text(message: Message, bot: Bot, settings: Settings, sheets: SheetsService) -> None:
    if message.text:
        await _run_search(message, message.text, bot, settings, sheets)


async def _health_check(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_health_server(port: int) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _health_check)
    app.router.add_get("/health", _health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    return runner


async def main() -> None:
    settings = load_settings()
    sheets = SheetsService(settings.spreadsheet_id, settings.google_credentials_file)
    bot = Bot(token=settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    runner = await start_health_server(settings.port)
    try:
        logger.info("Starting AajKyaDekhu_Bot polling...")
        await dp.start_polling(bot, settings=settings, sheets=sheets)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("AajKyaDekhu_Bot stopped.")
