"""
AajKyaDekhu_Bot — Telegram movie discovery bot backed by Google Sheets.

Tabs expected in the Google Spreadsheet:
  1. Bot_Lookup_Rows:
     Movie_ID | Movie_Name | Year | Language | Platform | Watch_Link | Channel_Source
  2. Requests:
     Request_ID | Requested_Movie | Preferred_Language | Requested_By | Request_Date | Status | Notes
"""

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
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Logging & Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("AajKyaDekhu_Bot")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CATALOG_TAB = "Bot_Lookup_Rows"
REQUESTS_TAB = "Requests"

YEAR_RE = re.compile(r"^(19\d{2}|20\d{2})$")
UID_RE = re.compile(r"uid:(\d+)")

STATUS_CODE_MAP = {
    "p": "Pending",
    "a": "Added",
    "u": "Unavailable",
}


@dataclass(frozen=True)
class Settings:
    bot_token: str
    spreadsheet_id: str
    google_credentials_file: str
    admin_chat_ids: tuple[int, ...]
    webapp_url: Optional[str] = None


def _parse_admin_ids(raw: Optional[str]) -> tuple[int, ...]:
    if not raw:
        return ()
    ids: list[int] = []
    for part in raw.split(","):
        cleaned = part.strip()
        if cleaned and cleaned.lstrip("-").isdigit():
            ids.append(int(cleaned))
    return tuple(ids)


def load_settings() -> Settings:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    google_credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
    admin_chat_ids = _parse_admin_ids(os.getenv("ADMIN_CHAT_ID"))
    webapp_url = os.getenv("WEBAPP_URL", "").strip() or None

    missing: list[str] = []
    if not bot_token:
        missing.append("BOT_TOKEN")
    if not spreadsheet_id:
        missing.append("SPREADSHEET_ID")
    if not google_credentials_file:
        missing.append("GOOGLE_CREDENTIALS_FILE")

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    return Settings(
        bot_token=bot_token,
        spreadsheet_id=spreadsheet_id,
        google_credentials_file=google_credentials_file,
        admin_chat_ids=admin_chat_ids,
        webapp_url=webapp_url,
    )


# ---------------------------------------------------------------------------
# Data Models & Token Helpers
# ---------------------------------------------------------------------------


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
        if base:
            return base
        return f"{self.movie_name.strip().lower()}::{self.year.strip()}"

    @property
    def movie_token(self) -> str:
        return short_hash(self.movie_key, length=10)

    @property
    def language_token(self) -> str:
        return short_hash(self.language, length=10)


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
        match = UID_RE.search(self.requested_by)
        return int(match.group(1)) if match else None


class SheetsError(Exception):
    """Raised when Google Sheets operations fail."""


# ---------------------------------------------------------------------------
# Google Sheets Service (Thread-offloaded, Crash-Resistant)
# ---------------------------------------------------------------------------


class SheetsService:
    def __init__(self, spreadsheet_id: str, credentials_file: str) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._credentials_file = credentials_file
        self._client: Optional[gspread.Client] = None

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        if self._client is None:
            creds = Credentials.from_service_account_file(
                self._credentials_file,
                scopes=SCOPES,
            )
            self._client = gspread.authorize(creds)
        return self._client.open_by_key(self._spreadsheet_id)

    def _get_worksheet(self, title: str) -> gspread.Worksheet:
        return self._get_spreadsheet().worksheet(title)

    @staticmethod
    def _clean_record(raw: dict[str, object]) -> dict[str, str]:
        return {str(k).strip(): str(v).strip() for k, v in raw.items()}

    def _fetch_catalog_sync(self) -> list[CatalogRow]:
        ws = self._get_worksheet(CATALOG_TAB)
        records = ws.get_all_records(default_blank="")
        rows: list[CatalogRow] = []
        for raw in records:
            r = self._clean_record(raw)
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
            logger.exception("Error reading %s tab from Google Sheets", CATALOG_TAB)
            raise SheetsError(str(exc)) from exc

    @staticmethod
    def group_movies(rows: list[CatalogRow]) -> list[MovieSummary]:
        grouped: dict[str, list[CatalogRow]] = {}
        for row in rows:
            grouped.setdefault(row.movie_key, []).append(row)

        summaries: list[MovieSummary] = []
        for key, group_rows in grouped.items():
            first = group_rows[0]
            summaries.append(
                MovieSummary(
                    movie_key=key,
                    movie_token=short_hash(key, length=10),
                    movie_id=first.movie_id,
                    movie_name=first.movie_name,
                    year=first.year,
                    rows=tuple(group_rows),
                )
            )
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
        year: Optional[str] = None
        language: Optional[str] = None
        platform: Optional[str] = None
        has_kv = False

        for token in tokens:
            if "=" in token:
                key, val = token.split("=", 1)
                k = key.strip().lower()
                v = val.strip()
                if v:
                    if k in ("year", "y"):
                        year = v
                        has_kv = True
                        continue
                    if k in ("language", "lang", "l"):
                        language = v
                        has_kv = True
                        continue
                    if k in ("platform", "plat", "p"):
                        platform = v
                        has_kv = True
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

        # If the full query directly matches a movie name (e.g. "2012" or "English Patient"),
        # keep it intact as the movie title instead of splitting off tokens.
        full_lower = cleaned.lower()
        if any(
            row.movie_name.lower() == full_lower or full_lower in row.movie_name.lower()
            for row in catalog
        ):
            return SearchFilters(movie_query=cleaned)

        known_langs = {row.language.lower(): row.language for row in catalog if row.language}
        known_plats = {row.platform.lower(): row.platform for row in catalog if row.platform}

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
            movie_query=movie_query,
            year=year,
            language=language,
            platform=platform,
        )

    async def search_movies(self, raw_query: str) -> SearchResult:
        catalog = await self.fetch_catalog()
        filters = self.parse_query(raw_query, catalog)

        filtered = catalog
        if filters.year:
            filtered = [r for r in filtered if r.year.lower() == filters.year.lower()]
        if filters.language:
            lang_q = filters.language.lower()
            filtered = [r for r in filtered if lang_q in r.language.lower()]
        if filters.platform:
            plat_q = filters.platform.lower()
            filtered = [r for r in filtered if plat_q in r.platform.lower()]

        exact_match = False
        if filters.movie_query:
            q = filters.movie_query.lower()
            exact_rows = [r for r in filtered if r.movie_name.lower() == q]
            if exact_rows:
                matched_rows = exact_rows
                exact_match = True
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
        for movie in self.group_movies(catalog):
            if movie.movie_token == movie_token:
                return movie
        return None

    @staticmethod
    def get_unique_languages(movie: MovieSummary) -> list[str]:
        seen: set[str] = set()
        languages: list[str] = []
        for row in movie.rows:
            low = row.language.lower()
            if low not in seen:
                seen.add(low)
                languages.append(row.language)
        return languages

    @staticmethod
    def get_platforms_for_language(
        movie: MovieSummary, language_token: str
    ) -> tuple[Optional[str], list[CatalogRow]]:
        rows = [row for row in movie.rows if row.language_token == language_token]
        if not rows:
            return None, []
        return rows[0].language, rows

    def _create_request_sync(
        self,
        requested_movie: str,
        preferred_language: str,
        user_id: int,
        username: Optional[str],
        full_name: str,
        notes: str = "",
    ) -> RequestRecord:
        ws = self._get_worksheet(REQUESTS_TAB)
        existing_ids: set[str] = set()
        try:
            for rec in ws.get_all_records(default_blank=""):
                rid = str(rec.get("Request_ID", "")).strip().upper()
                if rid:
                    existing_ids.add(rid)
        except Exception:
            existing_ids = set()

        request_id = ""
        for _ in range(30):
            candidate = f"REQ-{random.randint(1000, 9999)}"
            if candidate not in existing_ids:
                request_id = candidate
                break
        if not request_id:
            request_id = f"REQ-{random.randint(10000, 99999)}"

        identity = f"@{username}" if username else full_name
        requested_by = f"uid:{user_id} | {identity}"
        request_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        status = "Pending"

        ws.append_row(
            [
                request_id,
                requested_movie,
                preferred_language or "Any",
                requested_by,
                request_date,
                status,
                notes,
            ],
            value_input_option="USER_ENTERED",
        )

        return RequestRecord(
            request_id=request_id,
            requested_movie=requested_movie,
            preferred_language=preferred_language or "Any",
            requested_by=requested_by,
            request_date=request_date,
            status=status,
            notes=notes,
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
        ws = self._get_worksheet(REQUESTS_TAB)
        records = ws.get_all_records(default_blank="")
        output: list[RequestRecord] = []
        for raw in records:
            r = self._clean_record(raw)
            rid = r.get("Request_ID", "")
            if not rid:
                continue
            output.append(
                RequestRecord(
                    request_id=rid,
                    requested_movie=r.get("Requested_Movie", ""),
                    preferred_language=r.get("Preferred_Language", "") or "Any",
                    requested_by=r.get("Requested_By", ""),
                    request_date=r.get("Request_Date", ""),
                    status=r.get("Status", "") or "Pending",
                    notes=r.get("Notes", ""),
                )
            )
        return output

    async def get_user_requests(
        self, user_id: int, username: Optional[str]
    ) -> list[RequestRecord]:
        try:
            all_reqs = await asyncio.to_thread(self._fetch_requests_sync)
        except Exception as exc:
            logger.exception("Error reading user requests from %s", REQUESTS_TAB)
            raise SheetsError(str(exc)) from exc

        uid_marker = f"uid:{user_id}"
        uname_marker = f"@{username.lower()}" if username else None

        matched = [
            rec
            for rec in all_reqs
            if uid_marker in rec.requested_by
            or (uname_marker and uname_marker in rec.requested_by.lower())
        ]
        matched.reverse()
        return matched

    async def get_pending_requests(self) -> list[RequestRecord]:
        try:
            all_reqs = await asyncio.to_thread(self._fetch_requests_sync)
        except Exception as exc:
            logger.exception("Error reading pending requests from %s", REQUESTS_TAB)
            raise SheetsError(str(exc)) from exc

        return [
            rec
            for rec in all_reqs
            if rec.status.strip().lower() in ("pending", "")
        ]

    def _update_request_status_sync(
        self, request_id: str, new_status: str
    ) -> Optional[RequestRecord]:
        ws = self._get_worksheet(REQUESTS_TAB)
        all_values = ws.get_all_values()
        if not all_values:
            return None

        headers = [h.strip() for h in all_values[0]]
        try:
            id_col = headers.index("Request_ID") + 1
            status_col = headers.index("Status") + 1
        except ValueError as exc:
            raise SheetsError(
                "Requests sheet is missing 'Request_ID' or 'Status' header."
            ) from exc

        def _col_idx(col_name: str) -> Optional[int]:
            return headers.index(col_name) if col_name in headers else None

        target = request_id.strip().upper()
        for row_num, row in enumerate(all_values[1:], start=2):
            cell_id = row[id_col - 1].strip().upper() if len(row) >= id_col else ""
            if cell_id == target:
                ws.update_cell(row_num, status_col, new_status.strip())

                def _val(col_name: str) -> str:
                    idx = _col_idx(col_name)
                    if idx is None or idx >= len(row):
                        return ""
                    return row[idx].strip()

                return RequestRecord(
                    request_id=cell_id,
                    requested_movie=_val("Requested_Movie"),
                    preferred_language=_val("Preferred_Language") or "Any",
                    requested_by=_val("Requested_By"),
                    request_date=_val("Request_Date"),
                    status=new_status.strip(),
                    notes=_val("Notes"),
                )
        return None

    async def update_request_status(
        self, request_id: str, new_status: str
    ) -> Optional[RequestRecord]:
        try:
            return await asyncio.to_thread(
                self._update_request_status_sync, request_id, new_status
            )
        except SheetsError:
            raise
        except Exception as exc:
            logger.exception("Error updating status for %s", request_id)
            raise SheetsError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Request Token Cache (keeps callback_data well under 64 bytes)
# ---------------------------------------------------------------------------


class RequestTokenCache:
    def __init__(self, max_size: int = 2000) -> None:
        self._max_size = max_size
        self._store: OrderedDict[str, tuple[str, str]] = OrderedDict()

    def put(self, movie_name: str, preferred_language: str = "Any") -> str:
        key = f"{movie_name.strip().lower()}::{preferred_language.strip().lower()}"
        token = short_hash(key, length=12)
        self._store[token] = (movie_name.strip(), preferred_language.strip() or "Any")
        self._store.move_to_end(token)
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)
        return token

    def get(self, token: str) -> Optional[tuple[str, str]]:
        return self._store.get(token)


request_cache = RequestTokenCache()
router = Router()


# ---------------------------------------------------------------------------
# Formatting & Keyboard Builders
# ---------------------------------------------------------------------------


def _is_valid_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def _format_movie_header(movie: MovieSummary) -> str:
    year_str = f" ({html.escape(movie.year)})" if movie.year else ""
    return f"🎬 <b>{html.escape(movie.movie_name)}{year_str}</b>"


def _build_request_keyboard(
    movie_name: str, preferred_language: str = "Any"
) -> InlineKeyboardMarkup:
    token = request_cache.put(movie_name, preferred_language)
    builder = InlineKeyboardBuilder()
    builder.button(text="📩 Request Movie", callback_data=f"req:{token}")
    return builder.as_markup()


def _build_movie_picker_keyboard(movies: tuple[MovieSummary, ...]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for movie in movies[:25]:
        year_str = f" ({movie.year})" if movie.year else ""
        label = f"🎬 {movie.movie_name}{year_str}"
        builder.button(text=label[:64], callback_data=f"mv:{movie.movie_token}")
    builder.adjust(1)
    return builder.as_markup()


def _build_languages_keyboard(movie: MovieSummary) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for lang in SheetsService.get_unique_languages(movie):
        lang_tok = short_hash(lang, length=10)
        builder.button(
            text=f"🌐 {lang}"[:64],
            callback_data=f"lg:{movie.movie_token}:{lang_tok}",
        )
    builder.adjust(2)
    return builder.as_markup()


def _build_platforms_message_and_keyboard(
    movie: MovieSummary,
    language_name: str,
    rows: list[CatalogRow],
) -> tuple[str, InlineKeyboardMarkup]:
    builder = InlineKeyboardBuilder()
    platform_blocks: list[str] = []

    for row in rows:
        plat = html.escape(row.platform)
        link = row.watch_link.strip()
        if _is_valid_http_url(link):
            platform_blocks.append(f"<b>{plat}</b>\n{html.escape(link)}")
            builder.row(
                InlineKeyboardButton(
                    text=f"▶️ {row.platform}"[:64],
                    url=link,
                )
            )
        else:
            source_hint = (
                f" (Source: {html.escape(row.channel_source)})"
                if row.channel_source
                else ""
            )
            platform_blocks.append(
                f"<b>{plat}</b>\n<i>Link currently unavailable{source_hint}</i>"
            )

    builder.row(
        InlineKeyboardButton(
            text="⬅️ Back to Languages",
            callback_data=f"mv:{movie.movie_token}",
        )
    )

    body = "\n\n".join(platform_blocks)
    text = (
        f"{_format_movie_header(movie)}\n"
        f"🌐 Language: <b>{html.escape(language_name)}</b>\n\n"
        f"{body}"
    )
    return text, builder.as_markup()


def _build_admin_requests_view(
    pending: list[RequestRecord],
) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    if not pending:
        return "✅ No pending movie requests right now.", None

    builder = InlineKeyboardBuilder()
    lines = [f"🛠 <b>Pending Movie Requests ({len(pending)})</b>\n"]

    # Show up to 8 pending requests per message to stay well within Telegram's limits
    for rec in pending[:8]:
        lines.append(
            f"• <code>{html.escape(rec.request_id)}</code> — "
            f"<b>{html.escape(rec.requested_movie)}</b> "
            f"({html.escape(rec.preferred_language)})\n"
            f"  By: {html.escape(rec.requested_by)} | {html.escape(rec.request_date)}"
        )
        builder.row(
            InlineKeyboardButton(
                text=f"⏳ {rec.request_id}",
                callback_data=f"st:{rec.request_id}:p",
            ),
            InlineKeyboardButton(
                text="✅ Added",
                callback_data=f"st:{rec.request_id}:a",
            ),
            InlineKeyboardButton(
                text="❌ Unavailable",
                callback_data=f"st:{rec.request_id}:u",
            ),
        )

    if len(pending) > 8:
        lines.append(f"\n<i>Showing first 8 of {len(pending)} pending requests.</i>")

    return "\n\n".join(lines), builder.as_markup()


async def _safe_edit_or_reply(
    callback: CallbackQuery,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    if not callback.message:
        return
    try:
        await callback.message.edit_text(
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        await callback.message.answer(
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )


# ---------------------------------------------------------------------------
# Bot Command & Message Handlers
# ---------------------------------------------------------------------------


@router.message(CommandStart())
async def cmd_start(message: Message, settings: Settings) -> None:
    name = html.escape(message.from_user.first_name if message.from_user else "there")
    text = (
        f"👋 Hey <b>{name}</b>, welcome to <b>AajKyaDekhu</b>!\n\n"
        "Send any <b>movie name</b> to find where to watch it.\n\n"
        "<b>Search examples:</b>\n"
        "• <code>Inception</code>\n"
        "• <code>Inception 2010 English</code>\n"
        "• <code>/search Inception Year=2010 Language=English Platform=Netflix</code>\n\n"
        "<b>Commands:</b>\n"
        "• /myrequests — Check the status of your requested movies\n"
        "• /help — Show search & command tips"
    )

    reply_markup: Optional[InlineKeyboardMarkup] = None
    if settings.webapp_url and _is_valid_http_url(settings.webapp_url):
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="Open",
                web_app=WebAppInfo(url=settings.webapp_url),
            )
        )
        reply_markup = builder.as_markup()

    await message.answer(text, reply_markup=reply_markup)


@router.message(Command("help"))
async def cmd_help(message: Message, settings: Settings) -> None:
    is_admin = bool(message.from_user and message.from_user.id in settings.admin_chat_ids)
    text = (
        "📖 <b>AajKyaDekhu_Bot Help</b>\n\n"
        "• Type a movie name directly (e.g. <code>3 Idiots</code>)\n"
        "• Add inline filters (e.g. <code>Inception 2010 English</code>)\n"
        "• Use explicit filters:\n"
        "  <code>/search MovieName Year=2010 Language=English Platform=Netflix</code>\n"
        "• View your submitted requests: /myrequests"
    )
    if is_admin:
        text += (
            "\n\n<b>Admin Commands:</b>\n"
            "• /requests — View pending requests with status buttons"
        )
    await message.answer(text)


async def _run_movie_search(
    message: Message, raw_query: str, sheets: SheetsService
) -> None:
    query = raw_query.strip()
    if not query:
        await message.answer(
            "Please enter a movie name or search filter.\n"
            "Example: <code>/search Inception Year=2010 Language=English</code>"
        )
        return

    try:
        result = await sheets.search_movies(query)
    except SheetsError:
        await message.answer(
            "⚠️ Temporarily unable to reach our movie catalog. Please try again in a moment."
        )
        return

    filters = result.filters

    if not result.movies:
        requested_title = filters.movie_query or query
        pref_lang = filters.language or "Any"
        kb = _build_request_keyboard(requested_title, pref_lang)
        if filters.has_facets:
            await message.answer(
                "It's not in our records for those filters "
                f"(<i>{html.escape(filters.describe())}</i>).\n\n"
                "Tap below if you'd like to request this movie:",
                reply_markup=kb,
            )
        else:
            await message.answer(
                "It's not in our records.\n\n"
                "Tap below to request it:",
                reply_markup=kb,
            )
        return

    if len(result.movies) > 1:
        extra = ""
        if len(result.movies) > 25:
            extra = "\n<i>Showing the first 25 matches. Add a year or full title to narrow results.</i>"
        await message.answer(
            f"🔎 Found <b>{len(result.movies)}</b> matching movies. Choose one:{extra}",
            reply_markup=_build_movie_picker_keyboard(result.movies),
        )
        return

    # Single movie match
    movie = result.movies[0]

    # If the user already specified a valid language (or platform) in their search,
    # skip language selection and display the platform links directly.
    if filters.language or filters.platform:
        matched_rows = [
            r for r in result.matching_rows if r.movie_key == movie.movie_key
        ]
        # Group by language if multiple languages matched a platform-only filter
        unique_langs: list[str] = []
        seen_langs: set[str] = set()
        for r in matched_rows:
            if r.language.lower() not in seen_langs:
                seen_langs.add(r.language.lower())
                unique_langs.append(r.language)

        if len(unique_langs) == 1:
            text, kb = _build_platforms_message_and_keyboard(
                movie, unique_langs[0], matched_rows
            )
            await message.answer(
                text, reply_markup=kb, disable_web_page_preview=True
            )
            return

    await message.answer(
        f"{_format_movie_header(movie)}\n\nSelect a language:",
        reply_markup=_build_languages_keyboard(movie),
    )


@router.message(Command("search"))
async def cmd_search(
    message: Message, command: CommandObject, sheets: SheetsService
) -> None:
    await _run_movie_search(message, command.args or "", sheets)


@router.message(Command("myrequests"))
async def cmd_myrequests(message: Message, sheets: SheetsService) -> None:
    if not message.from_user:
        return
    try:
        records = await sheets.get_user_requests(
            user_id=message.from_user.id,
            username=message.from_user.username,
        )
    except SheetsError:
        await message.answer(
            "⚠️ Could not fetch your requests right now. Please try again shortly."
        )
        return

    if not records:
        await message.answer(
            "📭 You haven't submitted any movie requests yet.\n"
            "Search for a movie first — if it's missing, tap <b>Request Movie</b>."
        )
        return

    lines = ["📋 <b>Your Movie Requests</b>\n"]
    for rec in records[:20]:
        s_low = rec.status.lower()
        if "add" in s_low or "avail" in s_low and "un" not in s_low:
            icon = "✅"
        elif "unavail" in s_low or "not" in s_low or "reject" in s_low:
            icon = "❌"
        else:
            icon = "⏳"

        note_line = f"\n   <i>Note: {html.escape(rec.notes)}</i>" if rec.notes else ""
        lines.append(
            f"{icon} <b>{html.escape(rec.requested_movie)}</b> "
            f"(<code>{html.escape(rec.request_id)}</code>)\n"
            f"   Language: {html.escape(rec.preferred_language)} • "
            f"Status: <b>{html.escape(rec.status)}</b>"
            f"{note_line}"
        )

    await message.answer("\n\n".join(lines))


@router.message(Command("requests"))
async def cmd_admin_requests(
    message: Message, settings: Settings, sheets: SheetsService
) -> None:
    if not message.from_user or message.from_user.id not in settings.admin_chat_ids:
        await message.answer("⛔ You are not authorized to access admin requests.")
        return

    try:
        pending = await sheets.get_pending_requests()
    except SheetsError:
        await message.answer(
            "⚠️ Failed to load pending requests from Google Sheets."
        )
        return

    text, kb = _build_admin_requests_view(pending)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("mv:"))
async def cb_select_movie(callback: CallbackQuery, sheets: SheetsService) -> None:
    await callback.answer()
    if not callback.data:
        return
    movie_token = callback.data.split(":", 1)[1]

    try:
        movie = await sheets.get_movie_by_token(movie_token)
    except SheetsError:
        await _safe_edit_or_reply(
            callback,
            "⚠️ Could not load movie details right now. Please try searching again.",
        )
        return

    if not movie:
        await _safe_edit_or_reply(
            callback,
            "That movie is no longer available in the catalog. Please search again.",
        )
        return

    await _safe_edit_or_reply(
        callback,
        f"{_format_movie_header(movie)}\n\nSelect a language:",
        reply_markup=_build_languages_keyboard(movie),
    )


@router.callback_query(F.data.startswith("lg:"))
async def cb_select_language(callback: CallbackQuery, sheets: SheetsService) -> None:
    await callback.answer()
    if not callback.data:
        return
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        return
    _, movie_token, lang_token = parts

    try:
        movie = await sheets.get_movie_by_token(movie_token)
    except SheetsError:
        await _safe_edit_or_reply(
            callback,
            "⚠️ Could not load platform links right now. Please try again.",
        )
        return

    if not movie:
        await _safe_edit_or_reply(
            callback,
            "That movie entry could not be found. Please search again.",
        )
        return

    language_name, rows = SheetsService.get_platforms_for_language(movie, lang_token)
    if not language_name or not rows:
        await _safe_edit_or_reply(
            callback,
            f"{_format_movie_header(movie)}\n\nNo platforms found for that language.",
            reply_markup=_build_languages_keyboard(movie),
        )
        return

    text, kb = _build_platforms_message_and_keyboard(movie, language_name, rows)
    await _safe_edit_or_reply(callback, text, reply_markup=kb)


@router.callback_query(F.data.startswith("req:"))
async def cb_request_movie(
    callback: CallbackQuery,
    bot: Bot,
    settings: Settings,
    sheets: SheetsService,
) -> None:
    if not callback.data or not callback.from_user:
        await callback.answer()
        return

    token = callback.data.split(":", 1)[1]
    payload = request_cache.get(token)
    if not payload:
        await callback.answer(
            "Request button expired. Please type the movie name again.",
            show_alert=True,
        )
        return

    requested_movie, preferred_language = payload
    await callback.answer("Saving your request...")

    try:
        record = await sheets.create_request(
            requested_movie=requested_movie,
            preferred_language=preferred_language,
            user_id=callback.from_user.id,
            username=callback.from_user.username,
            full_name=callback.from_user.full_name,
        )
    except SheetsError:
        await _safe_edit_or_reply(
            callback,
            "⚠️ Could not save your request right now. Please try again later.",
        )
        return

    await _safe_edit_or_reply(
        callback,
        f"✅ <b>Movie Request Saved!</b>\n\n"
        f"• Request ID: <code>{html.escape(record.request_id)}</code>\n"
        f"• Movie: <b>{html.escape(record.requested_movie)}</b>\n"
        f"• Preferred Language: {html.escape(record.preferred_language)}\n"
        f"• Status: <b>{html.escape(record.status)}</b>\n\n"
        f"Use /myrequests anytime to check its status.",
    )

    for admin_id in settings.admin_chat_ids:
        try:
            admin_kb = InlineKeyboardBuilder()
            admin_kb.row(
                InlineKeyboardButton(
                    text="✅ Added",
                    callback_data=f"st:{record.request_id}:a",
                ),
                InlineKeyboardButton(
                    text="❌ Unavailable",
                    callback_data=f"st:{record.request_id}:u",
                ),
            )
            await bot.send_message(
                chat_id=admin_id,
                text=(
                    f"🆕 <b>New Movie Request</b>\n\n"
                    f"• ID: <code>{html.escape(record.request_id)}</code>\n"
                    f"• Movie: <b>{html.escape(record.requested_movie)}</b>\n"
                    f"• Language: {html.escape(record.preferred_language)}\n"
                    f"• By: {html.escape(record.requested_by)}"
                ),
                reply_markup=admin_kb.as_markup(),
            )
        except Exception as exc:
            logger.warning("Failed to notify admin %s: %s", admin_id, exc)


@router.callback_query(F.data.startswith("st:"))
async def cb_admin_update_status(
    callback: CallbackQuery,
    bot: Bot,
    settings: Settings,
    sheets: SheetsService,
) -> None:
    if not callback.from_user or callback.from_user.id not in settings.admin_chat_ids:
        await callback.answer("⛔ Admin access required.", show_alert=True)
        return

    if not callback.data:
        await callback.answer()
        return

    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Invalid action.", show_alert=True)
        return

    _, request_id, status_code = parts
    new_status = STATUS_CODE_MAP.get(status_code)
    if not new_status:
        await callback.answer("Unknown status code.", show_alert=True)
        return

    await callback.answer(f"Updating {request_id} → {new_status}...")

    try:
        updated = await sheets.update_request_status(request_id, new_status)
    except SheetsError:
        await callback.message.answer(  # type: ignore[union-attr]
            f"⚠️ Failed to update <code>{html.escape(request_id)}</code> in Google Sheets."
        )
        return

    if not updated:
        await callback.message.answer(  # type: ignore[union-attr]
            f"❌ Request <code>{html.escape(request_id)}</code> was not found."
        )
        return

    # Notify original requester when status changes to Added or Unavailable
    requester_id = updated.requester_user_id
    if requester_id and new_status != "Pending":
        try:
            await bot.send_message(
                chat_id=requester_id,
                text=(
                    f"🔔 <b>Your Movie Request Was Updated</b>\n\n"
                    f"• Request ID: <code>{html.escape(updated.request_id)}</code>\n"
                    f"• Movie: <b>{html.escape(updated.requested_movie)}</b>\n"
                    f"• Status: <b>{html.escape(updated.status)}</b>"
                ),
            )
        except Exception as exc:
            logger.warning("Could not notify requester %s: %s", requester_id, exc)

    # Refresh the pending queue if triggered from /requests, otherwise confirm inline
    try:
        pending = await sheets.get_pending_requests()
        if callback.message and callback.message.text and "Pending Movie Requests" in callback.message.text:
            text, kb = _build_admin_requests_view(pending)
            await _safe_edit_or_reply(callback, text, reply_markup=kb)
            return
    except SheetsError:
        pass

    await _safe_edit_or_reply(
        callback,
        f"✅ Updated <code>{html.escape(updated.request_id)}</code> "
        f"(<b>{html.escape(updated.requested_movie)}</b>) → "
        f"<b>{html.escape(updated.status)}</b>.",
    )


@router.message(F.text & ~F.text.startswith("/"))
async def handle_text_search(message: Message, sheets: SheetsService) -> None:
    if not message.text:
        return
    await _run_movie_search(message, message.text, sheets)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


async def main() -> None:
    settings = load_settings()
    sheets = SheetsService(
        spreadsheet_id=settings.spreadsheet_id,
        credentials_file=settings.google_credentials_file,
    )

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    logger.info("Starting AajKyaDekhu_Bot polling...")
    await dp.start_polling(bot, settings=settings, sheets=sheets)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("AajKyaDekhu_Bot stopped.")
