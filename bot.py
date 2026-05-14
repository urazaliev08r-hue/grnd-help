import os
import sys
import re
import json
import difflib
import sqlite3
import asyncio
import time
import random
import glob
import logging
import hashlib
import hmac
import secrets
from datetime import datetime, timezone, timedelta, time as datetime_time
from typing import Optional, List, Dict, Tuple, Any

# Windows terminalida UTF-8 emojilarini ko'rsatish uchun
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import discord
from dotenv import load_dotenv
from discord.ext import commands, tasks
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GOOGLE_CREDENTIALS = "credentials.json"
ARCHIVE_NOTIFICATION_CHANNEL_ID = os.getenv("ARCHIVE_NOTIFICATION_CHANNEL_ID") # New config

TABLES_FILE = "tables.json"
ROLES_FILE = "roles.json"
ARCHIVE_DB_DIR = "archive_db"
ARCHIVE_MAX_SIZE = 50 * 1024 * 1024  # 50 MB
PASSWORD_CACHE_FILE = "password_cache.json"
PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 240000

ADMINS_COLUMN = 1
HEADER_DATE_ROW = 1
HEADER_LABEL_ROW = 2
DATA_START_ROW = 3

SELECT_TIMEOUT = 60
MESSAGE_TIMEOUT = 120
GSHEETS_MAX_RETRIES = 5
GSHEETS_BASE_DELAY = 1.5

UZ_TZ = timezone(timedelta(hours=5))

DONATE_FIELDS = [
    {"key": "complaints", "title": "Жалобы", "aliases": ["жалобы", "complaints", "shikoyat", "жалoba", "жалоба"]},
    {"key": "fw", "title": "ФВ", "aliases": ["фв", "fw"]},
    {"key": "ml_admin", "title": "МЛ.АДМИН", "aliases": ["мл.админ", "мл админ", "ml.admin", "ml admin", "младмин"]},
    {"key": "grp_gmp", "title": "ГРП/ГМП", "aliases": ["грп/гмп", "грп", "гмп", "grp/gmp", "grp", "gmp"]},
    {"key": "opg", "title": "ОПГ", "aliases": ["опг", "opg"]},
    {"key": "zp", "title": "ЗП", "aliases": ["зп", "zp", "зарплата"]},
    {"key": "extra_zp", "title": "Доп. к ЗП", "aliases": ["доп. к зп", "доп к зп", "доп.к зп", "доп кзп", "extra zp", "extra_zp", "bonus zp"]},
]

LOGIN_ALIASES = ["login", "логин", "логины"]

LVL_COLOR_MAP = {
    1: {"red": 0.33, "green": 1.0, "blue": 1.0}, # #55FFFF
    2: {"red": 0.33, "green": 1.0, "blue": 0.33}, # #55FF55
    3: {"red": 1.0, "green": 1.0, "blue": 0.33}, # #FFFF55
    4: {"red": 1.0, "green": 0.67, "blue": 0.0}, # #FFAA00
    5: {"red": 1.0, "green": 0.33, "blue": 1.0}, # #FF55FF
    6: {"red": 0.7, "green": 0.0, "blue": 1.0}, # Binafsha
}

# =========================================================
# AUTOMATIC TASKS
# =========================================================
@tasks.loop(time=datetime_time(hour=8, minute=0, tzinfo=UZ_TZ))
async def auto_archive_task():
    """Har kuni soat 08:00 da barcha jadvallarni avtomatik arxivlash va kechagi sana tekshiruvi"""
    total_archived_rows = 0
    processed_tables_count = 0
    error_messages = []
    missing_yesterday = []
    notification_channel = None

    now = datetime.now(UZ_TZ)
    yesterday_str = (now - timedelta(days=1)).strftime("%d.%m.%Y")
    now_ts = now.strftime("%d.%m.%Y %H:%M:%S")

    # Bildirishnoma kanalini olish
    if ARCHIVE_NOTIFICATION_CHANNEL_ID:
        try:
            channel_id = int(ARCHIVE_NOTIFICATION_CHANNEL_ID)
            notification_channel = bot.get_channel(channel_id)
            if not notification_channel:
                logger.warning(f"Archive notification channel not found: {channel_id}")
        except ValueError:
            logger.error(f"Invalid ARCHIVE_NOTIFICATION_CHANNEL_ID: {ARCHIVE_NOTIFICATION_CHANNEL_ID}")
        except Exception as e:
            logger.error(f"Error getting notification channel: {e}")

    try:
        tables = load_tables()
        logger.info(f"Avtomatik arxivlash boshlandi: {len(tables)} ta jadval.")
        
        for table in tables:
            processed_tables_count += 1
            try:
                sh = await run_blocking(gs.open_by_key, table["id"])
                ws = await run_blocking(find_online_worksheet, sh)
                values = await run_blocking(ws.get_all_values)
                
                # Kechagi sana ma'lumotlarini tekshirish
                o_col, r_col = find_date_columns_in_values(values, yesterday_str)
                data_found = False
                if o_col or r_col:
                    for r_idx in range(DATA_START_ROW, len(values) + 1):
                        o_v = _cell(values, r_idx, o_col) if o_col else ""
                        r_v = _cell(values, r_idx, r_col) if r_col else ""
                        # Agar katak bo'sh bo'lmasa yoki placeholder bo'lmasa
                        if (o_v and o_v not in ["", "xx:xx"]) or (r_v and r_v not in ["", "xx"]):
                            data_found = True
                            break
                if not data_found:
                    missing_yesterday.append(table["name"])

                if values and len(values) >= DATA_START_ROW:
                    archived_rows = build_archive_rows_from_values(values, table["name"], ws.title, now_ts)
                    if archived_rows:
                        inserted = await run_blocking(archive_insert_many, archived_rows)
                        total_archived_rows += inserted
                        logger.info(f"Auto-arxiv: {table['name']} - {inserted} qator.")
            except Exception as e:
                error_msg = f"Auto-arxiv xatolik ({table.get('name')}): {e}"
                logger.error(error_msg)
                error_messages.append(error_msg)

        # 8:00 Arxivlashdan keyin bazani avtomatik optimallashtirish
        try:
            await run_blocking(archive_maintenance)
        except Exception as e:
            logger.error(f"Auto-maintenance xatolik: {e}")

        # Arxivlash yakunlangandan so'ng bildirishnoma yuborish
        notification_message = f"✅ Avtomatik arxivlash yakunlandi!\n" \
                               f"📚 Jami jadvallar: **{processed_tables_count}**\n" \
                               f"🗂 Jami arxivlangan qatorlar: **{total_archived_rows}**"

        if missing_yesterday:
            m_list = "\n".join([f"• **{name}**" for name in missing_yesterday])
            notification_message += f"\n\n@here ⚠️ **DIQQAT!** Kechagi (**{yesterday_str}**) sana uchun normalar to'ldirilmagan:\n{m_list}\n" \
                                    f"Iltimos, jadvallarni to'ldirib qo'ying!"

        if error_messages:
            notification_message += "\n\n⚠️ **Xatoliklar ro'yxati:**\n" + "\n".join(error_messages)

        if notification_channel:
            try:
                await notification_channel.send(notification_message)
            except Exception as e:
                logger.error(f"Failed to send archive notification to channel {notification_channel.id}: {e}")

    except Exception as e:
        logger.error(f"Global auto-archive xatolik: {e}")
        if notification_channel:
            try:
                await notification_channel.send(f"❌ Avtomatik arxivlashda global xatolik yuz berdi: {e}")
            except Exception as send_e:
                logger.error(f"Failed to send global error notification: {send_e}")

# =========================================================
# DISCORD
# =========================================================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# =========================================================
# GOOGLE AUTH
# =========================================================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS, scope)
gs = gspread.authorize(creds)

# =========================================================
# GSPREAD RETRY / ANTI-QUOTA
# =========================================================
def is_gspread_quota_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        'quota exceeded' in text
        or '429' in text
        or 'read requests per minute per user' in text
        or 'too many requests' in text
        or '503' in text
        or 'service unavailable' in text
        or 'rate limit' in text
    )

def friendly_api_error(exc: Exception) -> str:
    if is_gspread_quota_error(exc):
        return (
            "Google Sheets limitiga urildi (429). 10-20 soniya kutib qayta urinib ko'ring. "
            "Men botga avtomatik retry qo'shdim, lekin juda ko'p ketma-ket komanda bersa vaqtincha limitga tushishi mumkin."
        )
    return str(exc)

def gspread_retry(func, *args, **kwargs):
    last_exc = None
    for attempt in range(GSHEETS_MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if not is_gspread_quota_error(exc):
                raise
            delay = GSHEETS_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.6)
            time.sleep(delay)
    raise last_exc

def _patch_gspread_method(cls, method_name: str):
    original = getattr(cls, method_name, None)
    if not original or getattr(original, '_quota_patched', False):
        return

    def wrapped(self, *args, **kwargs):
        return gspread_retry(original, self, *args, **kwargs)

    wrapped._quota_patched = True
    setattr(cls, method_name, wrapped)

try:
    from gspread.client import Client as _GClient
    from gspread.spreadsheet import Spreadsheet as _GSpreadsheet
    from gspread.worksheet import Worksheet as _GWorksheet

    for _name in ['open_by_key', 'open']:
        _patch_gspread_method(_GClient, _name)

    for _name in ['worksheet', 'worksheets', 'batch_update', 'values_batch_get']:
        _patch_gspread_method(_GSpreadsheet, _name)

    for _name in [
        'get_all_values', 'get', 'batch_get', 'col_values', 'row_values', 'acell', 'cell',
        'update', 'batch_update', 'append_row', 'insert_row', 'delete_rows',
        'update_cell', 'update_cells', 'clear', 'find', 'range'
    ]:
        _patch_gspread_method(_GWorksheet, _name)
except Exception:
    pass

# =========================================================
# BASIC HELPERS
# =========================================================
def norm(s) -> str:
    if s is None:
        return ""
    return str(s).replace("\u200b", "").replace("\ufeff", "").strip()

def norm_key(s) -> str:
    return norm(s).lower()

def nick_key(s) -> str:
    s = norm_key(s)
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s)
    return s

def today_str() -> str:
    return datetime.now(UZ_TZ).strftime("%d.%m.%Y")

def short_date(date_str: str) -> str:
    return datetime.strptime(date_str, "%d.%m.%Y").strftime("%d.%m")

def short_date_loose(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%d.%m.%Y")
    return f"{dt.day}.{dt.month}"

def day_name_ru(date_str: str) -> str:
    names = [
        "Понедельник", "Вторник", "Среда", "Четверг",
        "Пятница", "Суббота", "Воскресенье"
    ]
    dt = datetime.strptime(date_str, "%d.%m.%Y")
    return names[dt.weekday()]

def parse_date_any(s: str) -> Optional[str]:
    s = norm(s).replace("/", ".").replace(",", ".")
    if not s:
        return None

    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2}|\d{4}))?", s)
    if not m:
        return None

    dd = int(m.group(1))
    mm = int(m.group(2))
    yy = m.group(3)

    if yy is None:
        # Dinamik yil: Agar dekabrda bo'lsak va yanvar sanasi kiritilsa - keyingi yilni oladi
        now = datetime.now(UZ_TZ)
        year = now.year
        if mm < now.month - 1 and mm < 3: # Ehtimoliy yangi yil o'tishi
            year += 1
    elif len(yy) == 2:
        year = 2000 + int(yy)
    else:
        year = int(yy)

    try:
        dt = datetime(year, mm, dd)
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return None

def archive_date_key(date_str: str) -> str:
    parsed = parse_date_any(date_str)
    if not parsed:
        raise ValueError(f"Sana noto'g'ri: {date_str}")
    return datetime.strptime(parsed, "%d.%m.%Y").strftime("%Y%m%d")

def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
    date_key = archive_date_key(date_str)
    return archive_date_key(start_date) <= date_key <= archive_date_key(end_date)

def hhmm_to_minutes(s: str) -> int:
    s = norm(s)
    if not re.fullmatch(r"\d{1,2}:\d{2}", s):
        return 0
    h, m = s.split(":")
    return int(h) * 60 + int(m)

def minutes_to_hhmm(total: int) -> str:
    h = total // 60
    m = total % 60
    return f"{h:02d}:{m:02d}"

def hhmm_to_sheet_fraction(value: str) -> float:
    value = norm(value)
    if not re.fullmatch(r"\d{1,2}:\d{2}", value):
        raise ValueError("HH:MM format xato")
    h, m = value.split(":")
    total_minutes = int(h) * 60 + int(m)
    return total_minutes / 1440.0

def parse_duration_to_minutes(value: str) -> int:
    value = norm(value)
    if not value:
        return 0

    if re.fullmatch(r"\d{1,2}:\d{2}", value):
        h, m = value.split(":")
        return int(h) * 60 + int(m)

    try:
        f = float(value)
        return int(round(f * 1440))
    except Exception:
        return 0

def chunk_text(text: str, max_len: int = 1800) -> List[str]:
    parts = []
    current = []
    current_len = 0

    for line in text.splitlines():
        if current_len + len(line) + 1 > max_len:
            parts.append("\n".join(current))
            current = [line]
            current_len = len(line) + 1
        else:
            current.append(line)
            current_len += len(line) + 1

    if current:
        parts.append("\n".join(current))
    return parts

async def safe_delete_message(msg):
    try:
        if msg:
            await msg.delete()
    except Exception:
        pass

async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

def split_nick_parts(nick: str) -> Tuple[str, str]:
    n = norm(nick).replace(" ", "_")
    if "_" not in n:
        return n.lower(), ""
    first, rest = n.split("_", 1)
    return first.lower(), rest.lower()

def calc_needed_blocks(current_minutes: int, current_report: int, req_minutes: int, req_report: int) -> Tuple[int, int]:
    remain_minutes = max(0, req_minutes - current_minutes)
    remain_report = max(0, req_report - current_report)
    return remain_minutes, remain_report

# =========================================================
# JSON FILES
# =========================================================
def ensure_json_file(path: str, default_data=None):
    if default_data is None:
        default_data = []
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_data, f, ensure_ascii=False, indent=2)

def load_json(path: str) -> List[Dict[str, Any]]:
    ensure_json_file(path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, data: List[Dict[str, Any]]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def hash_password_value(password: str) -> Dict[str, Any]:
    password = norm(password)
    if not password:
        raise ValueError("Parol bo'sh bo'lmasligi kerak.")

    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        PASSWORD_HASH_ITERATIONS,
    )
    return {
        "scheme": PASSWORD_HASH_SCHEME,
        "iterations": PASSWORD_HASH_ITERATIONS,
        "salt": salt,
        "digest": digest.hex(),
    }

def is_password_hash_record(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("scheme") == PASSWORD_HASH_SCHEME
        and bool(value.get("salt"))
        and bool(value.get("digest"))
    )

def verify_password_value(stored_value: Any, entered_password: str) -> bool:
    entered_password = norm(entered_password)
    if not entered_password:
        return False

    if isinstance(stored_value, str):
        return hmac.compare_digest(norm(stored_value), entered_password)

    if not is_password_hash_record(stored_value):
        return False

    try:
        iterations = int(stored_value.get("iterations", PASSWORD_HASH_ITERATIONS))
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            entered_password.encode("utf-8"),
            bytes.fromhex(str(stored_value["salt"])),
            iterations,
        )
        return hmac.compare_digest(digest.hex(), str(stored_value["digest"]))
    except Exception:
        return False

def _migrate_password_records(records: List[Dict[str, Any]]) -> bool:
    changed = False

    for record in records:
        if not isinstance(record, dict):
            continue

        stored_password = record.get("password")
        if isinstance(stored_password, str) and norm(stored_password):
            record["password"] = hash_password_value(stored_password)
            changed = True

        if "password_version" not in record:
            record["password_version"] = 1
            changed = True

    return changed

def get_password_version(record: Dict[str, Any]) -> int:
    try:
        return max(1, int(record.get("password_version", 1)))
    except Exception:
        return 1

def load_tables() -> List[Dict[str, Any]]:
    tables = load_json(TABLES_FILE)
    if _migrate_password_records(tables):
        save_tables(tables)
    return tables

def save_tables(data: List[Dict[str, Any]]):
    save_json(TABLES_FILE, data)

def load_roles() -> List[Dict[str, Any]]:
    roles = load_json(ROLES_FILE)
    if _migrate_password_records(roles):
        save_roles(roles)
    return roles

def save_roles(data: List[Dict[str, Any]]):
    save_json(ROLES_FILE, data)

def load_password_cache() -> Dict[str, Dict[str, int]]:
    ensure_json_file(PASSWORD_CACHE_FILE, {})
    with open(PASSWORD_CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    normalized: Dict[str, Dict[str, int]] = {}
    changed = not isinstance(data, dict)

    if isinstance(data, dict):
        for user_id, table_map in data.items():
            user_key = str(user_id)
            normalized_tables: Dict[str, int] = {}

            if isinstance(table_map, dict):
                for table_id, version in table_map.items():
                    table_key = norm(table_id)
                    if not table_key:
                        continue
                    try:
                        normalized_tables[table_key] = max(1, int(version))
                    except Exception:
                        normalized_tables[table_key] = 1
                        changed = True
            elif isinstance(table_map, list):
                changed = True
                for table_id in table_map:
                    table_key = norm(table_id)
                    if table_key:
                        normalized_tables[table_key] = 1
            else:
                changed = True

            if normalized_tables:
                normalized[user_key] = dict(sorted(normalized_tables.items()))

    if changed:
        save_password_cache(normalized)

    return normalized

def save_password_cache(data: Dict[str, Dict[str, int]]):
    with open(PASSWORD_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_table_unlocked_for_user(user_id: int, table_id: str, password_version: int) -> bool:
    cache = load_password_cache()
    return cache.get(str(user_id), {}).get(norm(table_id)) == max(1, int(password_version))

def remember_table_for_user(user_id: int, table_id: str, password_version: int):
    cache = load_password_cache()
    key = str(user_id)
    current = dict(cache.get(key, {}))
    current[norm(table_id)] = max(1, int(password_version))
    cache[key] = dict(sorted(current.items()))
    save_password_cache(cache)

def forget_table_for_user(user_id: int, table_id: str):
    cache = load_password_cache()
    key = str(user_id)
    current = dict(cache.get(key, {}))
    if norm(table_id) in current:
        current.pop(norm(table_id), None)
        if current:
            cache[key] = dict(sorted(current.items()))
        else:
            cache.pop(key, None)
        save_password_cache(cache)

def forget_table_for_all_users(table_id: str):
    cache = load_password_cache()
    changed = False
    table_id = norm(table_id)

    for user_key in list(cache.keys()):
        user_tables = dict(cache.get(user_key, {}))
        if table_id in user_tables:
            user_tables.pop(table_id, None)
            changed = True
            if user_tables:
                cache[user_key] = dict(sorted(user_tables.items()))
            else:
                cache.pop(user_key, None)

    if changed:
        save_password_cache(cache)

def get_table_by_name(name: str) -> Optional[Dict[str, Any]]:
    nk = norm_key(name)
    for t in load_tables():
        if norm_key(t.get("name", "")) == nk:
            return t
    return None

def get_role_by_name(name: str) -> Optional[Dict[str, Any]]:
    nk = norm_key(name)
    for r in load_roles():
        if norm_key(r.get("name", "")) == nk:
            return r
    return None

def get_allowed_role_ids() -> set:
    ids = set()
    for r in load_roles():
        try:
            ids.add(int(r["role_id"]))
        except Exception:
            pass
    return ids

def require_access(ctx) -> Optional[str]:
    if not ctx.guild or not hasattr(ctx.author, "roles"):
        return "❌ Bu komanda faqat server ichida ishlaydi.\n❌ Эта команда работает только внутри сервера."

    allowed = get_allowed_role_ids()
    if not allowed:
        return "❌ Hali ruxsatli role qo'shilmagan.\n❌ Пока не добавлена ни одна разрешённая роль."

    if not any(role.id in allowed for role in ctx.author.roles):
        return "❌ Sizda bu komandani ishlatish uchun ruxsat yo'q.\n❌ У вас нет доступа к этой команде."

    return None

def can_bootstrap_first_role(ctx) -> bool:
    if not ctx.guild or not hasattr(ctx.author, "guild_permissions"):
        return False

    perms = ctx.author.guild_permissions
    return (
        ctx.author.id == ctx.guild.owner_id
        or getattr(perms, "administrator", False)
        or getattr(perms, "manage_guild", False)
    )

# =========================================================
# ARCHIVE DATABASE MANAGEMENT (50MB ROTATE)
# =========================================================
def ensure_archive_dir():
    os.makedirs(ARCHIVE_DB_DIR, exist_ok=True)

def get_archive_files() -> List[str]:
    ensure_archive_dir()
    return sorted(glob.glob(os.path.join(ARCHIVE_DB_DIR, "archive_*.db")))

def get_archive_file_number(db_path: str) -> int:
    match = re.search(r"archive_(\d+)\.db$", norm(db_path))
    if not match:
        return 0
    return int(match.group(1))

def is_archive_db_usable(db_path: str) -> bool:
    try:
        with open(db_path, "rb") as f:
            header = f.read(16)
        if not header.startswith(b"SQLite format 3"):
            return False

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive_records' LIMIT 1"
        )
        exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception:
        return False

def split_archive_files() -> Tuple[List[str], List[str]]:
    valid_files = []
    invalid_files = []

    for db_file in get_archive_files():
        if is_archive_db_usable(db_file):
            valid_files.append(db_file)
        else:
            invalid_files.append(db_file)

    return valid_files, invalid_files

def next_archive_db_path() -> str:
    archive_files = get_archive_files()
    max_num = max((get_archive_file_number(path) for path in archive_files), default=0)
    return os.path.join(ARCHIVE_DB_DIR, f"archive_{max_num + 1:03d}.db")

def get_current_active_db() -> str:
    """50MB gacha fayl, keyin yangi ochadi"""
    ensure_archive_dir()
    archive_files, invalid_files = split_archive_files()

    if invalid_files:
        logger.warning(f"Ignoring {len(invalid_files)} invalid archive database(s)")

    if not archive_files:
        new_db = next_archive_db_path()
        init_single_archive(new_db)
        return new_db

    latest = archive_files[-1]
    latest_size = os.path.getsize(latest)

    if latest_size >= ARCHIVE_MAX_SIZE:
        new_db = next_archive_db_path()
        init_single_archive(new_db)
        logger.info(f"New archive DB created: {new_db}")
        return new_db

    return latest

def init_single_archive(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS archive_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            nick TEXT NOT NULL,
            type TEXT NOT NULL,
            value TEXT NOT NULL,
            spreadsheet_name TEXT,
            sheet_name TEXT,
            archived_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_archive_nick_type_date
        ON archive_records (nick, type, date)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_archive_unique_row
        ON archive_records (date, lower(nick), type, ifnull(spreadsheet_name, ''), ifnull(sheet_name, ''))
    """)

    conn.commit()
    conn.close()

def init_archive_db():
    """Barcha DB larni tekshirish"""
    ensure_archive_dir()
    archive_files, invalid_files = split_archive_files()

    if invalid_files:
        logger.warning(f"Invalid archive DB files ignored: {', '.join(invalid_files)}")

    if not archive_files:
        db_path = get_current_active_db()
        logger.info(f"Archive DB initialized: {db_path}")
    else:
        logger.info(f"Found {len(archive_files)} archive databases")

def archive_insert_many(rows: list[tuple]):
    if not rows:
        return 0

    deduped = {}
    for row in rows:
        key = (row[0], nick_key(row[1]), row[2], norm(row[4]), norm(row[5]))
        deduped[key] = row

    db_path = get_current_active_db()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    cur = conn.cursor()

    inserted = 0
    for row in deduped.values():
        cur.execute("""
            INSERT OR IGNORE INTO archive_records (
                date, nick, type, value, spreadsheet_name, sheet_name, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row)
        inserted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    conn.commit()
    conn.close()
    return inserted

def archive_maintenance():
    """Barcha arxiv fayllarini dublikatlardan tozalash va VACUUM qilish"""
    ensure_archive_dir()
    archive_files, _ = split_archive_files()
    
    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()
            
            # 1. Dublikatlarni o'chirish (date, nick, type, spreadsheet, sheet bo'yicha)
            cur.execute("""
                DELETE FROM archive_records
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM archive_records
                    GROUP BY date, lower(nick), type, ifnull(spreadsheet_name, ''), ifnull(sheet_name, '')
                )
            """)
            
            # Commit qilish
            conn.commit()
            conn.close()
            
            # 2. VACUUM - alohida connection bilan (WAL mode siz)
            # VACUUM tranzaktsiya ichida ishlamaydi, shuning uchun alohida bajariladi
            conn2 = sqlite3.connect(db_file)
            conn2.isolation_level = None  # Auto-commit mode
            conn2.execute("VACUUM")
            conn2.close()
            
        except Exception as e:
            logger.error(f"Maintenance error in {db_file}: {e}")

def archive_delete_records(nick: str, spreadsheet_name: str, record_types: list):
    """Adminning ma'lum turdagi arxiv yozuvlarini o'chirish"""
    ensure_archive_dir()
    archive_files, _ = split_archive_files()
    deleted_total = 0
    for db_file in archive_files:
        try:
            with sqlite3.connect(db_file) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA temp_store=MEMORY;")
                cur = conn.cursor()
                placeholders = ', '.join(['?'] * len(record_types))
                query = f"DELETE FROM archive_records WHERE lower(nick) = lower(?) AND lower(spreadsheet_name) = lower(?) AND type IN ({placeholders})"
                cur.execute(query, (nick, spreadsheet_name, *record_types))
                deleted_total += cur.rowcount
                conn.commit()
        except Exception: continue
    return deleted_total

def archive_update_nick(old_nick: str, new_nick: str, spreadsheet_name: str):
    """Adminning barcha arxiv yozuvlarini yangi nickka o'tkazish"""
    ensure_archive_dir()
    archive_files, _ = split_archive_files()
    updated_total = 0
    for db_file in archive_files:
        try:
            with sqlite3.connect(db_file) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA temp_store=MEMORY;")
                cur = conn.cursor()
                query = "UPDATE archive_records SET nick = ? WHERE lower(nick) = lower(?) AND lower(spreadsheet_name) = lower(?)"
                cur.execute(query, (new_nick, old_nick, spreadsheet_name))
                updated_total += cur.rowcount
                conn.commit()
        except Exception: 
            continue
    return updated_total

def archive_insert_many_to_path(db_path: str, rows: list[tuple]):
    if not rows:
        return 0
    
    deduped = {}
    for row in rows:
        # date, nick, type, spreadsheet_name, sheet_name bo'yicha unikallik
        key = (row[0], nick_key(row[1]), row[2], norm(row[4]), norm(row[5]))
        deduped[key] = row

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    cur = conn.cursor()
    inserted = 0
    for row in deduped.values():
        cur.execute("""
            INSERT OR IGNORE INTO archive_records (
                date, nick, type, value, spreadsheet_name, sheet_name, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row)
        inserted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.commit()
    conn.close()
    return inserted

def archive_query_all_databases(
    nick: str,
    rec_type: str,
    start_date: str,
    end_date: str,
    spreadsheet_name: Optional[str] = None,
):
    """Barcha DB lardan qidirish"""
    ensure_archive_dir()
    archive_files, _invalid_files = split_archive_files()
    start_key = archive_date_key(start_date)
    end_key = archive_date_key(end_date)
    all_rows = []

    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()

            query = """
                SELECT date, nick, type, value, spreadsheet_name, sheet_name, archived_at
                FROM archive_records
                WHERE lower(nick) LIKE lower(?)
                  AND type = ?
                  AND (substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)) >= ?
                  AND (substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)) <= ?
            """
            params: List[Any] = [nick, rec_type, start_key, end_key]

            if spreadsheet_name:
                query += """
                  AND ifnull(spreadsheet_name, '') = ?
                """
                params.append(norm(spreadsheet_name))

            query += """
                ORDER BY substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)
            """

            cur.execute(query, params)

            rows = cur.fetchall()
            all_rows.extend(rows)
            conn.close()
        except Exception as e:
            logger.error(f"Error querying {db_file}: {e}")
            continue
    
    return sorted(all_rows, key=lambda row: (archive_date_key(row[0]), nick_key(row[1]), row[2]))

def archive_get_latest(nick: str, rec_type: str, spreadsheet_name: Optional[str] = None):
    """Eng so'nggi yozuv (barcha DB lardan)"""
    ensure_archive_dir()
    archive_files, _invalid_files = split_archive_files()
    archive_files = sorted(archive_files, reverse=True)

    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()

            query = """
                SELECT date, nick, type, value, spreadsheet_name, sheet_name, archived_at
                FROM archive_records
                WHERE lower(nick) = lower(?)
                  AND type = ?
            """
            params: List[Any] = [nick, rec_type]

            if spreadsheet_name:
                query += """
                  AND ifnull(spreadsheet_name, '') = ?
                """
                params.append(norm(spreadsheet_name))

            query += """
                ORDER BY substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2) DESC,
                         id DESC
                LIMIT 1
            """

            cur.execute(query, params)

            row = cur.fetchone()
            conn.close()

            if row:
                return row
        except Exception as e:
            logger.error(f"Error getting latest from {db_file}: {e}")
            continue

    return None

def archive_get_last_activity(nick: str, spreadsheet_name: str) -> Optional[str]:
    """Admin oxirgi marta qachon norma yoki report bajarganini aniqlaydi"""
    ensure_archive_dir()
    archive_files, _ = split_archive_files()
    # Eng yangi fayllardan boshlab qidiramiz
    for db_file in sorted(archive_files, reverse=True):
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()
            query = """
                SELECT date FROM archive_records 
                WHERE lower(nick) = lower(?) AND type IN ('norma', 'report') 
                AND lower(spreadsheet_name) = lower(?)
                ORDER BY substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2) DESC
                LIMIT 1
            """
            cur.execute(query, (nick, spreadsheet_name))
            row = cur.fetchone()
            conn.close()
            if row:
                return row[0]
        except Exception as e:
            logger.error(f"Error getting last activity: {e}")
            continue
    return None

def archive_get_earliest(nick: str, rec_type: str, spreadsheet_name: Optional[str] = None):
    """Eng eski yozuv (barcha DB lardan)"""
    ensure_archive_dir()
    archive_files, _invalid_files = split_archive_files()
    archive_files = sorted(archive_files) # Sort ascending for earliest

    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()

            query = """
                SELECT date, nick, type, value, spreadsheet_name, sheet_name, archived_at
                FROM archive_records
                WHERE lower(nick) = lower(?)
                  AND type = ?
            """
            params: List[Any] = [nick, rec_type]

            if spreadsheet_name:
                query += """
                  AND ifnull(spreadsheet_name, '') = ?
                """
                params.append(norm(spreadsheet_name))

            query += """
                ORDER BY substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2) ASC,
                         id ASC
                LIMIT 1
            """

            cur.execute(query, params)

            row = cur.fetchone()
            conn.close()

            if row:
                return row
        except Exception as e:
            logger.error(f"Error getting earliest from {db_file}: {e}")
            continue
    return None

def archive_get_penalty_history(nick: str, penalty_type: str, count: int, spreadsheet_name: Optional[str] = None):
    """
    Berilgan admin uchun barcha jazo yozuvlarini sanasi bo'yicha qaytaradi.
    
    Args:
        nick: Admin nick nomi
        penalty_type: Jazo turi ('vig' yoki 'pred')
        count: Kutilayotgan jazolar soni (ishlatilmaydi, lekin API mosligini saqlash uchun)
        spreadsheet_name: Jadval nomi (ixtiyoriy)
    
    Returns:
        List[Tuple[str, str]]: [(sana, sabab), ...] tartiblangan ro'yxat (eng eski birinchi)
    """
    ensure_archive_dir()
    archive_files, _invalid_files = split_archive_files()
    archive_files = sorted(archive_files)  # Eng eski fayllardan boshlash
    
    all_penalties = []
    
    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()
            
            query = """
                SELECT date, value
                FROM archive_records
                WHERE lower(nick) = lower(?)
                  AND type = ?
            """
            params: List[Any] = [nick, penalty_type]
            
            if spreadsheet_name:
                query += """
                  AND ifnull(spreadsheet_name, '') = ?
                """
                params.append(norm(spreadsheet_name))
            
            query += """
                ORDER BY substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2) ASC,
                         id ASC
            """
            
            cur.execute(query, params)
            rows = cur.fetchall()
            conn.close()
            
            for row in rows:
                all_penalties.append((row[0], row[1]))  # (date, reason)
                
        except Exception as e:
            logger.error(f"Error getting penalty history from {db_file}: {e}")
            continue
    
    return all_penalties

def cleanup_old_records(days_to_keep: int = 90):
    """Eski yozuvlarni o'chirish"""
    ensure_archive_dir()
    archive_files, _invalid_files = split_archive_files()

    cutoff_date = (datetime.now(UZ_TZ) - timedelta(days=days_to_keep)).strftime("%d.%m.%Y")
    cutoff_key = archive_date_key(cutoff_date)
    total_deleted = 0

    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()

            cur.execute(
                """
                DELETE FROM archive_records
                WHERE (substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)) < ?
                """,
                (cutoff_key,),
            )
            deleted = cur.rowcount
            total_deleted += deleted

            conn.commit()
            conn.close()
            
            # VACUUM - alohida connection bilan
            if deleted > 0:
                conn2 = sqlite3.connect(db_file)
                conn2.isolation_level = None
                conn2.execute("VACUUM")
                conn2.close()
                
        except Exception as e:
            logger.error(f"Error cleaning {db_file}: {e}")
            continue

    return total_deleted

def get_archive_stats() -> dict:
    """Arxiv statistikasi"""
    ensure_archive_dir()
    archive_files, invalid_files = split_archive_files()

    total_size = sum(os.path.getsize(f) for f in archive_files + invalid_files)
    total_records = 0
    total_files = len(archive_files)

    for db_file in archive_files:
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM archive_records")
            count = cur.fetchone()[0]
            total_records += count
            conn.close()
        except Exception as e:
            logger.error(f"Error getting stats from {db_file}: {e}")
            continue

    if total_size < 1024 * 1024:
        size_str = f"{total_size / 1024:.2f} KB"
    elif total_size < 1024 * 1024 * 1024:
        size_str = f"{total_size / (1024 * 1024):.2f} MB"
    else:
        size_str = f"{total_size / (1024 * 1024 * 1024):.2f} GB"

    return {
        "total_files": total_files,
        "invalid_files": len(invalid_files),
        "total_records": total_records,
        "total_size": size_str,
        "size_bytes": total_size,
        "max_size_mb": ARCHIVE_MAX_SIZE / (1024 * 1024)
    }

# =========================================================
# TABLE / ROLE CRUD
# =========================================================
def add_table_record(name: str, spreadsheet_id: str, password: str, owner_id: int):
    name = norm(name)
    spreadsheet_id = norm(spreadsheet_id)
    password = norm(password)

    # Ssilka bo'lsa ID ni ajratib olish / Извлечение ID из ссылки
    url_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_id)
    if url_match:
        spreadsheet_id = url_match.group(1)

    if not name or not spreadsheet_id or not password:
        raise ValueError("Nom, spreadsheet ID yoki parol bo'sh. / Имя, ID таблицы или пароль пустые.")

    tables = load_tables()
    for t in tables:
        if t["id"] == spreadsheet_id:
            raise ValueError("Bu spreadsheet allaqachon qo'shilgan. / Эта таблица уже добавлена.")
        if norm_key(t["name"]) == norm_key(name):
            raise ValueError("Bu nom bilan spreadsheet allaqachon bor. / Таблица с таким именем уже есть.")

    gs.open_by_key(spreadsheet_id)

    tables.append({
        "name": name,
        "id": spreadsheet_id,
        "password": hash_password_value(password),
        "password_version": 1,
        "owner": owner_id
    })
    save_tables(tables)

def remove_table_record(name: str, requester_id: int):
    tables = load_tables()
    found = None
    for t in tables:
        if norm_key(t["name"]) == norm_key(name):
            found = t
            break

    if not found:
        raise ValueError("Jadval topilmadi. / Таблица не найдена.")
    if int(found.get("owner", 0)) != int(requester_id):
        raise ValueError("Faqat owner jadvalni o'chira oladi. / Только владелец может удалить таблицу.")

    new_tables = [t for t in tables if norm_key(t["name"]) != norm_key(name)]
    save_tables(new_tables)
    forget_table_for_all_users(found["id"])

def update_table_password(name: str, old_password: str, new_password: str, requester_id: int):
    tables = load_tables()
    found = None
    for t in tables:
        if norm_key(t["name"]) == norm_key(name):
            found = t
            break

    if not found:
        raise ValueError("Jadval topilmadi. / Таблица не найдена.")
    if int(found.get("owner", 0)) != int(requester_id):
        raise ValueError("Faqat owner parolni o'zgartira oladi. / Только владелец может менять пароль.")
    if not verify_password_value(found.get("password"), old_password):
        raise ValueError("Eski parol noto'g'ri. / Старый пароль неверный.")
    if not norm(new_password):
        raise ValueError("Yangi parol bo'sh bo'lmasligi kerak. / Новый пароль не должен быть пустым.")

    found["password"] = hash_password_value(new_password)
    found["password_version"] = get_password_version(found) + 1
    save_tables(tables)
    forget_table_for_all_users(found["id"])

def add_role_record(name: str, role_id: int, password: str, owner_id: int):
    name = norm(name)
    password = norm(password)

    if not name or not password:
        raise ValueError("Role nomi yoki parol bo'sh. / Имя роли или пароль пустые.")

    roles = load_roles()
    for r in roles:
        if int(r["role_id"]) == int(role_id):
            raise ValueError("Bu role ID allaqachon qo'shilgan. / Этот role ID уже добавлен.")
        if norm_key(r["name"]) == norm_key(name):
            raise ValueError("Bu nom bilan role allaqachon bor. / Роль с таким именем уже есть.")

    roles.append({
        "name": name,
        "role_id": int(role_id),
        "password": hash_password_value(password),
        "password_version": 1,
        "owner": owner_id
    })
    save_roles(roles)

def remove_role_record(name: str, requester_id: int):
    roles = load_roles()
    found = None
    for r in roles:
        if norm_key(r["name"]) == norm_key(name):
            found = r
            break

    if not found:
        raise ValueError("Role topilmadi. / Роль не найдена.")
    if int(found.get("owner", 0)) != int(requester_id):
        raise ValueError("Faqat owner role ni o'chira oladi. / Только владелец может удалить роль.")

    new_roles = [r for r in roles if norm_key(r["name"]) != norm_key(name)]
    save_roles(new_roles)

def update_role_password(name: str, old_password: str, new_password: str, requester_id: int):
    roles = load_roles()
    found = None
    for r in roles:
        if norm_key(r["name"]) == norm_key(name):
            found = r
            break

    if not found:
        raise ValueError("Role topilmadi. / Роль не найдена.")
    if int(found.get("owner", 0)) != int(requester_id):
        raise ValueError("Faqat owner parolni o'zgartira oladi. / Только владелец может менять пароль.")
    if not verify_password_value(found.get("password"), old_password):
        raise ValueError("Eski parol noto'g'ri. / Старый пароль неверный.")
    if not norm(new_password):
        raise ValueError("Yangi parol bo'sh bo'lmasligi kerak. / Новый пароль не должен быть пустым.")

    found["password"] = hash_password_value(new_password)
    found["password_version"] = get_password_version(found) + 1
    save_roles(roles)

# =========================================================
# INPUT PARSERS
# =========================================================
def parse_bulk_online(text: str) -> Tuple[str, List[Tuple[str, str]]]:
    lines = [norm(x) for x in text.splitlines() if norm(x)]
    if not lines:
        raise ValueError("Ma'lumot kiritilmadi. / Данные не введены.")

    date_str = today_str()
    start_idx = 0
    maybe_date = parse_date_any(lines[0])

    if maybe_date and len(lines[0].split()) == 1:
        date_str = maybe_date
        start_idx = 1

    pairs = []
    for line in lines[start_idx:]:
        m = re.match(r"^(.*?)\s+(\d{1,2}:\d{2})$", line)
        if m:
            pairs.append((norm(m.group(1)), m.group(2)))

    if not pairs:
        raise ValueError("Online format topilmadi. / Формат Online не найден.")
    return date_str, pairs

def parse_bulk_report(text: str) -> Tuple[str, List[Tuple[str, int]]]:
    lines = [norm(x) for x in text.splitlines() if norm(x)]
    if not lines:
        raise ValueError("Ma'lumot kiritilmadi. / Данные не введены.")

    date_str = today_str()
    start_idx = 0
    maybe_date = parse_date_any(lines[0])

    if maybe_date and len(lines[0].split()) == 1:
        date_str = maybe_date
        start_idx = 1

    pairs = []
    for line in lines[start_idx:]:
        m = re.match(r"^(.*?)\s+(-?\d+)$", line)
        if m:
            pairs.append((norm(m.group(1)), int(m.group(2))))

    if not pairs:
        raise ValueError("Report format topilmadi. / Формат Report не найден.")
    return date_str, pairs

def parse_single_user_value(raw_text: str, target: str) -> Tuple[str, str, object]:
    text = norm(raw_text)
    if not text:
        raise ValueError("Ma'lumot kiritilmadi. / Данные не введены.")

    parts = text.split()
    if len(parts) < 2:
        raise ValueError("Format xato. / Неверный формат.")

    date_str = today_str()
    if parse_date_any(parts[0]):
        date_str = parse_date_any(parts[0])
        parts = parts[1:]

    if len(parts) < 2:
        raise ValueError("Format xato. / Неверный формат.")

    value_raw = parts[-1]
    nick = " ".join(parts[:-1]).strip()

    if target == "online":
        if not re.fullmatch(r"\d{1,2}:\d{2}", value_raw):
            raise ValueError("Online HH:MM formatda bo'lishi kerak. / Online должен быть в формате HH:MM.")
        value = value_raw
    else:
        if not re.fullmatch(r"-?\d+", value_raw):
            raise ValueError("Report son bo'lishi kerak. / Report должен быть числом.")
        value = int(value_raw)

    if not nick:
        raise ValueError("Nick topilmadi. / Ник не найден.")

    return date_str, nick, value

def parse_clear_user(raw_text: str) -> Tuple[str, str]:
    text = norm(raw_text)
    if not text:
        raise ValueError("Nick kiritilmadi. / Ник не введён.")

    parts = text.split()
    date_str = today_str()

    if parse_date_any(parts[0]):
        date_str = parse_date_any(parts[0])
        nick = " ".join(parts[1:]).strip()
    else:
        nick = " ".join(parts).strip()

    if not nick:
        raise ValueError("Nick kiritilmadi. / Ник не введён.")
    return date_str, nick

def parse_generic_pairs(text: str) -> List[Tuple[str, str]]:
    lines = [norm(x) for x in text.splitlines() if norm(x)]
    if not lines:
        raise ValueError("Ma'lumot kiritilmadi. / Данные не введены.")

    pairs = []
    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            continue
        value = parts[-1]
        nick = " ".join(parts[:-1]).strip()
        if nick and value:
            pairs.append((nick, value))

    if not pairs:
        raise ValueError("Format topilmadi. / Формат не найден.")
    return pairs

# =========================================================
# SHEET HELPERS (PART 1)
# =========================================================
def _cell(ws_values: List[List[str]], row_1based: int, col_1based: int) -> str:
    r = row_1based - 1
    c = col_1based - 1
    try:
        return norm(ws_values[r][c])
    except Exception:
        return ""

def _date_matches(cell_value: str, target_date_str: str) -> bool:
    if not cell_value:
        return False

    cell_n = norm(cell_value).replace("/", ".").replace(",", ".")
    if cell_n in {short_date(target_date_str), short_date_loose(target_date_str)}:
        return True

    parsed = parse_date_any(cell_n)
    return parsed == target_date_str

def find_date_columns_in_values(values: List[List[str]], date_str: str) -> Tuple[Optional[int], Optional[int]]:
    if not values:
        return None, None

    max_cols = max((len(r) for r in values), default=0)
    if max_cols == 0:
        return None, None

    for col in range(1, max_cols + 1):
        date_cell = _cell(values, HEADER_DATE_ROW, col)
        label_cell = _cell(values, HEADER_LABEL_ROW, col).lower()
        next_label = _cell(values, HEADER_LABEL_ROW, col + 1).lower() if col + 1 <= max_cols else ""

        if _date_matches(date_cell, date_str):
            if ("online" in label_cell or "онлайн" in label_cell) and ("report" in next_label or "репорт" in next_label or "отчет" in next_label):
                return col, col + 1

    for col in range(1, max_cols):
        date_cell = _cell(values, HEADER_DATE_ROW, col)
        if not _date_matches(date_cell, date_str):
            continue
        label_cell = _cell(values, HEADER_LABEL_ROW, col).lower()
        next_label = _cell(values, HEADER_LABEL_ROW, col + 1).lower()
        if ("online" in label_cell or "онлайн" in label_cell) or ("report" in next_label or "репорт" in next_label or "отчет" in next_label):
            return col, col + 1

    return None, None

def find_date_columns(ws, date_str: str) -> Tuple[Optional[int], Optional[int]]:
    return find_date_columns_in_values(ws.get_all_values(), date_str)

def find_column_by_aliases(ws, aliases: List[str], scan_rows: int = 3) -> Optional[int]:
    values = ws.get_all_values()
    if not values:
        return None

    max_cols = max((len(r) for r in values), default=0)
    alias_set = {norm_key(a) for a in aliases}

    for row in range(1, min(scan_rows, len(values)) + 1):
        for col in range(1, max_cols + 1):
            cell_val = norm_key(_cell(values, row, col))
            if cell_val in alias_set:
                return col

    for row in range(1, min(scan_rows, len(values)) + 1):
        for col in range(1, max_cols + 1):
            cell_val = norm_key(_cell(values, row, col))
            for alias in alias_set:
                if alias and alias in cell_val:
                    return col

    return None

def find_header_column_by_aliases(ws, aliases: List[str], scan_rows: int = 3) -> Optional[int]:
    return find_column_by_aliases(ws, aliases, scan_rows)

def build_admin_to_row(ws, admin_col: int = ADMINS_COLUMN, start_row: int = 2) -> Dict[str, int]:
    admins = ws.col_values(admin_col)
    mapping = {}
    for row_idx, val in enumerate(admins, start=1):
        v = norm(val)
        if not v:
            continue
        # Ba'zi listlarda adminlar 2-qatordan boshlangani uchun start_row ishlatamiz
        if row_idx < start_row:
            continue
        # Sarlavha bo'lishi mumkin bo'lgan so'zlarni tashlab ketamiz
        if nick_key(v) in {"admins", "admin", "nickname", "nick", "nik", "ник", "discord", "логин", "login"}:
            continue
        mapping[nick_key(v)] = row_idx
    return mapping

def resolve_exact_or_close_row(nick_to_row: Dict[str, int], input_nick: str) -> Optional[int]:
    key = nick_key(input_nick)
    if key in nick_to_row:
        return nick_to_row[key]

    keys = list(nick_to_row.keys())
    matches = difflib.get_close_matches(key, keys, n=1, cutoff=0.92)
    if matches:
        return nick_to_row[matches[0]]
    return None

def get_real_nick_by_row(ws, row: int, admin_col: int = ADMINS_COLUMN) -> str:
    return norm(ws.cell(row, admin_col).value)

def collect_batch_rename_candidates(
    ws,
    nick_to_row: Dict[str, int],
    pairs: List[Tuple[str, object]],
    admin_col: int = ADMINS_COLUMN
) -> Tuple[List[Tuple[str, object, int]], List[Tuple[str, str, int, object]], List[str]]:
    exact_matches = []
    rename_proposals = []
    not_found = []
    used_rows_for_rename = set()

    for input_nick, value in pairs:
        row = resolve_exact_or_close_row(nick_to_row, input_nick)
        if row:
            exact_matches.append((input_nick, value, row))
            continue

        input_first, _ = split_nick_parts(input_nick)
        same_first = []
        for existing_key, existing_row in nick_to_row.items():
            first, _ = split_nick_parts(existing_key)
            if first == input_first:
                same_first.append((existing_key, existing_row))

        if len(same_first) == 1:
            candidate_key, candidate_row = same_first[0]
            if candidate_row not in used_rows_for_rename:
                old_real = get_real_nick_by_row(ws, candidate_row, admin_col) or candidate_key
                rename_proposals.append((old_real, input_nick, candidate_row, value))
                used_rows_for_rename.add(candidate_row)
                continue

        keys = list(nick_to_row.keys())
        close = difflib.get_close_matches(nick_key(input_nick), keys, n=3, cutoff=0.55)
        if len(close) == 1:
            candidate_row = nick_to_row[close[0]]
            if candidate_row not in used_rows_for_rename:
                old_real = get_real_nick_by_row(ws, candidate_row, admin_col) or close[0]
                rename_proposals.append((old_real, input_nick, candidate_row, value))
                used_rows_for_rename.add(candidate_row)
                continue

        not_found.append(input_nick)

    return exact_matches, rename_proposals, not_found

def get_same_type_format_source_col(target_col: int, max_cols: int) -> Optional[int]:
    if target_col - 2 >= 1:
        return target_col - 2
    if target_col + 2 <= max_cols:
        return target_col + 2
    return None

def find_online_worksheet(sh):
    """Online/Report yoziladigan asosiy listni topish"""
    for ws in sh.worksheets():
        t = ws.title.lower()
        if any(x in t for x in ["online", "онлайн", "актив", "active"]):
            return ws
    # Agar maxsus nom topilmasa, birinchi listni qaytaradi
    return sh.get_worksheet(0)

def copy_column_format(ws, src_col: int, dst_col: int, start_row: int, end_row: int):
    if not src_col or not dst_col or src_col == dst_col or end_row < start_row:
        return

    sheet_id = ws._properties["sheetId"]
    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": src_col - 1,
                        "endColumnIndex": src_col
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": dst_col - 1,
                        "endColumnIndex": dst_col
                    },
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }
    ws.spreadsheet.batch_update(body)

def set_duration_format(ws, col: int, start_row: int, end_row: int):
    if not col or end_row < start_row:
        return
    sheet_id = ws._properties["sheetId"]
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": col - 1,
                        "endColumnIndex": col
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "TIME", "pattern": "[h]:mm"},
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"
                }
            }
        ]
    }
    ws.spreadsheet.batch_update(body)

def apply_custom_formatting(ws, rows: List[int], col: int):
    if not rows or not col:
        return
    sheet_id = ws._properties["sheetId"]
    requests = []
    for r in rows:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": r - 1,
                    "endRowIndex": r,
                    "startColumnIndex": col - 1,
                    "endColumnIndex": col
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "foregroundColor": {
                                "red": 1.0,
                                "green": 1.0,
                                "blue": 1.0
                            }
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat.foregroundColor)"
            }
        })
    if requests:
        ws.spreadsheet.batch_update({"requests": requests})

def apply_level_formatting(ws, row: int, nick_col: int, lvl_col: int, level: int, max_cols: int = 15):
    sheet_id = get_sheet_id(ws)
    color = LVL_COLOR_MAP.get(level, {"red": 1.0, "green": 1.0, "blue": 1.0})
    
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": 0,
                    "endColumnIndex": max_cols
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat.foregroundColor)"
            }
        }
    ]
    
    for col in [nick_col, lvl_col]:
        if col:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row - 1,
                        "endRowIndex": row,
                        "startColumnIndex": col - 1,
                        "endColumnIndex": col
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"foregroundColor": color}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat.foregroundColor"
                }
            })
    ws.spreadsheet.batch_update({"requests": requests})

def rename_nick_in_sheet(ws, row: int, new_nick: str, admin_col: int = ADMINS_COLUMN):
    a1 = gspread.utils.rowcol_to_a1(row, admin_col)
    ws.update(a1, [[new_nick]])

def iter_online_report_pairs_from_values(values: List[List[str]]) -> List[Tuple[int, int, str]]:
    if not values:
        return []

    max_cols = max((len(r) for r in values), default=0)
    pairs: List[Tuple[int, int, str]] = []
    col = 1

    while col < max_cols:
        label1 = norm(_cell(values, HEADER_LABEL_ROW, col)).lower()
        label2 = norm(_cell(values, HEADER_LABEL_ROW, col + 1)).lower()
        parsed_date = parse_date_any(_cell(values, HEADER_DATE_ROW, col))

        if parsed_date and ("online" in label1 or "онлайн" in label1) and ("report" in label2 or "репорт" in label2 or "отчет" in label2):
            pairs.append((col, col + 1, parsed_date))
            col += 2
            continue

        col += 1

    return pairs

def read_day_pairs_from_values(values: List[List[str]], date_str: str):
    online_col, report_col = find_date_columns_in_values(values, date_str)
    if online_col is None or report_col is None:
        raise ValueError(f"{short_date(date_str)} sanasi uchun ustun topilmadi.")

    last_row = len(values)
    if last_row < DATA_START_ROW:
        return [], []

    online_pairs = []
    report_pairs = []

    for row in range(DATA_START_ROW, last_row + 1):
        nick = _cell(values, row, ADMINS_COLUMN)
        if not nick:
            continue

        online_val = _cell(values, row, online_col)
        report_val = _cell(values, row, report_col)

        if online_val:
            online_pairs.append((nick, online_val))
        if report_val:
            report_pairs.append((nick, report_val))

    return online_pairs, report_pairs

def read_day_pairs_from_sheet(ws, date_str: str):
    return read_day_pairs_from_values(ws.get_all_values(), date_str)

def build_archive_rows_from_values(
    values: List[List[str]],
    table_name: str,
    sheet_name: str,
    now_ts: str,
    ws_admin_col: int = ADMINS_COLUMN,
) -> List[tuple]: # Added ws_admin_col parameter
    if not values or len(values) < 2:
        return []

    archived_rows: List[tuple] = []
    last_row = len(values)

    for online_col, report_col, parsed_date in iter_online_report_pairs_from_values(values):
        for row in range(DATA_START_ROW, last_row + 1): # Use ws_admin_col
            nick = _cell(values, row, ws_admin_col)
            if not nick:
                continue

            online_val = _cell(values, row, online_col)
            report_val = _cell(values, row, report_col)

            if online_val:
                archived_rows.append((
                    parsed_date,
                    nick,
                    "norma",
                    str(online_val),
                    table_name,
                    sheet_name,
                    now_ts,
                ))

            if report_val:
                archived_rows.append((
                    parsed_date,
                    nick,
                    "report",
                    str(report_val),
                    table_name,
                    sheet_name,
                    now_ts,
                ))

    return archived_rows

def collect_live_rows_from_values(
    values: List[List[str]],
    nick: str,
    rec_type: str,
    start_date: str,
    end_date: str,
    table_name: str,
    sheet_name: str,
    now_ts: str,
    ws_admin_col: int = ADMINS_COLUMN,
) -> List[tuple]: # Added ws_admin_col parameter
    if not values:
        return []

    nick_to_row: Dict[str, int] = {}
    for row in range(DATA_START_ROW, len(values) + 1):
        current_nick = _cell(values, row, ws_admin_col) # Use ws_admin_col
        if current_nick:
            nick_to_row[nick_key(current_nick)] = row

    row = resolve_exact_or_close_row(nick_to_row, nick)
    if not row:
        return []
    
    real_nick = _cell(values, row, ws_admin_col) or nick # Use ws_admin_col
    result_rows: List[tuple] = []

    for online_col, report_col, parsed_date in iter_online_report_pairs_from_values(values):
        if not is_date_in_range(parsed_date, start_date, end_date):
            continue

        target_col = online_col if rec_type == "norma" else report_col
        value = _cell(values, row, target_col)
        if not value:
            continue

        result_rows.append((
            parsed_date,
            real_nick,
            rec_type,
            str(value),
            table_name,
            sheet_name,
            now_ts,
        ))

    return result_rows

def merge_archive_and_sheet_rows(archive_rows: List[tuple], sheet_rows: List[tuple]) -> List[tuple]:
    merged: Dict[Tuple[str, str, str], tuple] = {}

    for row in archive_rows:
        merged[(row[0], nick_key(row[1]), row[2])] = row

    for row in sheet_rows:
        merged[(row[0], nick_key(row[1]), row[2])] = row

    return sorted(
        merged.values(),
        key=lambda row: (archive_date_key(row[0]), nick_key(row[1]), row[2]),
    )

def get_current_online_report_columns(ws):
    values = ws.get_all_values()
    if not values:
        return None, None

    max_cols = max((len(r) for r in values), default=0)
    best_online = None
    best_report = None

    for col in range(1, max_cols):
        label1 = norm(_cell(values, HEADER_LABEL_ROW, col)).lower()
        label2 = norm(_cell(values, HEADER_LABEL_ROW, col + 1)).lower()
        date_cell = norm(_cell(values, HEADER_DATE_ROW, col))

        if "online" in label1 and "report" in label2 and date_cell:
            best_online = col
            best_report = col + 1

    return best_online, best_report

async def collect_live_data_for_multiple_nicks(sh, nicks: List[str], start_date: str, end_date: str, table_name: str):
    """Barcha listlarni bir marta o'qib, barcha kerakli adminlar uchun ma'lumot yig'adi (Optimallash)"""
    worksheets = await run_blocking(sh.worksheets)
    # natija: {nick_key: {"norma": [], "report": []}}
    cache = {nick_key(n): {"norma": [], "report": []} for n in nicks}
    now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
    
    for ws in worksheets:
        values = await run_blocking(ws.get_all_values)
        if not values or len(values) < 2: continue
        
        pairs = iter_online_report_pairs_from_values(values)
        if not pairs: continue

        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN

        # Ushbu sheetdagi barcha adminlarni mapping qilish
        nick_to_row = {}
        for r in range(DATA_START_ROW, len(values) + 1):
            n_val = _cell(values, r, ws_admin_col)
            if n_val: nick_to_row[nick_key(n_val)] = r

        for nk in cache.keys():
            row = resolve_exact_or_close_row(nick_to_row, nk)
            if not row: continue
            
            real_nick = _cell(values, row, ws_admin_col)
            for o_col, r_col, p_date in pairs:
                if not is_date_in_range(p_date, start_date, end_date): continue
                
                o_val = _cell(values, row, o_col)
                if o_val:
                    cache[nk]["norma"].append((p_date, real_nick, "norma", str(o_val), table_name, ws.title, now_ts))
                
                r_val = _cell(values, row, r_col)
                if r_val:
                    cache[nk]["report"].append((p_date, real_nick, "report", str(r_val), table_name, ws.title, now_ts))
    return cache

async def collect_live_rows_from_all_sheets(sh, nick, rec_type, start_date, end_date, table_name):
    """Jadvaldagi barcha listlardan berilgan nick va muddat bo'yicha ma'lumotlarni yig'adi"""
    worksheets = await run_blocking(sh.worksheets)
    all_rows = []
    now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
    
    for ws in worksheets:
        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN

        try:
            values = await run_blocking(ws.get_all_values)
            if not values or len(values) < 2:
                continue
            
            rows = collect_live_rows_from_values(
                values, nick, rec_type, start_date, end_date, table_name, ws.title, now_ts,
                ws_admin_col=ws_admin_col) # Pass ws_admin_col
            all_rows.extend(rows)
        except Exception:
            continue
    return all_rows

async def collect_all_reports_live_summary(sh, start_date, end_date):
    """Barcha adminlarning ma'lum muddatdagi reportlarini jami ko'rsatkichini yig'adi"""
    worksheets = await run_blocking(sh.worksheets)
    summary = {}
    start_k = archive_date_key(start_date)
    end_k = archive_date_key(end_date)

    for ws in worksheets:
        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN

        try:
            v = await run_blocking(ws.get_all_values)
            if not v or len(v) < 2: continue
            for o_col, r_col, p_date in iter_online_report_pairs_from_values(v):
                p_k = archive_date_key(p_date)
                if not (start_k <= p_k <= end_k): continue
                
                for row_idx in range(DATA_START_ROW, len(v) + 1):
                    nick = _cell(v, row_idx, ws_admin_col) # Use ws_admin_col
                    if not nick: continue
                    val = _cell(v, row_idx, r_col)
                    if not val or str(val).lower() in ["xx", "-", "0"]: continue
                    try:
                        num = int(float(str(val)))
                        nk = nick_key(nick)
                        if nk not in summary: summary[nk] = {"name": nick, "total": 0}
                        summary[nk]["total"] += num
                    except Exception as e: logger.error(f"Error parsing report value for {nick} in {ws.title}: {e}"); continue
        except: continue
    return summary

async def collect_full_weekly_data(sh, table_name, days=7):
    today_dt = datetime.now(UZ_TZ)
    start_dt = (today_dt - timedelta(days=days)).strftime("%d.%m.%Y")
    end_dt = today_dt.strftime("%d.%m.%Y")
    
    worksheets = await run_blocking(sh.worksheets)
    summary = {}

    for ws in worksheets:
        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN

        try:
            v = await run_blocking(ws.get_all_values)
            if not v or len(v) < 2: continue
            pairs = iter_online_report_pairs_from_values(v)
            if not pairs: continue

            for o_col, r_col, p_date in pairs:
                if not is_date_in_range(p_date, start_dt, end_dt): continue
                for row_idx in range(DATA_START_ROW, len(v) + 1): # Use ws_admin_col
                    nick = _cell(v, row_idx, ws_admin_col)
                    if not nick: continue
                    nk = nick_key(nick)
                    if nk not in summary: summary[nk] = {"name": nick, "minutes": 0, "report": 0, "on_leave": False}
                    
                    o_val = _cell(v, row_idx, o_col)
                    if str(o_val).strip() == "-": summary[nk]["on_leave"] = True
                    if o_val: summary[nk]["minutes"] += parse_duration_to_minutes(o_val)
                    
                    r_val = _cell(v, row_idx, r_col)
                    if str(r_val).strip() == "-": summary[nk]["on_leave"] = True
                    if r_val:
                        try: 
                            cleaned_r = str(r_val).replace(',', '.') # Use ws_admin_col
                            summary[nk]["report"] += int(float(cleaned_r))
                        except: pass
        except: continue
    return summary, start_dt, end_dt

def find_donate_worksheet(sh):
    for ws in sh.worksheets():
        try:
            found_count = 0
            for fld in DONATE_FIELDS:
                col = find_header_column_by_aliases(ws, fld["aliases"], scan_rows=3)
                if col:
                    found_count += 1
            if found_count >= 3:
                return ws
        except Exception:
            continue

    raise ValueError("Donate sheet topilmadi. / Лист доната не найден.")

def find_login_worksheet(sh):
    for ws in sh.worksheets():
        try:
            admin_col = find_header_column_by_aliases(
                ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=3
            )
            login_col = find_header_column_by_aliases(ws, LOGIN_ALIASES, scan_rows=3)

            if admin_col and login_col:
                return ws
        except Exception:
            continue

    raise ValueError("Login sheet topilmadi. / Лист с логинами не найден.")

async def find_adminlist_worksheet(sh):
    worksheets = await run_blocking(sh.worksheets)
    # Avval nom bo'yicha qidiramiz (tezroq)
    for ws in worksheets:
        t = ws.title.lower()
        if any(x in t for x in ["список", "admin", "состав", "staff"]):
            return ws
    # Agar topilmasa, ustunlar bo'yicha
    for ws in worksheets:
        v = await run_blocking(ws.get_all_values)
        if not v: continue
        headers = [norm_key(c) for c in v[min(1, len(v)-1)]]
        if any(x in headers for x in ["admins", "admin", "nick", "nickname", "ник"]):
            return ws
    raise ValueError("Adminlar ro'yxati listi topilmadi.")

def clear_column_for_day(ws, date_str: str, target: str):
    online_col, report_col = find_date_columns(ws, date_str)
    if online_col is None or report_col is None:
        raise ValueError(
            f"{short_date(date_str)} sanasi uchun ustun topilmadi.\n"
            f"Для даты {short_date(date_str)} столбцы не найдены."
        )

    target_col = online_col if target == "online" else report_col
    last_row = len(ws.col_values(ADMINS_COLUMN))

    if last_row < DATA_START_ROW:
        return {"cleared": 0, "sheet_title": ws.title, "date": date_str}

    start_a1 = gspread.utils.rowcol_to_a1(DATA_START_ROW, target_col)
    end_a1 = gspread.utils.rowcol_to_a1(last_row, target_col)
    ws.batch_clear([f"{start_a1}:{end_a1}"])

    return {"cleared": max(0, last_row - DATA_START_ROW + 1), "sheet_title": ws.title, "date": date_str}

def clear_day_both(ws, date_str: str):
    online_col, report_col = find_date_columns(ws, date_str)
    if online_col is None or report_col is None:
        raise ValueError(
            f"{short_date(date_str)} sanasi uchun ustun topilmadi.\n"
            f"Для даты {short_date(date_str)} столбцы не найдены."
        )

    last_row = len(ws.col_values(ADMINS_COLUMN))
    if last_row >= DATA_START_ROW:
        ws.batch_clear([
            f"{gspread.utils.rowcol_to_a1(DATA_START_ROW, online_col)}:{gspread.utils.rowcol_to_a1(last_row, online_col)}",
            f"{gspread.utils.rowcol_to_a1(DATA_START_ROW, report_col)}:{gspread.utils.rowcol_to_a1(last_row, report_col)}",
        ])

    return {"cleared": max(0, last_row - DATA_START_ROW + 1), "sheet_title": ws.title, "date": date_str}

async def get_latest_online_report_for_nick(online_ws, nick: str, spreadsheet_name: Optional[str] = None): # Made async
    values = online_ws.get_all_values()
    
    ws_admin_col = await run_blocking(find_header_column_by_aliases, online_ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
    if not ws_admin_col:
        ws_admin_col = ADMINS_COLUMN

    nick_to_row = {}
    for row_idx in range(DATA_START_ROW, len(values) + 1):
        current_nick = _cell(values, row_idx, ws_admin_col) # Use ws_admin_col
        if current_nick:
            nick_to_row[nick_key(current_nick)] = row_idx
    row = resolve_exact_or_close_row(nick_to_row, nick)

    online_raw = ""
    report_raw = ""

    if row:
        for online_col, report_col, _parsed_date in reversed(iter_online_report_pairs_from_values(values)):
            o_val = _cell(values, row, online_col)
            r_val = _cell(values, row, report_col)

            if o_val and o_val not in {"0", "0:00", "0:0"}:
                online_raw = o_val
            if r_val and r_val != "0":
                report_raw = r_val

            if online_raw or report_raw:
                break

    if not online_raw or online_raw in {"0", "0:00", "0:0"}:
        latest_norma = archive_get_latest(nick, "norma", spreadsheet_name=spreadsheet_name)
        if latest_norma:
            online_raw = norm(latest_norma[3])

    if not report_raw or report_raw == "0":
        latest_report = archive_get_latest(nick, "report", spreadsheet_name=spreadsheet_name)
        if latest_report:
            report_raw = norm(latest_report[3])

    online_minutes = parse_duration_to_minutes(online_raw)
    try:
        report_num = int(float(report_raw)) if report_raw else 0
    except Exception:
        report_num = 0

    return online_raw or "0:00", report_num, online_minutes

def parse_penalty_count(value: str) -> int:
    value = norm(value)
    if not value:
        return 0

    m = re.match(r"^\s*(\d+)", value)
    if m:
        return int(m.group(1))

    try:
        return int(float(value))
    except Exception:
        return 0
# =========================================================
# BUTTON VIEWS
# =========================================================
class CompactChoiceButton(discord.ui.Button):
    def __init__(self, index: int):
        super().__init__(label=str(index + 1), style=discord.ButtonStyle.secondary, row=index // 5)
        self.choice_index = index

    async def callback(self, interaction: discord.Interaction):
        view: "CompactChoiceView" = self.view
        if interaction.user.id != view.author_id:
            await interaction.response.send_message("❌ Bu tanlash siz uchun emas.", ephemeral=True)
            return
        view.selected_index = self.choice_index
        view.stop()
        await interaction.response.defer()

class CompactChoiceView(discord.ui.View):
    def __init__(self, author_id: int, option_count: int, timeout: int = SELECT_TIMEOUT):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.selected_index: Optional[int] = None
        for i in range(min(option_count, 25)):
            self.add_item(CompactChoiceButton(i))

class BatchRenameView(discord.ui.View):
    def __init__(self, author_id: int, timeout: int = 60):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.result: Optional[str] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugmalar siz uchun emas.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Hammasini almashtir", style=discord.ButtonStyle.success, row=0)
    async def replace_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "replace_all"
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="⏭ O'tkaz", style=discord.ButtonStyle.secondary, row=0)
    async def skip_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "skip_all"
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="❌ Bekor", style=discord.ButtonStyle.danger, row=0)
    async def cancel_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "cancel"
        self.stop()
        await interaction.response.defer()

class DeleteCommandView(discord.ui.View):
    def __init__(self, author_id: int, command_message: discord.Message, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.command_message = command_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugmalar siz uchun emas.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Ha", style=discord.ButtonStyle.success)
    async def yes_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_delete_message(self.command_message)
        try:
            await interaction.message.delete()
        except Exception:
            pass
        await interaction.response.defer()

    @discord.ui.button(label="❌ Yo'q", style=discord.ButtonStyle.secondary)
    async def no_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.message.delete()
        except Exception:
            pass
        await interaction.response.defer()

class VigChoiceView(discord.ui.View):
    def __init__(self, author_id: int, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.result = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugmalar siz uchun emas.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Tanbeh", style=discord.ButtonStyle.danger)
    async def vig_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "vig"
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Ogohlantirish", style=discord.ButtonStyle.secondary)
    async def pred_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "pred"
        self.stop()
        await interaction.response.defer()

async def choose_number_ctx(ctx, title: str, options: List[str]) -> int:
    if len(options) == 1:
        return 0

    if len(options) > 25:
        raise ValueError("Variantlar soni 25 tadan oshib ketdi. / Слишком много вариантов.")

    lines = [title]
    for i, opt in enumerate(options, start=1):
        lines.append(f"`{i}` {opt}")

    view = CompactChoiceView(ctx.author.id, len(options))
    ask_msg = await ctx.reply("\n".join(lines), view=view)
    await view.wait()

    if view.selected_index is None:
        await safe_delete_message(ask_msg)
        raise TimeoutError("Tanlash vaqti tugadi. / Время выбора истекло.")

    await safe_delete_message(ask_msg)
    return view.selected_index

class SecretValueModal(discord.ui.Modal):
    def __init__(self, future: asyncio.Future, title: str, label: str, placeholder: str):
        super().__init__(title=title)
        self.future = future
        self.secret_input = discord.ui.TextInput(label=label, placeholder=placeholder, required=True)
        self.add_item(self.secret_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.future.done():
            self.future.set_result(norm(self.secret_input.value))
        await interaction.response.send_message("✅ Parol qabul qilindi.", ephemeral=True)

class PasswordPromptView(discord.ui.View):
    def __init__(
        self,
        author_id: int,
        future: asyncio.Future,
        modal_title: str = "Jadval paroli",
        input_label: str = "Parol",
        placeholder: str = "Parolni kiriting",
        timeout: int = MESSAGE_TIMEOUT,
    ):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.future = future
        self.modal_title = modal_title
        self.input_label = input_label
        self.placeholder = placeholder

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugma siz uchun emas.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        if not self.future.done():
            self.future.set_result(None)

    @discord.ui.button(label="Parol kiritish", style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            SecretValueModal(
                self.future,
                title=self.modal_title,
                label=self.input_label,
                placeholder=self.placeholder,
            )
        )

class PasswordChangeModal(discord.ui.Modal):
    def __init__(self, future: asyncio.Future, title: str):
        super().__init__(title=title)
        self.future = future
        self.old_password = discord.ui.TextInput(label="Eski parol", placeholder="Eski parol", required=True)
        self.new_password = discord.ui.TextInput(label="Yangi parol", placeholder="Yangi parol", required=True)
        self.add_item(self.old_password)
        self.add_item(self.new_password)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.future.done():
            self.future.set_result((norm(self.old_password.value), norm(self.new_password.value)))
        await interaction.response.send_message("Qabul qilindi.", ephemeral=True)

class PasswordChangePromptView(discord.ui.View):
    def __init__(self, author_id: int, future: asyncio.Future, title: str, timeout: int = MESSAGE_TIMEOUT):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.future = future
        self.title = title

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("вќЊ Bu tugma siz uchun emas.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        if not self.future.done():
            self.future.set_result((None, None))

    @discord.ui.button(label="Parollarni kiritish", style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PasswordChangeModal(self.future, self.title))

class LevelChoiceButton(discord.ui.Button):
    def __init__(self, label_text: str, result_value: int, style: discord.ButtonStyle, row: int = 0):
        super().__init__(label=label_text, style=style, row=row)
        self.result_value = result_value

    async def callback(self, interaction: discord.Interaction):
        view: "LevelChoiceView" = self.view
        if interaction.user.id != view.author_id:
            await interaction.response.send_message("❌ Bu tanlash siz uchun emas.", ephemeral=True)
            return
        view.result = self.result_value
        view.stop()
        await interaction.response.defer()

class LevelChoiceView(discord.ui.View):
    def __init__(self, author_id: int, timeout: int = SELECT_TIMEOUT):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.result: Optional[int] = None
        items = [
            ("2 LVL", 2, discord.ButtonStyle.primary),
            ("3 LVL", 3, discord.ButtonStyle.success),
            ("4 LVL", 4, discord.ButtonStyle.secondary),
            ("5 LVL", 5, discord.ButtonStyle.danger),
            ("6 LVL", 6, discord.ButtonStyle.secondary),
        ]
        for i, (lbl, val, style) in enumerate(items):
            self.add_item(LevelChoiceButton(lbl, val, style, row=i // 3))

async def ask_level_with_buttons(ctx, title: str = "Qaysi level/rang?") -> int:
    view = LevelChoiceView(ctx.author.id)
    msg = await ctx.reply(title, view=view)
    await view.wait()
    await safe_delete_message(msg)
    if view.result is None:
        raise TimeoutError("Tanlash vaqti tugadi.")
    return view.result

def get_sheet_id(ws) -> int:
    return ws._properties["sheetId"]

def insert_empty_row_with_format(ws, insert_row: int, template_row: int):
    sheet_id = get_sheet_id(ws)
    ws.spreadsheet.batch_update({
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": insert_row - 1,
                        "endIndex": insert_row,
                    },
                    "inheritFromBefore": True if insert_row > 1 else False,
                }
            },
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": max(template_row - 1, 0),
                        "endRowIndex": max(template_row, 1),
                        "startColumnIndex": 0,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": insert_row - 1,
                        "endRowIndex": insert_row,
                        "startColumnIndex": 0,
                    },
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    })

def delete_row(ws, row: int):
    ws.delete_rows(row)

def copy_row_values(ws, source_row: int) -> List[str]:
    values = ws.row_values(source_row)
    return values[:] if values else []

def set_row_values(ws, row: int, values: List[str]):
    if not values:
        return
    end_col = len(values)
    rng = f"A{row}:{gspread.utils.rowcol_to_a1(row, end_col)}"
    ws.update(rng, [values], value_input_option="USER_ENTERED")

def find_level_column(ws) -> Optional[int]:
    return find_header_column_by_aliases(ws, ["lvl", "lvl.", "уровень", "level", "уровень/level", "daraja"], scan_rows=5)

def find_login_column(ws) -> Optional[int]:
    return find_header_column_by_aliases(ws, LOGIN_ALIASES, scan_rows=3)

def find_position_column(ws) -> Optional[int]:
    return find_header_column_by_aliases(ws, ["должность", "lavozim", "position"], scan_rows=3)

def find_level_date_column(ws, level: int) -> Optional[int]:
    aliases = [f"{level} уровень", f"{level}-уровень", f"{level} lvl", f"{level} level"]
    return find_header_column_by_aliases(ws, aliases, scan_rows=3)

def find_last_data_row(ws, admin_col: int) -> int:
    vals = ws.col_values(admin_col)
    for idx in range(len(vals), DATA_START_ROW - 1, -1):
        if norm(vals[idx - 1]):
            return idx
    return DATA_START_ROW - 1

def find_insert_row_for_level(ws, level: int, admin_col: int, lvl_col: Optional[int]) -> Tuple[int, int]:
    last_row = find_last_data_row(ws, admin_col)
    if last_row < DATA_START_ROW:
        return DATA_START_ROW, DATA_START_ROW

    if not lvl_col:
        return last_row + 1, last_row

    lvl_vals = ws.col_values(lvl_col)
    same_rows = []
    for row in range(DATA_START_ROW, last_row + 1):
        raw = norm(lvl_vals[row - 1]) if (row - 1 < len(lvl_vals)) else ""
        if not raw: continue
        
        try:
            # Darajani aniqroq aniqlash (masalan "1 lvl" bo'lsa ham)
            m = re.search(r"(\d+)", raw)
            if m:
                val = int(m.group(1))
                if val == int(level): same_rows.append(row)
        except Exception:
            pass

    if same_rows:
        return same_rows[-1] + 1, same_rows[-1]

    candidate = []
    for row in range(DATA_START_ROW, last_row + 1):
        raw = norm(lvl_vals[row - 1]) if row - 1 < len(lvl_vals) else ""
        try:
            val = int(float(raw))
            if val < int(level):
                candidate.append(row)
        except Exception:
            pass

    if candidate:
        return min(candidate), candidate[0]

    return last_row + 1, last_row

def find_row_by_nick(ws, nick: str, admin_col: int = ADMINS_COLUMN, start_row: int = 2) -> Optional[int]:
    return resolve_exact_or_close_row(build_admin_to_row(ws, admin_col=admin_col, start_row=start_row), nick)

def update_admin_row_values(ws, row: int, nick: str, level: Optional[int] = None, login: Optional[str] = None, appoint_date: Optional[str] = None):
    admin_col = find_header_column_by_aliases(ws, ["admins", "admin", "nickname", "nick", "ник", "discord"], scan_rows=3) or ADMINS_COLUMN
    lvl_col = find_level_column(ws)
    login_col = find_login_column(ws)
    pos_col = find_position_column(ws)
    values = copy_row_values(ws, row)
    max_len = max(len(values), admin_col, lvl_col or 0, login_col or 0, pos_col or 0, 15)
    if len(values) < max_len:
        values += [""] * (max_len - len(values))

    values[admin_col - 1] = nick
    if lvl_col and level is not None:
        values[lvl_col - 1] = str(level)
    if login_col and login is not None:
        values[login_col - 1] = login

    if appoint_date:
        for lv in range(1, 7):
            date_col = find_level_date_column(ws, lv)
            if date_col and date_col - 1 < len(values):
                values[date_col - 1] = appoint_date if lv == level else ""

    if pos_col and level is not None and admin_col == 1:
        default_positions = {2: "Админ", 3: "Админ", 4: "Админ", 5: "Админ", 6: "ГА"}
        if not norm(values[pos_col - 1]):
            values[pos_col - 1] = default_positions.get(level, values[pos_col - 1])

    set_row_values(ws, row, values)

def move_admin_in_sheet(ws, nick: str, level: int, appoint_date: str, sheet_type: str = "admin", admin_col: Optional[int] = None):
    if admin_col is None:
        admin_col = find_header_column_by_aliases(ws, ["admins", "admin", "nickname", "nick", "ник", "discord", "admin niki"], scan_rows=5) or ADMINS_COLUMN
    
    lvl_col = find_level_column(ws) or (2 if sheet_type != "admin" else 3)
    row = find_row_by_nick(ws, nick, admin_col, start_row=2)
    
    if not row:
        return False

    if level == 0:
        # Adminni o'chirish (barcha listlarda)
        try:
            ws.delete_rows(row)
            return True
        except Exception as e:
            logger.error(f"Error deleting row {row} from {ws.title}: {e}")
            return False

    # 1. Eski ma'lumotlarni nusxalash
    old_row_values = copy_row_values(ws, row)
    
    # 2. Yangi blok uchun qator topish
    insert_row, template_row = find_insert_row_for_level(ws, level, admin_col, lvl_col)
    if template_row < DATA_START_ROW: template_row = DATA_START_ROW
    
    # 3. Yangi qator ochish
    insert_empty_row_with_format(ws, insert_row, template_row)
    
    # 4. Ma'lumotlarni yangilash
    new_values = old_row_values[:]
    # Massiv uzunligini tekshirish
    needed_len = max(len(new_values), 12)
    if len(new_values) < needed_len:
        new_values += [""] * (needed_len - len(new_values))

    if sheet_type == "admin":
        # C ustuni (Daraja)
        if lvl_col: new_values[lvl_col - 1] = str(level)
        # D-I ustunlari (Sanalar) D=4, E=5, F=6, G=7, H=8, I=9
        date_col_idx = 3 + (level - 1)
        if 3 <= date_col_idx <= 8:
            new_values[date_col_idx] = appoint_date
    else:
        # Online va Donate uchun B ustuni - daraja
        if len(new_values) >= 2: new_values[1] = str(level)

    # 5. Yangi qatorni yozish
    set_row_values(ws, insert_row, new_values)
    
    # 6. Rang berish va formatlash
    apply_level_formatting(ws, insert_row, admin_col, lvl_col, level, len(new_values))
    
    # 7. Eski qatorni o'chirish
    # Agar yangi qator eskisidan oldin qo'shilgan bo'lsa, eski qator indexi 1 ga suriladi
    row_to_delete = row + 1 if insert_row <= row else row
    delete_row(ws, row_to_delete)
    return True

async def perform_lvlup_nicks(ctx, nicks: List[str], level: int, reason: Optional[str] = None, sh=None, table=None):
    if not sh or not table:
        sh, table = await choose_table_for_action(ctx)
    appoint_date = today_str()
    
    # Listlarni va ularning admin ustunlarini bir marta aniqlab olamiz (API limitini tejash uchun)
    admin_ws = await find_adminlist_worksheet(sh)
    online_ws = await run_blocking(find_online_worksheet, sh)
    donate_ws = await run_blocking(find_donate_worksheet, sh)
    
    a_col = await run_blocking(find_header_column_by_aliases, admin_ws, ["admins", "admin", "nickname", "nick", "ник", "discord", "admin niki"], 5) or ADMINS_COLUMN
    o_col = await run_blocking(find_header_column_by_aliases, online_ws, ["admins", "admin", "nickname", "nick", "ник", "discord", "admin niki"], 5) or ADMINS_COLUMN
    d_col = await run_blocking(find_header_column_by_aliases, donate_ws, ["admins", "admin", "nickname", "nick", "ник", "discord", "admin niki"], 5) or ADMINS_COLUMN

    success_nicks = []
    for nick in nicks:
        # Adminning hozirgi darajasini saqlash (arxiv uchun)
        current_level_before_removal = None
        if level == 0:
            # Level 0 bo'lsa (o'chirish), avval admin darajasini topamiz
            admin_data = await run_blocking(admin_ws.get_all_values)
            admin_row_idx = await run_blocking(find_row_by_nick, admin_ws, nick, a_col)
            if admin_row_idx and admin_row_idx <= len(admin_data):
                row_data = admin_data[admin_row_idx - 1]
                # Level ustunini dinamik topamiz
                lvl_col = find_level_column(admin_ws)
                if lvl_col and lvl_col <= len(row_data):
                    try:
                        v = norm(row_data[lvl_col - 1])
                        if v.isdigit():
                            current_level_before_removal = int(v)
                    except Exception:
                        pass
        
        # Har bir listda ko'chirish operatsiyasini bajarish
        res1 = await run_blocking(move_admin_in_sheet, admin_ws, nick, level, appoint_date, "admin", a_col)
        res2 = await run_blocking(move_admin_in_sheet, online_ws, nick, level, appoint_date, "online", o_col)
        res3 = await run_blocking(move_admin_in_sheet, donate_ws, nick, level, appoint_date, "donate", d_col)
        
        # Arxivga o'chirish ma'lumotini yozish
        if level == 0 and (res1 or res2 or res3):
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
            archive_reason = reason or "Sabab ko'rsatilmagan"
            if current_level_before_removal is not None: 
                archive_reason += f" ({current_level_before_removal}-lvl)"
            await run_blocking(archive_insert_many, [(appoint_date, nick, "admin_remove", archive_reason, table["name"], admin_ws.title, now_ts)])

        if res1 or res2 or res3:
            success_nicks.append(nick)

    if not success_nicks:
        msg = "❌ Nicklar birorta listdan ham topilmadi."
        if isinstance(ctx, discord.Interaction):
            return await ctx.followup.send(msg)
        return await ctx.reply(msg)

    msg = "✅ Adminlar darajasi yangilandi va bloklarga ko'chirildi" if level > 0 else "🗑 Adminlar barcha listlardan o'chirildi"
    lines = [f"{msg}:", f"📚 Jadval: **{table['name']}**"]
    if level > 0: 
        lines.append(f"🎚 Yangi Level: **{level}**")
    elif reason: # If removed, show reason
        lines.append(f"💬 Sabab: **{reason}**")
    lines.append("")
    lines.extend([f"• {n}" for n in success_nicks])
    
    out = "\n".join(lines)
    if isinstance(ctx, discord.Interaction):
        await ctx.followup.send(out)
    else:
        await ctx.reply(out)

class AdellModal(discord.ui.Modal, title="Adminlarni o'chirish"):
    nicks = discord.ui.TextInput(label="Admin niklari", placeholder="Nick_One\nNick_Two", style=discord.TextStyle.paragraph, required=True)
    reason = discord.ui.TextInput(label="O'chirish sababi", placeholder="O'z xohishi / Qoida buzish", style=discord.TextStyle.paragraph, required=True)

    def __init__(self, sh, table, cmd_msg):
        super().__init__()
        self.sh = sh
        self.table = table
        self.cmd_msg = cmd_msg

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        nick_list = [norm(n) for n in self.nicks.value.splitlines() if norm(n)]
        if not nick_list:
            return await interaction.followup.send("❌ Nicklar kiritilmadi.")
        
        try:
            # level=0 bo'lganda barcha listlardan o'chirish logikasi ishlaydi
            await perform_lvlup_nicks(interaction, nick_list, 0, reason=self.reason.value, sh=self.sh, table=self.table)
        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {e}")
        await safe_delete_message(self.cmd_msg)

class AdellButtonView(discord.ui.View):
    def __init__(self, sh, table, cmd_msg):
        super().__init__(timeout=60)
        self.sh = sh
        self.table = table
        self.cmd_msg = cmd_msg

    @discord.ui.button(label="O'chirish oynasini ochish", style=discord.ButtonStyle.danger, emoji="🗑")
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AdellModal(self.sh, self.table, self.cmd_msg))

@bot.command(name="adell")
async def adell_cmd(ctx):
    """Adminni barcha listlardan butunlay o'chirish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    
    try:
        sh, table = await choose_table_for_action(ctx)
        await ctx.reply(f"🗑 **{table['name']}** jadvalidan adminlarni o'chirish uchun tugmani bosing:", 
                        view=AdellButtonView(sh, table, ctx.message))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

class NewNikModal(discord.ui.Modal, title="Nikni almashtirish"):
    old_nick = discord.ui.TextInput(label="Eski Nik", placeholder="Eski_Nick", required=True)
    new_nick = discord.ui.TextInput(label="Yangi Nik", placeholder="Yangi_Nick", required=True)

    def __init__(self, sh, table, cmd_msg):
        super().__init__()
        self.sh = sh
        self.table = table
        self.cmd_msg = cmd_msg

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        old_n = norm(self.old_nick.value)
        new_n = norm(self.new_nick.value)
        
        try:
            admin_ws = await find_adminlist_worksheet(self.sh)
            online_ws = await run_blocking(find_online_worksheet, self.sh)
            donate_ws = await run_blocking(find_donate_worksheet, self.sh)

            success_sheets = []
            for ws in [admin_ws, online_ws, donate_ws]:
                row = await run_blocking(find_row_by_nick, ws, old_n)
                if row:
                    await run_blocking(rename_nick_in_sheet, ws, row, new_n)
                    success_sheets.append(ws.title)

            if not success_sheets:
                return await interaction.followup.send(f"❌ `{old_n}` hech qaysi listdan topilmadi.")

            updated_count = await run_blocking(archive_update_nick, old_n, new_n, self.table["name"])
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
            await run_blocking(archive_insert_many, [(today_str(), new_n, "nick_change", f"Eski nick: {old_n}", self.table["name"], "System", now_ts)])

            await interaction.followup.send(
                f"✅ **Nick muvaffaqiyatli almashtirildi!**\n"
                f"👤 `{old_n}` ➔ **{new_n}**\n"
                f"📄 Listlar yangilandi: {', '.join(success_sheets)}\n"
                f"🗂 Arxiv natijalari ko'chirildi: **{updated_count}** ta"
            )
            await safe_delete_message(self.cmd_msg)
        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {friendly_api_error(e)}")

class NewNikButtonView(discord.ui.View):
    def __init__(self, sh, table, cmd_msg):
        super().__init__(timeout=60)
        self.sh = sh
        self.table = table
        self.cmd_msg = cmd_msg

    @discord.ui.button(label="Nikni almashtirish", style=discord.ButtonStyle.primary, emoji="🔄")
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NewNikModal(self.sh, self.table, self.cmd_msg))

@bot.command(name="newnik")
async def newnik_cmd(ctx):
    """Admin nikini almashtirish va eski natijalarini ko'chirish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    
    try:
        sh, table = await choose_table_for_action(ctx)
        await ctx.reply(f"🔄 **{table['name']}** jadvalida nikni o'zgartirish uchun tugmani bosing:", 
                        view=NewNikButtonView(sh, table, ctx.message))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

class InaktivDetailsView(discord.ui.View):
    def __init__(self, author_id, sh, table, inactive_nicks, start_date, end_date):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.sh = sh
        self.table = table
        self.inactive_nicks = inactive_nicks
        self.start_date = start_date
        self.end_date = end_date

    @discord.ui.button(label="Batafsil hisobotni chiqarish", style=discord.ButtonStyle.secondary, emoji="📑")
    async def show_details(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Bu tugma siz uchun emas.", ephemeral=True)
        
        await interaction.response.defer()
        button.disabled = True
        await interaction.edit_original_response(view=self)

        for nick_info in self.inactive_nicks:
            nick = nick_info['name'] # Real nick
            nk = nick_key(nick) # Nick key for lookup
            
            # Get daily stats for this specific inactive nick
            admin_daily_data = self.daily_stats_for_inactive.get(nk, {})
            
            lines = [f"📊 **{nick}** — Batafsil (2 hafta):", ""]
            for d_str in sorted(admin_daily_data.keys(), key=lambda x: archive_date_key(x)):
                if d_str == "name": continue # Skip the name entry
                day_data = admin_daily_data[d_str]
                if day_data["is_otpusk"]:
                    lines.append(f"  {d_str} ➔ 🏖 Otpusk kuni")
                else:
                    lines.append(f"  {d_str} ➔ ⏱ {minutes_to_hhmm(day_data['minutes'])} | 📈 {day_data['report']} rep")
            
            lines.append(f"\n**Jami: {minutes_to_hhmm(nick_info['minutes'])} | {nick_info['report']} rep**")
            
            for part in chunk_text("\n".join(lines)):
                await interaction.followup.send(part)

@bot.command(name="inaktiv")
async def inaktiv_cmd(ctx):
    """Oxirgi 2 haftada 5 soatdan kam onlayn bo'lgan adminlarni aniqlash"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    
    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Oxirgi 2 haftalik ma'lumotlar tahlil qilinmoqda...")
        
        # 14 kunlik ma'lumotlarni yig'ish
        summary, start_dt, end_dt = await collect_full_weekly_data(sh, table["name"], days=14)
        
        inactive_list = []
        for nk, data in summary.items():
            if data.get("on_leave"): continue
            # 5 soat = 300 daqiqa
            if data['minutes'] < 300:
                inactive_list.append(data)

        await safe_delete_message(status_msg)
        
        if not inactive_list:
            return await ctx.reply(f"✅ **{table['name']}**: Oxirgi 2 haftada inaktiv adminlar topilmadi.")

        lines = [f"📉 **Inaktiv Adminlar ({start_dt} — {end_dt})**", f"*(Onlayni 5 soatdan kam)*", ""]
        for i, admin in enumerate(sorted(inactive_list, key=lambda x: x['minutes']), 1):
            lines.append(f"{i}. **{admin['name']}** ➔ ⏱ {minutes_to_hhmm(admin['minutes'])} | 📊 {admin['report']} rep")

        view = InaktivDetailsView(ctx.author.id, sh, table, inactive_list, start_dt, end_dt)
        await ctx.reply("\n".join(lines), view=view)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

def find_manageable_sheets(sh) -> List[Any]:
    result = []
    for finder in [find_adminlist_worksheet, find_donate_worksheet, find_online_worksheet]:
        try:
            ws = finder(sh)
            if ws and ws.id not in [x.id for x in result]:
                result.append(ws)
        except Exception:
            pass
    if not result:
        raise ValueError("Adminlar joylashgan listlar topilmadi.")
    return result

def find_online_date_pairs(ws) -> List[Tuple[int, int, str]]:
    return iter_online_report_pairs_from_values(ws.get_all_values())

def update_online_sheet_dates_to_current(ws) -> List[str]:
    pairs = find_online_date_pairs(ws)
    if not pairs:
        raise ValueError("Online/Report juft ustunlari topilmadi.")

    today = datetime.now(UZ_TZ).date()
    total = len(pairs)
    new_dates = []

    for idx, (online_col, report_col, _old_date) in enumerate(pairs):
        dt = today - timedelta(days=(total - 1 - idx))
        date_text = f"{dt.day}.{dt.month:02d}"
        new_dates.append(date_text)
        ws.update_acell(gspread.utils.rowcol_to_a1(HEADER_DATE_ROW, online_col), date_text)
        if report_col != online_col:
            ws.update_acell(gspread.utils.rowcol_to_a1(HEADER_DATE_ROW, report_col), "")

    return new_dates

async def prompt_secret_value(
    ctx,
    prompt_text: str,
    modal_title: str = "Jadval paroli",
    input_label: str = "Parol",
    placeholder: str = "Parolni kiriting",
) -> Optional[str]:
    future = asyncio.get_running_loop().create_future()
    view = PasswordPromptView(
        ctx.author.id,
        future,
        modal_title=modal_title,
        input_label=input_label,
        placeholder=placeholder,
    )
    msg = await ctx.reply(prompt_text, view=view)
    try:
        return await asyncio.wait_for(future, timeout=MESSAGE_TIMEOUT)
    except Exception:
        return None
    finally:
        await safe_delete_message(msg)

async def prompt_password_change(ctx, title: str, prompt_text: str) -> Tuple[Optional[str], Optional[str]]:
    future = asyncio.get_running_loop().create_future()
    view = PasswordChangePromptView(ctx.author.id, future, title)
    msg = await ctx.reply(prompt_text, view=view)
    try:
        return await asyncio.wait_for(future, timeout=MESSAGE_TIMEOUT)
    except Exception:
        return None, None
    finally:
        await safe_delete_message(msg)

async def ask_table_password(ctx, table: dict) -> bool:
    entered = await prompt_secret_value(
        ctx,
        prompt_text=f"🔑 **{table['name']}** uchun parolni tugma orqali kiriting.",
        modal_title=f"**{table['name']}** paroli",
        input_label="Parol",
        placeholder="Parolni kiriting",
    )
    return verify_password_value(table.get("password"), entered)

async def choose_spreadsheet_ctx(ctx):
    tables = load_tables()
    if not tables:
        raise ValueError("Hech qanday spreadsheet qo'shilmagan. Avval `!table_add` ishlating.\nРќРµ РґРѕР±Р°РІР»РµРЅРѕ РЅРё РѕРґРЅРѕР№ С‚Р°Р±Р»РёС†С‹. РЎРЅР°С‡Р°Р»Р° РёСЃРїРѕР»СЊР·СѓР№С‚Рµ `!table_add`.")

    idx = await choose_number_ctx(
        ctx,
        "рџ“љ Spreadsheet tanlang / Р’С‹Р±РµСЂРёС‚Рµ С‚Р°Р±Р»РёС†Сѓ:",
        [t["name"] for t in tables],
    )
    table = tables[idx]
    sh = await run_blocking(gs.open_by_key, table["id"])
    return sh, table

async def choose_table_for_action(ctx):
    sh, table = await choose_spreadsheet_ctx(ctx)

    password_version = get_password_version(table)
    if is_table_unlocked_for_user(ctx.author.id, table["id"], password_version):
        return sh, table

    ok = await ask_table_password(ctx, table)
    if not ok:
        forget_table_for_user(ctx.author.id, table["id"])
        raise ValueError("Parol noto'g'ri. / РќРµРІРµСЂРЅС‹Р№ РїР°СЂРѕР»СЊ.")

    remember_table_for_user(ctx.author.id, table["id"], password_version)
    return sh, table

async def ask_delete_command(ctx):
    try:
        view = DeleteCommandView(ctx.author.id, ctx.message)
        await ctx.reply("🗑 Komandani o'chiraymi? / Удалить команду?", view=view)
    except Exception:
        pass

# =========================================================
# WRITE ENGINES
# =========================================================
async def apply_pairs_to_target_col_with_confirm(
    ctx,
    ws,
    pairs: List[Tuple[str, object]],
    target_col: int,
    admin_col: int = ADMINS_COLUMN,
    copy_format_from_col: Optional[int] = None,
    force_duration_format: bool = False,
    fill_missing: bool = False,
    missing_value: object = ""
):
    nick_to_row = await run_blocking(build_admin_to_row, ws, admin_col=admin_col)
    last_row = await run_blocking(lambda: len(ws.col_values(admin_col)))
    if copy_format_from_col and last_row >= DATA_START_ROW:
        await run_blocking(copy_column_format, ws, copy_format_from_col, target_col, DATA_START_ROW, last_row)

    if force_duration_format and last_row >= DATA_START_ROW:
        await run_blocking(set_duration_format, ws, target_col, DATA_START_ROW, last_row)

    exact_matches, rename_proposals, not_found = await run_blocking(
        collect_batch_rename_candidates,
        ws,
        nick_to_row,
        pairs,
        admin_col=admin_col,
    )

    renamed = []

    if rename_proposals:
        lines = [
            "❓ Quyidagi nicklar mos deb topildi. Hammasini almashtiraymi?",
            "❓ Найдены похожие ники. Заменить все сразу?",
            ""
        ]
        for old_nick, new_nick, _row, _value in rename_proposals[:20]:
            lines.append(f"`{old_nick}` → `{new_nick}`")

        view = BatchRenameView(ctx.author.id)
        ask_msg = await ctx.reply("\n".join(lines), view=view)
        await view.wait()
        result = view.result
        await safe_delete_message(ask_msg)

        if result is None or result == "cancel":
            raise ValueError("Amal bekor qilindi. / Действие отменено.")

        if result == "replace_all":
            for old_nick, new_nick, row, value in rename_proposals:
                await run_blocking(rename_nick_in_sheet, ws, row, new_nick, admin_col=admin_col)
                renamed.append((old_nick, new_nick))
                exact_matches.append((new_nick, value, row))

        elif result == "skip_all":
            for _old_nick, new_nick, _row, _value in rename_proposals:
                not_found.append(new_nick)

    updates = []
    written_rows = set()

    for _input_nick, value, row in exact_matches:
        if row in written_rows:
            continue

        a1 = gspread.utils.rowcol_to_a1(row, target_col)
        cell_value = value

        updates.append({
            "range": a1,
            "values": [[cell_value]]
        })
        written_rows.add(row)

    if fill_missing:
        for _nick_key, row in nick_to_row.items():
            if row in written_rows:
                continue
            a1 = gspread.utils.rowcol_to_a1(row, target_col)
            updates.append({
                "range": a1,
                "values": [[missing_value]]
            })
            written_rows.add(row)

    if updates:
        await run_blocking(lambda: ws.batch_update(updates, value_input_option="USER_ENTERED"))
        await run_blocking(apply_custom_formatting, ws, list(written_rows), target_col)

    return {
        "written": len(updates),
        "not_found": not_found,
        "renamed": renamed
    }

async def apply_pairs_to_sheet_with_confirm(ctx, ws, date_str: str, pairs: List[Tuple[str, object]], target: str, fill_missing: bool = False):
    online_col, report_col = await run_blocking(find_date_columns, ws, date_str)
    if online_col is None or report_col is None:
        raise ValueError(
            f"{short_date(date_str)} sanasi uchun Online/Report ustunlari topilmadi.\n"
            f"Для даты {short_date(date_str)} не найдены столбцы Online/Report."
        )

    target_col = online_col if target == "online" else report_col
    all_values = await run_blocking(ws.get_all_values)
    max_cols = max((len(r) for r in all_values), default=0)
    fmt_src = get_same_type_format_source_col(target_col, max_cols)

    result = await apply_pairs_to_target_col_with_confirm(
        ctx=ctx,
        ws=ws,
        pairs=pairs,
        target_col=target_col,
        admin_col=ADMINS_COLUMN,
        copy_format_from_col=fmt_src,
        force_duration_format=(target == "online"),
        fill_missing=fill_missing,
        missing_value="xx:xx" if target == "online" else "xx"
    )

    result["date"] = date_str
    result["sheet_title"] = ws.title
    result["online_col"] = online_col
    result["report_col"] = report_col
    return result

# =========================================================
# COMMAND HELPERS
# =========================================================
async def choose_table_and_online_ws(ctx):
    sh, table = await choose_table_for_action(ctx)
    ws = await run_blocking(find_online_worksheet, sh)
    return sh, table, ws

async def choose_table_and_donate_ws(ctx):
    sh, table = await choose_table_for_action(ctx)
    ws = await run_blocking(find_donate_worksheet, sh)
    return sh, table, ws

async def choose_table_and_login_ws(ctx):
    sh, table = await choose_table_for_action(ctx)
    ws = await find_login_worksheet(sh) # Await directly
    return sh, table, ws


class AddAdminModal(discord.ui.Modal, title="Yangi Admin Qo'shish"):
    nick = discord.ui.TextInput(label="Admin Niki", placeholder="Ivan_Vasilyev", required=True)
    login = discord.ui.TextInput(label="Login", placeholder="ivan_v", required=True)
    level = discord.ui.TextInput(label="Darajasi (Level)", placeholder="1-6", min_length=1, max_length=1, required=True)
    date_input = discord.ui.TextInput(label="Sana (Date)", placeholder="DD.MM.YYYY", default=today_str(), required=True)
    reason = discord.ui.TextInput(label="Sabab", placeholder="Yangi kadr", style=discord.TextStyle.paragraph, required=True)

    def __init__(self, sh, table):
        super().__init__()
        self.sh = sh
        self.table = table

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            lvl = int(self.level.value)
            if not (1 <= lvl <= 6): raise ValueError("Level 1-6 oralig'ida bo'lishi kerak.")
        except:
            return await interaction.followup.send("❌ Level raqam bo'lishi kerak (1-6).")

        nick = self.nick.value.strip()
        login = self.login.value.strip()
        date_val = parse_date_any(self.date_input.value) or today_str()
        
        try:
            # 1. Список администрации (Admin List)
            admin_ws = await find_adminlist_worksheet(self.sh)
            a_admin_col = find_header_column_by_aliases(admin_ws, ["admins", "admin", "nick", "ник"], 3) or 1
            a_lvl_col = find_level_column(admin_ws) or 3
            a_login_col = find_login_column(admin_ws) or 11
            a_pos_col = find_position_column(admin_ws) or 10

            ins_row, temp_row = await run_blocking(find_insert_row_for_level, admin_ws, lvl, a_admin_col, a_lvl_col)
            await run_blocking(insert_empty_row_with_format, admin_ws, ins_row, temp_row if temp_row >= DATA_START_ROW else DATA_START_ROW)
            
            row_vals = [""] * 12
            row_vals[a_admin_col-1] = nick # Use a_admin_col
            row_vals[a_lvl_col-1] = str(lvl)
            
            date_col_idx = 4 + (lvl - 1)
            if date_col_idx <= 9:
                row_vals[date_col_idx-1] = date_val
            
            if a_login_col <= 12: row_vals[a_login_col-1] = login
            if a_pos_col <= 12: row_vals[a_pos_col-1] = "Админ" if lvl < 6 else "ГА"

            await run_blocking(set_row_values, admin_ws, ins_row, row_vals)
            await run_blocking(apply_level_formatting, admin_ws, ins_row, a_admin_col, a_lvl_col, lvl, 15)

            # 2. Онлайн (Online)
            online_ws = await run_blocking(find_online_worksheet, self.sh)
            o_admin_col = find_header_column_by_aliases(online_ws, ["admins", "admin", "nick", "ник", "nickname"], 5) or 1
            o_lvl_col = find_level_column(online_ws) or (2 if o_admin_col == 1 else 1)
            
            o_ins_row, o_temp_row = await run_blocking(find_insert_row_for_level, online_ws, lvl, o_admin_col, o_lvl_col)
            await run_blocking(insert_empty_row_with_format, online_ws, o_ins_row, o_temp_row if o_temp_row >= DATA_START_ROW else DATA_START_ROW)
            
            # Faqat admin niki va darajasini yozamiz (boshqa ustunlarga tegmaymiz)
            await run_blocking(lambda: online_ws.update_cell(o_ins_row, o_admin_col, nick))
            await run_blocking(lambda: online_ws.update_cell(o_ins_row, o_lvl_col, str(lvl)))
            await run_blocking(apply_level_formatting, online_ws, o_ins_row, o_admin_col, o_lvl_col, lvl, 10)

            # 3. Выплата доната (Donate)
            donate_ws = await run_blocking(find_donate_worksheet, self.sh)
            d_admin_col = find_header_column_by_aliases(donate_ws, ["admins", "admin", "nick", "ник", "nickname"], 5) or 1
            d_lvl_col = find_level_column(donate_ws) or (2 if d_admin_col == 1 else 1)
            d_login_col = find_login_column(donate_ws) or 10

            d_ins_row, d_temp_row = await run_blocking(find_insert_row_for_level, donate_ws, lvl, d_admin_col, d_lvl_col)
            await run_blocking(insert_empty_row_with_format, donate_ws, d_ins_row, d_temp_row if d_temp_row >= 2 else 2)
            
            await run_blocking(lambda: donate_ws.update_cell(d_ins_row, d_admin_col, nick))
            await run_blocking(lambda: donate_ws.update_cell(d_ins_row, d_lvl_col, str(lvl)))
            if d_login_col: await run_blocking(lambda: donate_ws.update_cell(d_ins_row, d_login_col, login))
            await run_blocking(apply_level_formatting, donate_ws, d_ins_row, d_admin_col, d_lvl_col, lvl, 15)

            # Arxivlash
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
            # Level ma'lumotini sababga qo'shib yozamiz
            archive_reason = f"Qabul: {self.reason.value} ({lvl}-lvl)"
            archive_data = (date_val, nick, "admin_add", archive_reason, self.table["name"], admin_ws.title, now_ts)
            await run_blocking(archive_insert_many, [archive_data])

            await interaction.followup.send(f"✅ **{nick}** muvaffaqiyatli qo'shildi va 3 ta list yangilandi.")
        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {e}")

# =========================================================
# BASIC COMMANDS
# =========================================================
@bot.command(name="help", aliases=["h"])
async def help_cmd(ctx):
    err = require_access(ctx)
    if err and not can_bootstrap_first_role(ctx):
        text = (
            "**📚 Bot Komandalari (Ruxsat berilmagan):**\n\n"
            "`!help` - Komandalar ro'yxatini chiqarish\n"
            "`!ping` - Bot statistikasi va holati\n"
            "`!auth <nom>` - Bot komandalariga ruxsat olish (Parol orqali)"
        )
        return await ctx.reply(text)

    text = (
        "**📚 Bot Komandalari Ro'yxati:**\n\n"
        "**🛠 Asosiy:**\n"
        "`!help` - Komandalar ro'yxatini chiqarish\n"
        "`!ping` - Bot statistikasi va holati\n"
        "`!auth <nom>` - Bot komandalariga ruxsat olish (Parol orqali)\n\n"
        "**📋 Jadval va Ruxsatnomalar:**\n"
        "`!table_add <nom> <link>` - Yangi jadval ulash\n"
        "`!table_list` - Ulangan jadvallarni ko'rish\n"
        "`!table_password <nom>` - Jadval parolini o'zgartirish\n"
        "`!role_add <nom> <role_id>` - Botdan foydalanishga ruxsat berish\n\n"
        "**⏱ Online va Report yozish:**\n"
        "`!dn [sana]` - Bir vaqtda ko'p admin onlineini yozish\n"
        "`!dr [sana]` - Bir vaqtda ko'p admin reportini yozish\n"
        "`!dn_user <nick> <HH:MM>` - Bitta admin uchun online\n"
        "`!dr_user <nick> <son>` - Bitta admin uchun report\n"
        "`!clear_day [sana]` - Kunlik natijalarni butunlay o'chirish\n\n"
        "**💾 Arxiv va Tekshiruv:**\n"
        "`!pv` - 📈 **Povisheniye (daraja ko'tarish) ro'yxatini ko'rish**\n"
        "`!adpv` - ✍ **Adminlarga tanbeh yoki ogohlantirish berish**\n"
        "`!import_weekly <sana1-sana2>` - 📅 **Haftalik ko'rsatkichlarni arxivga taqsimlash**\n"
        "`!vp` - ❗ **Vig/Pred holati va yechish normasini tekshirish**\n"
        "`!info <nick>` - 👤 **Adminning faoliyati haqida ma'lumot**\n"
        "`!norma <nick> <sana1> <sana2>` - Umumiy onlineni hisoblash\n"
        "`!report <nick> <sana1> <sana2>` - Umumiy reportni hisoblash\n"
        "`!reports <sana1> <sana2>` - Barcha adminlarning jami reportlari\n"
        "`!sr <sana1> <sana2>` - 🌐 **Server bo'yicha jami reportlar soni**\n"
        "`!top [son]` - 🏆 **Haftalik eng faol adminlar (Report/Online)**\n"
        "`!otpusk` - 🏖 **Adminlarga ta'til (otpusk) belgilash**\n"
        "`!otpusk_stats` - 📊 **Haftalik otpusk statistikasi**\n"
        "`!ats [sana1] [sana2]` - 📜 **Admin qo'shish/o'chirish tarixini ko'rish**\n"
        "`!adell <nick>` - 🗑 **Adminni barcha listlardan butunlay o'chirish**\n"
        "`!stw` - 📊 **Haftalik ko'rsatkichlar (Nik Report/Online)**\n"
        "`!arxiv` - Bugungi natijalarni arxiv bazasiga yozish\n"
        "`!arxiv_import` - Jadvaldagi barcha listlarni arxivga o'tkazish\n"
        "`!arxiv_merge` - Arxiv bazasini birlashtirish va optimallash\n"
        "`!arxiv_stats` - Arxivdagi yozuvlar soni va hajmi\n\n"
        "**👥 Adminlar Boshqaruvi:**\n"
        "`!add <nick> <login>` - Yangi admin qo'shish\n"
        "`!lvlup <nick> <level>` - Admin darajasini o'zgartirish yoki o'chirish\n"
        "`!logins` - Adminlar niki va loginlari ro'yxati\n"
        "`!donate` - Donate (shikoyat, gmp va h.k.) bo'limini to'ldirish\n"
        "`!wn` - 💰 **Online soatlarni yaxlitlab donate listiga yozish**\n"
        "`!nw` - Online listdagi sanalarni bugungi haftaga moslash"
    )
    for part in chunk_text(text):
        await ctx.reply(part)

@bot.command(name="ping", aliases=["p"])
async def ping_cmd(ctx):
    tables = load_tables()
    roles = load_roles()
    stats = await run_blocking(get_archive_stats)

    lines = [
        f"🏓 Pong! `{round(bot.latency * 1000)} ms`",
        "",
        f"📚 Jadvallar / Таблицы: **{len(tables)}**",
        f"🛡 Rollar / Роли: **{len(roles)}**",
        f"💾 Arxiv fayllar / Файлы: **{stats['total_files']}**",
        f"📝 Jami yozuvlar / Записи: **{stats['total_records']}**",
        f"📊 Hajmi / Размер: **{stats['total_size']}**"
    ]
    await ctx.reply("\n".join(lines))
    await ask_delete_command(ctx)

@bot.command(name="auth")
async def auth_cmd(ctx, name: str = None):
    """Role paroli orqali botdan foydalanishga ruxsat olish"""
    if not name:
        return await ctx.reply("❌ Misol: `!auth Curator` (Role nomini yozing)")

    role_data = get_role_by_name(name)
    if not role_data:
        return await ctx.reply(f"❌ '{name}' nomli role bot bazasida topilmadi.")

    entered = await prompt_secret_value(
        ctx,
        prompt_text=f"🔑 **{name}** roli uchun parolni tugma orqali kiriting.",
        modal_title="Role paroli",
        input_label="Parol",
        placeholder="Parolni kiriting"
    )

    if not entered:
        return 

    if verify_password_value(role_data.get("password"), entered):
        role_id = int(role_data["role_id"])
        role_obj = ctx.guild.get_role(role_id)
        if role_obj:
            try:
                await ctx.author.add_roles(role_obj)
                await ctx.reply(f"✅ Ruxsat berildi! Sizga serverda **{role_obj.name}** roli berildi.")
            except discord.Forbidden:
                await ctx.reply("❌ Botda rolni berish uchun ruxsat yetarli emas. Bot roli ierarxiyada yuqoriroq bo'lishi kerak.")
        else:
            await ctx.reply(f"❌ Serverda ID: `{role_id}` bo'lgan role topilmadi.")
    else:
        await ctx.reply("❌ Parol noto'g'ri.")

# =========================================================
# TABLE COMMANDS
# =========================================================
@bot.command(name="table_add")
async def table_add_cmd(ctx, name: str = None, spreadsheet_id: str = None, password: str = None):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not name or not spreadsheet_id:
        await ctx.reply("❌ Misol / Пример: `!table_add Admin38 10cW...`")
        await ask_delete_command(ctx)
        return

    try:
        if password:
            await safe_delete_message(ctx.message)
        password_value = norm(password) or await prompt_secret_value(
            ctx,
            prompt_text="Jadval uchun yangi parolni tugma orqali kiriting.",
            modal_title="Yangi jadval paroli",
            input_label="Yangi parol",
            placeholder="Yangi parol",
        )
        if not password_value:
            raise TimeoutError("Parol kiritilmadi.")

        await run_blocking(add_table_record, name, spreadsheet_id, password_value, ctx.author.id)
        await ctx.reply(
            f"✅ Jadval qo'shildi / Таблица добавлена\n"
            f"**Nom / Имя:** {name}\n"
            f"**ID:** `{spreadsheet_id}`\n"
            f"**Owner:** <@{ctx.author.id}>"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="table_list")
async def table_list_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    tables = load_tables()
    if not tables:
        await ctx.reply("ℹ Hech qanday jadval qo'shilmagan.\nℹ Таблицы не добавлены.")
        await ask_delete_command(ctx)
        return

    lines = ["**Qo'shilgan jadvallar / Добавленные таблицы:**"]
    for i, t in enumerate(tables, start=1):
        lines.append(f"{i}. **{t['name']}** — `{t['id']}` — owner: `<@{t.get('owner')}>`")

    await ctx.reply("\n".join(lines))
    await ask_delete_command(ctx)

@bot.command(name="table_remove")
async def table_remove_cmd(ctx, *, name: str = None):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not name:
        await ctx.reply("❌ Misol / П  имер: `!table_remove Admin38`")
        await ask_delete_command(ctx)
        return

    try:
        remove_table_record(name, ctx.author.id)
        await ctx.reply(f"🗑 Jadval o'chirildi / Таблица удалена: **{name}**")
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибka: {e}")

    await ask_delete_command(ctx)

@bot.command(name="table_password")
async def table_password_cmd(ctx, name: str = None, old_password: str = None, new_password: str = None):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not name:
        await ctx.reply("❌ Misol / Пример: `!table_password Admin38`")
        await ask_delete_command(ctx)
        return

    try:
        if old_password or new_password:
            await safe_delete_message(ctx.message)
        old_value = norm(old_password)
        new_value = norm(new_password)
        if not old_value or not new_value:
            old_value, new_value = await prompt_password_change(
                ctx,
                title=f"{name} parolini almashtirish",
                prompt_text=f"**{name}** uchun eski va yangi parolni kiriting.",
            )
        if not old_value or not new_value:
            raise TimeoutError("Parollar kiritilmadi.")
        update_table_password(name, old_value, new_value, ctx.author.id)
        await ctx.reply(f"✅ Jadval paroli o'zgardi / Пароль таблицы изменён: **{name}**")
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

# =========================================================
# ROLE COMMANDS
# =========================================================
@bot.command(name="role_add")
async def role_add_cmd(ctx, name: str = None, role_id: str = None, password: str = None):
    current_roles = load_roles()
    if current_roles:
        err = require_access(ctx)
        if err:
            await ctx.reply(err)
            return
    elif not can_bootstrap_first_role(ctx):
        await ctx.reply("Birinchi role ni faqat server owner yoki administrator qo'sha oladi.")
        return

    if not name or not role_id:
        await ctx.reply("❌ Misol / Пример: `!role_add Curator 1481118506480173129`")
        await ask_delete_command(ctx)
        return

    try:
        role_id_int = int(role_id)
        if password:
            await safe_delete_message(ctx.message)
        password_value = norm(password) or await prompt_secret_value(
            ctx,
            prompt_text="Role uchun yangi parolni tugma orqali kiriting.",
            modal_title="Yangi role paroli",
            input_label="Yangi parol",
            placeholder="Yangi parol",
        )
        if not password_value:
            raise TimeoutError("Parol kiritilmadi.")
        add_role_record(name, role_id_int, password_value, ctx.author.id)
        await ctx.reply(
            f"✅ Role qo'shildi / Роль добавлена\n"
            f"**Nom / Имя:** {name}\n"
            f"**Role ID:** `{role_id_int}`\n"
            f"**Owner:** <@{ctx.author.id}>"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="role_list")
async def role_list_cmd(ctx):
    err = require_access(ctx)
    if err and load_roles():
        await ctx.reply(err)
        return

    roles = load_roles()
    if not roles:
        await ctx.reply("ℹ Hech qanday role qo'shilmagan.\nℹ Роли не добавлены.")
        await ask_delete_command(ctx)
        return

    lines = ["**Qo'shilgan rolelar / Добавленные роли:**"]
    for i, r in enumerate(roles, start=1):
        lines.append(f"{i}. **{r['name']}** — role_id: `{r['role_id']}` — owner: `<@{r.get('owner')}>`")

    await ctx.reply("\n".join(lines))
    await ask_delete_command(ctx)

@bot.command(name="role_remove")
async def role_remove_cmd(ctx, *, name: str = None):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not name:
        await ctx.reply("❌ Misol / Пример: `!role_remove Curator`")
        await ask_delete_command(ctx)
        return

    try:
        remove_role_record(name, ctx.author.id)
        await ctx.reply(f"🗑 Role o'chirildi / Роль удалена: **{name}**")
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="role_password")
async def role_password_cmd(ctx, name: str = None, old_password: str = None, new_password: str = None):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not name:
        await ctx.reply("❌ Misol / Пример: `!role_password Curator`")
        await ask_delete_command(ctx)
        return

    try:
        if old_password or new_password:
            await safe_delete_message(ctx.message)
        old_value = norm(old_password)
        new_value = norm(new_password)
        if not old_value or not new_value:
            old_value, new_value = await prompt_password_change(
                ctx,
                title=f"{name} role parolini almashtirish",
                prompt_text=f"**{name}** role uchun eski va yangi parolni kiriting.",
            )
        if not old_value or not new_value:
            raise TimeoutError("Parollar kiritilmadi.")
        update_role_password(name, old_value, new_value, ctx.author.id)
        await ctx.reply(f"✅ Role paroli o'zgardi / Пароль роли изменён: **{name}**")
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

# =========================================================
# ONLINE / REPORT COMMANDS
# =========================================================
@bot.command(name="dn")
async def dn_cmd(ctx, *, data: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str, pairs = parse_bulk_online(data)
        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await apply_pairs_to_sheet_with_confirm(ctx, ws, date_str, pairs, "online", fill_missing=True)

        msg = (
            f"✅ Online yozildi / Online записан\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}** ({day_name_ru(date_str)})\n"
            f"📝 Yozilgan / Записано: **{result['written']}**"
        )
        if result["renamed"]:
            msg += "\n🔁 Almashtirilgan nicklar / Заменённые ники:\n" + "\n".join(
                f"`{old}` → `{new}`" for old, new in result["renamed"]
            )
        if result["not_found"]:
            msg += "\n⚠ Topilmadi / Не найдено: " + ", ".join(result["not_found"][:20])

        await ctx.reply(msg)

    except Exception as e:
        await ctx.reply(
            "❌ Xatolik / Ошибка: " + friendly_api_error(e) +
            "\n\nMisol / Пример:\n```text\n!dn 11.03\nNicolas_Johns 02:42\nIvan_Vasilyev 02:42\n```"
        )

    await ask_delete_command(ctx)

@bot.command(name="dr")
async def dr_cmd(ctx, *, data: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str, pairs = parse_bulk_report(data)
        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await apply_pairs_to_sheet_with_confirm(ctx, ws, date_str, pairs, "report", fill_missing=True)

        msg = (
            f"✅ Report yozildi / Report записан\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}** ({day_name_ru(date_str)})\n"
            f"📝 Yozilgan / Записано: **{result['written']}**"
        )
        if result["renamed"]:
            msg += "\n🔁 Almashtirilgan nicklar / Заменённые ники:\n" + "\n".join(
                f"`{old}` → `{new}`" for old, new in result["renamed"]
            )
        if result["not_found"]:
            msg += "\n⚠ Topilmadi / Не найдено: " + ", ".join(result["not_found"][:20])

        await ctx.reply(msg)

    except Exception as e:
        await ctx.reply(
            "❌ Xatolik / Ошибка: " + friendly_api_error(e) +
            "\n\nMisol / Пример:\n```text\n!dr 11.03\nNicolas_Johns 55\nIvan_Vasilyev 89\n```"
        )

    await ask_delete_command(ctx)

@bot.command(name="dn_user")
async def dn_user_cmd(ctx, *, raw_text: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str, nick, value = parse_single_user_value(raw_text, "online")
        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await apply_pairs_to_sheet_with_confirm(ctx, ws, date_str, [(nick, value)], "online")

        if result["not_found"]:
            await ctx.reply(f"⚠ Nick topilmadi / Ник не найден: {nick}")
            await ask_delete_command(ctx)
            return

        await ctx.reply(
            f"✅ Online yozildi / Online записан\n"
            f"👤 Nick: **{nick}**\n"
            f"🕒 Qiymat / Значение: **{value}**\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}\nMisol / Пример: `!dn_user 11.03 Nicolas_Johns 02:42`")

    await ask_delete_command(ctx)

@bot.command(name="dr_user")
async def dr_user_cmd(ctx, *, raw_text: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str, nick, value = parse_single_user_value(raw_text, "report")
        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await apply_pairs_to_sheet_with_confirm(ctx, ws, date_str, [(nick, value)], "report")

        if result["not_found"]:
            await ctx.reply(f"⚠ Nick topilmadi / Ник не найден: {nick}")
            await ask_delete_command(ctx)
            return

        await ctx.reply(
            f"✅ Report yozildi / Report записан\n"
            f"👤 Nick: **{nick}**\n"
            f"🔢 Qiymat / Значение: **{value}**\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}\nMisol / Пример: `!dr_user 11.03 Nicolas_Johns 55`")

    await ask_delete_command(ctx)

# =========================================================
# CLEAR COMMANDS
# =========================================================
@bot.command(name="clear_dn")
async def clear_dn_cmd(ctx, date_raw: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str = parse_date_any(date_raw) if date_raw else today_str()
        if not date_str:
            raise ValueError("Sana noto'g'ri. / Неверная дата.")

        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await run_blocking(clear_column_for_day, ws, date_str, "online")

        await ctx.reply(
            f"🧹 Online tozalandi / Online очищен\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**\n"
            f"📝 Qatorlar / Строки: **{result['cleared']}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="clear_dr")
async def clear_dr_cmd(ctx, date_raw: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str = parse_date_any(date_raw) if date_raw else today_str()
        if not date_str:
            raise ValueError("Sana noto'g'ri. / Неверная дата.")

        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await run_blocking(clear_column_for_day, ws, date_str, "report")

        await ctx.reply(
            f"🧹 Report tozalandi / Report очищен\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**\n"
            f"📝 Qatorlar / Строки: **{result['cleared']}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="clear_day")
async def clear_day_cmd(ctx, date_raw: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str = parse_date_any(date_raw) if date_raw else today_str()
        if not date_str:
            raise ValueError("Sana noto'g'ri. / Неверная dата.")

        _sh, table, ws = await choose_table_and_online_ws(ctx)
        result = await run_blocking(clear_day_both, ws, date_str)

        await ctx.reply(
            f"🧹 Kun tozalandi / День очищен (Online + Report)\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**\n"
            f"📝 Qatorlar / Строки: **{result['cleared']}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="clear_user")
async def clear_user_cmd(ctx, *, raw_text: str = ""):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        date_str, nick = parse_clear_user(raw_text)
        _sh, table, ws = await choose_table_and_online_ws(ctx)

        online_col, report_col = await run_blocking(find_date_columns, ws, date_str)
        if online_col is None or report_col is None:
            raise ValueError("Sana ustunlari topilmadi. / Столбцы даты не найдены.")

        nick_to_row = await run_blocking(build_admin_to_row, ws)
        row = resolve_exact_or_close_row(nick_to_row, nick)
        if not row:
            raise ValueError(f"Nick topilmadi: {nick} / Ник не найден: {nick}")

        a1_online = gspread.utils.rowcol_to_a1(row, online_col)
        a1_report = gspread.utils.rowcol_to_a1(row, report_col)
        await run_blocking(ws.batch_clear, [a1_online, a1_report])

        await ctx.reply(
            f"🧹 User tozalandi / Пользователь очищен\n"
            f"👤 Nick: **{nick}**\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Sana / Дата: **{date_str}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}\nMisol / Пример: `!clear_user 11.03 Nicolas_Johns`")

    await ask_delete_command(ctx)

# =========================================================
# DONATE / LOGINS
# =========================================================
@bot.command(name="donate")
async def donate_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        _sh, table, ws = await choose_table_and_donate_ws(ctx)

        available_fields = []
        for fld in DONATE_FIELDS:
            col = await run_blocking(find_header_column_by_aliases, ws, fld["aliases"], 3)
            if col:
                available_fields.append((fld, col))

        if not available_fields:
            raise ValueError("Donate bo'limlari topilmadi. / Поля donate не найдены.")

        idx = await choose_number_ctx(
            ctx,
            "💸 Qaysi bo'limni to'ldirmoqchisiz? / Какой раздел заполнить?",
            [fld["title"] for fld, _col in available_fields]
        )

        chosen_field, chosen_col = available_fields[idx]

        input_text = await prompt_user_message(
            ctx,
            f"✍ Endi `nick value` formatda yuboring.\n"
            f"Masalan / Пример:\n```text\nNicolas_Johns 25\nIvan_Vasilyev 100\n```"
        )

        pairs = parse_generic_pairs(input_text)

        result = await apply_pairs_to_target_col_with_confirm(
            ctx=ctx,
            ws=ws,
            pairs=pairs,
            target_col=chosen_col,
            admin_col=ADMINS_COLUMN,
            copy_format_from_col=None,
            force_duration_format=False
        )

        msg = (
            f"✅ Bo'lim to'ldirildi / Раздел заполнен\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📌 Bo'lim / Раздел: **{chosen_field['title']}**\n"
            f"📝 Yozilgan / Записано: **{result['written']}**"
        )

        if result["renamed"]:
            msg += "\n🔁 Almashtirilgan nicklar / Заменённые ники:\n" + "\n".join(
                f"`{old}` → `{new}`" for old, new in result["renamed"]
            )

        if result["not_found"]:
            msg += "\n⚠ Topilmadi / Не найдено: " + ", ".join(result["not_found"][:20])

        await ctx.reply(msg)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

@bot.command(name="logins")
async def logins_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        _sh, table, ws = await choose_table_and_login_ws(ctx)

        admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], 3)
        if not admin_col:
            admin_col = ADMINS_COLUMN

        login_col = await run_blocking(find_header_column_by_aliases, ws, LOGIN_ALIASES, 3)
        if not login_col:
            raise ValueError("Login ustuni topilmadi. / Столбец Login не найден.")

        admins = await run_blocking(ws.col_values, admin_col)
        logins = await run_blocking(ws.col_values, login_col)

        lines = [f"**{table['name']} / {ws.title} — Nick + Login**"]
        count = 0
        max_len = max(len(admins), len(logins))

        for i in range(DATA_START_ROW, max_len + 1):
            nick_val = norm(admins[i - 1]) if i - 1 < len(admins) else ""
            login_val = norm(logins[i - 1]) if i - 1 < len(logins) else ""
            if not nick_val:
                continue
            lines.append(f"{nick_val} — {login_val}")
            count += 1

        if count == 0:
            await ctx.reply("ℹ Hech qanday nick/login topilmadi.\nℹ Ник/логин не найдены.")
            await ask_delete_command(ctx)
            return

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

# =========================================================
# POVISHENIYE (PROMOTION) CHECK COMMAND
# =========================================================
@bot.command(name="pv")
async def pv_check_cmd(ctx):
    """Adminlar povisheniye (daraja ko'tarish) holatini tekshirish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Povisheniye uchun ma'lumotlar yig'ilmoqda, kuting...")
        
        admin_ws = await find_adminlist_worksheet(sh)
        data = await run_blocking(admin_ws.get_all_values)
        if not data: raise ValueError("Jadval bo'sh.")

        today = today_str()
        # Ko'tarilish talablari: {eski_lvl: {"min": daqiqa, "rep": report}}
        reqs = {
            1: {"min": 14 * 60, "rep": 500, "target": 2}, # 1 -> 2
            2: {"min": 28 * 60, "rep": 1000, "target": 3}, # 2 -> 3
            3: {"min": 42 * 60, "rep": 1500, "target": 4}  # 3 -> 4
        }

        admins_to_check = []
        for row_idx in range(1, len(data)):
            row = data[row_idx]
            if len(row) < 15: continue
            
            nick = row[0]
            try:
                lvl = int(float(row[2])) # C ustuni
            except: continue
            
            if lvl not in reqs: continue
            
            # Start date: 1lvl bo'lsa D(3), 2lvl bo'lsa E(4), 3lvl bo'lsa F(5)
            date_col_idx = lvl + 2 
            start_date = parse_date_any(row[date_col_idx]) if date_col_idx < len(row) else None
            if not start_date: continue
            
            admins_to_check.append({
                "nick": nick,
                "lvl": lvl,
                "start_date": start_date,
                "vigs": parse_penalty_count(row[12]), # M
                "preds": parse_penalty_count(row[14]) # O
            })

        if not admins_to_check:
            await safe_delete_message(status_msg)
            return await ctx.reply("ℹ Hozircha povisheniye oladigan (1-3 lvl) adminlar topilmadi.")

        # Barcha listlardan ma'lumotlarni yig'ish (Optimallash)
        nicks_list = [a["nick"] for a in admins_to_check]
        all_live_cache = await collect_live_data_for_multiple_nicks(sh, nicks_list, "01.01.2024", today, table["name"])

        ready_list = []    # 1. Povisheniye oladiganlar
        blocked_list = []  # 2. Normasi yetgan lekin jazosi borlar
        process_list = []  # 3. Normasi yetmaganlar

        for admin in admins_to_check:
            nick = admin["nick"]
            nk = nick_key(nick)
            start = admin["start_date"]
            req = reqs[admin["lvl"]]

            # Norma va Reportni hisoblash (Arxiv + Cache)
            arc_n = await run_blocking(archive_query_all_databases, nick, "norma", start, today, table["name"])
            sh_n = [r for r in all_live_cache.get(nk, {}).get("norma", []) if is_date_in_range(r[0], start, today)]
            cur_min = sum(hhmm_to_minutes(str(r[3])) for r in merge_archive_and_sheet_rows(arc_n, sh_n))

            arc_r = await run_blocking(archive_query_all_databases, nick, "report", start, today, table["name"])
            sh_r = [r for r in all_live_cache.get(nk, {}).get("report", []) if is_date_in_range(r[0], start, today)]
            cur_rep = sum(int(float(str(r[3]))) for r in merge_archive_and_sheet_rows(arc_r, sh_r) if str(r[3]).replace('.','',1).isdigit())

            has_penalty = (admin["vigs"] > 0 or admin["preds"] > 0)
            met_norms = (cur_min >= req["min"] and cur_rep >= req["rep"])

            info = f"• **{nick}** ({admin['lvl']} ➔ {req['target']})\n  └ 📅 Dan: {start} | ⏱ {minutes_to_hhmm(cur_min)} | 📊 {cur_rep}"

            if met_norms:
                if not has_penalty:
                    ready_list.append(info)
                else:
                    penalty_str = f"(Vig: {admin['vigs']}, Pred: {admin['preds']})"
                    blocked_list.append(info + f"\n  ⚠️ *Bloklangan: {penalty_str}*")
            else:
                rem_min = max(0, req["min"] - cur_min)
                rem_rep = max(0, req["rep"] - cur_rep)
                process_list.append(info + f"\n  📉 Qoldi: {minutes_to_hhmm(rem_min)} onlayn, {rem_rep} report")

        await safe_delete_message(status_msg)
        
        final_text = [f"📈 **Povisheniye Tahlili — {table['name']}**\n"]
        
        if ready_list:
            final_text.append("✅ **Povisheniye oladiganlar:**")
            final_text.extend(ready_list)
            final_text.append("")

        if blocked_list:
            final_text.append("⚠️ **Normasi yetgan, lekin jazosi borlar:**")
            final_text.extend(blocked_list)
            final_text.append("")

        if process_list:
            final_text.append("⏳ **Normasi yetmaganlar:**")
            final_text.extend(process_list)

        for part in chunk_text("\n".join(final_text)):
            await ctx.reply(part)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

# =========================================================
# ARCHIVE COMMANDS
# =========================================================
@bot.command(name="arxiv")
async def archive_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        _sh, table, ws = await choose_table_and_online_ws(ctx)

        values = await run_blocking(ws.get_all_values)
        if not values or len(values) < 2:
            raise ValueError("Sheet bo'sh yoki header topilmadi.")

        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN

        now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
        archived_rows = build_archive_rows_from_values(values, table["name"], ws.title, now_ts, ws_admin_col) # Pass ws_admin_col
        inserted = await run_blocking(archive_insert_many, archived_rows)

        await ctx.reply(
            f"✅ Arxivlandi / Архивировано\n"
            f"📚 Spreadsheet: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"🗂 Yozuvlar / Записи: **{inserted}**"
        )

    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

# =========================================================
# ARCHIVE IMPORT (ALL SHEETS)
# =========================================================
@bot.command(name="arxiv_import")
async def arxiv_import_cmd(ctx):
    """Tanlangan jadvaldagi barcha listlardan ma'lumotlarni arxivga ko'chiradi"""
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply(f"🔄 **{table['name']}** jadvali tahlil qilinmoqda, barcha listlar tekshirilmoqda...")

        worksheets = await run_blocking(sh.worksheets)
        total_inserted = 0
        processed_sheets = []

        now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")

        for ws in worksheets:
            values = await run_blocking(ws.get_all_values)
            if not values or len(values) < 2:
                continue

            ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
            if not ws_admin_col:
                ws_admin_col = ADMINS_COLUMN

            archived_rows = build_archive_rows_from_values(values, table["name"], ws.title, now_ts, ws_admin_col) # Pass ws_admin_col
            if archived_rows:
                inserted = await run_blocking(archive_insert_many, archived_rows)
                total_inserted += inserted
                processed_sheets.append(f"📄 {ws.title} (**{inserted}**)")

        sheets_str = "\n".join(processed_sheets) if processed_sheets else "Hech qanday ma'lumot topilmadi."
        
        await status_msg.edit(content=(
            f"✅ **Import yakunlandi!**\n"
            f"📚 Jadval: **{table['name']}**\n"
            f"🗂 Jami yozuvlar: **{total_inserted}**\n\n"
            f"**Qayta ishlangan listlar:**\n{sheets_str}"
        ))

    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {friendly_api_error(e)}")

    await ask_delete_command(ctx)

@bot.command(name="arxiv_fix")
async def arxiv_fix_cmd(ctx):
    """Arxiv bazasini qo'lda tozalash va optimallashtirish (Deduplicate + Vacuum)"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    
    status_msg = await ctx.reply("🛠 Arxiv bazasi optimallashtirilmoqda va dublikatlar tozalanmoqda...")
    try:
        await run_blocking(archive_maintenance)
        stats = await run_blocking(get_archive_stats)
        await status_msg.edit(content=(
            f"✅ **Optimallashtirish yakunlandi!**\n"
            f"✨ Dublikatlar tozalandi va `VACUUM` qilindi.\n"
            f"📊 Yangi hajmi: **{stats['total_size']}**\n"
            f"📝 Jami yozuvlar: **{stats['total_records']}**"
        ))
    except Exception as e:
        await status_msg.edit(content=f"❌ Xatolik yuz berdi: {e}")

# =========================================================
# ARCHIVE MERGE COMMAND
# =========================================================
@bot.command(name="arxiv_merge")
async def arxiv_merge_cmd(ctx):
    """Barcha arxiv fayllarini bitta sog'lom faylga birlashtiradi"""
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    status_msg = await ctx.reply("🔄 Arxivlar tahlil qilinmoqda, kuting...")
    
    try:
        ensure_archive_dir()
        all_files = glob.glob(os.path.join(ARCHIVE_DB_DIR, "archive_*.db"))
        
        if not all_files:
            await status_msg.edit(content="❌ Arxiv fayllari topilmadi.")
            return

        all_rows = []
        valid_count = 0
        
        for db_file in all_files:
            if is_archive_db_usable(db_file):
                conn = sqlite3.connect(db_file)
                cur = conn.cursor()
                cur.execute("SELECT date, nick, type, value, spreadsheet_name, sheet_name, archived_at FROM archive_records")
                rows = cur.fetchall()
                all_rows.extend(rows)
                conn.close()
                valid_count += 1
            
        # Yangi toza fayl yaratish
        merged_db_path = os.path.join(ARCHIVE_DB_DIR, "archive_merged_tmp.db")
        if os.path.exists(merged_db_path):
            os.remove(merged_db_path)
        
        init_single_archive(merged_db_path)
        total_inserted = archive_insert_many_to_path(merged_db_path, all_rows)
        
        # Eski fayllarni o'chirish
        for db_file in all_files:
            os.remove(db_file)
            
        # Yangi faylni asosiy nomga o'tkazish
        os.rename(merged_db_path, os.path.join(ARCHIVE_DB_DIR, "archive_001.db"))

        await status_msg.edit(content=(
            f"✅ **Birlashtirish yakunlandi!**\n"
            f"📁 Fayllar birlashtirildi: **{valid_count}** ta\n"
            f"📝 Jami yozuvlar tiklandi: **{total_inserted}** ta\n"
            f"🗂 Yangi yagona arxiv: `archive_001.db`"
        ))
    except Exception as e:
        await status_msg.edit(content=f"❌ Birlashtirishda xatolik: {e}")

# =========================================================
# REPORT / NORMA QUERY COMMANDS (DUAL SOURCE)
# =========================================================
# Birlashtirilgan norma va report komandalari (eski dublikatlar olib tashlandi)
@bot.command(name="reports")
async def reports_period_cmd(ctx, start_date: str = None, end_date: str = None):
    """📊 Arxiv + Jadvaldan barcha adminlarning reportlarini yig'ib chiqarish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    if not start_date or not end_date:
        return await ctx.reply("❌ Misol: `!reports 02.03 18.04`")

    try:
        start_parsed = parse_date_any(start_date)
        end_parsed = parse_date_any(end_date)
        if not start_parsed or not end_parsed: 
            raise ValueError("Sana noto'g'ri formatda yoki yil aniqlanmadi.")

        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply(f"🔍 **{table['name']}** bo'yicha hisobotlar qidirilmoqda (bu biroz vaqt olishi mumkin)...")

        # 1. Arxivdan qidirish (Case-insensitive spreadsheet name)
        def get_archive_summary():
            ensure_archive_dir()
            archive_files, _ = split_archive_files()
            s_key = archive_date_key(start_parsed)
            e_key = archive_date_key(end_parsed)
            summary = {}
            for db_file in archive_files:
                try:
                    with sqlite3.connect(db_file) as conn:
                        conn.execute("PRAGMA journal_mode=WAL;")
                        conn.execute("PRAGMA temp_store=MEMORY;")
                        cur = conn.cursor()
                        query = """
                            SELECT nick, value FROM archive_records 
                            WHERE type='report' AND LOWER(spreadsheet_name) = LOWER(?)
                            AND (substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)) BETWEEN ? AND ?
                        """
                        cur.execute(query, (table["name"], s_key, e_key))
                        for nick_val, val in cur.fetchall():
                            try:
                                num = int(float(str(val)))
                                nk = nick_key(nick_val)
                                if nk not in summary: summary[nk] = {"name": nick_val, "total": 0}
                                summary[nk]["total"] += num
                            except: continue
                except: continue
            return summary

        total_summary = await run_blocking(get_archive_summary)

        # 2. Jadvaldagi barcha listlardan (Live) qidirish
        live_summary = await collect_all_reports_live_summary(sh, start_parsed, end_parsed)

        # 3. Birlashtirish
        for nk, data in live_summary.items():
            if nk in total_summary:
                total_summary[nk]["total"] += data["total"]
            else:
                total_summary[nk] = data

        await safe_delete_message(status_msg)
        if not total_summary: 
            return await ctx.reply(f"ℹ {start_parsed} — {end_parsed} oralig'ida hech qanday report topilmadi.")

        sorted_list = sorted(total_summary.values(), key=lambda x: x["total"], reverse=True)
        lines = [f"📊 **Adminlar Reportlari ({start_parsed} — {end_parsed})**", ""]
        for i, item in enumerate(sorted_list, 1):
            lines.append(f"{i}. **{item['name']}**: {item['total']} ta")

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

class TopAdminsView(discord.ui.View):
    def __init__(self, author_id, amount, sh, table):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.amount = amount
        self.sh = sh
        self.table = table

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugmalar siz uchun emas.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Report", style=discord.ButtonStyle.primary)
    async def report_top(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        summary, start, end = await collect_full_weekly_data(self.sh, self.table["name"])
        filtered = [v for v in summary.values() if not v.get("on_leave")]
        sorted_top = sorted(filtered, key=lambda x: x["report"], reverse=True)[:self.amount]
        
        embed = discord.Embed(title=f"🏆 Top {self.amount} Adminlar (Report)", color=discord.Color.blue())
        embed.description = f"📅 {start} — {end}\nJadval: **{self.table['name']}**"
        for i, admin in enumerate(sorted_top, 1):
            embed.add_field(name=f"{i}. {admin['name']}", value=f"📊 **{admin['report']}** ta report", inline=False)
        await interaction.followup.send(embed=embed)

    @discord.ui.button(label="Online", style=discord.ButtonStyle.success)
    async def online_top(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        summary, start, end = await collect_full_weekly_data(self.sh, self.table["name"])
        filtered = [v for v in summary.values() if not v.get("on_leave")]
        sorted_top = sorted(filtered, key=lambda x: x["minutes"], reverse=True)[:self.amount]
        
        embed = discord.Embed(title=f"🏆 Top {self.amount} Adminlar (Online)", color=discord.Color.green())
        embed.description = f"📅 {start} — {end}\nJadval: **{self.table['name']}**"
        for i, admin in enumerate(sorted_top, 1):
            embed.add_field(name=f"{i}. {admin['name']}", value=f"🕒 **{minutes_to_hhmm(admin['minutes'])}**", inline=False)
        await interaction.followup.send(embed=embed)

@bot.command(name="top")
async def top_admins_cmd(ctx, amount: int = 3):
    """Top adminlarni tanlash (report/online)"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        view = TopAdminsView(ctx.author.id, amount, sh, table)
        await ctx.reply(f"📊 Top **{amount}** adminlarni hisoblash turini tanlang:", view=view)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

@bot.command(name="stw")
async def stw_cmd(ctx):
    """Adminlarning haftalik online va reportlarini chiqarish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Haftalik normalar hisoblanmoqda...")

        # Adminlar tartibini aniqlash uchun asosiy ro'yxatni o'qiymiz
        admin_ws = await find_adminlist_worksheet(sh)
        admin_values = await run_blocking(admin_ws.get_all_values)

        summary, start, end = await collect_full_weekly_data(sh, table["name"])
        await safe_delete_message(status_msg)
        
        online_lines = [f"⏱ **Haftalik Online ({start} - {end})**", ""]
        report_lines = [f"📊 **Haftalik Report ({start} - {end})**", ""]
        
        # Jadvaldagi tartib bo'yicha ma'lumotlarni saralash
        for r_idx in range(DATA_START_ROW, len(admin_values) + 1):
            nick = _cell(admin_values, r_idx, ADMINS_COLUMN)
            if not nick: continue
            
            nk = nick_key(nick)
            if nk in summary:
                admin = summary[nk]
                if admin.get("on_leave"): continue
                online_lines.append(f"{admin['name']} {minutes_to_hhmm(admin['minutes'])}")
                report_lines.append(f"{admin['name']} {admin['report']}")
            
        # Online ma'lumotlarini alohida xabar sifatida yuborish
        for part in chunk_text("\n".join(online_lines)):
            await ctx.reply(part)
            
        # Report ma'lumotlarini alohida xabar sifatida yuborish
        for part in chunk_text("\n".join(report_lines)):
            await ctx.reply(part)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

@bot.command(name="wn")
async def wn_cmd(ctx):
    """'Выплата доната' listida online soatlarini yaxlitlab (C2 dan boshlab) yozish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔄 'Выплата доната' listi yangilanmoqda...")
        
        donate_ws = await run_blocking(find_donate_worksheet, sh)
        values = await run_blocking(donate_ws.get_all_values)
        if not values: raise ValueError("Donate listi bo'sh.")
        
        # Online ustunini topish yoki C (3) deb hisoblash
        online_col = await run_blocking(find_header_column_by_aliases, donate_ws, ["online", "онлайн", "часы"], 3) or 3
        
        summary, _, _ = await collect_full_weekly_data(sh, table["name"])
        
        updates = []
        updated_rows = []
        for row_idx in range(2, len(values) + 1): # C2 (Row 2)
            nick = _cell(values, row_idx, ADMINS_COLUMN)
            if not nick: continue
            
            nk = nick_key(nick)
            total_mins = summary.get(nk, {}).get("minutes", 0)
            
            # Yaxlitlash: 15:14 -> 15, 15:34 -> 16
            rounded_hours = (total_mins + 30) // 60
            
            updates.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, online_col),
                "values": [[str(rounded_hours)]]
            })
            updated_rows.append(row_idx)
            
        if updates:
            await run_blocking(lambda: donate_ws.batch_update(updates, value_input_option="USER_ENTERED"))
            await run_blocking(apply_custom_formatting, donate_ws, updated_rows, online_col)
            await safe_delete_message(status_msg)
            await ctx.reply(f"✅ **{table['name']}**: 'Выплата доната' listida **{len(updates)}** ta adminning online soati yangilandi.")
        else:
            await safe_delete_message(status_msg)
            await ctx.reply("ℹ Yangilash uchun adminlar topilmadi.")
            
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

@bot.command(name="serverreports", aliases=["sr"])
async def server_reports_cmd(ctx, start_date: str = None, end_date: str = None):
    """🌐 Server bo'yicha barcha adminlar bajargan reportlar yig'indisi"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    if not start_date or not end_date:
        return await ctx.reply("❌ Misol: `!sr 02.03 18.04` (yoki `!serverreports`) ")

    try:
        start_parsed = parse_date_any(start_date)
        end_parsed = parse_date_any(end_date)
        if not start_parsed or not end_parsed: 
            raise ValueError("Sana noto'g'ri formatda.")

        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply(f"🔍 **{table['name']}** bo'yicha umumiy statistika hisoblanmoqda...")

        # 1. Arxivdan jami summani olish
        def get_archive_total():
            ensure_archive_dir()
            archive_files, _ = split_archive_files()
            s_key = archive_date_key(start_parsed)
            e_key = archive_date_key(end_parsed)
            grand_total = 0
            for db_file in archive_files:
                try:
                    with sqlite3.connect(db_file) as conn:
                        conn.execute("PRAGMA journal_mode=WAL;")
                        conn.execute("PRAGMA temp_store=MEMORY;")
                        cur = conn.cursor()
                        query = """
                            SELECT SUM(CAST(value AS INTEGER)) FROM archive_records 
                            WHERE type='report' AND LOWER(spreadsheet_name) = LOWER(?)
                            AND (substr(date, 7, 4) || substr(date, 4, 2) || substr(date, 1, 2)) BETWEEN ? AND ?
                        """
                        cur.execute(query, (table["name"], s_key, e_key))
                        res = cur.fetchone()[0]
                        if res: grand_total += int(res)
                except: continue
            return grand_total

        archive_sum = await run_blocking(get_archive_total)

        # 2. Live listlardan jami summani olish
        live_summary = await collect_all_reports_live_summary(sh, start_parsed, end_parsed)
        live_sum = sum(item["total"] for item in live_summary.values())

        total_server_reports = archive_sum + live_sum

        await safe_delete_message(status_msg)
        
        embed = discord.Embed(title="🌐 Server Umumiy Statistikasi", color=discord.Color.blue())
        embed.add_field(name="📅 Muddat", value=f"{start_parsed} — {end_parsed}", inline=False)
        embed.add_field(name="📊 Jami reportlar", value=f"**{total_server_reports:,}** ta", inline=True)
        embed.set_footer(text=f"Jadval: {table['name']}")
        
        await ctx.reply(embed=embed)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

# =========================================================
# ADMIN PENALTY (ADPV) COMMANDS
# =========================================================
class PenaltyModal(discord.ui.Modal):
    def __init__(self, penalty_type, admin_ws, sh, table):
        title = "Ogohlantirish Berish" if penalty_type == "pred" else "Tanbeh Berish"
        super().__init__(title=title)
        self.penalty_type = penalty_type
        self.admin_ws = admin_ws
        self.sh = sh
        self.table = table
        
        self.nick_input = discord.ui.TextInput(label="Admin Niki", placeholder="Ivan_Vasilyev", required=True)
        self.date_input = discord.ui.TextInput(label="Sana", placeholder=today_str(), default=today_str(), required=True)
        self.reason_input = discord.ui.TextInput(label="Sababi", placeholder="Flood/Qoidalarni buzish", required=True, style=discord.TextStyle.paragraph)
        
        self.add_item(self.nick_input)
        self.add_item(self.date_input)
        self.add_item(self.reason_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        nick = self.nick_input.value.strip()
        date_val = parse_date_any(self.date_input.value) or today_str()
        reason = self.reason_input.value.strip()

        try:
            data = await run_blocking(self.admin_ws.get_all_values)
            nick_to_row = {}
            for r_idx in range(DATA_START_ROW, len(data) + 1):
                n_val = _cell(data, r_idx, ADMINS_COLUMN)
                if n_val: nick_to_row[nick_key(n_val)] = r_idx
            
            row_idx = resolve_exact_or_close_row(nick_to_row, nick)
            if not row_idx:
                return await interaction.followup.send(f"❌ Admin topilmadi: `{nick}`")
            
            row_data = data[row_idx - 1]
            real_nick = row_data[0]
            
            # Penalti ustunlari: M=13(idx12), N=14(idx13), O=15(idx14), P=16(idx15)
            curr_vig_count = parse_penalty_count(row_data[12]) if len(row_data) > 12 else 0
            curr_pred_count = parse_penalty_count(row_data[14]) if len(row_data) > 14 else 0
            
            updates = []
            final_msg = ""
            p_type = self.penalty_type

            if p_type == "pred":
                if curr_pred_count >= 1:
                    p_type = "vig"
                    # Pred vigga aylanganda pred kataklarini butunlay tozalash
                    updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 15), "values": [[""]]})
                    updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 16), "values": [[""]]})
                    final_msg = f"⚠ **{real_nick}**da 1/2 ogohlantirish bor edi, u Tanbehga (Vig) aylandi!\n"
                else:
                    updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 15), "values": [["1/2"]]})
                    updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 16), "values": [[date_val]]})
                    await self.apply_penalty_format(row_idx, 15)
                    await self.apply_penalty_format(row_idx, 16)
                    final_msg = f"✅ **{real_nick}**ga ogohlantirish (1/2) berildi."

            if p_type == "vig":
                new_vig_val = f"{curr_vig_count + 1}/3" # count/limit formati
                updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 13), "values": [[new_vig_val]]})
                updates.append({"range": gspread.utils.rowcol_to_a1(row_idx, 14), "values": [[date_val]]})
                await self.apply_penalty_format(row_idx, 13)
                await self.apply_penalty_format(row_idx, 14)
                final_msg += f"✅ **{real_nick}**ga tanbeh ({new_vig_val}) berildi."
                if (curr_vig_count + 1) == 2:
                    final_msg += "\n🔔 **DIQQAT: Tanbeh 2/3 bo'ldi!**"

            await run_blocking(lambda: self.admin_ws.batch_update(updates, value_input_option="RAW"))
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
            await run_blocking(archive_insert_many, [(date_val, real_nick, p_type, reason, self.table["name"], self.admin_ws.title, now_ts)])
            
            await interaction.followup.send(final_msg)

        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {e}")

    async def apply_penalty_format(self, row, col):
        await run_blocking(apply_custom_formatting, self.admin_ws, [row], col)

class NegativeNormaModal(discord.ui.Modal):
    def __init__(self, mode, online_ws, sh, table):
        title = "Minus Report" if mode == "report" else "Minus Online"
        super().__init__(title=title)
        self.mode = mode
        self.online_ws = online_ws
        self.sh = sh
        self.table = table
        
        self.nick_input = discord.ui.TextInput(label="Admin Niki", placeholder="Ivan_Vasilyev", required=True)
        self.amount_input = discord.ui.TextInput(label="Miqdori", placeholder="100 yoki 02:00", required=True)
        self.reason_input = discord.ui.TextInput(label="Sababi", placeholder="Farm report/online", required=True, style=discord.TextStyle.paragraph)
        
        self.add_item(self.nick_input)
        self.add_item(self.amount_input)
        self.add_item(self.reason_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        nick = self.nick_input.value.strip()
        amount = self.amount_input.value.strip()
        reason = self.reason_input.value.strip()
        today = today_str()

        try:
            online_col, report_col = await run_blocking(find_date_columns, self.online_ws, today)
            if not online_col: 
                return await interaction.followup.send("❌ Bugungi sana ustuni topilmadi.")
            
            nick_to_row = await run_blocking(build_admin_to_row, self.online_ws)
            row_idx = resolve_exact_or_close_row(nick_to_row, nick)
            if not row_idx: 
                return await interaction.followup.send("❌ Admin topilmadi.")

            target_col = report_col if self.mode == "report" else online_col
            current_val = await run_blocking(lambda: self.online_ws.cell(row_idx, target_col).value)
            
            if self.mode == "report":
                curr_num = int(float(current_val)) if current_val and str(current_val).replace('.','',1).isdigit() else 0
                sub_num = int(amount)
                new_val = str(max(0, curr_num - sub_num))
                log_val = f"-{sub_num} report"
            else:
                curr_min = parse_duration_to_minutes(current_val)
                sub_min = hhmm_to_minutes(amount)
                new_val = minutes_to_hhmm(max(0, curr_min - sub_min))
                log_val = f"-{amount} online"

            await run_blocking(lambda: self.online_ws.update_cell(row_idx, target_col, new_val))
            await run_blocking(apply_custom_formatting, self.online_ws, [row_idx], target_col)
            
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
            await run_blocking(archive_insert_many, [(today, nick, f"fine_{self.mode}", f"{log_val}: {reason}", self.table["name"], self.online_ws.title, now_ts)])
            
            await interaction.followup.send(f"✅ **{nick}** uchun jazo qo'llanildi: {log_val}\nSabab: {reason}")
        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {e}")

class ClearHistoryModal(discord.ui.Modal, title="Jazolar Tarixini Tozalash"):
    nick_input = discord.ui.TextInput(label="Admin Niki", placeholder="Ivan_Vasilyev", required=True)

    def __init__(self, table):
        super().__init__()
        self.table = table

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        nick = self.nick_input.value.strip()
        deleted = await run_blocking(archive_delete_records, nick, self.table["name"], ["pred", "vig", "fine_report", "fine_online"])
        await interaction.followup.send(f"🧹 **{nick}** ning barcha jazo tarixlari arxivdan o'chirildi. Jami: {deleted} ta yozuv.")

class AdpvChoiceView(discord.ui.View):
    def __init__(self, author_id, admin_ws, online_ws, sh, table):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.admin_ws = admin_ws
        self.online_ws = online_ws
        self.sh = sh
        self.table = table

    @discord.ui.button(label="Ogohlantirish (Pred)", style=discord.ButtonStyle.secondary, row=0)
    async def pred_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PenaltyModal("pred", self.admin_ws, self.sh, self.table))

    @discord.ui.button(label="Tanbeh (Vig)", style=discord.ButtonStyle.danger, row=0)
    async def vig_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PenaltyModal("vig", self.admin_ws, self.sh, self.table))

    @discord.ui.button(label="-100 Report", style=discord.ButtonStyle.primary, row=1)
    async def minus_rep_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NegativeNormaModal("report", self.online_ws, self.sh, self.table))

    @discord.ui.button(label="-2 Soat Online", style=discord.ButtonStyle.primary, row=1)
    async def minus_on_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(NegativeNormaModal("online", self.online_ws, self.sh, self.table))

    @discord.ui.button(label="Tarixni Tozalash", style=discord.ButtonStyle.gray, row=2)
    async def clear_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ClearHistoryModal(self.table))

@bot.command(name="adpv")
async def adpv_cmd(ctx):
    """Adminlarga tanbeh yoki ogohlantirish berish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        admin_ws = await find_adminlist_worksheet(sh)
        online_ws = await run_blocking(find_online_worksheet, sh)
        view = AdpvChoiceView(ctx.author.id, admin_ws, online_ws, sh, table)
        await ctx.reply("✍ Jazo turini tanlang:", view=view)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

@bot.command(name="ats")
async def ats_cmd(ctx, start_date: str = None, end_date: str = None):
    """Admin qo'shish va o'chirish tarixini ko'rish (Sana yozilmasa shu hafta)"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    now = datetime.now(UZ_TZ)
    if not start_date:
        # Joriy haftaning dushanba kuni
        monday = now - timedelta(days=now.weekday())
        start_parsed = monday.strftime("%d.%m.%Y")
        end_parsed = now.strftime("%d.%m.%Y")
    else:
        start_parsed = parse_date_any(start_date)
        end_parsed = parse_date_any(end_date) if end_date else now.strftime("%d.%m.%Y")

    if not start_parsed or not end_parsed:
        return await ctx.reply("❌ Sana xato. Misol: `!ats 01.10 07.10` yoki shunchaki `!ats` (bu hafta)")

    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply(f"🔍 **{table['name']}**: {start_parsed} — {end_parsed} oralig'idagi tarix qidirilmoqda...")

        # Admin qo'shilgan va o'chirilgan yozuvlarni olish
        add_rows = await run_blocking(archive_query_all_databases, "%", "admin_add", start_parsed, end_parsed, table["name"])
        rem_rows = await run_blocking(archive_query_all_databases, "%", "admin_remove", start_parsed, end_parsed, table["name"])
        
        all_history = sorted(add_rows + rem_rows, key=lambda x: archive_date_key(x[0]))
        await safe_delete_message(status_msg)

        if not all_history:
            return await ctx.reply(f"ℹ {start_parsed} — {end_parsed} oralig'ida o'zgarishlar topilmadi.")

        lines = [f"📜 **Adminlar Tarixi ({start_parsed} — {end_parsed})**", ""]
        for row in all_history:
            icon = "✅ [+] " if row[2] == "admin_add" else "🗑 [-] "
            lines.append(f"{icon}{row[0]}: **{row[1]}**\n   └ {row[3]}")

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

@bot.command(name="report")
async def report_archive_cmd(ctx, nick: str = None, start_date: str = None, end_date: str = None):
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    if not nick or not start_date or not end_date:
        await ctx.reply("❌ Misol: `!report Ivan_Vasilyev 08.04 16.04`")
        await ask_delete_command(ctx)
        return

    try:
        start_parsed = parse_date_any(start_date)
        end_parsed = parse_date_any(end_date)
        if not start_parsed or not end_parsed:
            raise ValueError("Sana noto'g'ri.")

        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Ma'lumotlar yig'ilmoqda...")

        archive_rows = await run_blocking(archive_query_all_databases, nick, "report", start_parsed, end_parsed, table["name"])
        sheet_rows = await collect_live_rows_from_all_sheets(sh, nick, "report", start_parsed, end_parsed, table["name"])

        all_rows = merge_archive_and_sheet_rows(archive_rows, sheet_rows)
        await safe_delete_message(status_msg)

        if not all_rows:
            await ctx.reply("ℹ Ma'lumot topilmadi.")
            return

        total = sum(int(float(str(r[3]))) for r in all_rows if str(r[3]).replace('.','',1).isdigit())
        lines = [f"📊 Report: **{nick}** ({start_parsed} - {end_parsed})", ""]
        for date_val, _, _, val, _, _, _ in all_rows:
            lines.append(f"  {date_val} — {val}")
        lines.append(f"\n**Jami: {total}**")

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

@bot.command(name="norma")
async def norma_archive_cmd(ctx, nick: str = None, start_date: str = None, end_date: str = None):
    """⏱ Arxiv + Jadvaldan norma qidirish"""
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not nick or not start_date or not end_date:
        await ctx.reply("❌ Misol: `!norma Ivan_Vasilyev 08.04 16.04`")
        await ask_delete_command(ctx)
        return

    try:
        start_parsed = parse_date_any(start_date)
        end_parsed = parse_date_any(end_date)
        if not start_parsed or not end_parsed:
            raise ValueError("Sana noto'g'ri.")

        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Ma'lumotlar yig'ilmoqda...")

        archive_rows = await run_blocking(archive_query_all_databases, nick, "norma", start_parsed, end_parsed, table["name"])
        sheet_rows = await collect_live_rows_from_all_sheets(sh, nick, "norma", start_parsed, end_parsed, table["name"])

        all_rows = merge_archive_and_sheet_rows(archive_rows, sheet_rows)
        await safe_delete_message(status_msg)

        if not all_rows:
            await ctx.reply("ℹ Ma'lumot topilmadi.")
            return

        total_min = sum(hhmm_to_minutes(str(r[3])) for r in all_rows)
        lines = [f"⏱ Norma: **{nick}** ({start_parsed} - {end_parsed})", ""]
        for date_val, _, _, val, _, _, _ in all_rows:
            lines.append(f"  {date_val} — {val}")
        lines.append(f"\n**Jami: {minutes_to_hhmm(total_min)}**")

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

# =========================================================
# VIG/PRED CHECK COMMAND
# =========================================================
class VpSelectionView(discord.ui.View):
    def __init__(self, author_id, ready_lines, active_lines, admin_ws, ready_candidates):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.ready_lines = ready_lines
        self.active_lines = active_lines
        self.admin_ws = admin_ws
        self.ready_candidates = ready_candidates
        
        # Text formatni yaratish
        self.text_ready_lines = []
        for c in ready_candidates:
            nick = c['nick']
            # Viglar uchun
            for date in c.get('vig_dates', []):
                self.text_ready_lines.append(f"{nick} {date} tanbeh yechildi")
            # Predlar uchun
            for date in c.get('pred_dates', []):
                self.text_ready_lines.append(f"{nick} {date} ogohlantirish yechildi")

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu menyu siz uchun emas.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="1. Tayyorlar", style=discord.ButtonStyle.success)
    async def show_ready(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        content = "\n".join(self.ready_lines) if len(self.ready_lines) > 2 else "Hozircha jazosi yechiladigan adminlar yo'q."
        for part in chunk_text(content):
            await interaction.followup.send(part)

    @discord.ui.button(label="2. Jarayondagilar", style=discord.ButtonStyle.primary)
    async def show_active(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        content = "\n".join(self.active_lines) if len(self.active_lines) > 2 else "Hozircha jazo muddatini o'tayotgan adminlar yo'q."
        for part in chunk_text(content):
            await interaction.followup.send(part)

    @discord.ui.button(label="3. Tayyor format", style=discord.ButtonStyle.secondary)
    async def show_text(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.text_ready_lines:
            content = "\n".join(self.text_ready_lines)
            for part in chunk_text(content):
                await interaction.followup.send(part)
            # Text format ko'rsatgandan keyin o'chirish so'rovi
            await self.ask_removal(interaction)
        else:
            await interaction.followup.send("ℹ️ Hozircha tayyor jazolar yo'q.")

    async def ask_removal(self, interaction):
        view = PenaltyRemovalConfirmationView(self.author_id, self.admin_ws, self.ready_candidates)
        await interaction.followup.send("❓ Jadvaldan ham o'chiraveraymi?", view=view)

class PenaltyRemovalConfirmationView(discord.ui.View):
    def __init__(self, author_id, admin_ws, candidates):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.admin_ws = admin_ws
        self.candidates = candidates

    @discord.ui.button(label="Ha", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        updates = []
        for c in self.candidates:
            row = c['row_idx']
            # M=13, N=14, O=15, P=16 ustunlarni tozalash
            updates.append({"range": gspread.utils.rowcol_to_a1(row, 13), "values": [[""]]})
            updates.append({"range": gspread.utils.rowcol_to_a1(row, 14), "values": [[""]]})
            updates.append({"range": gspread.utils.rowcol_to_a1(row, 15), "values": [[""]]})
            updates.append({"range": gspread.utils.rowcol_to_a1(row, 16), "values": [[""]]})
        
        if updates:
            await run_blocking(self.admin_ws.batch_update, updates)
            await interaction.followup.send("✅ Jadval yangilandi: Barcha tayyor jazo choralari o'chirildi.")
        self.stop()

    @discord.ui.button(label="Yo'q", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Amal bekor qilindi.", ephemeral=True)
        self.stop()

@bot.command(name="vp")
async def vp_check_cmd(ctx):
    """Tanbeh va ogohlantirishlarni yechish holatini tekshirish"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply("🔍 Adminlar ro'yxati yuklanmoqda...")
        admin_ws = await find_adminlist_worksheet(sh)
        data = await run_blocking(admin_ws.get_all_values)
        if not data: raise ValueError("Jadval bo'sh.")

        today = today_str()
        admins_to_check = []
        
        # 1. Vig/Predi bor adminlarni aniqlash (BARCHA LEVELLAR, Row 2 dan boshlab)
        # Level filtri YO'Q - 1-6 level barcha adminlar tekshiriladi
        for row_idx in range(1, len(data)): # 1 index = 2-qator (header skip)
            row = data[row_idx]
            if len(row) < 16: continue
            
            # Ustunlar: M=12 (Vig count), N=13 (Vig date), O=14 (Pred count), P=15 (Pred date)
            vigs = parse_penalty_count(row[12]) if len(row) > 12 else 0  # M ustun - Tanbeh soni
            preds = parse_penalty_count(row[14]) if len(row) > 14 else 0  # O ustun - Ogohlantirish soni
            
            # Faqat pred yoki vig bor adminlarni qo'shamiz (level qanday bo'lishidan qat'i nazar)
            if vigs > 0 or preds > 0:
                admins_to_check.append({"nick": row[0], "row": row, "row_idx_real": row_idx + 1})

        if not admins_to_check:
            await safe_delete_message(status_msg)
            return await ctx.reply("ℹ Hozircha tanbehi bor adminlar yo'q.")

        await status_msg.edit(content=f"🔍 {len(admins_to_check)} admin uchun barcha listlar tahlil qilinmoqda...")
        
        # 2. Barcha listlardan ma'lumotlarni BIR MARTA yig'ish
        earliest_date = "01.01.2024" # Qidiruvni boshlash uchun eng eski sana
        nicks_list = [a["nick"] for a in admins_to_check]
        all_live_cache = await collect_live_data_for_multiple_nicks(sh, nicks_list, earliest_date, today, table["name"])

        ready_lines = ["✅ **1-ROYHAT: Jazosi yechilishi kerak bo'lganlar (Tayyor):**", ""]
        active_lines = ["❗ **2-ROYHAT: Aktual jazo choralari bor adminlar:**", ""]
        ready_candidates = []

        for admin_data in admins_to_check:
            row = admin_data["row"]
            nick = admin_data["nick"]
            nk = nick_key(nick)
            
            vigs = parse_penalty_count(row[12])
            preds = parse_penalty_count(row[14])

            # Arxivdan barcha jazolarni olamiz, lekin faqat jadvalda ko'rsatilgan sonini ishlatamiz
            all_vig_history = await run_blocking(archive_get_penalty_history, nick, "vig", vigs, table["name"])
            all_pred_history = await run_blocking(archive_get_penalty_history, nick, "pred", preds, table["name"])
            
            # Faqat eng oxirgi N ta jazoni olamiz (jadvalda ko'rsatilgan son bo'yicha)
            vig_history = all_vig_history[-vigs:] if vigs > 0 else []
            pred_history = all_pred_history[-preds:] if preds > 0 else []

            status = f"🔸 **{nick}**: Tanbeh: {vigs}/3, Ogohl.: {preds}/2\n"
            all_penalties_ready = True

            # Tanbehlarni (Vig) alohida hisoblash
            for i, (v_date, v_reason) in enumerate(reversed(vig_history), 1):
                arc_n = await run_blocking(archive_query_all_databases, nick, "norma", v_date, today, table["name"])
                sh_n = [r for r in all_live_cache.get(nk, {}).get("norma", []) if is_date_in_range(r[0], v_date, today)]
                # "-" ni e'tiborsiz qoldirish
                cur_v_min = sum(hhmm_to_minutes(str(r[3])) for r in merge_archive_and_sheet_rows(arc_n, sh_n) if str(r[3]) not in ["-", "", "xx:xx"])

                arc_r = await run_blocking(archive_query_all_databases, nick, "report", v_date, today, table["name"])
                sh_r = [r for r in all_live_cache.get(nk, {}).get("report", []) if is_date_in_range(r[0], v_date, today)]
                cur_v_rep = sum(int(float(str(r[3]))) for r in merge_archive_and_sheet_rows(arc_r, sh_r) if str(r[3]).replace('.','',1).isdigit())

                # 1 vig = 20 soat (1200 minut), report faqat ko'rsatish uchun
                v_required_min = 1200
                v_required_rep = 1000  # Faqat ko'rsatish uchun
                v_met = (cur_v_min >= v_required_min)  # Faqat online norma tekshiriladi
                if not v_met: all_penalties_ready = False
                
                # Aniq raqamlarni ko'rsatish
                v_status = f"{minutes_to_hhmm(cur_v_min)}/{minutes_to_hhmm(v_required_min)} | {cur_v_rep}/{v_required_rep} rep"
                if v_met:
                    v_status = "✅ " + v_status
                else:
                    v_status = "⏳ " + v_status
                    
                status += f"   🔹 Tanbeh #{i} ({v_date}): {v_status}\n"

            # Ogohlantirishlarni (Pred) alohida hisoblash
            for i, (p_date, p_reason) in enumerate(reversed(pred_history), 1):
                arc_n = await run_blocking(archive_query_all_databases, nick, "norma", p_date, today, table["name"])
                sh_n = [r for r in all_live_cache.get(nk, {}).get("norma", []) if is_date_in_range(r[0], p_date, today)]
                # "-" ni e'tiborsiz qoldirish
                cur_p_min = sum(hhmm_to_minutes(str(r[3])) for r in merge_archive_and_sheet_rows(arc_n, sh_n) if str(r[3]) not in ["-", "", "xx:xx"])

                arc_r = await run_blocking(archive_query_all_databases, nick, "report", p_date, today, table["name"])
                sh_r = [r for r in all_live_cache.get(nk, {}).get("report", []) if is_date_in_range(r[0], p_date, today)]
                cur_p_rep = sum(int(float(str(r[3]))) for r in merge_archive_and_sheet_rows(arc_r, sh_r) if str(r[3]).replace('.','',1).isdigit())

                # 1 pred = 10 soat (600 minut), report faqat ko'rsatish uchun
                p_required_min = 600
                p_required_rep = 500  # Faqat ko'rsatish uchun
                p_met = (cur_p_min >= p_required_min)  # Faqat online norma tekshiriladi
                if not p_met: all_penalties_ready = False

                # Aniq raqamlarni ko'rsatish
                p_status = f"{minutes_to_hhmm(cur_p_min)}/{minutes_to_hhmm(p_required_min)} | {cur_p_rep}/{p_required_rep} rep"
                if p_met:
                    p_status = "✅ " + p_status
                else:
                    p_status = "⏳ " + p_status
                    
                status += f"   🔹 Ogohl. #{i} ({p_date}): {p_status}\n"

            if all_penalties_ready:
                ready_lines.append(status + "   🎉 **Barcha normalar bajarilgan!**")
                ready_candidates.append({
                    "nick": nick,
                    "row_idx": admin_data["row_idx_real"],
                    "vigs": vigs,
                    "preds": preds,
                    "vig_dates": [v_date for v_date, _ in reversed(vig_history)],
                    "pred_dates": [p_date for p_date, _ in reversed(pred_history)]
                })
            else:
                active_lines.append(status)

        await safe_delete_message(status_msg)
        
        view = VpSelectionView(ctx.author.id, ready_lines, active_lines, admin_ws, ready_candidates)
        await ctx.reply("📋 **Vig/Pred Tahlili**. Kerakli bo'limni tanlang:", view=view)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

# =========================================================
# ADMIN INFO COMMAND
# =========================================================
@bot.command(name="info")
async def admin_info_cmd(ctx, nick: str = None):
    """Admin haqida batafsil ma'lumot (lvllar tarixi va jami report)"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    if not nick:
        return await ctx.reply("❌ Misol: `!info Ivan_Vasilyev`")

    try:
        sh, table = await choose_table_for_action(ctx)
        status_msg = await ctx.reply(f"🔍 **{nick}** haqidagi ma'lumotlar tahlil qilinmoqda...")
        
        admin_ws = await find_adminlist_worksheet(sh)
        data = await run_blocking(admin_ws.get_all_values)

        row_idx = find_row_by_nick(admin_ws, nick)
        if not row_idx:
            await safe_delete_message(status_msg)
            return await ctx.reply(f"❌ Admin topilmadi: `{nick}`")

        row = data[row_idx - 1]
        
        # Loginni aniqlash (admin listdan yoki donate/login listdan)
        login_val = ""
        login_col = find_login_column(admin_ws)
        if login_col and login_col <= len(row):
            login_val = norm(row[login_col - 1])
        
        # Agar admin listda login topilmasa, login sheetdan qidiramiz
        if not login_val:
            try:
                login_ws = await run_blocking(find_login_worksheet, sh)
                login_data = await run_blocking(login_ws.get_all_values)
                l_admin_col = find_header_column_by_aliases(login_ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=3) or 1
                l_login_col = find_header_column_by_aliases(login_ws, LOGIN_ALIASES, scan_rows=3)
                if l_login_col:
                    for r_idx2 in range(DATA_START_ROW, len(login_data) + 1):
                        n_v = _cell(login_data, r_idx2, l_admin_col)
                        if nick_key(n_v) == nick_key(nick):
                            login_val = norm(_cell(login_data, r_idx2, l_login_col))
                            break
            except Exception:
                pass
        
        # Lavozimni aniqlash (J ustuni - 10-ustun, index 9) - Active adminlar uchun
        pos_col = await run_blocking(find_position_column, admin_ws) or 10
        pos_val = norm(row[pos_col - 1]) if pos_col <= len(row) else ""
        if not pos_val: pos_val = "Yo'q"

        # Lvl sanalarini yig'ish (D-I ustunlari: 3-8 index)
        lvl_history = []
        join_date = None
        start_lvl = 1
        
        for i in range(1, 7):
            col_idx = i + 2 # D=3, E=4...
            dt_val = parse_date_any(row[col_idx]) if col_idx < len(row) else None
            if dt_val:
                lvl_history.append(f"• **{i}-lvl**: {dt_val}")
                if join_date is None:
                    join_date = dt_val
                    start_lvl = i

        if not join_date:
            await safe_delete_message(status_msg)
            return await ctx.reply(f"❌ `{nick}` uchun lvllar tarixi topilmadi (jadvalda sanalar ko'rsatilmagan).")

        # Adminlik muddatini kunlarda hisoblash
        join_dt_obj = datetime.strptime(join_date, "%d.%m.%Y").replace(tzinfo=None)
        calc_end_dt_obj = datetime.now(UZ_TZ).replace(tzinfo=None)
        days_diff = (calc_end_dt_obj - join_dt_obj).days

        # Reportlarni hisoblash (Join date dan hozirgi kungacha)
        today = today_str()
        arc_r = await run_blocking(archive_query_all_databases, nick, "report", join_date, today, table["name"]) # Use admin_data["row_idx_real"]
        sh_r = await collect_live_rows_from_all_sheets(sh, nick, "report", join_date, today, table["name"])
        all_r = merge_archive_and_sheet_rows(arc_r, sh_r)
        
        total_rep = sum(int(float(str(r[3]))) for r in all_r if str(r[3]).replace('.','',1).isdigit())
        
        # Onlineni hisoblash
        arc_n = await run_blocking(archive_query_all_databases, nick, "norma", join_date, today, table["name"])
        sh_n = await collect_live_rows_from_all_sheets(sh, nick, "norma", join_date, today, table["name"]) # Use admin_data["row_idx_real"]
        all_n = merge_archive_and_sheet_rows(arc_n, sh_n)
        total_min = sum(hhmm_to_minutes(str(r[3])) for r in all_n)

        # Jazolar tarixini yig'ish
        arc_p = await run_blocking(archive_query_all_databases, nick, "pred", join_date, today, table["name"])
        arc_v = await run_blocking(archive_query_all_databases, nick, "vig", join_date, today, table["name"])
        arc_add = await run_blocking(archive_query_all_databases, nick, "admin_add", join_date, today, table["name"])
        arc_fr = await run_blocking(archive_query_all_databases, nick, "fine_report", join_date, today, table["name"])
        arc_fo = await run_blocking(archive_query_all_databases, nick, "fine_online", join_date, today, table["name"])
        
        # Niklar tarixini olish
        arc_nc = await run_blocking(archive_query_all_databases, nick, "nick_change", "01.01.2024", today, table["name"])
        nick_history = []
        for r in arc_nc:
            nick_history.append(f"• {r[0]}: {r[3]}")
        
        penalties = []
        for r in arc_add: penalties.append(f"• [Qabul] {r[0]}: {r[3]}")
        for r in arc_p: penalties.append(f"• [Pred] {r[0]}: {r[3]}")
        for r in arc_v: penalties.append(f"• [Vig] {r[0]}: {r[3]}")
        for r in arc_fr: penalties.append(f"• [Jarimalar] {r[0]}: {r[3]}")
        for r in arc_fo: penalties.append(f"• [Jarimalar] {r[0]}: {r[3]}")
        penalties_text = "\n".join(penalties) if penalties else "Toza"

        await safe_delete_message(status_msg)

        embed = discord.Embed(title=f"👤 Admin Ma'lumotlari: {row[0]}", color=discord.Color.blue())
        embed.add_field(name="🏅 Daraja", value=f"{row[2]}-lvl", inline=True)
        embed.add_field(name="💼 Lavozimi", value=pos_val, inline=True)
        embed.add_field(name="🔑 Login", value=login_val or "Ko'rsatilmagan", inline=True)
        embed.add_field(name="⏳ Muddat", value=f"{days_diff} kun", inline=True)
        embed.add_field(name="📅 Kelgan sana", value=f"{join_date} ({start_lvl}-lvl)", inline=False)
        
        embed.add_field(name="📜 Darajalar tarixi", value="\n".join(lvl_history) or "Tarix yo'q", inline=False)
        embed.add_field(name="📜 Jazolar tarixi", value=penalties_text, inline=False)
        embed.add_field(name="🔄 Niklar tarixi", value="\n".join(nick_history) or "Tarix yo'q", inline=False)
        
        stats_text = (
            f"✅ Jami report: **{total_rep}** ta\n"
            f"🕒 Jami online: **{minutes_to_hhmm(total_min)}**"
        )
        embed.add_field(name="📊 Jami natijalar", value=stats_text, inline=False)
        embed.set_footer(text=f"Jadval: {table['name']}")

        await ctx.reply(embed=embed)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

# =========================================================
# WEEKLY DISTRIBUTED IMPORT
# =========================================================
@bot.command(name="import_weekly")
async def import_weekly_cmd(ctx, date_range: str = None):
    """Haftalik jami ko'rsatkichlarni kunlarga bo'lib arxivga yozadi"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)

    if not date_range or "-" not in date_range:
        return await ctx.reply("❌ Misol: `!import_weekly 02.03.2026-08.03.2026`")

    try:
        start_str, end_str = date_range.split("-", 1)
        start_dt = datetime.strptime(parse_date_any(start_str), "%d.%m.%Y")
        end_dt = datetime.strptime(parse_date_any(end_str), "%d.%m.%Y")
        
        days_list = []
        curr = start_dt
        while curr <= end_dt:
            days_list.append(curr.strftime("%d.%m.%Y"))
            curr += timedelta(days=1)
        
        num_days = len(days_list)
        if num_days == 0: raise ValueError("Sana oralig'i xato.")

        # 1. Online ma'lumotlarni so'rash
        online_text = await prompt_user_message(ctx, f"📅 **{num_days}** kun uchun **Online** soatlarni yuboring (Nick HH:MM):")
        online_pairs = parse_generic_pairs(online_text)
        
        # 2. Report ma'lumotlarni so'rash
        report_text = await prompt_user_message(ctx, f"📊 **{num_days}** kun uchun **Report** sonlarini yuboring (Nick Son):")
        report_pairs = parse_generic_pairs(report_text)

        sh, table = await choose_table_for_action(ctx)
        now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")
        all_rows = []

        # Online taqsimlash
        for nick, hhmm in online_pairs:
            total_min = hhmm_to_minutes(hhmm)
            base = total_min // num_days
            extra = total_min % num_days
            for i, d_str in enumerate(days_list):
                d_min = base + (1 if i < extra else 0)
                if d_min > 0:
                    all_rows.append((d_str, nick, "norma", minutes_to_hhmm(d_min), table["name"], "Import", now_ts))

        # Report taqsimlash
        for nick, rep_val in report_pairs:
            try:
                total_rep = int(rep_val)
                base = total_rep // num_days
                extra = total_rep % num_days
                for i, d_str in enumerate(days_list):
                    d_rep = base + (1 if i < extra else 0)
                    if d_rep > 0:
                        all_rows.append((d_str, nick, "report", str(d_rep), table["name"], "Import", now_ts))
            except: continue

        if not all_rows:
            return await ctx.reply("❌ Hech qanday ma'lumot tahlil qilinmadi.")

        inserted = await run_blocking(archive_insert_many, all_rows)
        
        await ctx.reply(
            f"✅ **Muvaffaqiyatli import qilindi!**\n"
            f"📅 Muddat: {date_range} ({num_days} kun)\n"
            f"👤 Adminlar: {len(set(r[1] for r in all_rows))}\n"
            f"🗂 Arxivga qo'shildi: **{inserted}** ta yozuv"
        )

    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

async def prompt_user_message(ctx, prompt_text: str, timeout: int = 300) -> str:
    """Foydalanuvchidan uzun matnli xabar kutadi"""
    ask_msg = await ctx.reply(prompt_text)
    
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        msg = await bot.wait_for("message", check=check, timeout=timeout)
        return msg.content
    except asyncio.TimeoutError:
        raise TimeoutError("Kutish vaqti tugadi (5 daqiqa).")
    finally:
        await safe_delete_message(ask_msg)

# =========================================================
# ARCHIVE STATS & CLEANUP
# =========================================================
@bot.command(name="arxiv_stats")
async def arxiv_stats_cmd(ctx):
    """📊 Arxiv statistikasi"""
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return
    
    try:
        stats = await run_blocking(get_archive_stats)
        
        lines = [
            "📊 **Arxiv Statistikasi / Статистика Архива**",
            "",
            f"📁 Fayllar soni / Файлы: **{stats['total_files']}**",
            f"📝 Jami yozuvlar / Записи: **{stats['total_records']}**",
            f"💾 Jami hajmi / Размер: **{stats['total_size']}**",
            f"📈 Maksimum / Максимум: **{stats['max_size_mb']:.0f} MB**"
        ]
        
        await ctx.reply("\n".join(lines))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")
    
    await ask_delete_command(ctx)

@bot.command(name="arxiv_cleanup")
async def arxiv_cleanup_cmd(ctx, days: str = "90"):
    """🧹 Eski yozuvlarni o'chirish"""
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return
    
    try:
        days_int = int(days) if days.isdigit() else 90
        deleted = await run_blocking(cleanup_old_records, days_int)
        
        await ctx.reply(
            f"🧹 **Arxiv Tozalandi**\n"
            f"📅 {days_int} kundan ko'eski yozuvlar o'chirildi\n"
            f"🗑 O'chirilgan yozuvlar: **{deleted}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")
    
    await ask_delete_command(ctx)

# =========================================================
# VIG COMMAND
# =========================================================
@bot.command(name="vig")
async def vig_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        sh, table = await choose_table_for_action(ctx)

        admin_ws = await run_blocking(find_adminlist_worksheet, sh)
        online_ws = await run_blocking(find_online_worksheet, sh)

        view = VigChoiceView(ctx.author.id)
        ask_msg = await ctx.reply(
            "⚠ Qaysi bo'limni ko'rmoqchisiz? / Какой раздел показать?",
            view=view
        )
        await view.wait()
        await safe_delete_message(ask_msg)

        if not view.result:
            await ctx.reply("⌛ Tanlash vaqti tugadi. / Время выбора истекло.")
            await ask_delete_command(ctx)
            return

        admin_col = await run_blocking(find_column_by_aliases, admin_ws, ["admins", "admin", "discord", "nick", "nickname"])
        if not admin_col:
            admin_col = ADMINS_COLUMN

        vig_col = await run_blocking(find_column_by_aliases, admin_ws, ["выг", "vig", "tanbeh"])
        vig_date_col = await run_blocking(find_column_by_aliases, admin_ws, ["дата выговора", "data vig", "vig date", "выг дата"])

        pred_col = await run_blocking(find_column_by_aliases, admin_ws, ["пред", "pred", "ogohlantirish", "ogoxlantirish"])
        pred_date_col = await run_blocking(find_column_by_aliases, admin_ws, ["дата преда", "pred date", "data pred"])

        admins = await run_blocking(admin_ws.col_values, admin_col)
        vig_vals = await run_blocking(admin_ws.col_values, vig_col) if vig_col else []
        vig_dates = await run_blocking(admin_ws.col_values, vig_date_col) if vig_date_col else []
        pred_vals = await run_blocking(admin_ws.col_values, pred_col) if pred_col else []
        pred_dates = await run_blocking(admin_ws.col_values, pred_date_col) if pred_date_col else []

        lines = []

        if view.result == "vig":
            lines.append(f"📋 Tanbehlar / Выговоры — **{table['name']}**")
            req_minutes_one = 20 * 60
            req_report_one = 500

            for row in range(DATA_START_ROW, len(admins) + 1):
                nick = norm(admins[row - 1]) if row - 1 < len(admins) else ""
                if not nick:
                    continue

                raw_vig = norm(vig_vals[row - 1]) if row - 1 < len(vig_vals) else ""
                vig_count = parse_penalty_count(raw_vig)
                if vig_count <= 0:
                    continue

                vig_date = norm(vig_dates[row - 1]) if row - 1 < len(vig_dates) else "-"

                online_raw, report_num, online_minutes = await run_blocking(get_latest_online_report_for_nick, online_ws, nick, table["name"])

                total_req_minutes = req_minutes_one * vig_count # Use get_latest_online_report_for_nick
                total_req_report = req_report_one * vig_count

                remain_minutes, remain_report = calc_needed_blocks(
                    online_minutes,
                    report_num,
                    total_req_minutes,
                    total_req_report
                )

                status = "✅ Yechish mumkin" if remain_minutes == 0 else "⏳ Hali qolgan"

                lines.append(
                    f"{nick} — tanbeh: {raw_vig or vig_count} | sana: {vig_date} | "
                    f"norma: {online_raw} | report: {report_num} | "
                    f"qoldi: {minutes_to_hhmm(remain_minutes)} + {remain_report} report | {status}"
                )

        elif view.result == "pred":
            lines.append(f"📋 Ogohlantirishlar / Предупреждения — **{table['name']}**")
            req_minutes_one = 10 * 60
            req_report_one = 250

            for row in range(DATA_START_ROW, len(admins) + 1):
                nick = norm(admins[row - 1]) if row - 1 < len(admins) else ""
                if not nick:
                    continue

                raw_pred = norm(pred_vals[row - 1]) if row - 1 < len(pred_vals) else ""
                pred_count = parse_penalty_count(raw_pred)
                if pred_count <= 0:
                    continue

                pred_date = norm(pred_dates[row - 1]) if row - 1 < len(pred_dates) else "-"

                online_raw, report_num, online_minutes = await run_blocking(get_latest_online_report_for_nick, online_ws, nick, table["name"])

                total_req_minutes = req_minutes_one * pred_count # Use get_latest_online_report_for_nick
                total_req_report = req_report_one * pred_count

                remain_minutes, remain_report = calc_needed_blocks(
                    online_minutes,
                    report_num,
                    total_req_minutes,
                    total_req_report
                )

                status = "✅ Yechish mumkin" if remain_minutes == 0 else "⏳ Hali qolgan"

                lines.append(
                    f"{nick} — ogohlantirish: {raw_pred or pred_count} | sana: {pred_date} | "
                    f"norma: {online_raw} | report: {report_num} | "
                    f"qoldi: {minutes_to_hhmm(remain_minutes)} + {remain_report} report | {status}"
                )

        if len(lines) == 1:
            lines.append("Hech kim topilmadi. / Никто не найден.")

        for part in chunk_text("\n".join(lines)):
            await ctx.reply(part)

    except Exception as e:
        await ctx.reply(f"❌ Xatolik / Ошибка: {e}")

    await ask_delete_command(ctx)

# =========================================================
# ADMIN MANAGEMENT
# =========================================================
class LevelActionView(discord.ui.View):
    def __init__(self, author_id: int, allow_remove: bool = True, timeout: int = 60):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.selected_level: Optional[int] = None
        for level in range(1, 6):
            self.add_item(LevelActionButton(level))
        if allow_remove:
            self.add_item(LevelRemoveButton())
        self.add_item(LevelCancelButton())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Bu tugmalar siz uchun emas.", ephemeral=True)
            return False
        return True

class LevelActionButton(discord.ui.Button):
    def __init__(self, level: int):
        super().__init__(label=str(level), style=discord.ButtonStyle.primary, row=0)
        self.level = level

    async def callback(self, interaction: discord.Interaction):
        view: LevelActionView = self.view
        view.selected_level = self.level
        view.stop()
        await interaction.response.defer()

class LevelRemoveButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="0", style=discord.ButtonStyle.danger, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: LevelActionView = self.view
        view.selected_level = 0
        view.stop()
        await interaction.response.defer()

class LevelCancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Bekor qilish", style=discord.ButtonStyle.secondary, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: LevelActionView = self.view
        view.selected_level = None
        view.stop()
        await interaction.response.defer()

def parse_multi_nicks(tokens: List[str]) -> List[str]:
    nicks: List[str] = []
    for token in tokens:
        for part in token.split(','):
            nick = norm(part)
            if nick and nick not in nicks:
                nicks.append(nick)
    return nicks

def parse_add_specs(tokens: List[str]) -> List[Tuple[str, str]]:
    if not tokens:
        return []

    specs: List[Tuple[str, str]] = []

    if len(tokens) == 2 and ':' not in tokens[0] and ':' not in tokens[1]:
        return [(norm(tokens[0]), norm(tokens[1]))]

    for token in tokens:
        if ':' not in token:
            raise ValueError("Bir nechta admin qo'shishda format shunday bo'lsin: `Nick:login Nick2:login2`")
        nick, login = token.split(':', 1)
        nick = norm(nick)
        login = norm(login)
        if not nick or not login:
            raise ValueError("`Nick:login` formatini to'g'ri yozing.")
        specs.append((nick, login))

    unique_specs: List[Tuple[str, str]] = []
    seen = set()
    for nick, login in specs:
        key = (nick.lower(), login.lower())
        if key not in seen:
            seen.add(key)
            unique_specs.append((nick, login))
    return unique_specs

async def ask_level_via_buttons(ctx, title: str, allow_remove: bool = True) -> int:
    view = LevelActionView(ctx.author.id, allow_remove=allow_remove)
    msg = await ctx.reply(title, view=view)
    await view.wait()
    try:
        await safe_delete_message(msg)
    except Exception:
        pass

    if view.selected_level is None:
        raise ValueError("Amal bekor qilindi.")
    return view.selected_level

async def perform_add_specs(ctx, specs: List[Tuple[str, str]], level: int):
    sh, table = await choose_table_for_action(ctx)
    sheets = await run_blocking(find_manageable_sheets, sh)
    appoint_date = today_str()

    for nick, login in specs:
        for ws in sheets: # Use admin_data["row_idx_real"]
            await run_blocking(add_admin_to_sheet, ws, nick, level, login, appoint_date)

    lines = [
        "✅ Admin qo'shildi" if len(specs) == 1 else "✅ Adminlar qo'shildi",
        f"📚 Jadval: **{table['name']}**",
        f"🎚 Level: **{level}**",
        f"📅 Sana: **{appoint_date}**",
        "",
    ]
    for nick, login in specs[:20]:
        lines.append(f"• **{nick}** — `{login}`")
    if len(specs) > 20:
        lines.append(f"… va yana **{len(specs) - 20}** ta")
    await ctx.reply("\n".join(lines))

@bot.command(name="add")
async def add_admin_cmd(ctx, *args):
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        await ctx.reply("Yangi admin qo'shish uchun modal oynani oching:", view=AddAdminButtonView(sh, table))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

class AddAdminButtonView(discord.ui.View):
    def __init__(self, sh, table):
        super().__init__(timeout=60)
        self.sh = sh
        self.table = table

    @discord.ui.button(label="Admin Qo'shish", style=discord.ButtonStyle.success)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddAdminModal(self.sh, self.table))

class OtpuskModal(discord.ui.Modal, title="Otpuskni Rasmiylashtirish"):
    nicks = discord.ui.TextInput(label="Admin niklari", placeholder="Nick_One\nNick_Two", style=discord.TextStyle.paragraph, required=True)
    dates = discord.ui.TextInput(label="Sana(lar)", placeholder="20.04.2026\n21.04.2026", style=discord.TextStyle.paragraph, required=True, default=today_str())

    def __init__(self, sh, table):
        super().__init__()
        self.sh = sh
        self.table = table

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        nick_list = [norm(n) for n in self.nicks.value.splitlines() if norm(n)]
        date_list = [parse_date_any(d) for d in self.dates.value.splitlines() if parse_date_any(d)]
        
        if not nick_list or not date_list:
            return await interaction.followup.send("❌ Nicklar yoki sanalar noto'g'ri kiritildi.")

        try:
            ws = await run_blocking(find_online_worksheet, self.sh)
            values = await run_blocking(ws.get_all_values)
            nick_to_row = await run_blocking(build_admin_to_row, ws)
            
            updates = []
            formatted_rows_cols = []
            now_ts = datetime.now(UZ_TZ).strftime("%d.%m.%Y %H:%M:%S")

            ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
            if not ws_admin_col:
                ws_admin_col = ADMINS_COLUMN

            for d_str in date_list:
                o_col, r_col = find_date_columns_in_values(values, d_str)
                if not o_col or not r_col: continue
                
                for nick in nick_list:
                    row = resolve_exact_or_close_row(nick_to_row, nick)
                    if not row: continue

                    real_nick = _cell(values, row, ADMINS_COLUMN)
                    # Online ga '-'
                    updates.append({"range": gspread.utils.rowcol_to_a1(row, o_col), "values": [["-"]]})
                    # Report ga '-'
                    updates.append({"range": gspread.utils.rowcol_to_a1(row, r_col), "values": [["-"]]})
                    
                    formatted_rows_cols.append((row, o_col))
                    formatted_rows_cols.append((row, r_col))
                    
                    # Arxivlash
                    await run_blocking(archive_insert_many, [(d_str, real_nick, "otpusk", "Ta'til (-)", self.table["name"], ws.title, now_ts)])

            if updates:
                await run_blocking(lambda: ws.batch_update(updates, value_input_option="USER_ENTERED"))
                # Formatlash (Oq rang va markazda)
                for r, c in formatted_rows_cols:
                    await run_blocking(apply_custom_formatting, ws, [r], c)
                
                await interaction.followup.send(f"✅ **{len(nick_list)}** ta admin uchun **{len(date_list)}** kunlik otpusk rasmiylashtirildi.")
            else:
                await interaction.followup.send("❌ Hech qanday ma'lumot yangilanmadi (sana yoki nick topilmadi).")
        except Exception as e:
            await interaction.followup.send(f"❌ Xatolik: {e}")

@bot.command(name="otpusk")
async def otpusk_cmd(ctx):
    """Adminlarga ta'til (otpusk) belgilash"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        await ctx.reply(f"🏖 **{table['name']}** jadvali uchun otpusk oynasini oching:", view=OtpuskButtonView(sh, table))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

class OtpuskButtonView(discord.ui.View):
    def __init__(self, sh, table):
        super().__init__(timeout=60)
        self.sh = sh
        self.table = table

    @discord.ui.button(label="Otpusk Oynasini Ochish", style=discord.ButtonStyle.primary, emoji="🏖")
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(OtpuskModal(self.sh, self.table))

@bot.command(name="otpusk_stats")
async def otpusk_stats_cmd(ctx, start_date: str = None, end_date: str = None):
    """Haftalik otpusk olgan adminlar sonini sanash"""
    err = require_access(ctx)
    if err: return await ctx.reply(err)
    try:
        sh, table = await choose_table_for_action(ctx)
        summary, start, end = await collect_full_weekly_data(sh, table["name"], days=7)
        
        leave_counts = {} # {nick: count}
        # Bu yerda collect_full_weekly_data dan tashqari arxivdan ham aniqroq sanash mumkin
        # Lekin user so'roviga ko'ra jadvaldan sanaymiz.
        
        ws_admin_col = await run_blocking(find_header_column_by_aliases, ws, ["nick", "nickname", "admins", "admin", "ник", "discord"], scan_rows=5)
        if not ws_admin_col:
            ws_admin_col = ADMINS_COLUMN
        worksheets = await run_blocking(sh.worksheets)
        for ws in worksheets:
            v = await run_blocking(ws.get_all_values)
            if not v or len(v) < DATA_START_ROW: continue
            pairs = iter_online_report_pairs_from_values(v)
            for o_col, r_col, p_date in pairs:
                if not is_date_in_range(p_date, start, end): continue
                for row in range(DATA_START_ROW, len(v) + 1): # Use ws_admin_col
                    nick = _cell(v, row, ws_admin_col)
                    if not nick: continue
                    if _cell(v, row, o_col) == "-" and _cell(v, row, r_col) == "-":
                        nk = nick_key(nick)
                        leave_counts[nk] = leave_counts.get(nk, 0) + 1
        
        if not leave_counts:
            return await ctx.reply(f"ℹ **{start} - {end}** oralig'ida otpusk olganlar yo'q.")
            
        lines = [f"🏖 **Haftalik Otpusk Statistikasi ({start} - {end})**", ""]
        for nk, count in sorted(leave_counts.items(), key=lambda x: x[1], reverse=True):
            # Nickni chiroyli ko'rsatish uchun summarydan ismini olamiz
            name = summary.get(nk, {}).get("name", nk)
            lines.append(f"• **{name}**: {count} kun")
            
        lines.append(f"\nJami adminlar soni: **{len(leave_counts)}** ta")
        await ctx.reply("\n".join(lines))
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {e}")

@bot.command(name="lvlup")
async def lvlup_cmd(ctx, *args):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    if not args:
        await ctx.reply(
            "❌ Misollar:\n"
            "`!lvlup Ivan_Vasilyev`\n"
            "`!lvlup Ivan_Vasilyev 3`\n"
            "`!lvlup Ivan_Vasilyev Makenzo_Hatred Nicolas_Johns`"
        )
        await ask_delete_command(ctx)
        return

    try:
        direct_level = None
        work_args = list(args)
        if work_args and work_args[-1].isdigit() and 0 <= int(work_args[-1]) <= 5:
            direct_level = int(work_args.pop())

        nicks = parse_multi_nicks(work_args)
        if not nicks:
            raise ValueError("Kamida bitta nick yozing.")

        if direct_level is None:
            level = await ask_level_via_buttons(
                ctx,
                "🎚 Qaysi levelga o'tkazilsin?\n1–5 tugmalaridan birini tanlang.\n0 — jadvaldan chiqarish.",
                allow_remove=True,
            )
        else:
            level = direct_level

        await perform_lvlup_nicks(ctx, nicks, level)
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

    await ask_delete_command(ctx)

@bot.command(name="nw")
async def nw_cmd(ctx):
    err = require_access(ctx)
    if err:
        await ctx.reply(err)
        return

    try:
        sh, table, ws = await choose_table_and_online_ws(ctx)
        dates = await run_blocking(update_online_sheet_dates_to_current, ws)
        await ctx.reply(
            f"✅ Online list sanalari yangilandi\n\n"
            f"📚 Jadval: **{table['name']}**\n"
            f"📄 Sheet: **{ws.title}**\n"
            f"📅 Yangi sanalar: **{' | '.join(dates)}**"
        )
    except Exception as e:
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(e)}")

    await ask_delete_command(ctx)

# =========================================================
# ERROR HANDLER
# =========================================================
@bot.event
async def on_command_error(ctx, error):
    original = getattr(error, 'original', error)
    if isinstance(original, commands.CommandNotFound):
        invoked = ctx.invoked_with
        # Barcha mavjud komandalar va ularning aliaslarini yig'ish
        all_cmds = [c.name for c in bot.commands] + [a for c in bot.commands for a in c.aliases]
        # Eng yaqin o'xshashini qidirish
        matches = difflib.get_close_matches(invoked, all_cmds, n=1, cutoff=0.6)
        
        if matches:
            await ctx.reply(f"❌ `!{invoked}` komandasi topilmadi. Balki siz `!{matches[0]}` demoqchidiriz?\n💡 Barcha komandalarni ko'rish uchun: `!help`")
        else:
            await ctx.reply("❌ Bunday komanda yo'q. Barcha komandalarni ko'rish uchun `!help` yozing.")
        return
    if isinstance(original, commands.MissingRequiredArgument):
        await ctx.reply("❌ Argument yetishmayapti. To'g'ri formatni ko'rish uchun `!help` yozing.")
        return
    if is_gspread_quota_error(original):
        await ctx.reply(f"❌ Xatolik: {friendly_api_error(original)}")
        return
    if isinstance(original, discord.HTTPException) and original.code == 50035:
        await ctx.reply("❌ Xatolik: Xabar uzunligi juda katta (2000 belgidan oshdi).")
        return
    raise error

# =========================================================
# READY EVENT
# =========================================================
@bot.event
async def on_ready():
    init_archive_db()
    if not auto_archive_task.is_running():
        auto_archive_task.start()
    logger.info(f"Bot ishga tushdi: {bot.user}")
    print(f"Bot ishga tushdi: {bot.user}")

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    ensure_json_file(TABLES_FILE)
    ensure_json_file(ROLES_FILE)
    ensure_json_file(PASSWORD_CACHE_FILE, {})

    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN topilmadi. / DISCORD_TOKEN не найден.")

    logger.info("Bot boshlanmoqda... / Бот запускается...")
    bot.run(DISCORD_TOKEN)
