"""When a typed capture happened, read deterministically from its words (understand-capture-time D1–D4).

The text is the owner's folded note and `device_time` is when the phone sent it. Every phrase the parser
reads becomes a piece (a day, a part of the day, a clock time or an offset); the pieces are combined into
one local moment, a set of plausible moments to ask about, or a phrase only the model fallback may read.
Nothing here ever resolves to a moment after the device time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from app.i18n import t


# A clock time a few minutes ahead of the device clock is the owner rounding ("jam 2" at 13:55).
FUTURE_TOLERANCE = timedelta(minutes=15)
# Without a named day, a reading is only taken from the last 24 hours, and one more than 12 hours old is
# asked when another reading exists: the owner usually records soon after paying.
UNANCHORED_WINDOW = timedelta(hours=24)
UNANCHORED_RECENCY = timedelta(hours=12)
# 00:00-04:59 is an unlikely time to pay unless the owner is clearly still awake: it was recent.
DEEP_NIGHT_END = 5
DEEP_NIGHT_RECENT = timedelta(hours=6)
OFFSET_LIMITS = {"hours": 72, "minutes": 600}
DAYS_AGO_LIMIT = 366

NUMBER_WORDS = {
    "satu": 1, "dua": 2, "tiga": 3, "empat": 4, "lima": 5, "enam": 6, "tujuh": 7, "delapan": 8,
    "sembilan": 9, "sepuluh": 10, "sebelas": 11, "dua belas": 12,
}
WORD_NUMBER = "dua belas|sepuluh|sebelas|satu|dua|tiga|empat|lima|enam|tujuh|delapan|sembilan"


@dataclass(frozen=True)
class DayPart:
    # Hours (24-hour) a reading in this part may take, and the moment used when no clock is given.
    hours: frozenset[int]
    default: time
    # Night hours after midnight belong to the night of the day before ("kemarin jam 1 malam").
    next_day_hours: frozenset[int] = frozenset()


def hour_range(start: int, end: int) -> frozenset[int]:
    return frozenset(range(start, end + 1))


DAY_PARTS = {
    "pagi": DayPart(hour_range(0, 10), time(8, 0)),
    "morning": DayPart(hour_range(0, 11), time(8, 0)),
    "siang": DayPart(hour_range(10, 15), time(12, 0)),
    "afternoon": DayPart(hour_range(12, 17), time(15, 0)),
    "sore": DayPart(hour_range(14, 19), time(16, 0)),
    "evening": DayPart(hour_range(17, 22), time(19, 0)),
    "malam": DayPart(hour_range(18, 23) | hour_range(0, 3), time(20, 0), hour_range(0, 3)),
    "night": DayPart(hour_range(18, 23) | hour_range(0, 3), time(20, 0), hour_range(0, 3)),
    "dini hari": DayPart(hour_range(0, 5), time(3, 0)),
    "subuh": DayPart(hour_range(3, 6), time(4, 30)),
    "am": DayPart(hour_range(0, 11), time(8, 0)),
    "pm": DayPart(hour_range(12, 23), time(15, 0)),
}
PART_WORDS = "dini hari|subuh|pagi|siang|sore|malam|morning|afternoon|evening|night"
PART_ALIASES = {"a.m.": "am", "p.m.": "pm"}

WEEKDAYS = {
    "senin": 0, "selasa": 1, "rabu": 2, "kamis": 3, "jumat": 4, "jum'at": 4, "sabtu": 5, "minggu": 6,
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}
WEEKDAY_WORDS = "senin|selasa|rabu|kamis|jum'at|jumat|sabtu|minggu|monday|tuesday|wednesday|thursday|friday|" \
                "saturday|sunday"
MONTHS = {
    "jan": 1, "januari": 1, "january": 1, "feb": 2, "februari": 2, "february": 2, "mar": 3, "maret": 3,
    "march": 3, "apr": 4, "april": 4, "mei": 5, "may": 5, "jun": 6, "juni": 6, "june": 6, "jul": 7, "juli": 7,
    "july": 7, "agu": 8, "agt": 8, "agustus": 8, "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10, "oct": 10, "october": 10, "nov": 11, "november": 11, "des": 12, "desember": 12,
    "dec": 12, "december": 12,
}
MONTH_WORDS = "|".join(sorted(MONTHS, key=len, reverse=True))
YESTERDAY_WORDS = "kemarin|kemaren|kmrn|kmarin|yesterday"

SLASH_DATE = re.compile(r"(?<![\w/])(?P<day>\d{1,2})/(?P<month>\d{1,2})(?:/(?P<year>\d{2}|\d{4}))?(?![\w/])")
MONTH_DATE = re.compile(
    rf"\b(?:(?:tgl|tanggal)\.?\s*)?(?P<day>\d{{1,2}})\s+(?P<month>{MONTH_WORDS})(?:\s+(?P<year>\d{{4}}))?\b"
)
DAY_DATE = re.compile(r"\b(?:tgl|tanggal)\.?\s*(?P<day>\d{1,2})\b")
WEEKDAY = re.compile(
    rf"\b(?:(?P<hari>hari)\s+|(?P<last>last)\s+)?(?P<day>{WEEKDAY_WORDS})"
    rf"(?:\s+(?P<suffix>minggu lalu|minggu kemarin|pekan lalu|last week|{YESTERDAY_WORDS}|lalu))?\b"
)
VAGUE_PERIOD = re.compile(
    r"\b(?:(?:\d{1,2}|se|dua|tiga)\s*)?(?:minggu|pekan|bulan|tahun)\s+(?:yang\s+|yg\s+)?"
    r"(?:lalu|kemarin|kemaren|ini)\b|\blast\s+(?:week|month|year)\b|\bthis\s+(?:week|month)\b"
)
DAYS_AGO = re.compile(
    rf"\b(?P<count>\d{{1,3}}|{WORD_NUMBER})\s+(?:hari|days?)\s+(?:yang\s+|yg\s+)?(?:lalu|ago)\b"
)
RELATIVE_DAY = re.compile(rf"\b(?P<word>kemarin lusa|{YESTERDAY_WORDS}|hari ini|today)\b")
OFFSET = re.compile(
    rf"\b(?P<count>\d{{1,3}}|setengah|half an|an|a|se|{WORD_NUMBER})\s*"
    r"(?P<unit>jam|menit|mnt|hours?|hrs?|minutes?|mins?)\s+(?:yang\s+|yg\s+)?(?:lalu|ago)\b"
)
CLOCK_PREFIXED = re.compile(
    r"\b(?:(?:sekitar|kira-kira|kira2|sktr)\s+)?(?:jam|jm|pukul|pkl)\.?\s*"
    r"(?:(?P<half>setengah|stgh)\s*)?"
    rf"(?P<hour>\d{{1,2}}|{WORD_NUMBER})(?:[.:](?P<minute>\d{{2}}))?"
    r"(?:\s+(?P<shift>lewat|kurang)\s+(?P<shift_minutes>\d{1,2}|seperempat)(?:\s*menit)?)?"
    r"(?:\s*-?\s*an)?"
    r"(?:\s*(?P<meridiem>a\.m\.|p\.m\.|am|pm))?"
    rf"(?:\s+(?P<part>{PART_WORDS}))?"
    r"(?:\s+wib)?"
    r"(?![\w]|[.,:]\d)"
)
CLOCK_MERIDIEM = re.compile(
    r"(?<![\w.,:/])(?:at\s+)?(?P<hour>\d{1,2})(?:[.:](?P<minute>\d{2}))?\s*(?P<meridiem>a\.m\.|p\.m\.|am|pm)(?!\w)"
)
CLOCK_COLON = re.compile(
    rf"(?<![\w.,:/])(?:at\s+)?(?P<hour>\d{{1,2}}):(?P<minute>\d{{2}})(?![\w]|[.,:]\d)(?:\s+(?P<part>{PART_WORDS}))?"
)
CLOCK_NAMED = re.compile(r"\b(?P<word>tengah malam|midnight|tengah hari|noon|midday)\b")
NOW = re.compile(r"\b(?:barusan|baru saja|baru aja|just now|sekarang|skrg|right now)\b")
RECENT_NIGHT = re.compile(r"\b(?:semalam|tadi malam|td malam|malam tadi|last night)\b")
THIS_PART = re.compile(
    rf"\b(?:(?:tadi|td|tdi)\s+(?P<before>{PART_WORDS})|(?P<after>{PART_WORDS})\s+(?:tadi|td|ini)"
    r"|this\s+(?P<english>morning|afternoon|evening)|(?P<tonight>tonight))\b"
)
EARLIER_TODAY = re.compile(r"\b(?:tadi|td|tdi)\b")
# An answer naming a later day is never a time something was paid; in a capture text it may be a purpose.
FUTURE_WORDS = re.compile(r"\b(?:besok|bsk|(?<!kemarin )lusa|nanti|tomorrow)\b")
PART_WORD = re.compile(rf"\b(?P<part>{PART_WORDS})\b")
# Phrases that name a time the parser cannot place; only these are offered to the model fallback.
HINTS = (
    re.compile(
        r"\b(?:(?:abis|habis|sehabis|setelah|sesudah|sebelum|pas|waktu|wkt)\s+)?"
        r"(?:sholat\s+|shalat\s+|solat\s+)?(?:maghrib|magrib|isya|isha|dzuhur|zuhur|lohor|ashar|asar)\b"
    ),
    re.compile(
        r"\b(?:abis|habis|sehabis|setelah|sesudah|sebelum|pas|waktu|wkt|after|before|during)\s+"
        r"(?:sarapan|makan siang|makan malam|lunch|dinner|breakfast|pulang|berangkat|kerja|kantor|sholat|shalat|"
        r"solat|gereja|misa|kuliah|sekolah|rapat|meeting|olahraga|gym|work)\b"
    ),
    # A clock word before a number the parser could not read ("jam 25"), never a price ("jam 150rb").
    re.compile(
        rf"\b(?:jam|jm|pukul|pkl)\.?\s*(?:\d{{1,2}}(?![\d.,a-z]|\s*(?:rb|ribu|k|jt|juta)\b)|(?:{WORD_NUMBER})\b)"
    ),
)


class CaptureDateError(ValueError):
    """An explicit date that is impossible, in the future or given more than once."""


@dataclass(frozen=True)
class Piece:
    kind: str  # day, vague_day, part, clock, offset, now, earlier_today
    start: int
    end: int
    day: date | None = None
    explicit: bool = False
    anchored: bool = True
    part: str | None = None
    hour: int | None = None
    minute: int = 0
    exact_hour: bool = False
    offset: timedelta | None = None
    recent_night: bool = False


@dataclass(frozen=True)
class TimeReading:
    """How the capture's moment was read.

    `status` is `none` (no time words: the device time), `resolved`, `ambiguous` (ask between `options`),
    or `unresolved` (only the model fallback may read `hints`). `moment` is the resolved moment, or the
    device time (or the named day at the device's time of day) while the time is still open.
    """

    status: str
    moment: datetime
    spans: tuple[tuple[int, int], ...] = ()
    options: tuple[datetime, ...] = ()
    reason: str | None = None
    hints: tuple[tuple[int, int], ...] = ()
    day: date | None = None
    explicit_date: date | None = None
    day_question: bool = False


@dataclass
class Reading:
    moment: datetime
    plausible: bool


@dataclass
class Found:
    pieces: list[Piece] = field(default_factory=list)
    taken: list[tuple[int, int]] = field(default_factory=list)

    def free(self, start: int, end: int) -> bool:
        return not any(start < used_end and used_start < end for used_start, used_end in self.taken)

    def add(self, piece: Piece) -> None:
        self.pieces.append(piece)
        self.taken.append((piece.start, piece.end))


def number_value(token: str) -> int | None:
    if token.isdigit():
        return int(token)
    return NUMBER_WORDS.get(" ".join(token.split()))


# --- Days -----------------------------------------------------------------------------------------


def latest_day(day: int, today: date) -> date:
    """The most recent date with this day of the month that is not after today."""
    year, month = today.year, today.month
    for _ in range(12):
        try:
            candidate = date(year, month, day)
        except ValueError:
            candidate = None
        if candidate is not None and candidate <= today:
            return candidate
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    raise CaptureDateError("invalid_day")


def explicit_day(day: int, month: int, year: str | None, today: date) -> date:
    year_value = today.year if year is None else int(year) + (2000 if len(year) == 2 else 0)
    try:
        explicit = date(year_value, month, day)
    except ValueError:
        raise CaptureDateError("invalid_date") from None
    if explicit > today:
        raise CaptureDateError("future_date")
    return explicit


def last_weekday(weekday: int, today: date, *, before_today: bool, previous_week: bool) -> date:
    if previous_week:
        monday = today - timedelta(days=today.weekday() + 7)
        return monday + timedelta(days=weekday)
    back = (today.weekday() - weekday) % 7
    if back == 0 and before_today:
        back = 7
    return today - timedelta(days=back)


def find_days(text: str, today: date, found: Found) -> None:
    for match in SLASH_DATE.finditer(text):
        day = explicit_day(int(match["day"]), int(match["month"]), match["year"], today)
        found.add(Piece("day", match.start(), match.end(), day=day, explicit=True))
    for match in MONTH_DATE.finditer(text):
        if found.free(match.start(), match.end()):
            day = explicit_day(int(match["day"]), MONTHS[match["month"]], match["year"], today)
            found.add(Piece("day", match.start(), match.end(), day=day, explicit=True))
    for match in DAY_DATE.finditer(text):
        if found.free(match.start(), match.end()):
            day = int(match["day"])
            if not 1 <= day <= 31:
                raise CaptureDateError("invalid_day")
            found.add(Piece("day", match.start(), match.end(), day=latest_day(day, today), explicit=True))
    for match in WEEKDAY.finditer(text):
        if not found.free(match.start(), match.end()):
            continue
        name, suffix = match["day"], match["suffix"]
        following = text[match.end():match.end() + 12]
        if name == "minggu" and not match["hari"]:
            # "minggu" alone is also "week"; it names Sunday only as "hari minggu" or before a part of day.
            if suffix or not PART_WORD.match(following.lstrip()):
                continue
        previous_week = suffix in {"minggu lalu", "minggu kemarin", "pekan lalu", "last week"}
        before_today = bool(suffix or match["last"])
        day = last_weekday(WEEKDAYS[name], today, before_today=before_today, previous_week=previous_week)
        found.add(Piece("day", match.start(), match.end(), day=day))
    for match in VAGUE_PERIOD.finditer(text):
        if found.free(match.start(), match.end()):
            found.add(Piece("vague_day", match.start(), match.end()))
    for match in DAYS_AGO.finditer(text):
        count = number_value(match["count"])
        if found.free(match.start(), match.end()) and count is not None and count <= DAYS_AGO_LIMIT:
            found.add(Piece("day", match.start(), match.end(), day=today - timedelta(days=count)))
    for match in RELATIVE_DAY.finditer(text):
        if not found.free(match.start(), match.end()):
            continue
        word = match["word"]
        if word in {"hari ini", "today"}:
            found.add(Piece("day", match.start(), match.end(), day=today, anchored=False))
        else:
            back = 2 if word == "kemarin lusa" else 1
            found.add(Piece("day", match.start(), match.end(), day=today - timedelta(days=back)))


# --- Clock times and parts of the day -------------------------------------------------------------


def clock_piece(match: re.Match, part_word: str | None) -> Piece | None:
    hour = number_value(match["hour"])
    minute = int(match["minute"]) if match["minute"] else 0
    if hour is None or hour > 23 or minute > 59:
        return None
    # "14", "0" and a leading zero ("08:30") are 24-hour readings; 1-12 may be either half of the day.
    exact = hour > 12 or hour == 0 or match["hour"].startswith("0")
    groups = match.groupdict()
    if groups.get("half"):
        if not 1 <= hour <= 12:
            return None
        hour, minute = hour - 1, 30
    shift = groups.get("shift")
    if shift:
        amount = 15 if groups["shift_minutes"] == "seperempat" else int(groups["shift_minutes"])
        if amount > 59:
            return None
        total = hour * 60 + minute + (amount if shift == "lewat" else -amount)
        hour, minute = divmod(total % (24 * 60), 60)
    return Piece("clock", match.start(), match.end(), part=part_word, hour=hour, minute=minute, exact_hour=exact)


def find_clocks(text: str, found: Found) -> None:
    for match in OFFSET.finditer(text):
        if not found.free(match.start(), match.end()):
            continue
        token = match["count"]
        count = {"se": 1, "an": 1, "a": 1, "setengah": 0.5, "half an": 0.5}.get(token)
        if count is None:
            count = number_value(token)
        unit = "hours" if match["unit"] in {"jam", "hour", "hours", "hr", "hrs"} else "minutes"
        if count is not None and 0 < count <= OFFSET_LIMITS[unit]:
            found.add(Piece("offset", match.start(), match.end(), offset=timedelta(**{unit: count})))
    for pattern in (CLOCK_PREFIXED, CLOCK_MERIDIEM, CLOCK_COLON):
        for match in pattern.finditer(text):
            if not found.free(match.start(), match.end()):
                continue
            groups = match.groupdict()
            meridiem = PART_ALIASES.get(groups.get("meridiem") or "", groups.get("meridiem"))
            part = groups.get("part")
            if meridiem and part:
                # "2pm sore" names the half twice; both must agree, so keep the narrower part.
                part = part if DAY_PARTS[part].hours <= DAY_PARTS[meridiem].hours else meridiem
            piece = clock_piece(match, part or meridiem)
            if piece:
                found.add(piece)
    for match in CLOCK_NAMED.finditer(text):
        if found.free(match.start(), match.end()):
            hour = 0 if match["word"] in {"tengah malam", "midnight"} else 12
            found.add(Piece("clock", match.start(), match.end(), hour=hour, exact_hour=True))


def find_parts(text: str, found: Found) -> None:
    for match in RECENT_NIGHT.finditer(text):
        if found.free(match.start(), match.end()):
            found.add(Piece("part", match.start(), match.end(), part="malam", recent_night=True))
    for match in THIS_PART.finditer(text):
        if not found.free(match.start(), match.end()):
            continue
        part = match["before"] or match["after"] or match["english"] or ("night" if match["tonight"] else None)
        found.add(Piece("part", match.start(), match.end(), part=part, anchored=False))
    for match in NOW.finditer(text):
        if found.free(match.start(), match.end()):
            found.add(Piece("now", match.start(), match.end()))
    for match in EARLIER_TODAY.finditer(text):
        if found.free(match.start(), match.end()):
            found.add(Piece("earlier_today", match.start(), match.end()))
    # A bare part of the day counts only next to another time word; after "makan" it names a meal.
    for match in PART_WORD.finditer(text):
        if not found.free(match.start(), match.end()):
            continue
        before = text[:match.start()].rstrip()
        after = text[match.end():].lstrip()
        if before.endswith("makan"):
            continue
        offset_before = len(text[:match.start()]) - len(before)
        offset_after = len(text[match.end():]) - len(after)
        touches = any(
            piece.end == match.start() - offset_before or piece.start == match.end() + offset_after
            for piece in found.pieces if piece.kind in {"day", "clock", "earlier_today"}
        )
        if touches:
            found.add(Piece("part", match.start(), match.end(), part=match["part"]))


def find_hints(text: str, found: Found) -> list[tuple[int, int]]:
    hints = []
    for pattern in HINTS:
        for match in pattern.finditer(text):
            if found.free(match.start(), match.end()):
                hints.append((match.start(), match.end()))
    return sorted(hints)


# --- Combination ----------------------------------------------------------------------------------


def at(day: date, moment: time, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, moment, tz)


def clock_hours(piece: Piece, parts: list[str]) -> list[int]:
    """The 24-hour readings of a clock time that fit every part of the day around it."""
    if piece.exact_hour:
        hours = {piece.hour}
    else:
        hours = {piece.hour % 12, piece.hour % 12 + 12}
    for part in parts:
        hours &= DAY_PARTS[part].hours
    return sorted(hours)


def readings_for(piece: Piece, parts: list[str], days: list[date], now: datetime, tz: ZoneInfo) -> list[Reading]:
    named = bool(parts) or piece.exact_hour
    night_parts = [DAY_PARTS[part] for part in parts if DAY_PARTS[part].next_day_hours]
    readings = []
    for day in days:
        for hour in clock_hours(piece, parts):
            shift = 1 if any(hour in part.next_day_hours for part in night_parts) else 0
            moment = at(day + timedelta(days=shift), time(hour, piece.minute), tz)
            deep_night = hour < DEEP_NIGHT_END
            plausible = named or not deep_night or timedelta(0) <= now - moment <= DEEP_NIGHT_RECENT
            readings.append(Reading(moment, plausible))
    return readings


def not_future(readings: list[Reading], now: datetime) -> list[Reading]:
    kept = []
    for reading in readings:
        if reading.moment <= now:
            kept.append(reading)
        elif reading.moment - now <= FUTURE_TOLERANCE:
            kept.append(Reading(now, reading.plausible))
    unique = {}
    for reading in kept:
        current = unique.get(reading.moment)
        unique[reading.moment] = Reading(reading.moment, reading.plausible or bool(current and current.plausible))
    return sorted(unique.values(), key=lambda item: item.moment, reverse=True)


def choose(readings: list[Reading], now: datetime, mode: str) -> tuple[datetime | None, list[datetime], str | None]:
    """(moment, options to ask, reason) from candidate readings under the D2 rule.

    `anchored`: the owner named a past day, so recency says nothing and two plausible readings are asked.
    `today` and `unanchored`: the most recent plausible reading; unanchored readings stay within 24 hours,
    and one older than 12 hours is asked when another reading exists.
    """
    past = not_future(readings, now)
    if mode == "unanchored":
        past = [item for item in past if now - item.moment < UNANCHORED_WINDOW]
    if not past:
        return None, [], "future"
    plausible = [item for item in past if item.plausible]
    if not plausible:
        if len(past) == 1:
            return past[0].moment, [], None
        return None, [item.moment for item in past], "ambiguous_hour"
    if mode == "anchored":
        if len(plausible) == 1:
            return plausible[0].moment, [], None
        return None, [item.moment for item in plausible], "ambiguous_hour"
    best = plausible[0]
    if mode == "unanchored" and len(past) > 1 and now - best.moment > UNANCHORED_RECENCY:
        return None, [item.moment for item in past], "ambiguous_hour"
    return best.moment, [], None


def part_moment(parts: list[str], days: list[date], now: datetime, tz: ZoneInfo) -> tuple[datetime | None, str | None]:
    """The default moment of a part of the day on the most recent day it is not in the future."""
    part = DAY_PARTS[parts[0]]
    candidates = sorted((at(day, part.default, tz) for day in days), reverse=True)
    past = [moment for moment in candidates if moment <= now]
    if past:
        return past[0], None
    # Earlier in the same part of today ("tadi pagi" at 07:00) the owner means just now.
    if now.date() in days and now.hour in part.hours:
        return now, None
    return None, "future"


def week_options(now: datetime, moment_of_day: time, tz: ZoneInfo) -> list[datetime]:
    monday = now.date() - timedelta(days=now.weekday() + 7)
    options = [at(monday + timedelta(days=offset), moment_of_day, tz) for offset in range(7)]
    return sorted((moment for moment in options if moment <= now), reverse=True)


def parse_time(text: str, device_time: datetime, tz: str) -> TimeReading:
    """The capture's moment from the folded text; raises CaptureDateError for an unusable explicit date."""
    zone = ZoneInfo(tz)
    now = device_time.astimezone(zone)
    today = now.date()
    found = Found()
    find_days(text, today, found)
    find_clocks(text, found)
    find_parts(text, found)
    hints = find_hints(text, found)
    pieces = sorted(found.pieces, key=lambda item: item.start)
    spans = tuple((piece.start, piece.end) for piece in pieces)

    days = [piece for piece in pieces if piece.kind == "day"]
    explicit = next((piece.day for piece in days if piece.explicit), None)
    if len({piece.day for piece in days}) > 1:
        raise CaptureDateError("several_dates")
    named_day = days[0].day if days else None
    anchored_day = any(piece.anchored for piece in days)
    clocks = [piece for piece in pieces if piece.kind == "clock"]
    offsets = [piece for piece in pieces if piece.kind == "offset"]
    parts = [piece for piece in pieces if piece.kind == "part"]
    part_names = list(dict.fromkeys(piece.part for piece in parts))
    clock_parts = list(dict.fromkeys(piece.part for piece in clocks if piece.part))
    vague = any(piece.kind == "vague_day" for piece in pieces) and named_day is None
    earlier_today = any(piece.kind in {"earlier_today", "now"} for piece in pieces) or any(
        not piece.anchored and not piece.recent_night for piece in parts)
    open_moment = at(named_day, now.timetz().replace(tzinfo=None), zone) if named_day else now

    def result(status: str, moment: datetime | None = None, **values) -> TimeReading:
        return TimeReading(status, moment or open_moment, spans, day=named_day, explicit_date=explicit, **values)

    if not pieces and not hints:
        return TimeReading("none", now)
    if hints and not clocks and not offsets:
        return result("unresolved", hints=tuple(hints))
    all_parts = part_names + clock_parts
    several_clocks = len(clocks) > 1 or len(offsets) > 1 or bool(offsets and (clocks or days or parts))
    if clocks:
        clashing_parts = not clock_hours(clocks[0], all_parts)
    else:
        clashing_parts = len(set(all_parts)) > 1
    if several_clocks or clashing_parts:
        return result("ambiguous", options=(), reason="conflict")
    if offsets:
        return result("resolved", now - offsets[0].offset)

    if named_day is not None:
        mode, candidate_days = ("anchored" if anchored_day else "today"), [named_day]
    elif any(piece.recent_night for piece in parts):
        mode, candidate_days = "today", [today, today - timedelta(days=1)]
    elif earlier_today:
        mode, candidate_days = "today", [today]
    else:
        mode, candidate_days = "unanchored", [today - timedelta(days=back) for back in range(3)]

    if vague:
        clock_of_day = now.timetz().replace(tzinfo=None)
        if clocks:
            hours = clock_hours(clocks[0], all_parts)
            if len(hours) == 1:
                clock_of_day = time(hours[0], clocks[0].minute)
        elif part_names:
            clock_of_day = DAY_PARTS[part_names[0]].default
        return result("ambiguous", options=tuple(week_options(now, clock_of_day, zone)), reason="ambiguous_day",
                      day_question=True)
    if clocks:
        moment, options, reason = choose(
            readings_for(clocks[0], all_parts, candidate_days, now, zone), now, mode)
        if moment is None:
            return result("ambiguous", options=tuple(options), reason=reason)
        return result("resolved", moment)
    if part_names:
        moment, reason = part_moment(part_names, candidate_days, now, zone)
        if moment is None:
            return result("ambiguous", options=(), reason=reason)
        return result("resolved", moment)
    if named_day is not None:
        return result("resolved", open_moment)
    # "tadi", "barusan" or "sekarang" alone: earlier today, best known as the device time.
    return result("resolved", now)


def parse_time_answer(text: str, evidence: str | None, device_time: datetime, tz: str) -> datetime | None:
    """The moment an owner's typed answer names, read together with the open phrase, then on its own."""
    if FUTURE_WORDS.search(text):
        return None
    attempts = [f"{text} {evidence}", f"{evidence} {text}", text] if evidence else [text]
    for attempt in attempts:
        try:
            reading = parse_time(attempt, device_time, tz)
        except CaptureDateError:
            continue
        if reading.status == "resolved":
            return reading.moment
    return None


# Every phrase pattern that can name a time; a bare part of the day ("makan siang") is not one.
TIME_WORD_PATTERNS = (
    SLASH_DATE, MONTH_DATE, DAY_DATE, WEEKDAY, VAGUE_PERIOD, DAYS_AGO, RELATIVE_DAY, OFFSET, CLOCK_PREFIXED,
    CLOCK_MERIDIEM, CLOCK_COLON, CLOCK_NAMED, NOW, RECENT_NIGHT, THIS_PART, EARLIER_TODAY, *HINTS,
)


def time_word_spans(text: str) -> list[tuple[int, int]]:
    """Spans of words in the folded text that may say when something happened, whatever they resolve to."""
    return sorted(match.span() for pattern in TIME_WORD_PATTERNS for match in pattern.finditer(text))


# --- Labels -----------------------------------------------------------------------------------------

def moment_label(language: str, moment: datetime, device_time: datetime, tz: str) -> str:
    """A label such as "Hari ini 14.00", "Kemarin 14.00" or "Sab 31 Okt 14.00", in the account's language."""
    zone = ZoneInfo(tz)
    local = moment.astimezone(zone)
    today = device_time.astimezone(zone).date()
    clock = f"{local.hour:02d}.{local.minute:02d}"
    if local.date() == today:
        return t(language, "labels.moment.today", clock=clock)
    if local.date() == today - timedelta(days=1):
        return t(language, "labels.moment.yesterday", clock=clock)
    day = t(language, "labels.moment.day", weekday=t(language, f"labels.moment.weekday.{local.weekday()}"),
            day=local.day, month=t(language, f"labels.moment.month.{local.month}"))
    if local.year != today.year:
        day = t(language, "labels.moment.day_year", date=day, year=local.year)
    return t(language, "labels.moment.at", date=day, clock=clock)
