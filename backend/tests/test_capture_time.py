"""When a typed capture happened (understand-capture-time). All texts and times are synthetic; no model is used."""
from datetime import datetime, timedelta, timezone

import pytest

from app.services.capture_normalization import CaptureTimeInterpretation, validate_capture_time
from app.services.capture_parsing import parse_capture
from app.services.capture_time import CaptureDateError, moment_label, parse_time, parse_time_answer, time_word_spans

JAKARTA = timezone(timedelta(hours=7))
TZ = "Asia/Jakarta"


def sent(day, hour, minute=0):
    """A device time in Jakarta; day 2 is Monday 2 Nov 2026, day 1 Sunday, 31 Oct Saturday (as day 31, month 10)."""
    month = 10 if day > 2 else 11
    return datetime(2026, month, day, hour, minute, tzinfo=JAKARTA)


def local(month, day, hour, minute=0):
    return datetime(2026, month, day, hour, minute, tzinfo=JAKARTA)


# (text, device time, expected moment) for phrases the parser resolves on its own.
RESOLVED = [
    # The owner's example: "jam 2" sent at 15:00 is 14:00 today, not 02:00.
    ("bayar parkir di ptc tadi jam 2", sent(2, 15), local(11, 2, 14)),
    ("parkir jam 2", sent(2, 15), local(11, 2, 14)),
    # Sent at 01:00, 02:00 today is still ahead, so the most recent past reading is 14:00 yesterday.
    ("parkir jam 2", sent(2, 1), local(11, 1, 14)),
    # Sent at 03:00, 02:00 an hour ago is plausible: the owner is clearly awake.
    ("parkir jam 2", sent(2, 3), local(11, 2, 2)),
    ("parkir jam 8", sent(2, 23), local(11, 2, 20)),
    ("parkir jam 8", sent(2, 9), local(11, 2, 8)),
    ("parkir jam 8", sent(2, 7), local(11, 1, 20)),
    # A few minutes ahead is the owner rounding; it is never stored in the future.
    ("parkir jam 2", sent(2, 13, 55), local(11, 2, 13, 55)),
    ("jam 14 kopi", sent(2, 15), local(11, 2, 14)),
    ("jam 14 kopi", sent(2, 13), local(11, 1, 14)),
    ("jam 14.30 kopi", sent(2, 15), local(11, 2, 14, 30)),
    ("jam 08:30 bubur", sent(2, 20), local(11, 2, 8, 30)),
    ("pukul 9 bubur", sent(2, 10), local(11, 2, 9)),
    ("jam 2 siang bakso", sent(2, 15), local(11, 2, 14)),
    ("jam 3 sore bensin", sent(2, 20), local(11, 2, 15)),
    ("jam 7 malam sate", sent(2, 21), local(11, 2, 19)),
    ("jam 9 pagi bubur", sent(2, 21), local(11, 2, 9)),
    ("jam 2 malam nasi goreng", sent(2, 9), local(11, 2, 2)),
    ("jam dua siang bakso", sent(2, 15), local(11, 2, 14)),
    ("jam setengah tiga sore", sent(2, 16), local(11, 2, 14, 30)),
    ("jam 2 lewat 15", sent(2, 15), local(11, 2, 14, 15)),
    ("jam 1 kurang 15", sent(2, 13), local(11, 2, 12, 45)),
    ("jam setengah 1 siang", sent(2, 13), local(11, 2, 12, 30)),
    ("jam 2an parkir", sent(2, 15), local(11, 2, 14)),
    ("2pm coffee", sent(2, 15), local(11, 2, 14)),
    ("coffee at 9:15am", sent(2, 15), local(11, 2, 9, 15)),
    ("14:30 kopi", sent(2, 15), local(11, 2, 14, 30)),
    ("tengah malam nasi goreng", sent(2, 1), local(11, 2, 0)),
    # Midnight edges: 23:00 just before midnight belongs to yesterday.
    ("jam 11 malam martabak", sent(2, 0, 30), local(11, 1, 23)),
    ("jam 12 malam martabak", sent(2, 1), local(11, 2, 0)),
    ("jam 12 parkir", sent(2, 0, 5), local(11, 2, 0)),
    ("tadi pagi bubur", sent(2, 9, 30), local(11, 2, 8)),
    ("tadi pagi bubur", sent(2, 7), local(11, 2, 7)),
    ("tadi siang bakso", sent(2, 15), local(11, 2, 12)),
    ("tadi sore bensin", sent(2, 20), local(11, 2, 16)),
    ("sore tadi bensin", sent(2, 20), local(11, 2, 16)),
    ("tadi malam nasi goreng", sent(2, 9, 30), local(11, 1, 20)),
    ("tadi malam nasi goreng", sent(2, 22), local(11, 2, 20)),
    ("semalam nasi goreng", sent(2, 9), local(11, 1, 20)),
    ("last night dinner", sent(2, 9), local(11, 1, 20)),
    ("this morning coffee", sent(2, 15), local(11, 2, 8)),
    ("barusan kopi", sent(2, 15, 7), local(11, 2, 15, 7)),
    ("tadi kopi", sent(2, 15, 7), local(11, 2, 15, 7)),
    ("2 jam lalu bensin", sent(2, 15), local(11, 2, 13)),
    ("30 menit yang lalu kopi", sent(2, 15), local(11, 2, 14, 30)),
    ("sejam lalu kopi", sent(2, 15), local(11, 2, 14)),
    ("setengah jam lalu kopi", sent(2, 15), local(11, 2, 14, 30)),
    ("an hour ago coffee", sent(2, 15), local(11, 2, 14)),
    ("kemarin bakso", sent(2, 9, 30), local(11, 1, 9, 30)),
    ("kemaren bakso", sent(2, 9, 30), local(11, 1, 9, 30)),
    ("kemarin lusa bakso", sent(2, 9, 30), local(10, 31, 9, 30)),
    ("yesterday 9am coffee", sent(2, 15), local(11, 1, 9)),
    # A named past day with one plausible reading: 02:00 yesterday is implausible, 14:00 is not.
    ("kemarin jam 2 parkir", sent(2, 9), local(11, 1, 14)),
    ("kemarin sore bensin", sent(2, 9), local(11, 1, 16)),
    ("kemarin malam jam 11", sent(2, 9), local(11, 1, 23)),
    # The night of yesterday runs past midnight into today.
    ("kemarin jam 1 malam", sent(2, 9), local(11, 2, 1)),
    ("2 hari lalu bensin", sent(2, 10), local(10, 31, 10)),
    ("sabtu bensin", sent(2, 15), local(10, 31, 15)),
    ("sabtu kemarin bensin", sent(2, 15), local(10, 31, 15)),
    ("sabtu malam nonton", sent(2, 10), local(10, 31, 20)),
    ("senin bensin", sent(2, 15), local(11, 2, 15)),
    ("senin kemarin bensin", sent(2, 15), local(10, 26, 15)),
    ("senin minggu lalu bensin", sent(2, 15), local(10, 26, 15)),
    ("hari minggu parkir", sent(2, 15), local(11, 1, 15)),
    ("minggu pagi lari", sent(2, 15), local(11, 1, 8)),
    ("last friday 2pm", sent(2, 15), local(10, 30, 14)),
    ("tgl 30 jam 2 sewa", sent(2, 15), local(10, 30, 14)),
    ("3/10 jam 14.30 makan", sent(2, 15), local(10, 3, 14, 30)),
    ("31 okt jam 7 malam", sent(2, 15), local(10, 31, 19)),
]


@pytest.mark.parametrize("text, device_time, expected", RESOLVED)
def test_resolved_phrases(text, device_time, expected):
    reading = parse_time(text, device_time, TZ)
    assert (reading.status, reading.moment) == ("resolved", expected)
    assert reading.moment <= device_time


# (text, device time, options in order) where a person would ask instead of guessing.
AMBIGUOUS = [
    # 10:00: 02:00 today is implausible and 14:00 yesterday is 20 hours ago, so both are offered.
    ("parkir jam 2", sent(2, 10), [local(11, 2, 2), local(11, 1, 14)]),
    ("parkir jam 4", sent(2, 15), [local(11, 2, 4), local(11, 1, 16)]),
    # A named past day gives recency no weight: 08:00 and 20:00 are both plausible.
    ("kemarin jam 8 makan", sent(2, 9), [local(11, 1, 20), local(11, 1, 8)]),
]


@pytest.mark.parametrize("text, device_time, options", AMBIGUOUS)
def test_ambiguous_hours_are_asked(text, device_time, options):
    reading = parse_time(text, device_time, TZ)
    assert (reading.status, reading.reason, list(reading.options)) == ("ambiguous", "ambiguous_hour", options)
    # While the hour is open the moment is the named day at the device's time of day.
    assert reading.moment.time() == device_time.time() and reading.moment <= device_time


def test_last_week_is_asked_as_a_day():
    reading = parse_time("minggu lalu bensin", sent(2, 15), TZ)
    assert (reading.status, reading.reason, reading.day_question) == ("ambiguous", "ambiguous_day", True)
    assert reading.options[0] == local(11, 1, 15) and reading.options[-1] == local(10, 26, 15)
    assert len(reading.options) == 7
    with_clock = parse_time("minggu lalu jam 14 bensin", sent(2, 15), TZ)
    assert with_clock.options[0] == local(11, 1, 14)
    assert parse_time("bulan lalu sewa", sent(2, 15), TZ).reason == "ambiguous_day"


@pytest.mark.parametrize("text, device_time, reason", [
    ("tgl 2 jam 14 sewa", sent(2, 10), "future"),
    ("tadi sore bensin", sent(2, 10), "future"),
    ("jam 2 jam 3 parkir", sent(2, 15), "conflict"),
    ("kemarin 2 jam lalu", sent(2, 15), "conflict"),
    ("jam 9 malam pagi", sent(2, 23), "conflict"),
])
def test_impossible_or_conflicting_times_are_asked(text, device_time, reason):
    reading = parse_time(text, device_time, TZ)
    assert (reading.status, reading.reason) == ("ambiguous", reason)


@pytest.mark.parametrize("text, hint", [
    ("parkir abis maghrib", "abis maghrib"),
    ("bakso pas makan siang", "pas makan siang"),
    ("kemarin abis isya", "abis isya"),
    ("parkir jam 25", "jam 25"),
])
def test_unplaced_phrases_are_left_to_the_fallback(text, hint):
    reading = parse_time(text, sent(2, 20), TZ)
    assert reading.status == "unresolved"
    assert [text[start:end] for start, end in reading.hints] == [hint]


@pytest.mark.parametrize("text", [
    "bayar parkir 10rb", "beli jam tangan 150rb", "beli jam 150rb", "makan siang 30rb", "makan malam 50rb",
    "selamat pagi kopi 10rb", "parkir 2 jam 10rb", "langganan 2 minggu 50rb",
])
def test_words_that_are_not_times_keep_the_device_time(text):
    reading = parse_time(text, sent(2, 15), TZ)
    assert (reading.status, reading.moment, reading.spans) == ("none", sent(2, 15), ())


def test_meal_words_stay_in_the_description_and_time_words_do_not():
    facts = parse_capture("makan siang kemarin 30rb", sent(2, 9), accounts=[], aliases=[], tz=TZ)
    assert (facts.description, facts.date) == ("Makan siang", local(11, 1, 9))
    owner = parse_capture("bayar parkir di ptc tadi jam 2 10rb", sent(2, 15), accounts=[], aliases=[], tz=TZ)
    assert (owner.amount, owner.phrase, owner.date) == (10_000, "parkir ptc", local(11, 2, 14))
    assert owner.time["evidence"] == "tadi jam 2" and owner.time["basis"] == "parsed"
    watch = parse_capture("beli jam 150rb", sent(2, 15), accounts=[], aliases=[], tz=TZ)
    assert (watch.amount, watch.time["basis"]) == (150_000, "device_time")
    dated = parse_capture("3/10/2025 jam 2 makan 20rb", sent(2, 15), accounts=[], aliases=[], tz=TZ)
    assert (dated.amount, dated.date) == (20_000, datetime(2025, 10, 3, 14, tzinfo=JAKARTA))


def test_device_time_in_another_offset_is_read_in_the_owner_timezone():
    # 08:00 UTC is 15:00 in Jakarta, so "jam 2" is 14:00 Jakarta (07:00 UTC).
    reading = parse_time("parkir jam 2", datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc), TZ)
    assert reading.moment == local(11, 2, 14)
    assert reading.moment.utcoffset() == timedelta(hours=7)


def test_daylight_saving_zone_keeps_wall_clock_readings():
    # The parser stays correct for a zone with daylight saving: 1 Nov 2026 is the US fall-back day.
    device_time = datetime(2026, 11, 1, 15, 0, tzinfo=timezone(timedelta(hours=-5)))
    reading = parse_time("kemarin jam 2", device_time, "America/New_York")
    assert (reading.moment.date().isoformat(), reading.moment.hour) == ("2026-10-31", 14)
    assert reading.moment.utcoffset() == timedelta(hours=-4)


@pytest.mark.parametrize("text", ["3/12 makan 20rb", "31/2 makan 20rb", "tgl 32 makan 20rb", "kemarin tgl 3 makan 20rb",
                                  "kemarin sabtu makan 20rb"])
def test_unusable_explicit_dates_are_still_rejected(text):
    with pytest.raises(CaptureDateError):
        parse_time(text, sent(2, 15), TZ)


def test_answers_are_read_with_the_open_phrase_first():
    device_time = sent(2, 9)
    assert parse_time_answer("malam", "kemarin jam 8", device_time, TZ) == local(11, 1, 20)
    assert parse_time_answer("jam 2 siang", None, device_time, TZ) == local(11, 1, 14)
    assert parse_time_answer("senin", "minggu lalu", device_time, TZ) == local(10, 26, 9)
    assert parse_time_answer("tidak tahu", "kemarin jam 8", device_time, TZ) is None
    assert parse_time_answer("tgl 5/11", None, device_time, TZ) is None
    assert parse_time_answer("besok jam 8", None, device_time, TZ) is None
    assert parse_time_answer("kemarin lusa", None, device_time, TZ) == local(10, 31, 9)


def test_labels_name_the_day_and_clock():
    device_time = sent(2, 15)
    assert moment_label("id", local(11, 2, 14), device_time, TZ) == "Hari ini 14.00"
    assert moment_label("id", local(11, 1, 14), device_time, TZ) == "Kemarin 14.00"
    assert moment_label("id", local(10, 26, 9, 5), device_time, TZ) == "Sen 26 Okt 09.05"
    assert moment_label("id", datetime(2025, 10, 3, 14, tzinfo=JAKARTA), device_time, TZ) == "Jum 3 Okt 2025 14.00"


def test_time_word_spans_cover_every_phrase_kind():
    text = "tadi jam 2 kemarin sabtu 3/10 2 jam lalu abis maghrib barusan"
    found = {text[start:end] for start, end in time_word_spans(text)}
    assert {"tadi", "jam 2", "kemarin", "3/10", "2 jam lalu", "abis maghrib", "barusan"} <= found
    assert time_word_spans("makan siang 30rb") == []


# --- Model fallback checks (D5) -------------------------------------------------------------------


def unresolved(text, device_time=None):
    facts = parse_capture(text, device_time or sent(2, 20), accounts=[], aliases=[], tz=TZ)
    assert facts.time["status"] == "unresolved"
    return facts.time


def model_time(span, moment, confidence=0.9):
    return CaptureTimeInterpretation(span=span, local_datetime=moment, confidence=confidence)


def check(result, time_facts, text="parkir abis maghrib 10rb", device_time=None):
    return validate_capture_time(result, text=text, time_facts=time_facts, device_time=device_time or sent(2, 20),
                                 tz=TZ, threshold=0.8)


def test_model_time_is_accepted_only_for_a_flagged_phrase():
    facts = unresolved("parkir abis maghrib 10rb")
    accepted, outcome = check(model_time("Abis Maghrib", "2026-11-02T18:15:00+07:00"), facts)
    assert outcome == "accepted"
    assert accepted == {"date": "2026-11-02T18:15:00+07:00", "evidence": "abis maghrib", "confidence": 0.9}


@pytest.mark.parametrize("result, outcome", [
    (model_time(None, None, 0), "no_time"),
    (model_time("maghrib", None), "no_time"),
    (model_time("abis isya", "2026-11-02T19:30:00+07:00"), "span_not_in_text"),
    (model_time("parkir", "2026-11-02T18:15:00+07:00"), "span_not_in_text"),
    (model_time("abis maghrib", "kemarin sore"), "invalid_datetime"),
    (model_time("abis maghrib", "2026-11-02T18:15:00"), "invalid_datetime"),
    (model_time("abis maghrib", "2026-11-02T20:30:00+07:00"), "future"),
    (model_time("abis maghrib", "2026-09-20T18:15:00+07:00"), "too_old"),
])
def test_model_time_rejections(result, outcome):
    accepted, code = check(result, unresolved("parkir abis maghrib 10rb"))
    assert (accepted, code) == (None, outcome)


def test_model_time_below_the_threshold_is_only_a_suggestion():
    accepted, outcome = check(model_time("abis maghrib", "2026-11-02T18:15:00+07:00", 0.6),
                              unresolved("parkir abis maghrib 10rb"))
    assert outcome == "low_confidence" and accepted["suggestion_only"] is True


def test_model_time_must_fall_on_the_parsed_day_and_is_ignored_when_not_needed():
    facts = unresolved("kemarin abis maghrib parkir 10rb")
    text = "kemarin abis maghrib parkir 10rb"
    assert check(model_time("abis maghrib", "2026-11-02T18:15:00+07:00"), facts, text)[1] == "day_mismatch"
    assert check(model_time("kemarin abis maghrib", "2026-11-01T18:15:00+07:00"), facts, text)[1] == "accepted"
    # "tgl 3" sent on 2 Nov is 3 Oct: older than 31 days is accepted because the owner stated the date.
    explicit = unresolved("tgl 3 abis maghrib parkir 10rb")
    text = "tgl 3 abis maghrib parkir 10rb"
    assert check(model_time("abis maghrib", "2026-10-03T18:15:00+07:00"), explicit, text)[1] == "accepted"
    assert check(model_time("abis maghrib", "2026-10-04T18:15:00+07:00"), explicit, text)[1] == "day_mismatch"
    september = unresolved("3/9 abis maghrib parkir 10rb")
    assert check(model_time("abis maghrib", "2026-09-03T18:15:00+07:00"), september,
                 "3/9 abis maghrib parkir 10rb")[1] == "accepted"
    resolved = parse_capture("parkir jam 2 10rb", sent(2, 20), accounts=[], aliases=[], tz=TZ).time
    assert check(model_time("jam 2", "2026-11-02T02:00:00+07:00"), resolved, "parkir jam 2 10rb") == (
        None, "not_needed")


@pytest.mark.parametrize("text, amount, description", [
    ("makan siang 30rb", 30_000, "Makan siang"),
    ("beli jam tangan 150rb", 150_000, "Beli jam tangan"),
    ("parkir 2 jam 10rb", 10_000, "Parkir 2 jam"),
])
def test_non_time_words_keep_amount_and_description(text, amount, description):
    facts = parse_capture(text, sent(2, 15), accounts=[], aliases=[], tz=TZ)
    assert (facts.amount, facts.description, facts.date, facts.time["basis"]) == (
        amount, description, sent(2, 15), "device_time")


def test_stated_date_with_time_keeps_the_amount():
    facts = parse_capture("3/10 jam 14.30 makan 20rb", sent(2, 15), accounts=[], aliases=[], tz=TZ)
    assert (facts.amount, facts.date, facts.description) == (20_000, local(10, 3, 14, 30), "Makan")
    assert facts.time["evidence"] == "3/10 jam 14.30"


THIRTEENTH = datetime(2026, 11, 13, 10, 0, tzinfo=JAKARTA)


@pytest.mark.parametrize("text, day", [
    ("tgl 11 kemarin, beli pedigree 47619", "2026-11-11"),
    ("tanggal 11 kmrn beli pakan", "2026-11-11"),
    ("tgl 11 yang kemarin beli pakan", "2026-11-11"),
    ("tgl 11 lalu beli pakan", "2026-11-11"),
    ("tgl 20 kemarin beli pakan", "2026-10-20"),
    ("11 november kemarin beli pakan", "2026-11-11"),
    ("tgl 12 kemarin beli pakan", "2026-11-12"),
])
def test_a_past_word_after_a_date_belongs_to_it(text, day):
    reading = parse_time(text, THIRTEENTH, TZ)
    assert reading.moment.date().isoformat() == day
    assert reading.explicit_date.isoformat() == day


@pytest.mark.parametrize("text", ["kemarin tgl 11 beli pakan", "tgl 11 beli pakan kemarin"])
def test_a_past_word_away_from_the_date_still_names_yesterday(text):
    with pytest.raises(CaptureDateError):
        parse_time(text, THIRTEENTH, TZ)
