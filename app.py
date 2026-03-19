import os
import re
import json
import random
import time
import threading
from datetime import datetime

import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from google import genai
import firebase_admin
from firebase_admin import credentials, firestore, auth as firebase_auth

# Optional cookie package
COOKIE_MANAGER_AVAILABLE = True
try:
    from st_cookies_manager import EncryptedCookieManager
except Exception:
    COOKIE_MANAGER_AVAILABLE = False
    EncryptedCookieManager = None

# =================================================
# PAGE CONFIG
# =================================================
st.set_page_config(
    page_title="Python Coding Arena 2026",
    page_icon="🐍",
    layout="wide"
)
st.title("Python Coding Arena 🐍")
st.caption("Practice like a game: podiums, XP, streaks, challenges, live competition, and teacher arena events.")

# =================================================
# SAFE SECRETS / ENV
# =================================================
def read_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def read_env(key: str, default=None):
    return os.getenv(key, default)


# =================================================
# KEYS / SETTINGS
# =================================================
API_KEY = (
    read_secret("GEMINI_API_KEY")
    or read_secret("GOOGLE_API_KEY")
    or read_env("GEMINI_API_KEY")
    or read_env("GOOGLE_API_KEY")
    or ""
)

FIREBASE_SERVICE_ACCOUNT_JSON = read_secret("FIREBASE_SERVICE_ACCOUNT_JSON", None)

FIREBASE_WEB_API_KEY = (
    read_secret("FIREBASE_WEB_API_KEY")
    or read_env("FIREBASE_WEB_API_KEY")
    or ""
)

TEACHER_EMAILS_RAW = (
    read_secret("TEACHER_EMAILS")
    or read_env("TEACHER_EMAILS")
    or ""
)

COOKIE_PASSWORD = (
    read_secret("COOKIE_PASSWORD")
    or read_env("COOKIE_PASSWORD")
    or "change-this-cookie-password"
)

MODEL = "gemini-2.5-flash"

BATCH_SIZE = 25
BANK_TARGET = 100
BANK_CALLS = max(1, BANK_TARGET // BATCH_SIZE)
ALL_DOMAINS_TARGET = 25
ALL_DOMAINS_BATCH_SIZE = 25

CHALLENGE_QUESTIONS = 5
XP_CORRECT = 10
XP_WRONG = 0
XP_WIN = 50
XP_LOSS = 0
XP_DRAW = 30

STREAK_BONUS_EVERY = 5
STREAK_BONUS_XP = 20

COOLDOWN_SECONDS = 1
MAX_CHALLENGE_HISTORY_PER_COLUMN = 2
RESULT_POPUP_WINDOW_SECONDS = 45

PERIOD_OPTIONS = ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Period 7", "Period 8", "Other"]

# =================================================
# COOKIES
# =================================================
cookies = None
if COOKIE_MANAGER_AVAILABLE:
    try:
        cookies = EncryptedCookieManager(
            prefix="python_arena_",
            password=COOKIE_PASSWORD,
        )
        if not cookies.ready():
            st.stop()
    except Exception:
        cookies = None

# =================================================
# DOMAINS
# =================================================
DOMAINS = [
    "1. Data Types and Operators",
    "2. Flow Control (if / loops)",
    "3. Input and Output Operations",
    "4. Code Documentation and Structure",
    "5. Troubleshooting and Errors",
    "6. Python Modules (math, random, sys, os)",
    "7. Data and Data Type Operations",
    "8. Math, Datetime, and Random Functions",
    "9. Exception Handling (try, except, else, finally, raise)",
    "10. Console Input and Output + Formatting (f-strings, format)",
    "11. Unit Testing (unittest and assert methods)",
    "12. File Handling and System Operations (io, os, sys.argv)",
    "13. Functions (def, return, parameters, defaults, pass)",
    "14. Loops (while, for, break, continue, nested)",
    "15. Data Conversion and List Operations",
    "16. Data Structures (sets, tuples, dictionaries)",
    "17. Identity Operators (is, is not)",
]

# =================================================
# FALLBACK QUESTIONS
# =================================================
FALLBACK_QUESTIONS = [
    {
        "question": "What is the output of `print(type(3.14).__name__)`?",
        "A": "`float`",
        "B": "`int`",
        "C": "`number`",
        "D": "`decimal`",
        "correct": "A",
        "explanation": "`3.14` is a floating-point number, so its type name is `float`."
    },
    {
        "question": "Python uses indentation to define code blocks.",
        "A": "True",
        "B": "False",
        "C": "Not used",
        "D": "Not used",
        "correct": "A",
        "explanation": "Indentation is required in Python and defines code blocks."
    },
    {
        "question": "Put these steps in the correct order to create a list and print its first item.",
        "A": "Create list → access index 0 → print value",
        "B": "Print value → create list → access index 0",
        "C": "Access index 0 → create list → print value",
        "D": "Create list → print value → access index 0",
        "correct": "A",
        "explanation": "You create the list first, access the first item, then print it."
    },
]

# =================================================
# HELPERS
# =================================================
def now_utc():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def parse_iso_utc_to_ts(iso_str: str) -> float:
    try:
        if not iso_str:
            return 0.0
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def parse_service_account(raw_value):
    if not raw_value:
        return None

    if isinstance(raw_value, dict):
        return raw_value

    if isinstance(raw_value, str):
        cleaned = raw_value.strip()

        if cleaned.startswith("'''") and cleaned.endswith("'''"):
            cleaned = cleaned[3:-3].strip()
        elif cleaned.startswith('"""') and cleaned.endswith('"""'):
            cleaned = cleaned[3:-3].strip()

        return json.loads(cleaned)

    raise ValueError("FIREBASE_SERVICE_ACCOUNT_JSON must be a JSON string or dict.")


def firebase_config_present() -> bool:
    return bool(FIREBASE_SERVICE_ACCOUNT_JSON and str(FIREBASE_SERVICE_ACCOUNT_JSON).strip())


def get_teacher_emails():
    raw = str(TEACHER_EMAILS_RAW or "").strip()
    if not raw:
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def period_key(period_label: str) -> str:
    key = re.sub(r"[^A-Za-z0-9]+", "_", str(period_label).strip())
    return key.strip("_") or "Other"


def event_mode_from_title(title: str) -> str:
    raw = str(title or "").strip().lower()
    if raw.startswith("[class]") or raw.startswith("class:") or raw.startswith("class "):
        return "class"
    return "period"


def clean_event_title(title: str) -> str:
    raw = str(title or "").strip()
    lowered = raw.lower()

    if lowered.startswith("[class]"):
        cleaned = raw[7:].strip()
        return cleaned or "Python Arena Event"

    if lowered.startswith("class:"):
        cleaned = raw[6:].strip()
        return cleaned or "Python Arena Event"

    if lowered.startswith("class "):
        cleaned = raw[6:].strip()
        return cleaned or "Python Arena Event"

    return raw or "Python Arena Event"


def challenge_sort_key(challenge_row: dict):
    return str(challenge_row.get("created_utc", ""))


def my_challenge_score_field(challenge_row: dict, player_id_lower_: str):
    challenger_name = str(challenge_row.get("challenger", "")).strip().lower()
    opponent_name = str(challenge_row.get("opponent", "")).strip().lower()

    if challenger_name == player_id_lower_:
        return "challenger_score"
    if opponent_name == player_id_lower_:
        return "opponent_score"
    return None


def my_challenge_already_completed(challenge_row: dict, player_id_lower_: str) -> bool:
    score_field = my_challenge_score_field(challenge_row, player_id_lower_)
    if not score_field:
        return False
    return challenge_row.get(score_field) is not None


def is_active_challenge(challenge_row: dict) -> bool:
    return challenge_row.get("status") in ("pending", "accepted")


def player_has_active_challenge(player_id_value: str, challenges: list) -> bool:
    pid = str(player_id_value).strip().lower()
    if not pid:
        return False

    for c in challenges:
        challenger = str(c.get("challenger", "")).strip().lower()
        opponent = str(c.get("opponent", "")).strip().lower()
        if is_active_challenge(c) and (challenger == pid or opponent == pid):
            return True
    return False


def challenge_is_locked_for_ui(challenge_id: str) -> bool:
    return (
        st.session_state.get("challenge_mode", False)
        and str(st.session_state.get("challenge_id", "")).strip() == str(challenge_id).strip()
    )


def any_quiz_mode_running() -> bool:
    return bool(
        st.session_state.get("challenge_mode", False)
        or st.session_state.get("event_mode", False)
    )


def check_and_show_finished_challenge_result(challenges: list, player_id_lower_: str):
    now_ts = time.time()

    for c in sorted(challenges, key=challenge_sort_key, reverse=True):
        if c.get("status") != "done":
            continue

        cid = str(c.get("challenge_id", "")).strip()
        if not cid:
            continue

        if cid in st.session_state.shown_result_challenge_ids:
            continue

        challenger = str(c.get("challenger", "")).strip().lower()
        opponent = str(c.get("opponent", "")).strip().lower()

        if player_id_lower_ not in (challenger, opponent):
            continue

        completed_ts = parse_iso_utc_to_ts(str(c.get("completed_utc", "")).strip())
        if completed_ts <= 0:
            continue

        if now_ts - completed_ts > RESULT_POPUP_WINDOW_SECONDS:
            continue

        cs = safe_int(c.get("challenger_score", 0))
        os_ = safe_int(c.get("opponent_score", 0))

        if cs == os_:
            st.session_state.challenge_result_popup_text = "TIE GAME"
            st.session_state.challenge_result_popup_kind = "tie"
        else:
            i_am_challenger = (player_id_lower_ == challenger)
            i_won = (cs > os_) if i_am_challenger else (os_ > cs)

            if i_won:
                st.session_state.challenge_result_popup_text = "YOU WON!"
                st.session_state.challenge_result_popup_kind = "win"
            else:
                st.session_state.challenge_result_popup_text = "YOU LOST"
                st.session_state.challenge_result_popup_kind = "loss"

        st.session_state.challenge_result_popup_nonce += 1
        st.session_state.shown_result_challenge_ids.append(cid)
        st.session_state.shown_result_challenge_ids = st.session_state.shown_result_challenge_ids[-100:]
        break


# =================================================
# FIREBASE / FIRESTORE
# =================================================
@st.cache_resource
def get_firestore_client():
    creds_dict = parse_service_account(FIREBASE_SERVICE_ACCOUNT_JSON)
    if not creds_dict:
        raise ValueError("Missing FIREBASE_SERVICE_ACCOUNT_JSON in secrets.")

    if not firebase_admin._apps:
        cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def check_firestore():
    if not firebase_config_present():
        return False, "Missing FIREBASE_SERVICE_ACCOUNT_JSON in secrets."

    try:
        db_ = get_firestore_client()
        list(db_.collection("players").limit(1).stream())
        return True, ""
    except Exception as e:
        return False, str(e)


def db():
    return get_firestore_client()


def player_ref(player_id: str):
    return db().collection("players").document(player_id)


def session_ref():
    return db().collection("sessions").document()


def challenge_ref(challenge_id: str):
    return db().collection("challenges").document(challenge_id)


def event_ref(event_id: str):
    return db().collection("challenge_events").document(event_id)


def event_participant_ref(event_id: str, player_id: str):
    return event_ref(event_id).collection("participants").document(player_id)


def firestore_enabled():
    return st.session_state.get("firebase_ok", False)


# =================================================
# AUTH
# =================================================
def firebase_sign_in_email_password(email: str, password: str):
    if not FIREBASE_WEB_API_KEY.strip():
        raise ValueError("Missing FIREBASE_WEB_API_KEY in secrets.")

    get_firestore_client()

    url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={FIREBASE_WEB_API_KEY}"
    )
    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }

    resp = requests.post(url, json=payload, timeout=20)
    data = resp.json()

    if resp.status_code != 200:
        err_msg = data.get("error", {}).get("message", "Authentication failed.")
        raise ValueError(err_msg)

    id_token = data.get("idToken", "")
    refresh_token = data.get("refreshToken", "")
    local_id = data.get("localId", "")

    if not id_token:
        raise ValueError("No Firebase ID token returned.")

    return {
        "id_token": id_token,
        "refresh_token": refresh_token,
        "local_id": local_id,
    }


def verify_firebase_id_token(id_token: str):
    get_firestore_client()
    return firebase_auth.verify_id_token(id_token)


def create_firebase_session_cookie(id_token: str, expires_days: int = 5):
    get_firestore_client()
    expires_in_seconds = expires_days * 24 * 60 * 60
    return firebase_auth.create_session_cookie(
        id_token,
        expires_in=expires_in_seconds
    )


def verify_firebase_session_cookie(session_cookie: str):
    get_firestore_client()
    return firebase_auth.verify_session_cookie(session_cookie, check_revoked=True)


def restore_auth_from_cookie():
    if cookies is None:
        return False

    session_cookie = cookies.get("firebase_session", "")
    if not session_cookie:
        return False

    try:
        decoded = verify_firebase_session_cookie(session_cookie)
        email = str(decoded.get("email", "")).strip().lower()
        teacher_emails = get_teacher_emails()

        st.session_state.auth_verified = True
        st.session_state.auth_user = {
            "uid": decoded.get("uid", ""),
            "email": email,
            "email_verified": bool(decoded.get("email_verified", False)),
            "is_teacher": email in teacher_emails,
        }
        st.session_state.is_teacher = email in teacher_emails
        return True
    except Exception:
        cookies["firebase_session"] = ""
        cookies.save()
        return False


def persist_auth_cookie(id_token: str):
    if cookies is None:
        return
    session_cookie = create_firebase_session_cookie(id_token, expires_days=5)
    cookies["firebase_session"] = session_cookie
    cookies.save()


def sign_out():
    st.session_state.auth_verified = False
    st.session_state.auth_user = None
    st.session_state.auth_id_token = ""
    st.session_state.auth_refresh_token = ""
    st.session_state.firebase_ok = False
    st.session_state.firebase_error = ""

    st.session_state.challenge_mode = False
    st.session_state.challenge_id = None
    st.session_state.challenge_count = 0
    st.session_state.challenge_correct = 0

    st.session_state.event_mode = False
    st.session_state.event_id = None
    st.session_state.event_count = 0
    st.session_state.event_correct = 0
    st.session_state.event_question_count = 0
    st.session_state.event_title = ""

    st.session_state.active_domain = None
    st.session_state.active_difficulty = None
    st.session_state.id_locked = False

    st.session_state.first_name = ""
    st.session_state.student_id = ""
    st.session_state.player_id = ""
    st.session_state.last_synced_player_id = ""
    st.session_state.last_synced_period = ""

    st.session_state.leaderboard_cache = []
    st.session_state.challenge_cache = []
    st.session_state.last_db_sync = 0

    st.session_state.challenge_result_popup_text = ""
    st.session_state.challenge_result_popup_kind = ""
    st.session_state.create_student_form_cleared = False

    if cookies is not None:
        cookies["firebase_session"] = ""
        cookies.save()


# =================================================
# FIRESTORE READ LAYER
# =================================================
@st.cache_data(ttl=60)
def load_players():
    docs = db().collection("players").stream()
    rows = []
    for doc in docs:
        data = doc.to_dict() or {}
        if "name" not in data:
            data["name"] = doc.id
        rows.append(data)
    return rows


@st.cache_data(ttl=60)
def load_challenges():
    docs = db().collection("challenges").stream()
    rows = []
    for doc in docs:
        data = doc.to_dict() or {}
        if "challenge_id" not in data:
            data["challenge_id"] = doc.id
        rows.append(data)
    return rows


@st.cache_data(ttl=20)
def load_challenge_events():
    docs = (
        db()
        .collection("challenge_events")
        .order_by("created_utc", direction=firestore.Query.DESCENDING)
        .stream()
    )
    rows = []
    for doc in docs:
        data = doc.to_dict() or {}
        if "event_id" not in data:
            data["event_id"] = doc.id
        if "mode" not in data:
            data["mode"] = "period"
        rows.append(data)
    return rows


@st.cache_data(ttl=60)
def load_sessions():
    docs = (
        db()
        .collection("sessions")
        .order_by("timestamp_utc", direction=firestore.Query.DESCENDING)
        .limit(100)
        .stream()
    )
    return [doc.to_dict() or {} for doc in docs]


@st.cache_data(ttl=60)
def load_student_profiles():
    docs = db().collection("student_profiles").stream()
    rows = []
    for doc in docs:
        data = doc.to_dict() or {}
        data["uid"] = doc.id
        rows.append(data)
    return rows


def clear_db_caches():
    load_players.clear()
    load_challenges.clear()
    load_challenge_events.clear()
    load_sessions.clear()
    load_student_profiles.clear()


def mark_db_data_stale():
    st.session_state.last_db_sync = 0


def get_app_data():
    now_ts = time.time()

    if (
        not st.session_state.leaderboard_cache
        or not st.session_state.challenge_cache
        or now_ts - st.session_state.last_db_sync > 60
    ):
        st.session_state.leaderboard_cache = load_players()
        st.session_state.challenge_cache = load_challenges()
        st.session_state.last_db_sync = now_ts

    return st.session_state.leaderboard_cache, st.session_state.challenge_cache


# =================================================
# STUDENT PROFILE HELPERS
# =================================================
def get_student_profile(uid: str):
    if not uid:
        return None

    snap = db().collection("student_profiles").document(uid).get()
    if not snap.exists:
        return None

    data = snap.to_dict() or {}
    if not data.get("active", True):
        return None

    data["uid"] = snap.id
    return data


def create_student_account_and_profile(
    email: str,
    password: str,
    first_name: str,
    student_id: str,
    period: str,
    active: bool = True,
):
    get_firestore_client()

    email = email.strip().lower()
    first_name = first_name.strip()
    student_id = str(student_id).strip()
    period = period.strip()

    if not email:
        raise ValueError("Student email is required.")
    if not password or len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")
    if not first_name:
        raise ValueError("First name is required.")
    if not student_id.isdigit():
        raise ValueError("Student ID must be numeric.")
    if not period:
        raise ValueError("Period is required.")

    existing_profiles = db().collection("student_profiles").where("student_id", "==", student_id).limit(1).stream()
    if any(True for _ in existing_profiles):
        raise ValueError(f"Student ID {student_id} already exists.")

    existing_email_profiles = db().collection("student_profiles").where("email", "==", email).limit(1).stream()
    if any(True for _ in existing_email_profiles):
        raise ValueError(f"Email {email} already exists in student profiles.")

    user = firebase_auth.create_user(
        email=email,
        password=password,
    )
    uid = user.uid

    db().collection("student_profiles").document(uid).set({
        "uid": uid,
        "email": email,
        "first_name": first_name,
        "student_id": student_id,
        "period": period,
        "display_name": f"{first_name}-{student_id}",
        "active": bool(active),
        "created_utc": now_utc(),
    })

    clear_db_caches()
    mark_db_data_stale()

    return {
        "uid": uid,
        "email": email,
        "display_name": f"{first_name}-{student_id}",
    }


def update_student_profile(uid: str, first_name: str, student_id: str, period: str, active: bool):
    uid = str(uid).strip()
    first_name = first_name.strip()
    student_id = str(student_id).strip()
    period = period.strip()

    if not uid:
        raise ValueError("UID is required.")
    if not first_name:
        raise ValueError("First name is required.")
    if not student_id.isdigit():
        raise ValueError("Student ID must be numeric.")
    if not period:
        raise ValueError("Period is required.")

    snap = db().collection("student_profiles").document(uid).get()
    if not snap.exists:
        raise ValueError("Student profile not found.")

    current = snap.to_dict() or {}

    dup_student_id = db().collection("student_profiles").where("student_id", "==", student_id).limit(10).stream()
    for doc in dup_student_id:
        if doc.id != uid:
            raise ValueError(f"Student ID {student_id} already exists.")

    db().collection("student_profiles").document(uid).set({
        "uid": uid,
        "email": current.get("email", ""),
        "first_name": first_name,
        "student_id": student_id,
        "period": period,
        "display_name": f"{first_name}-{student_id}",
        "active": bool(active),
        "updated_utc": now_utc(),
    }, merge=True)

    clear_db_caches()
    mark_db_data_stale()


def set_student_profile_active(uid: str, active: bool):
    uid = str(uid).strip()
    if not uid:
        raise ValueError("UID is required.")

    db().collection("student_profiles").document(uid).set({
        "active": bool(active),
        "updated_utc": now_utc(),
    }, merge=True)

    clear_db_caches()
    mark_db_data_stale()


def delete_student_profile_and_auth(uid: str):
    uid = str(uid).strip()
    if not uid:
        raise ValueError("UID is required.")

    db().collection("student_profiles").document(uid).delete()
    firebase_auth.delete_user(uid)

    clear_db_caches()
    mark_db_data_stale()


# =================================================
# FIRESTORE WRITE HELPERS
# =================================================
def upsert_player(name: str, period: str):
    name = name.strip()
    if not name:
        return

    ref = player_ref(name)
    snap = ref.get()

    if snap.exists:
        data = snap.to_dict() or {}
        ref.set({
            "name": name,
            "period": period,
            "xp": safe_int(data.get("xp", 0)),
            "wins": safe_int(data.get("wins", 0)),
            "losses": safe_int(data.get("losses", 0)),
            "streak": safe_int(data.get("streak", 0)),
            "best_streak": safe_int(data.get("best_streak", 0)),
            "last_seen_utc": now_utc(),
        }, merge=True)
    else:
        ref.set({
            "name": name,
            "period": period,
            "xp": 0,
            "wins": 0,
            "losses": 0,
            "streak": 0,
            "best_streak": 0,
            "last_seen_utc": now_utc(),
        })

    clear_db_caches()
    mark_db_data_stale()


def add_xp_and_streak(name: str, delta_xp: int, streak_delta: int, win_delta=0, loss_delta=0):
    name = name.strip()
    if not name:
        return

    ref = player_ref(name)
    snap = ref.get()

    if not snap.exists:
        upsert_player(name, "Other")
        snap = ref.get()

    data = snap.to_dict() or {}

    xp = safe_int(data.get("xp", 0)) + int(delta_xp)
    wins = safe_int(data.get("wins", 0)) + int(win_delta)
    losses = safe_int(data.get("losses", 0)) + int(loss_delta)

    streak = safe_int(data.get("streak", 0))
    best = safe_int(data.get("best_streak", 0))

    if streak_delta == -999:
        streak = 0
    else:
        streak = max(0, streak + int(streak_delta))
        best = max(best, streak)

    ref.set({
        "name": name,
        "period": data.get("period", "Other"),
        "xp": xp,
        "wins": wins,
        "losses": losses,
        "streak": streak,
        "best_streak": best,
        "last_seen_utc": now_utc(),
    }, merge=True)

    clear_db_caches()
    mark_db_data_stale()


def log_session(name: str, period: str, score: int, answered: int):
    accuracy = round((score / answered) * 100, 2) if answered else 0.0
    session_ref().set({
        "timestamp_utc": now_utc(),
        "name": name,
        "period": period,
        "score": int(score),
        "answered": int(answered),
        "accuracy": accuracy,
    })
    clear_db_caches()


def create_challenge(challenger: str, opponent: str, domain: str, difficulty: str):
    existing = load_challenges()

    if player_has_active_challenge(challenger, existing):
        raise ValueError(f"{challenger} already has an active challenge.")

    if player_has_active_challenge(opponent, existing):
        raise ValueError(f"{opponent} already has an active challenge.")

    ref = db().collection("challenges").document()
    ref.set({
        "challenge_id": ref.id,
        "created_utc": now_utc(),
        "completed_utc": None,
        "challenger": challenger,
        "opponent": opponent,
        "domain": domain,
        "difficulty": difficulty,
        "status": "pending",
        "challenger_score": None,
        "opponent_score": None,
    })
    clear_db_caches()
    mark_db_data_stale()
    return ref.id


def update_challenge(cid: str, updates: dict):
    challenge_ref(cid).set(updates, merge=True)
    clear_db_caches()
    mark_db_data_stale()


def create_challenge_event(title: str, domain: str, difficulty: str, periods: list, question_count: int):
    title = str(title).strip()
    periods = [p for p in periods if str(p).strip()]

    if not title:
        raise ValueError("Event title is required.")
    if not periods:
        raise ValueError("Choose at least one period.")
    if question_count < 1:
        raise ValueError("Question count must be at least 1.")

    event_mode = event_mode_from_title(title)
    display_title = clean_event_title(title)

    scores = {}
    if event_mode == "period":
        for p in periods:
            pk = period_key(p)
            scores[pk] = {
                "label": p,
                "total": 0,
                "count": 0,
                "average": 0.0,
            }

    ref = db().collection("challenge_events").document()
    ref.set({
        "event_id": ref.id,
        "title": display_title,
        "created_utc": now_utc(),
        "completed_utc": None,
        "domain": domain,
        "difficulty": difficulty,
        "status": "active",
        "periods": periods,
        "question_count": int(question_count),
        "scores": scores,
        "class_scores": {},
        "mode": event_mode,
        "type": "teacher_async_event",
        "winner_periods": [],
        "winner_players": [],
        "winner_average": 0.0,
        "winner_score": 0,
        "result_type": "",
    })

    clear_db_caches()
    mark_db_data_stale()
    return ref.id


def end_challenge_event(event_id: str):
    snap = event_ref(event_id).get()
    if not snap.exists:
        raise ValueError("Event not found.")

    data = snap.to_dict() or {}
    event_mode = str(data.get("mode", "period")).strip().lower()

    if event_mode == "class":
        class_scores = data.get("class_scores", {}) or {}

        best_score = None
        winners = []

        for _, row in class_scores.items():
            player_name = str(row.get("player_id", "")).strip()
            score = safe_int(row.get("score", 0))

            if not player_name:
                continue

            if best_score is None or score > best_score:
                best_score = score
                winners = [player_name]
            elif score == best_score:
                winners.append(player_name)

        result_type = "tie" if len(winners) > 1 else "win"

        event_ref(event_id).set({
            "status": "done",
            "completed_utc": now_utc(),
            "winner_players": winners,
            "winner_score": safe_int(best_score, 0),
            "winner_average": 0.0,
            "winner_periods": [],
            "result_type": result_type,
        }, merge=True)

    else:
        scores = data.get("scores", {}) or {}

        best_avg = None
        winners = []

        for _, score_data in scores.items():
            avg = safe_float(score_data.get("average", 0.0))
            label = str(score_data.get("label", "")).strip()
            if not label:
                continue

            if best_avg is None or avg > best_avg:
                best_avg = avg
                winners = [label]
            elif avg == best_avg:
                winners.append(label)

        result_type = "tie" if len(winners) > 1 else "win"

        event_ref(event_id).set({
            "status": "done",
            "completed_utc": now_utc(),
            "winner_periods": winners,
            "winner_average": round(best_avg or 0.0, 2),
            "winner_players": [],
            "winner_score": 0,
            "result_type": result_type,
        }, merge=True)

    clear_db_caches()
    mark_db_data_stale()


@firestore.transactional
def _complete_event_transaction(transaction, event_id: str, player_id: str, period_label: str, score: int, question_count: int):
    eref = event_ref(event_id)
    pref = event_participant_ref(event_id, player_id)

    event_snap = eref.get(transaction=transaction)
    part_snap = pref.get(transaction=transaction)

    if not event_snap.exists:
        raise ValueError("Event not found.")

    event_data = event_snap.to_dict() or {}

    if str(event_data.get("status", "")).strip().lower() != "active":
        raise ValueError("This event is no longer active.")

    if part_snap.exists:
        return False

    event_mode = str(event_data.get("mode", "period")).strip().lower()

    transaction.set(pref, {
        "player_id": player_id,
        "period": period_label,
        "score": int(score),
        "question_count": int(question_count),
        "completed_utc": now_utc(),
        "result_seen": False,
        "result_seen_utc": None,
    })

    if event_mode == "class":
        class_scores = event_data.get("class_scores", {}) or {}

        class_scores[player_id] = {
            "player_id": player_id,
            "period": period_label,
            "score": int(score),
            "question_count": int(question_count),
            "completed_utc": now_utc(),
        }

        transaction.set(eref, {
            "class_scores": class_scores,
            "updated_utc": now_utc(),
        }, merge=True)

    else:
        pk = period_key(period_label)
        scores = event_data.get("scores", {}) or {}

        if pk not in scores:
            scores[pk] = {
                "label": period_label,
                "total": 0,
                "count": 0,
                "average": 0.0,
            }

        scores[pk]["total"] = safe_int(scores[pk].get("total", 0)) + int(score)
        scores[pk]["count"] = safe_int(scores[pk].get("count", 0)) + 1
        scores[pk]["average"] = round(
            scores[pk]["total"] / max(1, scores[pk]["count"]),
            2
        )

        transaction.set(eref, {
            "scores": scores,
            "updated_utc": now_utc(),
        }, merge=True)

    return True


def complete_event_attempt(event_id: str, player_id: str, period_label: str, score: int, question_count: int):
    transaction = db().transaction()
    completed = _complete_event_transaction(
        transaction,
        event_id,
        player_id,
        period_label,
        score,
        question_count,
    )
    clear_db_caches()
    mark_db_data_stale()
    return completed


def student_completed_event(event_id: str, player_id: str) -> bool:
    if not event_id or not player_id:
        return False
    try:
        snap = event_participant_ref(event_id, player_id).get()
        return snap.exists
    except Exception:
        return False


def mark_event_result_seen(event_id: str, player_id: str):
    if not event_id or not player_id:
        return
    try:
        event_participant_ref(event_id, player_id).set({
            "result_seen": True,
            "result_seen_utc": now_utc(),
        }, merge=True)
        clear_db_caches()
        mark_db_data_stale()
    except Exception:
        pass


def check_and_show_finished_event_result(events: list, player_id: str, student_period: str):
    for ev in sorted(events, key=lambda x: str(x.get("completed_utc", x.get("created_utc", ""))), reverse=True):
        if str(ev.get("status", "")).strip().lower() != "done":
            continue

        eid = str(ev.get("event_id", "")).strip()
        if not eid:
            continue

        try:
            participant_snap = event_participant_ref(eid, player_id).get()
        except Exception:
            continue

        if not participant_snap.exists:
            continue

        participant_data = participant_snap.to_dict() or {}
        if bool(participant_data.get("result_seen", False)):
            continue

        event_mode = str(ev.get("mode", "period")).strip().lower()
        result_type = str(ev.get("result_type", "")).strip().lower()

        if event_mode == "class":
            winner_players = ev.get("winner_players", []) or []

            if result_type == "tie":
                if player_id in winner_players:
                    st.session_state.challenge_result_popup_text = "TIE GAME"
                    st.session_state.challenge_result_popup_kind = "tie"
                else:
                    st.session_state.challenge_result_popup_text = "YOU LOST"
                    st.session_state.challenge_result_popup_kind = "loss"
            else:
                if player_id in winner_players:
                    st.session_state.challenge_result_popup_text = "YOU WON!"
                    st.session_state.challenge_result_popup_kind = "win"
                else:
                    st.session_state.challenge_result_popup_text = "YOU LOST"
                    st.session_state.challenge_result_popup_kind = "loss"
        else:
            winner_periods = ev.get("winner_periods", []) or []

            if result_type == "tie":
                if student_period in winner_periods:
                    st.session_state.challenge_result_popup_text = "TIE GAME"
                    st.session_state.challenge_result_popup_kind = "tie"
                else:
                    continue
            else:
                if student_period in winner_periods:
                    st.session_state.challenge_result_popup_text = "YOU WON!"
                    st.session_state.challenge_result_popup_kind = "win"
                else:
                    st.session_state.challenge_result_popup_text = "YOU LOST"
                    st.session_state.challenge_result_popup_kind = "loss"

        st.session_state.challenge_result_popup_nonce += 1
        mark_event_result_seen(eid, player_id)
        break


# =================================================
# SHARED QUESTION BANK - PER DOMAIN
# =================================================
@st.cache_resource
def get_shared_bank():
    return {"lock": threading.Lock(), "bank": {}, "updated": {}}


QB = get_shared_bank()


def qkey(topic: str, difficulty: str):
    return (topic, difficulty)


def bank_size(topic: str, difficulty: str) -> int:
    with QB["lock"]:
        return len(QB["bank"].get(qkey(topic, difficulty), []))


def bank_last_updated(topic: str, difficulty: str):
    with QB["lock"]:
        return QB["updated"].get(qkey(topic, difficulty))


def add_to_bank(topic: str, difficulty: str, questions: list):
    with QB["lock"]:
        QB["bank"].setdefault(qkey(topic, difficulty), [])
        QB["bank"][qkey(topic, difficulty)].extend(questions)
        QB["updated"][qkey(topic, difficulty)] = now_utc()


def get_bank(topic: str, difficulty: str):
    with QB["lock"]:
        QB["bank"].setdefault(qkey(topic, difficulty), [])
        return QB["bank"][qkey(topic, difficulty)]


# =================================================
# GEMINI
# =================================================
def parse_batch(raw: str):
    questions = []
    chunks = raw.split("###")
    for chunk in chunks:
        try:
            q = re.search(r"QUESTION:\s*(.*?)(?=\nA\))", chunk, re.S).group(1)
            A = re.search(r"\nA\)\s*(.*)", chunk).group(1)
            B = re.search(r"\nB\)\s*(.*)", chunk).group(1)
            C = re.search(r"\nC\)\s*(.*)", chunk).group(1)
            D = re.search(r"\nD\)\s*(.*)", chunk).group(1)
            correct = re.search(r"CORRECT:\s*([ABCD])", chunk).group(1)
            explanation = re.search(r"EXPLANATION:\s*(.*)", chunk, re.S).group(1)
            questions.append({
                "question": q.strip(),
                "A": A.strip(),
                "B": B.strip(),
                "C": C.strip(),
                "D": D.strip(),
                "correct": correct.strip().upper(),
                "explanation": explanation.strip(),
            })
        except Exception:
            pass
    return questions


def fetch_questions_from_gemini(topic: str, difficulty: str, count: int):
    prompt = f"""
You are a Python certification exam writer.
Create exactly {count} questions for a classroom quiz game.

DOMAIN: {topic}
DIFFICULTY: {difficulty}

IMPORTANT:
- Mix the question styles across the batch.
- Use these 3 styles:
  1. Standard multiple choice
  2. True/False
  3. Ordering-as-multiple-choice

QUESTION STYLE RULES:
- At least some questions should be standard MCQ.
- At least some questions should be True/False.
- At least some questions should be ordering questions written as MCQ choices.
- Do NOT use drag-and-drop.
- Do NOT require free response.
- Every question must still fit this exact answer model:
  A, B, C, or D

CONTENT RULES:
- Focus strictly on this Python domain.
- Use Python certification-style questions.
- Include short Python code snippets when helpful.
- Ask about syntax, output, logic, debugging, tracing, and concepts.
- Use realistic distractors.
- Use backticks around code when useful.

TRUE/FALSE RULES:
- For True/False questions, still format the answers as:
  A) True
  B) False
  C) Not used
  D) Not used
- Correct answer must be A or B only.

ORDERING RULES:
- For ordering questions, ask students to choose the correct sequence.
- Example style:
  QUESTION: Put the following steps in the correct order...
  A) 1, 2, 3, 4
  B) 1, 3, 2, 4
  C) 2, 1, 3, 4
  D) 3, 1, 2, 4
- Do NOT ask students to manually rearrange items.
- Ordering questions must still be answerable with A/B/C/D.

FORMAT (MUST MATCH EXACTLY):
- Each question separated by a line containing ONLY: ###
- Each question uses EXACT labels:

QUESTION: ...
A) ...
B) ...
C) ...
D) ...
CORRECT: A/B/C/D
EXPLANATION: ...

No extra text before the first QUESTION:
""".strip()

    if not API_KEY.strip():
        return [], "Gemini API key not set."

    last_err = None

    for _ in range(2):
        try:
            client = genai.Client(api_key=API_KEY)
            resp = client.models.generate_content(
                model=MODEL,
                contents=prompt
            )
            raw_text = getattr(resp, "text", "") or ""
            qs = parse_batch(raw_text)

            if qs:
                return qs, None

            last_err = "AI format error or empty response."
        except Exception as e:
            last_err = str(e)
            time.sleep(1)

    return [], last_err


# =================================================
# POPUPS / FEEDBACK
# =================================================
def show_xp_popup():
    popup_text = st.session_state.get("xp_popup_text", "").strip()
    popup_kind = st.session_state.get("xp_popup_kind", "good")
    popup_nonce = st.session_state.get("xp_popup_nonce", 0)

    if not popup_text:
        return

    bg = "linear-gradient(180deg, #22c55e, #16a34a)" if popup_kind == "good" else "linear-gradient(180deg, #f59e0b, #d97706)"
    border = "#166534" if popup_kind == "good" else "#92400e"

    st.markdown(
        f"""
        <style>
        @keyframes xpFloatFade-{popup_nonce} {{
            0% {{
                opacity: 0;
                transform: translate(-50%, 18px) scale(0.92);
            }}
            12% {{
                opacity: 1;
                transform: translate(-50%, 0px) scale(1.02);
            }}
            75% {{
                opacity: 1;
                transform: translate(-50%, -8px) scale(1.0);
            }}
            100% {{
                opacity: 0;
                transform: translate(-50%, -28px) scale(0.96);
            }}
        }}

        .xp-popup-{popup_nonce} {{
            position: fixed;
            left: 50%;
            top: 92px;
            transform: translateX(-50%);
            z-index: 9999;
            padding: 14px 22px;
            border-radius: 18px;
            color: white;
            font-weight: 800;
            font-size: 24px;
            letter-spacing: 0.3px;
            background: {bg};
            border: 3px solid {border};
            box-shadow: 0 14px 30px rgba(0,0,0,0.22);
            animation: xpFloatFade-{popup_nonce} 2.2s ease-out forwards;
            pointer-events: none;
            text-align: center;
            white-space: pre-line;
        }}
        </style>

        <div class="xp-popup-{popup_nonce}">
            {popup_text}
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_challenge_result_popup():
    popup_text = st.session_state.get("challenge_result_popup_text", "").strip()
    popup_kind = st.session_state.get("challenge_result_popup_kind", "")
    popup_nonce = st.session_state.get("challenge_result_popup_nonce", 0)

    if not popup_text:
        return

    if popup_kind == "win":
        bg = "linear-gradient(180deg, #22c55e, #15803d)"
        border = "#14532d"
        emoji = "🏆"
    elif popup_kind == "loss":
        bg = "linear-gradient(180deg, #ef4444, #b91c1c)"
        border = "#7f1d1d"
        emoji = "💀"
    else:
        bg = "linear-gradient(180deg, #f59e0b, #d97706)"
        border = "#92400e"
        emoji = "🤝"

    st.markdown(
        f"""
        <style>
        @keyframes challengeResultFade-{popup_nonce} {{
            0% {{
                opacity: 0;
                transform: translate(-50%, -50%) scale(0.88);
            }}
            10% {{
                opacity: 1;
                transform: translate(-50%, -50%) scale(1.02);
            }}
            85% {{
                opacity: 1;
                transform: translate(-50%, -50%) scale(1.0);
            }}
            100% {{
                opacity: 0;
                transform: translate(-50%, -50%) scale(0.94);
            }}
        }}

        .challenge-result-popup-{popup_nonce} {{
            position: fixed;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            z-index: 10000;
            min-width: 320px;
            max-width: 90vw;
            padding: 28px 26px;
            border-radius: 24px;
            color: white;
            font-weight: 900;
            text-align: center;
            background: {bg};
            border: 4px solid {border};
            box-shadow: 0 24px 60px rgba(0,0,0,0.35);
            animation: challengeResultFade-{popup_nonce} 3.2s ease-out forwards;
            pointer-events: none;
        }}

        .challenge-result-popup-{popup_nonce} .icon {{
            font-size: 54px;
            line-height: 1;
            margin-bottom: 10px;
        }}

        .challenge-result-popup-{popup_nonce} .text {{
            font-size: 34px;
            line-height: 1.15;
            white-space: pre-line;
        }}
        </style>

        <div class="challenge-result-popup-{popup_nonce}">
            <div class="icon">{emoji}</div>
            <div class="text">{popup_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.session_state.challenge_result_popup_text = ""
    st.session_state.challenge_result_popup_kind = ""


def render_combo_meter(streak_value: int):
    streak_value = max(0, int(streak_value))

    if streak_value >= 10:
        tier_label = "👑 Legendary Combo"
        glow = "#f59e0b"
        fill_pct = 100
    elif streak_value >= 5:
        tier_label = "⚡ Hot Streak"
        glow = "#22c55e"
        fill_pct = min(100, int((streak_value / 10) * 100))
    elif streak_value >= 3:
        tier_label = "🔥 Combo Active"
        glow = "#3b82f6"
        fill_pct = min(100, int((streak_value / 10) * 100))
    elif streak_value >= 1:
        tier_label = "✨ Building Combo"
        glow = "#a855f7"
        fill_pct = min(100, int((streak_value / 10) * 100))
    else:
        tier_label = "Start a combo"
        glow = "#64748b"
        fill_pct = 0

    st.markdown(
        f"""
        <style>
        .combo-wrap {{
            margin-top: 10px;
            margin-bottom: 8px;
            padding: 14px 16px;
            border-radius: 18px;
            background: linear-gradient(180deg, #0f172a, #1e293b);
            border: 2px solid {glow};
            box-shadow: 0 0 0 1px rgba(255,255,255,0.04), 0 10px 24px rgba(0,0,0,0.18);
        }}
        .combo-top {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            margin-bottom:10px;
            color:white;
            font-weight:800;
            font-size:18px;
        }}
        .combo-badge {{
            padding: 6px 12px;
            border-radius: 999px;
            background: {glow};
            color: white;
            font-weight: 900;
            font-size: 15px;
            box-shadow: 0 0 18px {glow};
        }}
        .combo-track {{
            width:100%;
            height:16px;
            background:#334155;
            border-radius:999px;
            overflow:hidden;
        }}
        .combo-fill {{
            width:{fill_pct}%;
            height:100%;
            background: linear-gradient(90deg, {glow}, #ffffff);
            border-radius:999px;
            transition: width 0.4s ease;
        }}
        .combo-caption {{
            margin-top:8px;
            color:#cbd5e1;
            font-size:14px;
            font-weight:600;
        }}
        </style>

        <div class="combo-wrap">
            <div class="combo-top">
                <div>{tier_label}</div>
                <div class="combo-badge">Combo x{streak_value}</div>
            </div>
            <div class="combo-track">
                <div class="combo-fill"></div>
            </div>
            <div class="combo-caption">
                3 = 🔥 Combo • 5 = ⚡ Hot Streak • 10 = 👑 Legendary
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def show_last_feedback():
    popup_text = st.session_state.get("last_feedback_text", "").strip()
    popup_kind = st.session_state.get("last_feedback_kind", "info")

    if not popup_text:
        return

    if popup_kind == "success":
        st.success(popup_text)
    elif popup_kind == "error":
        st.error(popup_text)
    elif popup_kind == "warning":
        st.warning(popup_text)
    else:
        st.info(popup_text)


# =================================================
# SESSION STATE
# =================================================
st.session_state.setdefault("score", 0)
st.session_state.setdefault("total_answered", 0)
st.session_state.setdefault("answered", False)
st.session_state.setdefault("question", None)
st.session_state.setdefault("next_allowed_time", 0.0)
st.session_state.setdefault("submit_locked", False)
st.session_state.setdefault("processing_submission", False)
st.session_state.setdefault("pending_auto_next", False)
st.session_state.setdefault("question_token", "")
st.session_state.setdefault("answered_tokens", [])
st.session_state.setdefault("last_challenge_sent_at", 0.0)
st.session_state.setdefault("seen_by_domain", {})

st.session_state.setdefault("first_name", "")
st.session_state.setdefault("student_id", "")
st.session_state.setdefault("player_id", "")
st.session_state.setdefault("student_period", "Period 1")
st.session_state.setdefault("id_locked", False)

st.session_state.setdefault("challenge_mode", False)
st.session_state.setdefault("challenge_id", None)
st.session_state.setdefault("challenge_count", 0)
st.session_state.setdefault("challenge_correct", 0)

st.session_state.setdefault("event_mode", False)
st.session_state.setdefault("event_id", None)
st.session_state.setdefault("event_title", "")
st.session_state.setdefault("event_count", 0)
st.session_state.setdefault("event_correct", 0)
st.session_state.setdefault("event_question_count", 0)

st.session_state.setdefault("active_domain", None)
st.session_state.setdefault("active_difficulty", None)

st.session_state.setdefault("is_teacher", False)
st.session_state.setdefault("is_generating", False)
st.session_state.setdefault("firebase_ok", False)
st.session_state.setdefault("firebase_error", "")
st.session_state.setdefault("leaderboard_cache", [])
st.session_state.setdefault("challenge_cache", [])
st.session_state.setdefault("last_db_sync", 0)
st.session_state.setdefault("session_logged", False)

st.session_state.setdefault("xp_popup_text", "")
st.session_state.setdefault("xp_popup_kind", "")
st.session_state.setdefault("xp_popup_nonce", 0)

st.session_state.setdefault("challenge_result_popup_text", "")
st.session_state.setdefault("challenge_result_popup_kind", "")
st.session_state.setdefault("challenge_result_popup_nonce", 0)
# =================================================
# FINAL SESSION STATE INIT (SAFE COMPLETION)
# =================================================
st.session_state.setdefault("answer_widget_nonce", 0)
st.session_state.setdefault("current_answer_widget_key", "answer_choice_0")

st.session_state.setdefault("last_synced_player_id", "")
st.session_state.setdefault("last_synced_period", "")

st.session_state.setdefault("auth_verified", False)
st.session_state.setdefault("auth_user", None)
st.session_state.setdefault("auth_id_token", "")
st.session_state.setdefault("auth_refresh_token", "")

st.session_state.setdefault("shown_result_challenge_ids", [])
st.session_state.setdefault("latest_result_checked_at", 0)
st.session_state.setdefault("create_student_form_cleared", False)

# =================================================
# AUTO LOAD FIRST QUESTION FOR CHALLENGE (HTML CLONE FIX)
# =================================================
if st.session_state.get("challenge_mode", False):
    if not st.session_state.get("question"):
        load_next_question_for_current_mode()

# =================================================
# AUTO NEXT QUESTION AFTER SUBMIT (HTML BEHAVIOR CLONE)
# =================================================
if (
    st.session_state.get("answered", False)
    and not st.session_state.get("challenge_mode", False)
):
    if time.time() > st.session_state.get("next_allowed_time", 0):
        load_next_question_for_current_mode()

# =================================================
# DISABLE START BUTTONS DURING ACTIVE CHALLENGE (HTML CLONE)
# =================================================
def challenge_buttons_disabled():
    return st.session_state.get("challenge_mode", False)

# =================================================
# EVENT SYSTEM (PERIOD + CLASS EVENTS)
# =================================================
def is_class_event(title: str):
    return str(title).strip().upper().startswith("[CLASS]")

def create_event(title, domain, difficulty, created_by):
    ref = db().collection("events").document()
    ref.set({
        "event_id": ref.id,
        "title": title,
        "domain": domain,
        "difficulty": difficulty,
        "created_by": created_by,
        "created_utc": now_utc(),
        "status": "waiting",
        "participants": [],
        "scores": {},
    })
    return ref.id

def join_event(event_id, player_id):
    ref = db().collection("events").document(event_id)
    snap = ref.get()
    if not snap.exists:
        return

    data = snap.to_dict()
    participants = data.get("participants", [])

    if player_id not in participants:
        participants.append(player_id)

    ref.set({"participants": participants}, merge=True)

def start_event(event_id):
    db().collection("events").document(event_id).set({
        "status": "active",
        "started_utc": now_utc()
    }, merge=True)

def finish_event(event_id, scores):
    db().collection("events").document(event_id).set({
        "status": "done",
        "scores": scores,
        "completed_utc": now_utc()
    }, merge=True)

# =================================================
# TEACHER EVENT MANAGER (HTML CLONE)
# =================================================
if st.session_state.get("is_teacher", False):
    st.markdown("## 🎯 Event Manager")

    with st.form("create_event_form"):
        event_title = st.text_input("Event Title ([CLASS] for class event)")
        event_domain = st.selectbox("Domain", DOMAINS)
        event_difficulty = st.selectbox("Difficulty", ["Easy", "Medium", "Hard"])

        if st.form_submit_button("Create Event"):
            try:
                create_event(
                    event_title,
                    event_domain,
                    event_difficulty,
                    st.session_state.player_id
                )
                st.success("Event created!")
                st.rerun()
            except Exception as e:
                st.error(str(e))

# =================================================
# STUDENT EVENT JOIN FLOW (HTML CLONE)
# =================================================
try:
    events = list(db().collection("events").stream())
except:
    events = []

if events:
    st.markdown("## 🏟️ Arena Events")

for ev in events:
    data = ev.to_dict()
    eid = ev.id

    st.write(f"**{data.get('title')}** • {data.get('status')}")

    if data.get("status") == "waiting":
        if st.button(f"Join {eid}", key=f"join_{eid}", disabled=challenge_buttons_disabled()):
            join_event(eid, st.session_state.player_id)
            st.success("Joined event!")
            st.rerun()

    elif data.get("status") == "active":
        if st.button(f"Start Event {eid}", key=f"start_event_{eid}", disabled=challenge_buttons_disabled()):
            st.session_state.challenge_mode = True
            st.session_state.active_domain = data.get("domain")
            st.session_state.active_difficulty = data.get("difficulty")
            st.session_state.challenge_count = 0
            st.session_state.challenge_correct = 0
            load_next_question_for_current_mode()
            st.rerun()

# =================================================
# FINAL SAFETY (PREVENT UI FREEZE)
# =================================================
if st.session_state.get("submit_locked", False) and not st.session_state.get("answered", False):
    st.session_state.submit_locked = False
