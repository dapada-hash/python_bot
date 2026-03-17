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
st.caption("Practice like a game: podiums, XP, streaks, challenges, and live competition.")

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
        "question": "Which keyword is used to define a function in Python?",
        "A": "`function`",
        "B": "`def`",
        "C": "`func`",
        "D": "`lambda`",
        "correct": "B",
        "explanation": "Python functions are defined using the `def` keyword."
    },
    {
        "question": "What does this code print? `print([1, 2, 3][-1])`",
        "A": "`1`",
        "B": "`2`",
        "C": "`3`",
        "D": "Error",
        "correct": "C",
        "explanation": "Index `-1` selects the last item in the list, which is `3`."
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


def challenge_sort_key(challenge_row: dict):
    return str(challenge_row.get("created_utc", ""))


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
Create exactly {count} multiple choice questions.

DOMAIN: {topic}
DIFFICULTY: {difficulty}

Requirements:
- Focus strictly on this Python domain.
- Use Python certification-style multiple choice questions.
- Include short Python code snippets when helpful.
- Ask about syntax, output, logic, debugging, and concepts.
- Use realistic distractors.
- Return only multiple-choice questions.
- Use backticks around code when useful.

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
# XP POPUP
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


# =================================================
# COMBO METER
# =================================================
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


# =================================================
# SESSION STATE
# =================================================
st.session_state.setdefault("score", 0)
st.session_state.setdefault("total_answered", 0)
st.session_state.setdefault("answered", False)
st.session_state.setdefault("question", None)
st.session_state.setdefault("next_allowed_time", 0.0)
st.session_state.setdefault("submit_locked", False)
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

# create-student form defaults
st.session_state.setdefault("create_student_email", "")
st.session_state.setdefault("create_student_password", "")
st.session_state.setdefault("create_student_first_name", "")
st.session_state.setdefault("create_student_id", "")
st.session_state.setdefault("teacher_student_period_create_select", "Period 1")
st.session_state.setdefault("create_student_active", True)

# =================================================
# RESTORE AUTH FROM COOKIE FIRST
# =================================================
if not st.session_state.auth_verified:
    restore_auth_from_cookie()

# =================================================
# AUTH GATE - BEFORE ANY FIRESTORE READ / WRITE
# =================================================
with st.sidebar:
    st.header("Firebase Sign In")

    if cookies is None:
        st.caption("Persistent cookies unavailable. Login will work for the current session only.")

    if not st.session_state.auth_verified:
        with st.form("firebase_login_form"):
            login_email = st.text_input("Email", key="auth_email_input")
            login_password = st.text_input("Password", type="password", key="auth_password_input")
            login_submit = st.form_submit_button("Sign In")

        if login_submit:
            try:
                sign_in_result = firebase_sign_in_email_password(
                    login_email.strip(),
                    login_password
                )
                decoded = verify_firebase_id_token(sign_in_result["id_token"])

                email = str(decoded.get("email", "")).strip().lower()
                teacher_emails = get_teacher_emails()

                st.session_state.auth_verified = True
                st.session_state.auth_id_token = sign_in_result["id_token"]
                st.session_state.auth_refresh_token = sign_in_result["refresh_token"]
                st.session_state.auth_user = {
                    "uid": decoded.get("uid", ""),
                    "email": email,
                    "email_verified": bool(decoded.get("email_verified", False)),
                    "is_teacher": email in teacher_emails,
                }
                st.session_state.is_teacher = email in teacher_emails

                persist_auth_cookie(sign_in_result["id_token"])

                st.success("Signed in successfully.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.info("Sign in to access the app.")
        st.stop()

    auth_user = st.session_state.auth_user or {}
    st.success(f"Signed in as: {auth_user.get('email', 'unknown')}")
    st.caption("Teacher" if auth_user.get("is_teacher") else "Student")

    if st.button("Sign Out", key="sign_out_btn"):
        sign_out()
        st.rerun()

# =================================================
# CHECK FIRESTORE - ONLY AFTER AUTH
# =================================================
firebase_ok, firebase_err = check_firestore()
st.session_state["firebase_ok"] = firebase_ok
st.session_state["firebase_error"] = firebase_err

if not firestore_enabled():
    st.warning("Firebase is not available.")
    st.code(st.session_state.get("firebase_error", "Unknown Firebase error"))
    st.stop()

# =================================================
# LOAD LOCKED PROFILE FOR STUDENTS
# =================================================
auth_user = st.session_state.auth_user or {}
auth_uid = str(auth_user.get("uid", "")).strip()
is_teacher_user = bool(auth_user.get("is_teacher", False))

if not is_teacher_user:
    profile = get_student_profile(auth_uid)
    if not profile:
        st.error("No active student profile found for this account.")
        st.stop()

    st.session_state.first_name = profile.get("first_name", "")
    st.session_state.student_id = str(profile.get("student_id", ""))
    st.session_state.student_period = profile.get("period", "Other")
    st.session_state.player_id = f"{st.session_state.first_name}-{st.session_state.student_id}"
    st.session_state.id_locked = True

# =================================================
# IN-APP IDENTITY UI
# =================================================
st.sidebar.header("Student Identity")

if is_teacher_user:
    st.sidebar.info("Teacher account")
    st.session_state.first_name = st.sidebar.text_input(
        "Preview First Name",
        value=st.session_state.first_name,
        disabled=False,
        key="sidebar_first_name_input"
    )
    st.session_state.student_id = st.sidebar.text_input(
        "Preview Student ID (numbers only)",
        value=st.session_state.student_id,
        disabled=False,
        key="sidebar_student_id_input"
    )

    teacher_period_options = ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"]
    st.session_state.student_period = st.sidebar.selectbox(
        "Preview Class / Period",
        teacher_period_options,
        index=teacher_period_options.index(st.session_state.student_period)
        if st.session_state.student_period in teacher_period_options
        else 0,
        key="sidebar_student_period_select"
    )

    teacher_preview_player_id = ""
    if st.session_state.first_name.strip() and st.session_state.student_id.strip() and st.session_state.student_id.strip().isdigit():
        teacher_preview_player_id = f"{st.session_state.first_name.strip()}-{st.session_state.student_id.strip()}"
        st.sidebar.success(f"Preview Player ID: {teacher_preview_player_id}")
        st.session_state.player_id = teacher_preview_player_id
    else:
        st.sidebar.caption("Teacher can preview a player identity here if needed.")
else:
    st.sidebar.write(f"**First Name:** {st.session_state.first_name}")
    st.sidebar.write(f"**Student ID:** {st.session_state.student_id}")
    st.sidebar.write(f"**Class / Period:** {st.session_state.student_period}")
    st.sidebar.success(f"✅ Player ID: {st.session_state.player_id}")

if not st.session_state.player_id and not is_teacher_user:
    st.warning("No valid player identity found.")
    st.stop()

# =================================================
# SYNC PLAYER ONLY WHEN IDENTITY CHANGES
# =================================================
if (
    st.session_state.player_id
    and (
        st.session_state.last_synced_player_id != st.session_state.player_id
        or st.session_state.last_synced_period != st.session_state.student_period
    )
):
    try:
        upsert_player(st.session_state.player_id, st.session_state.student_period)
        st.session_state.last_synced_player_id = st.session_state.player_id
        st.session_state.last_synced_period = st.session_state.student_period
    except Exception as e:
        st.warning("Could not sync your player record.")
        st.code(str(e))

st.sidebar.divider()
st.sidebar.header("Quiz Settings")
topic = st.sidebar.selectbox("Domain", DOMAINS, key="sidebar_domain_select")
difficulty = st.sidebar.selectbox("Difficulty", ["Easy", "Medium", "Hard"], key="sidebar_difficulty_select")
st.sidebar.caption(f"Shared bank for this domain: {bank_size(topic, difficulty)}")

lu = bank_last_updated(topic, difficulty)
if lu:
    st.sidebar.caption(f"Last teacher refill (UTC): {lu}")

st.sidebar.success("✅ Persistent mode: Firebase Firestore")

# =================================================
# AUTO REFRESH
# =================================================
if st.session_state.get("is_teacher", False):
    if (
        not st.session_state.get("is_generating", False)
        and not st.session_state.get("challenge_mode", False)
    ):
        st_autorefresh(
            interval=60 * 1000,
            limit=None,
            key="teacher_live_refresh_timer"
        )
        st.sidebar.caption("🔄 Teacher refresh every 60 seconds")
    else:
        if st.session_state.get("is_generating", False):
            st.sidebar.caption("⏸ Teacher refresh paused during question generation")
        elif st.session_state.get("challenge_mode", False):
            st.sidebar.caption("⏸ Teacher refresh paused during active challenge")

else:
    if not st.session_state.get("challenge_mode", False):
        st_autorefresh(
            interval=30 * 1000,
            limit=None,
            key="student_challenge_refresh_timer"
        )
        st.sidebar.caption("🔄 Challenges refresh every 30 seconds")
    else:
        st.sidebar.caption("⏸ Auto-refresh paused during active challenge")

    if st.sidebar.button("🔄 Check for new challenges", key="manual_student_refresh_btn"):
        st.rerun()

# =================================================
# SINGLE DATA FETCH
# =================================================
try:
    lb, ch_all = get_app_data()
except Exception as e:
    lb, ch_all = [], []
    st.warning("Could not load Firebase data.")
    st.code(str(e))

lb_sorted = sorted(lb, key=lambda r: safe_int(r.get("xp", 0)), reverse=True)

player_id_lower = st.session_state.player_id.strip().lower()
me = next(
    (r for r in lb if str(r.get("name", "")).strip().lower() == player_id_lower),
    {}
)
my_has_active_challenge = player_has_active_challenge(st.session_state.player_id, ch_all)

check_and_show_finished_challenge_result(ch_all, player_id_lower)

show_xp_popup()
show_challenge_result_popup()

# =================================================
# LEADERBOARD
# =================================================
st.markdown("## 🏆 Live Classroom Leaderboard")
st.caption("Global leaderboard across all domains.")

pod = lb_sorted[:3] + [{}] * max(0, 3 - len(lb_sorted))

col_left, col_mid, col_right = st.columns([1, 1.2, 1])

with col_left:
    if pod[1].get("name"):
        st.markdown(
            f"""
            <div style="text-align:center;background: linear-gradient(180deg, #e5e7eb, #cbd5e1);padding: 18px;border-radius: 18px;border: 2px solid #94a3b8;box-shadow: 0 6px 14px rgba(0,0,0,0.12);min-height: 220px;display:flex;flex-direction:column;justify-content:center;">
                <div style="font-size:48px;">🥈</div>
                <div style="font-size:26px; font-weight:800; margin-top:4px;">#2</div>
                <div style="font-size:22px; font-weight:700; margin-top:8px;">{pod[1]["name"]}</div>
                <div style="font-size:20px; margin-top:8px;">{safe_int(pod[1].get("xp"))} XP</div>
                <div style="font-size:16px; margin-top:8px;">🔥 Best streak: {safe_int(pod[1].get("best_streak"))}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <div style="text-align:center;background:#f1f5f9;padding:18px;border-radius:18px;min-height:220px;display:flex;flex-direction:column;justify-content:center;">
                <div style="font-size:48px;">🥈</div>
                <div style="font-size:22px; font-weight:700;">Open Spot</div>
                <div style="font-size:18px;">0 XP</div>
            </div>
            """,
            unsafe_allow_html=True
        )

with col_mid:
    if pod[0].get("name"):
        st.markdown(
            f"""
            <div style="text-align:center;background: linear-gradient(180deg, #fde68a, #fbbf24);padding: 22px;border-radius: 20px;border: 3px solid #d97706;box-shadow: 0 10px 24px rgba(0,0,0,0.18);min-height: 260px;display:flex;flex-direction:column;justify-content:center;transform: scale(1.03);">
                <div style="font-size:60px;">🥇</div>
                <div style="font-size:30px; font-weight:900; margin-top:4px;">#1</div>
                <div style="font-size:26px; font-weight:800; margin-top:10px;">{pod[0]["name"]}</div>
                <div style="font-size:24px; font-weight:700; margin-top:10px;">{safe_int(pod[0].get("xp"))} XP</div>
                <div style="font-size:18px; margin-top:10px;">🔥 Best streak: {safe_int(pod[0].get("best_streak"))}</div>
                <div style="font-size:16px; margin-top:10px;">👑 Current leader</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <div style="text-align:center;background:#fef3c7;padding:22px;border-radius:20px;min-height:260px;display:flex;flex-direction:column;justify-content:center;">
                <div style="font-size:60px;">🥇</div>
                <div style="font-size:24px; font-weight:800;">Open Spot</div>
                <div style="font-size:18px;">0 XP</div>
            </div>
            """,
            unsafe_allow_html=True
        )

with col_right:
    if pod[2].get("name"):
        st.markdown(
            f"""
            <div style="text-align:center;background: linear-gradient(180deg, #d6a779, #b87333);padding: 18px;border-radius: 18px;border: 2px solid #92400e;box-shadow: 0 6px 14px rgba(0,0,0,0.12);min-height: 220px;display:flex;flex-direction:column;justify-content:center;">
                <div style="font-size:48px;">🥉</div>
                <div style="font-size:26px; font-weight:800; margin-top:4px;">#3</div>
                <div style="font-size:22px; font-weight:700; margin-top:8px;">{pod[2]["name"]}</div>
                <div style="font-size:20px; margin-top:8px;">{safe_int(pod[2].get("xp"))} XP</div>
                <div style="font-size:16px; margin-top:8px;">🔥 Best streak: {safe_int(pod[2].get("best_streak"))}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <div style="text-align:center;background:#f5e1d1;padding:18px;border-radius:18px;min-height:220px;display:flex;flex-direction:column;justify-content:center;">
                <div style="font-size:48px;">🥉</div>
                <div style="font-size:22px; font-weight:700;">Open Spot</div>
                <div style="font-size:18px;">0 XP</div>
            </div>
            """,
            unsafe_allow_html=True
        )

st.markdown("<br>", unsafe_allow_html=True)

top_rows = []
for i, r in enumerate(lb_sorted[:25], start=1):
    top_rows.append({
        "Rank": i,
        "Name": r.get("name", ""),
        "Period": r.get("period", ""),
        "XP": safe_int(r.get("xp", 0)),
        "🔥 Streak": safe_int(r.get("streak", 0)),
        "🏅 Best": safe_int(r.get("best_streak", 0)),
        "W": safe_int(r.get("wins", 0)),
        "L": safe_int(r.get("losses", 0)),
    })

st.dataframe(top_rows, use_container_width=True, height=340)

# =================================================
# CHALLENGE DIRECTLY FROM LEADERBOARD
# =================================================
st.markdown("### ⚔️ Challenge Directly From the Leaderboard")

if my_has_active_challenge:
    st.caption("You already have an active challenge. Finish it before sending another one.")

for i, r in enumerate(lb_sorted[:10], start=1):
    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
    row_cols = st.columns([1, 4, 2, 2, 2])

    with row_cols[0]:
        st.markdown(f"**{medal}**")
    with row_cols[1]:
        st.markdown(f"**{r.get('name', '-')}**")
    with row_cols[2]:
        st.markdown(f"{safe_int(r.get('xp', 0))} XP")
    with row_cols[3]:
        st.markdown(f"🔥 {safe_int(r.get('streak', 0))}")
    with row_cols[4]:
        opp_name = r.get("name", "")
        opp_lower = str(opp_name).strip().lower()

        disabled_send = (
            not opp_name
            or opp_lower == player_id_lower
            or my_has_active_challenge
            or player_has_active_challenge(opp_name, ch_all)
        )

        button_label = "⚔️ Challenge"
        if opp_name and player_has_active_challenge(opp_name, ch_all):
            button_label = "Busy"

        if st.button(button_label, key=f"challenge_{opp_name}_{i}", disabled=disabled_send):
            if time.time() - st.session_state.last_challenge_sent_at < 5:
                st.warning("Please wait a few seconds before sending another challenge.")
            else:
                try:
                    create_challenge(st.session_state.player_id, opp_name, topic, difficulty)
                    st.session_state.last_challenge_sent_at = time.time()
                    st.success(f"Challenge sent to {opp_name}!")
                    st.rerun()
                except Exception as e:
                    st.warning("Could not create challenge.")
                    st.code(str(e))

# =================================================
# PERIOD VS PERIOD
# =================================================
st.markdown("## 🏫 Period vs Period Competition")

period_totals = {}
for r in lb:
    p = r.get("period", "Other")
    period_totals[p] = period_totals.get(p, 0) + safe_int(r.get("xp", 0))

period_rows = [
    {"Period": k, "Total XP": v}
    for k, v in sorted(period_totals.items(), key=lambda x: x[1], reverse=True)
]
st.dataframe(period_rows, use_container_width=True, height=220)

st.divider()

# =================================================
# STUDENT STATUS
# =================================================
my_xp = safe_int(me.get("xp", 0))
my_streak = safe_int(me.get("streak", 0))
my_best = safe_int(me.get("best_streak", 0))

st.markdown("## 🎮 Your Progress")
c1, c2, c3 = st.columns(3)
c1.metric("XP", my_xp)
c2.metric("🔥 Current Streak", my_streak)
c3.metric("🏅 Best Streak", my_best)

goal = 1000
st.progress(min(1.0, my_xp / goal))
st.caption(f"Race to {goal} XP")

render_combo_meter(my_streak)

st.divider()

# =================================================
# QUESTION PICKER / CHALLENGE START HELPERS
# =================================================
def pick_question(topic_: str, difficulty_: str):
    bank = get_bank(topic_, difficulty_)
    if not bank:
        return random.choice(FALLBACK_QUESTIONS)

    key = (topic_, difficulty_)
    seen = st.session_state.seen_by_domain.setdefault(key, set())

    if len(seen) >= len(bank):
        seen.clear()

    for _ in range(100):
        idx = random.randrange(len(bank))
        if idx not in seen:
            seen.add(idx)
            return bank[idx]

    return random.choice(bank)


def load_question(topic_: str, difficulty_: str):
    st.session_state.next_allowed_time = time.time() + max(COOLDOWN_SECONDS, 2)
    st.session_state.question = pick_question(topic_, difficulty_)
    st.session_state.answered = False
    st.session_state.submit_locked = False
    st.session_state.question_token = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"

    st.session_state.answer_widget_nonce += 1
    st.session_state.current_answer_widget_key = f"answer_choice_{st.session_state.answer_widget_nonce}"


def load_next_question_for_current_mode():
    active_topic_local = topic
    active_diff_local = difficulty

    if (
        st.session_state.challenge_mode
        and st.session_state.active_domain
        and st.session_state.active_difficulty
    ):
        active_topic_local = st.session_state.active_domain
        active_diff_local = st.session_state.active_difficulty

    load_question(active_topic_local, active_diff_local)


def start_challenge_attempt(challenge_row: dict):
    status = str(challenge_row.get("status", "")).strip().lower()

    if status != "accepted":
        raise ValueError("This challenge cannot start until the opponent accepts it.")

    st.session_state.challenge_mode = True
    st.session_state.challenge_id = challenge_row["challenge_id"]
    st.session_state.challenge_count = 0
    st.session_state.challenge_correct = 0
    st.session_state.active_domain = challenge_row["domain"]
    st.session_state.active_difficulty = challenge_row["difficulty"]
    load_question(challenge_row["domain"], challenge_row["difficulty"])


# =================================================
# CHALLENGE INBOX / OUTBOX
# =================================================
st.markdown("## 📩 Challenges")
st.caption("New incoming challenges appear automatically about every 30 seconds while you are not inside an active challenge.")

incoming = [
    c for c in ch_all
    if str(c.get("opponent", "")).strip().lower() == player_id_lower
    and c.get("status") in ("pending", "accepted", "done")
]

outgoing = [
    c for c in ch_all
    if str(c.get("challenger", "")).strip().lower() == player_id_lower
    and c.get("status") in ("pending", "accepted", "done")
]

incoming = sorted(incoming, key=challenge_sort_key, reverse=True)[:MAX_CHALLENGE_HISTORY_PER_COLUMN]
outgoing = sorted(outgoing, key=challenge_sort_key, reverse=True)[:MAX_CHALLENGE_HISTORY_PER_COLUMN]

left, right = st.columns(2)

with left:
    st.markdown("### Incoming")
    if not incoming:
        st.caption("No incoming challenges.")
    else:
        for c in incoming:
            already_completed = my_challenge_already_completed(c, player_id_lower)
            challenge_done = c.get("status") == "done"
            status = str(c.get("status", "")).strip().lower()

            st.write(
                f"**{c['challenger']}** challenged you • **{c['domain']}** ({c['difficulty']}) • `{c['status']}`"
            )

            if status == "pending":
                if st.button(f"Accept {c['challenge_id']}", key=f"accept_{c['challenge_id']}"):
                    try:
                        update_challenge(c["challenge_id"], {"status": "accepted"})
                        c["status"] = "accepted"
                        start_challenge_attempt(c)
                        st.success("Challenge accepted!")
                        st.rerun()
                    except Exception as e:
                        st.warning("Could not accept challenge.")
                        st.code(str(e))

            elif challenge_done:
                st.button(
                    "✅ Challenge Over",
                    key=f"incoming_done_{c['challenge_id']}",
                    disabled=True
                )

            elif already_completed:
                st.button(
                    "✅ Already Completed",
                    key=f"incoming_completed_{c['challenge_id']}",
                    disabled=True
                )

            elif status == "accepted":
                if st.button(f"Start {c['challenge_id']}", key=f"incoming_start_{c['challenge_id']}"):
                    try:
                        start_challenge_attempt(c)
                        st.success("Challenge attempt started!")
                        st.rerun()
                    except Exception as e:
                        st.warning("Could not start challenge.")
                        st.code(str(e))

with right:
    st.markdown("### Sent")
    if not outgoing:
        st.caption("No active sent challenges.")
    else:
        for c in outgoing:
            already_completed = my_challenge_already_completed(c, player_id_lower)
            challenge_done = c.get("status") == "done"
            status = str(c.get("status", "")).strip().lower()

            st.write(
                f"To **{c['opponent']}** • **{c['domain']}** ({c['difficulty']}) • `{c['status']}`"
            )

            if challenge_done:
                st.button(
                    "✅ Challenge Over",
                    key=f"start_done_{c['challenge_id']}",
                    disabled=True
                )

            elif already_completed:
                st.button(
                    "✅ Already Completed",
                    key=f"start_completed_{c['challenge_id']}",
                    disabled=True
                )

            elif status == "pending":
                st.button(
                    "⏳ Waiting for opponent",
                    key=f"waiting_{c['challenge_id']}",
                    disabled=True
                )

            elif status == "accepted":
                if st.button(f"Start {c['challenge_id']}", key=f"start_{c['challenge_id']}"):
                    try:
                        start_challenge_attempt(c)
                        st.success("Challenge attempt started!")
                        st.rerun()
                    except Exception as e:
                        st.warning("Could not start challenge.")
                        st.code(str(e))

st.divider()

# =================================================
# TEACHER PANEL CONTENT
# =================================================
if st.session_state.is_teacher:
    st.markdown("## 🔒 Teacher View")

    st.markdown("### 👩‍🏫 Student Manager")

    with st.form("create_student_form"):
        sm1, sm2 = st.columns(2)

        with sm1:
            new_student_email = st.text_input("Student Email", key="create_student_email")
            new_student_password = st.text_input("Temporary Password", type="password", key="create_student_password")
            new_first_name = st.text_input("First Name", key="create_student_first_name")

        with sm2:
            new_student_id = st.text_input("Student ID", key="create_student_id")
            new_period = st.selectbox(
                "Period",
                ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"],
                key="teacher_student_period_create_select"
            )
            new_active = st.checkbox("Active", value=True, key="create_student_active")

        create_student_submit = st.form_submit_button("Create Student")

    if create_student_submit:
        try:
            created = create_student_account_and_profile(
                email=new_student_email,
                password=new_student_password,
                first_name=new_first_name,
                student_id=new_student_id,
                period=new_period,
                active=new_active,
            )

            st.success(f"Student created successfully: {created['display_name']} | UID: {created['uid']}")

            st.session_state["create_student_email"] = ""
            st.session_state["create_student_password"] = ""
            st.session_state["create_student_first_name"] = ""
            st.session_state["create_student_id"] = ""
            st.session_state["teacher_student_period_create_select"] = "Period 1"
            st.session_state["create_student_active"] = True

            st.rerun()

        except Exception as e:
            st.error(str(e))

    try:
        student_profiles = load_student_profiles()
    except Exception as e:
        student_profiles = []
        st.warning("Could not load student profiles.")
        st.code(str(e))

    if student_profiles:
        st.markdown("#### Existing Students")

        student_rows = []
        for s in sorted(student_profiles, key=lambda x: (str(x.get("period", "")), str(x.get("first_name", "")))):
            student_rows.append({
                "UID": s.get("uid", ""),
                "Email": s.get("email", ""),
                "First Name": s.get("first_name", ""),
                "Student ID": s.get("student_id", ""),
                "Period": s.get("period", ""),
                "Active": bool(s.get("active", True)),
            })

        st.dataframe(student_rows, use_container_width=True, height=280)

        student_lookup = {
            f"{s.get('first_name', '')} | {s.get('student_id', '')} | {s.get('email', '')}": s
            for s in student_profiles
        }

        selected_student_label = st.selectbox(
            "Select Student to Edit",
            [""] + list(student_lookup.keys()),
            key="teacher_select_student_to_edit"
        )

        if selected_student_label:
            selected_student = student_lookup[selected_student_label]

            with st.form("edit_student_form"):
                es1, es2 = st.columns(2)

                with es1:
                    edit_first_name = st.text_input("Edit First Name", value=selected_student.get("first_name", ""))
                    edit_student_id = st.text_input("Edit Student ID", value=str(selected_student.get("student_id", "")))

                with es2:
                    edit_period_options = ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"]
                    edit_period = st.selectbox(
                        "Edit Period",
                        edit_period_options,
                        index=edit_period_options.index(selected_student.get("period", "Other"))
                        if selected_student.get("period", "Other") in edit_period_options
                        else 0,
                        key="teacher_student_period_edit_select"
                    )
                    edit_active = st.checkbox("Edit Active", value=bool(selected_student.get("active", True)))

                c1, c2 = st.columns(2)
                with c1:
                    update_student_submit = st.form_submit_button("Update Student")
                with c2:
                    deactivate_label = "Deactivate Student" if bool(selected_student.get("active", True)) else "Activate Student"
                    toggle_active_submit = st.form_submit_button(deactivate_label)

            if update_student_submit:
                try:
                    update_student_profile(
                        uid=selected_student.get("uid", ""),
                        first_name=edit_first_name,
                        student_id=edit_student_id,
                        period=edit_period,
                        active=edit_active,
                    )
                    st.success("Student updated successfully.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

            if toggle_active_submit:
                try:
                    set_student_profile_active(
                        uid=selected_student.get("uid", ""),
                        active=not bool(selected_student.get("active", True))
                    )
                    st.success("Student active status updated.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    st.divider()

    status_box = st.empty()
    progress_box = st.empty()
    result_box = st.empty()

    t1, t2, t3 = st.columns(3)

    with t1:
        if st.button(f"✅ Refill {topic} ({difficulty}) +{BATCH_SIZE}", key="teacher_refill_btn"):
            st.session_state.is_generating = True
            status_box.info("Generating AI questions...")
            progress = progress_box.progress(0)

            qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)
            progress.progress(100)

            if qs:
                add_to_bank(topic, difficulty, qs)
                result_box.success(f"Added {len(qs)} AI questions to shared bank.")
            else:
                result_box.warning("Gemini unavailable. No AI questions were added.")
                if err:
                    with result_box.container():
                        st.error(err)

            st.session_state.is_generating = False

    with t2:
        if st.button(f"🚀 Build {topic} ({difficulty}) bank (~100 questions)", key="teacher_build_bank_btn"):
            st.session_state.is_generating = True
            added = 0
            failures = []
            progress = progress_box.progress(0)

            for i in range(BANK_CALLS):
                status_box.info(f"Building bank... batch {i + 1}/{BANK_CALLS}")
                qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)

                if qs:
                    add_to_bank(topic, difficulty, qs)
                    added += len(qs)
                else:
                    failures.append(err or "Unknown Gemini error")
                    break

                progress.progress(int(((i + 1) / BANK_CALLS) * 100))
                time.sleep(1.0)

            if added:
                result_box.success(f"Done ✅ Added {added} AI questions.")
            else:
                result_box.warning("No AI questions were added.")

            if failures:
                with st.expander("Show AI generation errors"):
                    for f in failures:
                        st.write(f)

            st.session_state.is_generating = False

    with t3:
        if st.button(
            f"🚀 Generate {ALL_DOMAINS_TARGET} for EVERY domain ({difficulty})",
            key="teacher_generate_all_domains_btn"
        ):
            st.session_state.is_generating = True
            total = 0
            failures = []
            progress = progress_box.progress(0)

            for i, dom in enumerate(DOMAINS, start=1):
                status_box.info(f"Generating domain {i}/{len(DOMAINS)}")
                qs, err = fetch_questions_from_gemini(dom, difficulty, ALL_DOMAINS_BATCH_SIZE)

                if qs:
                    add_to_bank(dom, difficulty, qs)
                    total += len(qs)
                else:
                    failures.append(f"{dom} -> {err or 'Unknown Gemini error'}")

                progress.progress(int((i / len(DOMAINS)) * 100))
                time.sleep(1.2)

            if total:
                result_box.success(f"Done ✅ Added {total} AI questions across domains.")
            else:
                result_box.warning("No AI questions were added across domains.")

            if failures:
                result_box.warning(f"{len(failures)} domain(s) failed.")
                with st.expander("Show failed domains"):
                    for f in failures:
                        st.write(f)

            st.session_state.is_generating = False

    teacher_rows = []
    for i, r in enumerate(lb_sorted[:50], start=1):
        teacher_rows.append({
            "Rank": i,
            "name": r.get("name", ""),
            "period": r.get("period", ""),
            "xp": safe_int(r.get("xp", 0)),
            "wins": safe_int(r.get("wins", 0)),
            "losses": safe_int(r.get("losses", 0)),
            "streak": safe_int(r.get("streak", 0)),
            "best_streak": safe_int(r.get("best_streak", 0)),
        })
    st.dataframe(teacher_rows, use_container_width=True, height=240)

# =================================================
# QUESTION AREA
# =================================================
active_topic = topic
active_diff = difficulty

if st.session_state.challenge_mode and st.session_state.active_domain and st.session_state.active_difficulty:
    active_topic = st.session_state.active_domain
    active_diff = st.session_state.active_difficulty
    st.info(
        f"⚔️ Challenge Mode: {active_topic} ({active_diff}) — "
        f"Question {st.session_state.challenge_count + 1}/{CHALLENGE_QUESTIONS}"
    )

cooldown = int(max(0, st.session_state.next_allowed_time - time.time()))
if cooldown > 0:
    st.caption(f"Cooldown: {cooldown}s")

if not st.session_state.challenge_mode:
    if st.button(
        "Next Question",
        disabled=cooldown > 0 or st.session_state.submit_locked,
        key="next_question_btn"
    ):
        load_question(active_topic, active_diff)

q = st.session_state.get("question")
if not q:
    if st.session_state.challenge_mode:
        st.info("Challenge is ready. Your first question should load automatically.")
    else:
        st.info("Click **Next Question** to begin.")
    st.stop()

st.markdown("## 🧠 Question")
st.markdown(q["question"])
st.markdown(f"**A)** {q['A']}")
st.markdown(f"**B)** {q['B']}")
st.markdown(f"**C)** {q['C']}")
st.markdown(f"**D)** {q['D']}")

current_answer_widget_key = st.session_state.current_answer_widget_key

st.radio(
    "Answer",
    ["A", "B", "C", "D"],
    index=None,
    horizontal=True,
    key=current_answer_widget_key,
    disabled=st.session_state.answered
)

selected_answer = st.session_state.get(current_answer_widget_key, None)

if st.button(
    "Submit Answer",
    disabled=st.session_state.submit_locked or st.session_state.answered,
    key="submit_answer_btn"
):
    if selected_answer is None:
        st.warning("Select an answer first.")
    elif st.session_state.answered:
        st.warning("Already submitted.")
    else:
        token = st.session_state.get("question_token", "")
        if token and token in st.session_state.answered_tokens:
            st.warning("This question was already submitted.")
            st.stop()

        st.session_state.submit_locked = True
        st.session_state.id_locked = True
        st.session_state.answered = True
        st.session_state.total_answered += 1

        correct = (selected_answer == q["correct"])

        if correct:
            streak_before = safe_int(me.get("streak", 0))
            streak_after = streak_before + 1
            bonus = STREAK_BONUS_XP if streak_after % STREAK_BONUS_EVERY == 0 else 0

            st.session_state.score += 1

            try:
                add_xp_and_streak(st.session_state.player_id, XP_CORRECT + bonus, +1)
                mark_db_data_stale()
            except Exception as e:
                st.warning("Could not save score to Firebase.")
                st.code(str(e))

            if bonus:
                st.session_state.xp_popup_text = f"+{XP_CORRECT} XP\n🔥 Streak Bonus +{bonus}"
            else:
                st.session_state.xp_popup_text = f"+{XP_CORRECT} XP"

            st.session_state.xp_popup_kind = "good"
            st.session_state.xp_popup_nonce += 1

            if bonus:
                st.success(f"✅ Correct! +{XP_CORRECT} XP  🔥 Streak bonus +{bonus} XP!")
            else:
                st.success(f"✅ Correct! +{XP_CORRECT} XP")
        else:
            try:
                add_xp_and_streak(st.session_state.player_id, XP_WRONG, -999)
                mark_db_data_stale()
            except Exception as e:
                st.warning("Could not save score to Firebase.")
                st.code(str(e))

            st.session_state.xp_popup_text = "❌ Streak Reset"
            st.session_state.xp_popup_kind = "warn"
            st.session_state.xp_popup_nonce += 1

            st.error(f"❌ Incorrect. Correct answer: {q['correct']}")

        if token:
            st.session_state.answered_tokens = [t for t in st.session_state.answered_tokens if t != token]
            st.session_state.answered_tokens.append(token)
            st.session_state.answered_tokens = st.session_state.answered_tokens[-200:]

        st.info(q["explanation"])

        try:
            log_session(
                st.session_state.first_name.strip() or st.session_state.player_id,
                st.session_state.student_period,
                st.session_state.score,
                st.session_state.total_answered,
            )
        except Exception as e:
            st.warning("Could not save session log to Firebase.")
            st.code(str(e))

        if st.session_state.challenge_mode and st.session_state.challenge_id:
            cid = st.session_state.challenge_id
            st.session_state.challenge_count += 1
            if correct:
                st.session_state.challenge_correct += 1

            if st.session_state.challenge_count >= CHALLENGE_QUESTIONS:
                try:
                    current_snap = challenge_ref(cid).get()
                    challenge_row = current_snap.to_dict() if current_snap.exists else None

                    if challenge_row:
                        score_field = my_challenge_score_field(challenge_row, player_id_lower)

                        if score_field and challenge_row.get(score_field) is None:
                            update_challenge(cid, {score_field: st.session_state.challenge_correct})

                        refreshed_snap = challenge_ref(cid).get()
                        refreshed = refreshed_snap.to_dict() if refreshed_snap.exists else None

                        if (
                            refreshed
                            and refreshed.get("challenger_score") is not None
                            and refreshed.get("opponent_score") is not None
                        ):
                            if refreshed.get("status") != "done":
                                update_challenge(cid, {
                                    "status": "done",
                                    "completed_utc": now_utc()
                                })

                            final_snap = challenge_ref(cid).get()
                            final_row = final_snap.to_dict() if final_snap.exists else None

                            if final_row:
                                c_name = final_row["challenger"]
                                o_name = final_row["opponent"]
                                cs = safe_int(final_row.get("challenger_score", 0))
                                os_ = safe_int(final_row.get("opponent_score", 0))

                                if cs > os_:
                                    add_xp_and_streak(c_name, XP_WIN, 0, win_delta=1)
                                    add_xp_and_streak(o_name, XP_LOSS, 0, loss_delta=1)
                                    st.success(f"🏆 {c_name} wins! ({cs} vs {os_})")

                                    if player_id_lower == str(c_name).strip().lower():
                                        st.session_state.challenge_result_popup_text = "YOU WON!"
                                        st.session_state.challenge_result_popup_kind = "win"
                                    else:
                                        st.session_state.challenge_result_popup_text = "YOU LOST"
                                        st.session_state.challenge_result_popup_kind = "loss"

                                    st.session_state.challenge_result_popup_nonce += 1

                                elif os_ > cs:
                                    add_xp_and_streak(o_name, XP_WIN, 0, win_delta=1)
                                    add_xp_and_streak(c_name, XP_LOSS, 0, loss_delta=1)
                                    st.success(f"🏆 {o_name} wins! ({os_} vs {cs})")

                                    if player_id_lower == str(o_name).strip().lower():
                                        st.session_state.challenge_result_popup_text = "YOU WON!"
                                        st.session_state.challenge_result_popup_kind = "win"
                                    else:
                                        st.session_state.challenge_result_popup_text = "YOU LOST"
                                        st.session_state.challenge_result_popup_kind = "loss"

                                    st.session_state.challenge_result_popup_nonce += 1

                                else:
                                    add_xp_and_streak(c_name, XP_DRAW, 0)
                                    add_xp_and_streak(o_name, XP_DRAW, 0)
                                    st.success(f"🤝 Draw! ({cs} vs {os_})")

                                    st.session_state.challenge_result_popup_text = "TIE GAME"
                                    st.session_state.challenge_result_popup_kind = "tie"
                                    st.session_state.challenge_result_popup_nonce += 1
                        else:
                            st.success("✅ Challenge attempt submitted! Waiting for the other student.")
                except Exception as e:
                    st.warning("Could not update challenge.")
                    st.code(str(e))

                st.session_state.challenge_mode = False
                st.session_state.challenge_id = None
                st.session_state.challenge_count = 0
                st.session_state.challenge_correct = 0
                st.session_state.active_domain = None
                st.session_state.active_difficulty = None
                st.info("Challenge finished.")
            else:
                load_next_question_for_current_mode()
                st.rerun()
