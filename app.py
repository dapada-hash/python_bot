import os
import re
import json
import random
import time
import threading
from datetime import datetime

import streamlit as st
from streamlit_autorefresh import st_autorefresh
from google import genai
import gspread
from google.oauth2.service_account import Credentials

# =================================================
# PAGE CONFIG
# =================================================
st.set_page_config(import os
import re
import csv
import random
import time
import threading
from datetime import datetime

import streamlit as st
from google import genai

# =================================================
# PAGE CONFIG
# =================================================
st.set_page_config(page_title="Certiport Python Prep 2026", page_icon="🐍")
st.title("Certiport Python Practice Exam 🐍")

# =================================================
# SAFE SECRETS/ENV READERS
# =================================================
def read_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

def read_env(key: str, default=None):
    return os.getenv(key, default)

# =================================================
# API KEY + TEACHER PIN
# =================================================
API_KEY = (
    read_secret("GEMINI_API_KEY")
    or read_secret("GOOGLE_API_KEY")
    or read_env("GEMINI_API_KEY")
    or read_env("GOOGLE_API_KEY")
    or "YOUR_GEMINI_API_KEY_HERE"
)

TEACHER_PIN = (
    read_secret("TEACHER_PIN")
    or read_env("TEACHER_PIN")
    or "1234"  # fallback only
)

# =================================================
# SETTINGS
# =================================================
MODEL = "gemini-2.5-flash"

BATCH_SIZE = 200                  # per refill call for selected domain
BANK_TARGET = 1000                # per bank build for selected domain
BANK_CALLS = BANK_TARGET // BATCH_SIZE  # 5

# ✅ NEW: "generate for every domain" target
ALL_DOMAINS_TARGET = 100
ALL_DOMAINS_BATCH_SIZE = 100      # 1 API call per domain

COOLDOWN_SECONDS = 2
SCORES_FILE = "scores.csv"

# =================================================
# FALLBACK QUESTIONS (offline)
# =================================================
FALLBACK_QUESTIONS = [
    {
        "question": "What type does input() return?",
        "A": "int",
        "B": "float",
        "C": "str",
        "D": "bool",
        "correct": "C",
        "explanation": "input() always returns a string."
    },
    {
        "question": "What does 7 // 2 equal?",
        "A": "3.5",
        "B": "3",
        "C": "4",
        "D": "2",
        "correct": "B",
        "explanation": "// is floor division, so 7 // 2 = 3."
    },
    {
        "question": "What does len([1,2,3]) return?",
        "A": "2",
        "B": "3",
        "C": "6",
        "D": "1",
        "correct": "B",
        "explanation": "len() returns the number of items."
    },
]

# =================================================
# DOMAINS LIST (for "every domain" generator)
# =================================================
DOMAINS = [
    "1. Data Types and Operators",
    "2. Flow Control (If/Loops)",
    "3. Input/Output Operations",
    "4. Code Documentation/Structure",
    "5. Troubleshooting/Errors",
    "6. Modules (Math/Random/Sys)",
    "7. Perform and analyze data and data type operations",
    "8. Math/Datetime/Random Functions (fabs, ceil, floor, trunc, fmod, frexp, nan, isnan, sqrt, isqrt, pow, pi | now, strftime, weekday | randrange, randint, random, shuffle, choice, sample)",
    "10. try, except, else, finally, raise",
    "11. Read input from console, print formatted text (string.format(), f-strings), use command-line arguments",
    "12. Unittest + assert methods (assertIsInstance, assertEqual, assertTrue, assertIs, assertIn)",
    "13. io/os/os.path/sys (files + existence + sys.argv)",
    "14. Call signatures, defaults, return, def, pass",
    "15. Console input/output + formatting + command-line args",
    "16. Loops: while/for + break/continue/pass + nested + compound conditions",
    "17. Data conversion + indexing/slicing + list operations",
    "18. Construct data structures: sets, tuples, dictionaries",
    "19. Identity operator (is, is not)",
]

# =================================================
# SESSION STATE
# =================================================
st.session_state.setdefault("score", 0)
st.session_state.setdefault("total_answered", 0)
st.session_state.setdefault("answered", False)

# Per-domain queues: dict[(topic, difficulty)] -> list[question]
st.session_state.setdefault("queues", {})

st.session_state.setdefault("gemini_error", "")
st.session_state.setdefault("question", None)
st.session_state.setdefault("is_teacher", False)

# Radio selection storage (blank each new question)
st.session_state.setdefault("answer_choice", None)

# Student info
st.session_state.setdefault("student_name", "")
st.session_state.setdefault("student_period", "Period 1")

# cooldown
st.session_state.setdefault("next_allowed_time", 0.0)

# =================================================
# GLOBAL LOCK (safe file writes)
# =================================================
@st.cache_resource
def get_file_lock():
    return threading.Lock()

FILE_LOCK = get_file_lock()

# =================================================
# HELPERS: Gemini errors
# =================================================
def classify_gemini_error(msg: str) -> str:
    if "API_KEY_INVALID" in msg or "INVALID_ARGUMENT" in msg:
        return "invalid"
    if "GenerateRequestsPerDayPerProjectPerModel-FreeTier" in msg:
        return "daily_quota"
    if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
        return "rate_limit"
    return "other"

# =================================================
# PARSER
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

# =================================================
# DOMAIN HINTS (8, 10–19)
# =================================================
DOMAIN_HINTS = {
    "8.": (
        "Focus ONLY on Python modules and functions: "
        "math (fabs, ceil, floor, trunc, fmod, frexp, nan, isnan, sqrt, isqrt, pow, pi), "
        "datetime (now, strftime, weekday), "
        "random (randrange, randint, random, shuffle, choice, sample)."
    ),
    "10.": "Focus ONLY on exception handling: try, except, else, finally, raise.",
    "11.": (
        "Focus ONLY on console input/output: input(), formatting with string.format() and f-strings, "
        "and command-line arguments with sys.argv."
    ),
    "12.": (
        "Focus ONLY on unittest basics and assert methods: "
        "assertIsInstance, assertEqual, assertTrue, assertIs, assertIn."
    ),
    "13.": (
        "Focus ONLY on io, os, os.path, sys: importing modules, opening/reading files, "
        "checking file existence, and sys.argv."
    ),
    "14.": "Focus ONLY on functions: call signatures, default values, return, def, pass.",
    "15.": (
        "Focus ONLY on console I/O: input(), print formatted text using str.format() and f-strings, "
        "and command-line arguments."
    ),
    "16.": "Focus ONLY on loops: while, for, break, continue, pass, nested loops, compound conditions.",
    "17.": (
        "Focus ONLY on data ops: conversion, indexing, slicing, and list operations "
        "(sort, concat/merge, append, insert, remove, max/min, reverse)."
    ),
    "18.": (
        "Focus ONLY on constructing data structures: sets, tuples, dictionaries. "
        "Include creation, access, update, dict keys/values/items, set uniqueness/operations, tuple immutability."
    ),
    "19.": (
        "Focus ONLY on identity operators: is and is not. "
        "Include difference between == (equality) and is (identity), None checks, object references."
    ),
}

def get_domain_hint(topic_label: str) -> str:
    for prefix, hint in DOMAIN_HINTS.items():
        if topic_label.startswith(prefix):
            return hint
    return ""

# =================================================
# GEMINI (teacher only)
# =================================================
def get_client():
    return genai.Client(api_key=API_KEY)

def fetch_questions_from_gemini(topic: str, difficulty: str, count: int):
    """
    Fetch exactly `count` questions for a specific topic/difficulty.
    Uses the same strict format used by parse_batch().
    """
    domain_hint = get_domain_hint(topic)

    prompt = f"""
Create exactly {count} multiple choice Python questions.

Domain: {topic}
Difficulty: {difficulty}

{domain_hint}

FORMAT RULES (MUST FOLLOW EXACTLY):
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

    try:
        client = get_client()
        resp = client.models.generate_content(model=MODEL, contents=prompt)
        qs = parse_batch(resp.text or "")
        if len(qs) == 0:
            raise RuntimeError("AI format error (could not parse questions).")
        return qs, None
    except Exception as e:
        err = str(e)
        st.session_state.gemini_error = err
        return [], err

# =================================================
# QUEUE HELPERS (per-domain)
# =================================================
def get_queue(topic: str, difficulty: str):
    key = (topic, difficulty)
    if key not in st.session_state.queues:
        st.session_state.queues[key] = []
    return st.session_state.queues[key]

def ensure_queue_student_safe(topic: str, difficulty: str):
    q = get_queue(topic, difficulty)
    if len(q) == 0:
        q.append(random.choice(FALLBACK_QUESTIONS))

# =================================================
# SCOREBOARD STORAGE
# =================================================
def ensure_scores_file():
    if os.path.exists(SCORES_FILE):
        return
    with FILE_LOCK:
        if os.path.exists(SCORES_FILE):
            return
        with open(SCORES_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp_utc", "name", "period", "score", "answered", "accuracy"])

def append_score(name: str, period: str, score: int, answered: int):
    ensure_scores_file()
    accuracy = round((score / answered) * 100, 1) if answered > 0 else 0.0
    ts = datetime.utcnow().isoformat(timespec="seconds")
    with FILE_LOCK:
        with open(SCORES_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([ts, name, period, score, answered, accuracy])

def load_scores(limit: int = 200):
    if not os.path.exists(SCORES_FILE):
        return []
    with FILE_LOCK:
        with open(SCORES_FILE, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    rows.reverse()
    return rows[:limit]

def clear_scores():
    if not os.path.exists(SCORES_FILE):
        return
    with FILE_LOCK:
        os.remove(SCORES_FILE)

# =================================================
# SIDEBAR: STUDENT INFO + QUIZ SETTINGS
# =================================================
st.sidebar.title("Student Info")
st.session_state.student_name = st.sidebar.text_input("Your name", value=st.session_state.student_name)
st.session_state.student_period = st.sidebar.selectbox(
    "Class / Period",
    ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Period 7", "Period 8", "Other"],
    index=["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Period 7", "Period 8", "Other"].index(st.session_state.student_period)
    if st.session_state.student_period in ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Period 7", "Period 8", "Other"]
    else 0
)

st.sidebar.divider()
st.sidebar.title("Quiz Settings")

topic = st.sidebar.selectbox("Domain", DOMAINS)
difficulty = st.sidebar.selectbox("Difficulty", ["Easy", "Medium", "Hard"])

selected_queue = get_queue(topic, difficulty)
st.sidebar.caption(f"Queued for THIS Domain: {len(selected_queue)}")
st.sidebar.caption(f"Teacher refill: {BATCH_SIZE} • Bank: {BANK_TARGET} for {topic} ({difficulty})")

# =================================================
# SIDEBAR: SCORE + PROGRESS
# =================================================
st.sidebar.divider()
st.sidebar.metric("Score", st.session_state.score)
answered = st.session_state.total_answered
accuracy = (st.session_state.score / answered) if answered > 0 else 0.0
st.sidebar.progress(accuracy)
st.sidebar.caption(f"Accuracy: {st.session_state.score}/{answered}")

# =================================================
# TEACHER PANEL
# =================================================
with st.sidebar.expander("🔒 Teacher Panel"):
    pin_input = st.text_input("Enter Teacher PIN", type="password")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Unlock"):
            st.session_state.is_teacher = (pin_input == str(TEACHER_PIN))
            st.success("Teacher mode ON ✅" if st.session_state.is_teacher else "Wrong PIN ❌")
    with c2:
        if st.button("Lock"):
            st.session_state.is_teacher = False
            st.info("Teacher mode locked.")

    st.caption(f"Teacher mode: {'ON' if st.session_state.is_teacher else 'OFF'}")

    if st.session_state.is_teacher:
        st.divider()

        # Refill selected domain
        if st.button(f"✅ Refill {topic} ({difficulty}) +{BATCH_SIZE} questions"):
            with st.spinner(f"Calling Gemini for {topic} ({difficulty})..."):
                qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)
                if qs:
                    selected_queue.extend(qs)
                    st.success(f"Added {len(qs)} questions to {topic} ({difficulty}).")
                else:
                    st.warning("Gemini unavailable. Added fallback questions instead.")
                    selected_queue.extend(random.choice(FALLBACK_QUESTIONS) for _ in range(BATCH_SIZE))
                    if err:
                        st.caption(err)

        # Build bank for selected domain
        if st.button(f"🚀 Build {topic} ({difficulty}) bank (~{BANK_TARGET} questions)"):
            added_total = 0
            with st.spinner(f"Building ~{BANK_TARGET} questions for {topic} ({difficulty})..."):
                for _ in range(BANK_CALLS):
                    qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)
                    if qs:
                        selected_queue.extend(qs)
                        added_total += len(qs)
                    else:
                        st.warning("Stopped early (Gemini error/quota). Filling remaining with fallback.")
                        selected_queue.extend(random.choice(FALLBACK_QUESTIONS) for _ in range(BATCH_SIZE))
                        if err:
                            st.caption(err)
                        break
            st.success(f"Bank ready ✅ Added {added_total} AI questions to {topic} ({difficulty}).")

        # ✅ NEW: Generate 100 questions for every domain (one call per domain)
        if st.button(f"🚀 Generate {ALL_DOMAINS_TARGET} questions for EVERY domain ({difficulty})"):
            total_added = 0
            failures = 0
            with st.spinner(f"Generating {ALL_DOMAINS_TARGET} per domain for {difficulty}..."):
                for dom in DOMAINS:
                    q_dom = get_queue(dom, difficulty)
                    qs, err = fetch_questions_from_gemini(dom, difficulty, ALL_DOMAINS_BATCH_SIZE)
                    if qs:
                        q_dom.extend(qs)
                        total_added += len(qs)
                    else:
                        failures += 1
                        # Fill that domain with fallback so it's still usable
                        q_dom.extend(random.choice(FALLBACK_QUESTIONS) for _ in range(ALL_DOMAINS_BATCH_SIZE))
                        if err:
                            st.caption(f"{dom}: {err}")

            st.success(f"Done ✅ Added {total_added} AI questions across all domains ({difficulty}).")
            if failures:
                st.warning(f"{failures} domain(s) used fallback due to Gemini errors/quota.")

        st.divider()
        st.subheader("Teacher Dashboard")
        scores = load_scores(limit=200)
        if scores:
            st.dataframe(scores, use_container_width=True, height=220)

            with FILE_LOCK:
                csv_bytes = open(SCORES_FILE, "rb").read()
            st.download_button(
                "⬇️ Download scores.csv",
                data=csv_bytes,
                file_name="scores.csv",
                mime="text/csv"
            )
        else:
            st.info("No saved scores yet.")

        colx, coly = st.columns(2)
        with colx:
            if st.button("🧹 Clear score file"):
                clear_scores()
                st.success("scores.csv cleared.")
        with coly:
            st.caption("Clearing removes all saved scores on the server.")

# =================================================
# MAIN: Gemini status messages (if any)
# =================================================
err = st.session_state.gemini_error
if err:
    t = classify_gemini_error(err)
    if t == "invalid":
        st.warning("Gemini key invalid (teacher calls will fail). Students can still use fallback questions.")
    elif t == "daily_quota":
        st.warning("Gemini daily quota reached (teacher calls will fail today). Students can still use fallback questions.")
    elif t == "rate_limit":
        st.warning("Gemini rate-limited. Try again later.")
    else:
        st.warning("Gemini error. Teacher calls may fail. Students can still practice on fallback questions.")

# =================================================
# STUDENT: Save score
# =================================================
st.markdown("### Save your score")
save_disabled = (st.session_state.total_answered == 0) or (st.session_state.student_name.strip() == "")
if st.button("💾 Save My Score", disabled=save_disabled):
    append_score(
        name=st.session_state.student_name.strip(),
        period=st.session_state.student_period,
        score=int(st.session_state.score),
        answered=int(st.session_state.total_answered),
    )
    st.success("Saved ✅ (Teacher can download CSV from the Teacher Panel)")

if st.session_state.student_name.strip() == "":
    st.caption("Enter your name in the sidebar to enable saving your score.")
elif st.session_state.total_answered == 0:
    st.caption("Answer at least one question to enable saving your score.")

st.divider()

# =================================================
# NEXT QUESTION (student-safe, per-domain)
# =================================================
now = time.time()
cooldown = int(max(0, st.session_state.next_allowed_time - now))
if cooldown > 0:
    st.caption(f"Cooldown: {cooldown}s")

if st.button("Next Question", disabled=cooldown > 0):
    st.session_state.next_allowed_time = time.time() + COOLDOWN_SECONDS

    ensure_queue_student_safe(topic, difficulty)
    current_queue = get_queue(topic, difficulty)

    st.session_state.question = current_queue.pop(0)
    st.session_state.answered = False
    st.session_state.answer_choice = None  # reset selection each new question

# =================================================
# DISPLAY QUESTION
# =================================================
q = st.session_state.get("question")

if q:
    st.subheader("Question")
    st.write(q["question"])
    st.write(f"**A)** {q['A']}")
    st.write(f"**B)** {q['B']}")
    st.write(f"**C)** {q['C']}")
    st.write(f"**D)** {q['D']}")

    st.radio(
        "Answer",
        ["A", "B", "C", "D"],
        index=None,
        horizontal=True,
        key="answer_choice",
        disabled=st.session_state.answered
    )

    if st.button("Submit Answer"):
        if st.session_state.answer_choice is None:
            st.warning("Please select an answer before submitting.")
        elif not st.session_state.answered:
            st.session_state.answered = True
            st.session_state.total_answered += 1

            if st.session_state.answer_choice == q["correct"]:
                st.session_state.score += 1
                st.success("✅ Correct!")
            else:
                st.error(f"❌ Incorrect. Correct answer: {q['correct']}")

            st.info(q["explanation"])
else:
    st.info("Click **Next Question** to start.")
    page_title="Certiport HTML & CSS Arena 2026",
    page_icon="🌐",
    layout="wide"
)
st.title("Certiport HTML & CSS Arena 🌐")
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

TEACHER_PIN = (
    read_secret("TEACHER_PIN")
    or read_env("TEACHER_PIN")
    or "1234"
)

LEADERBOARD_SHEET_ID = read_secret("LEADERBOARD_SHEET_ID", None)
GOOGLE_SHEETS_CREDS_JSON = read_secret("GOOGLE_SHEETS_CREDS_JSON", None)

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

# =================================================
# DOMAINS
# =================================================
DOMAINS = [
    "1. script, noscript, style, link, meta tags (encoding, keywords, viewport, description)",
    "2. DOCTYPE, html, head, body, proper syntax, closing tags, commonly used symbols",
    "3. Inline vs internal vs external CSS; precedence; browser default style",
    "4. CSS rule set syntax; selectors: class, id, element, pseudo-class, descendant",
    "5. Common tags: table/tr/th/td, h1-h6, p, br, hr, div, span, ul/ol/li",
    "6. Semantic tags: header, nav, section, article, aside, footer, details/summary, figure/caption",
    "7. Links: target, a href, bookmark, relative vs absolute, folder hierarchies, map/area",
    "8. Forms: attributes, action/method, submission, input types & restrictions, select/textarea/button/option/label",
    "9. Images: img and picture elements and attributes",
    "10. Media: video, audio, track, source, iframe",
    "11. Layout: float/relative/absolute/static/fixed; max-width/overflow/height/width/align/display; inline vs block; visibility; box model; margins",
    "12. Typography: font-family/color/style/size/weight/variant; link colors; text formatting/alignment/decoration/indentation/line-height/word-wrap/letter-spacing; padding",
    "13. Borders & backgrounds: border-color/style/width; background properties; colors",
    "14. Responsive: units (% px em vw vh); viewport & media queries; frameworks/templates; breakpoints; grids",
    "15. CSS best practices: reuse rules, comments, web-safe fonts, cross-platform, usability, separation of HTML/CSS",
    "16. Accessibility: text alternatives, color contrast, legibility, tab order, resizing, hierarchy, translate",
    "17. Troubleshooting: syntax errors, tag mismatch, cascading issues",
]

# =================================================
# FALLBACK QUESTIONS
# =================================================
FALLBACK_QUESTIONS = [
    {
        "question": "Which tag is used to link an external CSS file?",
        "A": "`<style>`",
        "B": "`<link>`",
        "C": "`<meta>`",
        "D": "`<script>`",
        "correct": "B",
        "explanation": "`<link rel=\"stylesheet\" href=\"...\">` connects external CSS."
    },
    {
        "question": "Which selector targets an element with `id=\"main\"`?",
        "A": "`.main`",
        "B": "`#main`",
        "C": "`main`",
        "D": "`*main`",
        "correct": "B",
        "explanation": "`#main` selects an element by id."
    },
    {
        "question": "Which is the correct DOCTYPE for HTML5?",
        "A": "`<!DOCTYPE html>`",
        "B": "`<DOCTYPE html5>`",
        "C": "`<!HTML5>`",
        "D": "`<!DOCTYPE HTML PUBLIC>`",
        "correct": "A",
        "explanation": "HTML5 uses `<!DOCTYPE html>`."
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

def parse_google_sheets_creds(raw_value):
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

    raise ValueError("GOOGLE_SHEETS_CREDS_JSON must be a JSON string or dict.")

def sheets_config_present() -> bool:
    return bool(
        LEADERBOARD_SHEET_ID
        and GOOGLE_SHEETS_CREDS_JSON
        and str(LEADERBOARD_SHEET_ID).strip()
        and str(GOOGLE_SHEETS_CREDS_JSON).strip()
    )

# =================================================
# GOOGLE SHEETS
# =================================================
@st.cache_resource
def get_gsheet_client():
    creds_dict = parse_google_sheets_creds(GOOGLE_SHEETS_CREDS_JSON)
    if not creds_dict:
        raise ValueError("Google Sheets credentials are missing.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def get_sheet():
    gc = get_gsheet_client()
    return gc.open_by_key(LEADERBOARD_SHEET_ID)

def get_ws(tab_name: str):
    return get_sheet().worksheet(tab_name)

LB_HEADER = ["name", "period", "xp", "wins", "losses", "streak", "best_streak", "last_seen_utc"]
CH_HEADER = [
    "challenge_id", "created_utc",
    "challenger", "opponent",
    "domain", "difficulty",
    "status",
    "challenger_score", "opponent_score"
]

def ensure_sheet_tabs_and_headers():
    sh = get_sheet()

    try:
        ws1 = sh.worksheet("leaderboard")
    except Exception:
        ws1 = sh.add_worksheet(title="leaderboard", rows=5000, cols=12)
    if ws1.row_values(1) != LB_HEADER:
        ws1.update("A1:H1", [LB_HEADER])

    try:
        ws2 = sh.worksheet("challenges")
    except Exception:
        ws2 = sh.add_worksheet(title="challenges", rows=5000, cols=12)
    if ws2.row_values(1) != CH_HEADER:
        ws2.update("A1:I1", [CH_HEADER])

def check_google_sheets():
    if not sheets_config_present():
        return False, "Missing LEADERBOARD_SHEET_ID or GOOGLE_SHEETS_CREDS_JSON in secrets."

    try:
        gc = get_gsheet_client()
        gc.open_by_key(LEADERBOARD_SHEET_ID)
        return True, ""
    except Exception as e:
        return False, str(e)

def sheets_enabled():
    return st.session_state.get("google_sheets_ok", False)

# =================================================
# ONE READ LAYER
# =================================================
@st.cache_data(ttl=180)
def load_leaderboard_rows():
    ensure_sheet_tabs_and_headers()
    return get_ws("leaderboard").get_all_records()

@st.cache_data(ttl=180)
def load_challenge_rows():
    ensure_sheet_tabs_and_headers()
    return get_ws("challenges").get_all_records()

def clear_sheet_caches():
    load_leaderboard_rows.clear()
    load_challenge_rows.clear()

def mark_sheet_data_stale():
    st.session_state.last_sheet_sync = 0

def get_leaderboard_data():
    now_ts = time.time()

    if (
        not st.session_state.leaderboard_cache
        or not st.session_state.challenge_cache
        or now_ts - st.session_state.last_sheet_sync > 180
    ):
        st.session_state.leaderboard_cache = load_leaderboard_rows()
        st.session_state.challenge_cache = load_challenge_rows()
        st.session_state.last_sheet_sync = now_ts

    return st.session_state.leaderboard_cache, st.session_state.challenge_cache

# =================================================
# WRITE HELPERS
# =================================================
def lb_upsert_user(name: str, period: str):
    name = name.strip()
    if not name:
        return

    ensure_sheet_tabs_and_headers()
    ws = get_ws("leaderboard")
    rows = load_leaderboard_rows()

    matches = []
    for idx, r in enumerate(rows, start=2):
        if str(r.get("name", "")).strip().lower() == name.lower():
            matches.append((idx, r))

    if matches:
        first_idx, first_row = matches[0]
        ws.update(f"B{first_idx}:H{first_idx}", [[
            period,
            safe_int(first_row.get("xp", 0)),
            safe_int(first_row.get("wins", 0)),
            safe_int(first_row.get("losses", 0)),
            safe_int(first_row.get("streak", 0)),
            safe_int(first_row.get("best_streak", 0)),
            now_utc()
        ]])
    else:
        ws.append_row([name, period, 0, 0, 0, 0, 0, now_utc()], value_input_option="USER_ENTERED")

    clear_sheet_caches()
    mark_sheet_data_stale()

def lb_add_xp_and_streak(name: str, delta_xp: int, streak_delta: int, win_delta=0, loss_delta=0):
    name = name.strip()
    if not name:
        return

    ensure_sheet_tabs_and_headers()
    ws = get_ws("leaderboard")
    rows = load_leaderboard_rows()

    matches = []
    for idx, r in enumerate(rows, start=2):
        if str(r.get("name", "")).strip().lower() == name.lower():
            matches.append((idx, r))

    if not matches:
        lb_upsert_user(name, "Other")
        rows = load_leaderboard_rows()
        for idx, r in enumerate(rows, start=2):
            if str(r.get("name", "")).strip().lower() == name.lower():
                matches.append((idx, r))
                break

    if not matches:
        raise RuntimeError(f"Could not find or create leaderboard row for {name}.")

    first_idx, r = matches[0]

    xp = safe_int(r.get("xp", 0)) + int(delta_xp)
    wins = safe_int(r.get("wins", 0)) + int(win_delta)
    losses = safe_int(r.get("losses", 0)) + int(loss_delta)

    streak = safe_int(r.get("streak", 0))
    best = safe_int(r.get("best_streak", 0))

    if streak_delta == -999:
        streak = 0
    else:
        streak = max(0, streak + streak_delta)
        best = max(best, streak)

    ws.update(f"C{first_idx}:H{first_idx}", [[xp, wins, losses, streak, best, now_utc()]])
    clear_sheet_caches()
    mark_sheet_data_stale()

def ch_write_row(row: list):
    ensure_sheet_tabs_and_headers()
    get_ws("challenges").append_row(row, value_input_option="USER_ENTERED")
    clear_sheet_caches()
    mark_sheet_data_stale()

def ch_update(cid: str, updates: dict, ch_rows=None):
    rows = ch_rows if ch_rows is not None else load_challenge_rows()
    ws = get_ws("challenges")

    for idx, r in enumerate(rows, start=2):
        if str(r.get("challenge_id", "")) == cid:
            new_row = [
                cid,
                r.get("created_utc", ""),
                updates.get("challenger", r.get("challenger", "")),
                updates.get("opponent", r.get("opponent", "")),
                updates.get("domain", r.get("domain", "")),
                updates.get("difficulty", r.get("difficulty", "")),
                updates.get("status", r.get("status", "")),
                updates.get("challenger_score", r.get("challenger_score", "")),
                updates.get("opponent_score", r.get("opponent_score", "")),
            ]
            ws.update(f"A{idx}:I{idx}", [new_row])
            clear_sheet_caches()
            mark_sheet_data_stale()
            return

    raise RuntimeError(f"Challenge {cid} not found.")

def ch_create(challenger: str, opponent: str, domain: str, difficulty: str):
    cid = f"CH{int(time.time() * 1000)}"
    ch_write_row([cid, now_utc(), challenger, opponent, domain, difficulty, "pending", "", ""])
    return cid

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
You are a Certiport HTML/CSS certification exam writer.
Create exactly {count} multiple choice questions.

DOMAIN: {topic}
DIFFICULTY: {difficulty}

Requirements:
- Focus strictly on this domain.
- Focus on Certiport-style HTML/CSS exam prep.
- Use realistic distractors.
- Include short HTML/CSS snippets when helpful.
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

# =================================================
# SESSION STATE
# =================================================
st.session_state.setdefault("score", 0)
st.session_state.setdefault("total_answered", 0)
st.session_state.setdefault("answered", False)
st.session_state.setdefault("question", None)
st.session_state.setdefault("answer_choice", None)
st.session_state.setdefault("next_allowed_time", 0.0)
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
st.session_state.setdefault("google_sheets_ok", False)
st.session_state.setdefault("google_sheets_error", "")
st.session_state.setdefault("leaderboard_cache", [])
st.session_state.setdefault("challenge_cache", [])
st.session_state.setdefault("last_sheet_sync", 0)

st.session_state.setdefault("xp_popup_text", "")
st.session_state.setdefault("xp_popup_kind", "")
st.session_state.setdefault("xp_popup_nonce", 0)

# =================================================
# CHECK GOOGLE SHEETS
# =================================================
google_ok, google_err = check_google_sheets()
st.session_state["google_sheets_ok"] = google_ok
st.session_state["google_sheets_error"] = google_err

# =================================================
# LOGIN / TEACHER MODE FIRST
# =================================================
st.sidebar.header("Student Login (FirstName-ID)")

st.session_state.first_name = st.sidebar.text_input(
    "First Name",
    value=st.session_state.first_name,
    disabled=st.session_state.id_locked
)
st.session_state.student_id = st.sidebar.text_input(
    "Student ID (numbers only)",
    value=st.session_state.student_id,
    disabled=st.session_state.id_locked
)

player_id = ""
if st.session_state.first_name.strip() and st.session_state.student_id.strip():
    if not st.session_state.student_id.strip().isdigit():
        st.sidebar.error("Student ID must be numbers only.")
    else:
        player_id = f"{st.session_state.first_name.strip()}-{st.session_state.student_id.strip()}"
        st.sidebar.success(f"✅ Player ID: {player_id}")

st.session_state.player_id = player_id

st.session_state.student_period = st.sidebar.selectbox(
    "Class / Period",
    ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"],
    index=["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"].index(st.session_state.student_period)
    if st.session_state.student_period in ["Period 1", "Period 2", "Period 3", "Period 4", "Period 5", "Period 6", "Other"]
    else 0
)

with st.sidebar.expander("🔒 Teacher Panel"):
    pin_input = st.text_input("Teacher PIN", type="password")
    tc1, tc2 = st.columns(2)

    with tc1:
        if st.button("Unlock Teacher"):
            st.session_state.is_teacher = (pin_input == str(TEACHER_PIN))
            st.success("Teacher mode ON ✅" if st.session_state.is_teacher else "Wrong PIN ❌")

    with tc2:
        if st.button("Lock Teacher"):
            st.session_state.is_teacher = False
            st.info("Teacher mode OFF")

# =================================================
# AUTO REFRESH - TEACHER ONLY
# =================================================
if st.session_state.get("is_teacher", False):
    live_refresh = st.sidebar.checkbox("Live leaderboard refresh", value=False)
    refresh_seconds = st.sidebar.selectbox("Refresh speed", [30, 60, 120], index=0)

    if live_refresh and not st.session_state.get("is_generating", False):
        st_autorefresh(interval=refresh_seconds * 1000, limit=None, key="teacher_live_refresh")
        st.sidebar.caption(f"🔄 Teacher refresh every {refresh_seconds} seconds")
    elif st.session_state.get("is_generating", False):
        st.sidebar.caption("⏸ Auto-refresh paused during question generation")
else:
    st.sidebar.caption("Student mode: live refresh off")

if not st.session_state.player_id:
    st.warning("Enter First Name + numeric Student ID to start.")
    st.stop()

if not sheets_enabled():
    st.warning("Google Sheets is not available.")
    st.code(st.session_state.get("google_sheets_error", "Unknown Google Sheets error"))
    st.stop()

try:
    lb_upsert_user(st.session_state.player_id, st.session_state.student_period)
except Exception as e:
    st.warning("Could not sync your player record.")
    st.code(str(e))

st.sidebar.divider()
st.sidebar.header("Quiz Settings")
topic = st.sidebar.selectbox("Domain", DOMAINS)
difficulty = st.sidebar.selectbox("Difficulty", ["Easy", "Medium", "Hard"])
st.sidebar.caption(f"Shared bank for this domain: {bank_size(topic, difficulty)}")

lu = bank_last_updated(topic, difficulty)
if lu:
    st.sidebar.caption(f"Last teacher refill (UTC): {lu}")

st.sidebar.success("✅ Persistent mode: Google Sheets")

# =================================================
# SINGLE DATA FETCH
# =================================================
try:
    lb, ch_all = get_leaderboard_data()
except Exception as e:
    lb, ch_all = [], []
    st.warning("Could not load Google Sheets data.")
    st.code(str(e))

lb_sorted = sorted(lb, key=lambda r: safe_int(r.get("xp", 0)), reverse=True)

player_id_lower = st.session_state.player_id.strip().lower()
me = next(
    (r for r in lb if str(r.get("name", "")).strip().lower() == player_id_lower),
    {}
)

show_xp_popup()

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
        if opp_name and opp_name.lower() != player_id_lower:
            if st.button("⚔️ Challenge", key=f"challenge_{opp_name}_{i}"):
                try:
                    ch_create(st.session_state.player_id, opp_name, topic, difficulty)
                    st.success(f"Challenge sent to {opp_name}!")
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

period_rows = [{"Period": k, "Total XP": v} for k, v in sorted(period_totals.items(), key=lambda x: x[1], reverse=True)]
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

st.divider()

# =================================================
# CHALLENGE INBOX / OUTBOX
# =================================================
st.markdown("## 📩 Challenges")

incoming = [
    c for c in ch_all
    if str(c.get("opponent", "")).strip().lower() == player_id_lower
    and c.get("status") in ("pending", "accepted")
]

outgoing = [
    c for c in ch_all
    if str(c.get("challenger", "")).strip().lower() == player_id_lower
    and c.get("status") in ("pending", "accepted")
]

left, right = st.columns(2)

with left:
    st.markdown("### Incoming")
    if not incoming:
        st.caption("No incoming challenges.")
    else:
        for c in incoming[:10]:
            st.write(f"**{c['challenger']}** challenged you • **{c['domain']}** ({c['difficulty']}) • `{c['status']}`")
            if c["status"] == "pending":
                if st.button(f"Accept {c['challenge_id']}"):
                    try:
                        ch_update(c["challenge_id"], {"status": "accepted"}, ch_all)
                        st.session_state.challenge_mode = True
                        st.session_state.challenge_id = c["challenge_id"]
                        st.session_state.challenge_count = 0
                        st.session_state.challenge_correct = 0
                        st.session_state.active_domain = c["domain"]
                        st.session_state.active_difficulty = c["difficulty"]
                        st.success("Challenge accepted!")
                    except Exception as e:
                        st.warning("Could not accept challenge.")
                        st.code(str(e))

with right:
    st.markdown("### Sent")
    if not outgoing:
        st.caption("No active sent challenges.")
    else:
        for c in outgoing[:10]:
            st.write(f"To **{c['opponent']}** • **{c['domain']}** ({c['difficulty']}) • `{c['status']}`")
            if st.button(f"Start {c['challenge_id']}"):
                st.session_state.challenge_mode = True
                st.session_state.challenge_id = c["challenge_id"]
                st.session_state.challenge_count = 0
                st.session_state.challenge_correct = 0
                st.session_state.active_domain = c["domain"]
                st.session_state.active_difficulty = c["difficulty"]
                st.success("Challenge attempt started!")

st.divider()

# =================================================
# TEACHER PANEL CONTENT
# =================================================
if st.session_state.is_teacher:
    st.markdown("## 🔒 Teacher View")

    status_box = st.empty()
    progress_box = st.empty()
    result_box = st.empty()

    t1, t2, t3 = st.columns(3)

    with t1:
        if st.button(f"✅ Refill {topic} ({difficulty}) +{BATCH_SIZE}"):
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
        if st.button(f"🚀 Build {topic} ({difficulty}) bank (~100 questions)"):
            st.session_state.is_generating = True
            added = 0
            failures = []
            progress = progress_box.progress(0)

            for i in range(BANK_CALLS):
                status_box.info(f"Building bank... batch {i+1}/{BANK_CALLS}")
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
        if st.button(f"🚀 Generate {ALL_DOMAINS_TARGET} for EVERY domain ({difficulty})"):
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
# QUESTION PICKER
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

active_topic = topic
active_diff = difficulty

if st.session_state.challenge_mode and st.session_state.active_domain and st.session_state.active_difficulty:
    active_topic = st.session_state.active_domain
    active_diff = st.session_state.active_difficulty
    st.info(f"⚔️ Challenge Mode: {active_topic} ({active_diff}) — Question {st.session_state.challenge_count + 1}/{CHALLENGE_QUESTIONS}")

cooldown = int(max(0, st.session_state.next_allowed_time - time.time()))
if cooldown > 0:
    st.caption(f"Cooldown: {cooldown}s")

if st.button("Next Question", disabled=cooldown > 0):
    st.session_state.next_allowed_time = time.time() + COOLDOWN_SECONDS
    st.session_state.question = pick_question(active_topic, active_diff)
    st.session_state.answered = False
    st.session_state.answer_choice = None

# =================================================
# QUESTION DISPLAY
# =================================================
q = st.session_state.get("question")
if not q:
    st.info("Click **Next Question** to begin.")
    st.stop()

st.markdown("## 🧠 Question")
st.markdown(q["question"])
st.markdown(f"**A)** {q['A']}")
st.markdown(f"**B)** {q['B']}")
st.markdown(f"**C)** {q['C']}")
st.markdown(f"**D)** {q['D']}")

st.radio(
    "Answer",
    ["A", "B", "C", "D"],
    index=None,
    horizontal=True,
    key="answer_choice",
    disabled=st.session_state.answered
)

if st.button("Submit Answer"):
    if st.session_state.answer_choice is None:
        st.warning("Select an answer first.")
    elif st.session_state.answered:
        st.warning("Already submitted.")
    else:
        st.session_state.id_locked = True
        st.session_state.answered = True
        st.session_state.total_answered += 1

        correct = (st.session_state.answer_choice == q["correct"])

        if correct:
            streak_before = safe_int(me.get("streak", 0))
            streak_after = streak_before + 1
            bonus = STREAK_BONUS_XP if streak_after % STREAK_BONUS_EVERY == 0 else 0

            st.session_state.score += 1

            try:
                lb_add_xp_and_streak(st.session_state.player_id, XP_CORRECT + bonus, +1)
                mark_sheet_data_stale()
            except Exception as e:
                st.warning("Could not save score to Google Sheets.")
                st.code(str(e))

            if bonus:
                st.session_state.xp_popup_text = f"+{XP_CORRECT} XP\\n🔥 Streak Bonus +{bonus}"
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
                lb_add_xp_and_streak(st.session_state.player_id, XP_WRONG, -999)
                mark_sheet_data_stale()
            except Exception as e:
                st.warning("Could not save score to Google Sheets.")
                st.code(str(e))

            st.session_state.xp_popup_text = "❌ Streak Reset"
            st.session_state.xp_popup_kind = "warn"
            st.session_state.xp_popup_nonce += 1

            st.error(f"❌ Incorrect. Correct answer: {q['correct']}")

        st.info(q["explanation"])

        if st.session_state.challenge_mode and st.session_state.challenge_id:
            st.session_state.challenge_count += 1
            if correct:
                st.session_state.challenge_correct += 1

            if st.session_state.challenge_count >= CHALLENGE_QUESTIONS:
                cid = st.session_state.challenge_id
                challenge_row = next(
                    (row for row in ch_all if str(row.get("challenge_id", "")) == cid),
                    None
                )

                if challenge_row:
                    try:
                        if challenge_row["challenger"].lower() == player_id_lower:
                            ch_update(cid, {"challenger_score": str(st.session_state.challenge_correct)}, ch_all)
                        elif challenge_row["opponent"].lower() == player_id_lower:
                            ch_update(cid, {"opponent_score": str(st.session_state.challenge_correct)}, ch_all)

                        mark_sheet_data_stale()
                        refreshed_rows = load_challenge_rows()
                        refreshed = next(
                            (row for row in refreshed_rows if str(row.get("challenge_id", "")) == cid),
                            None
                        )

                        if refreshed and refreshed.get("challenger_score") != "" and refreshed.get("opponent_score") != "":
                            ch_update(cid, {"status": "done"}, refreshed_rows)
                            mark_sheet_data_stale()
                            final_rows = load_challenge_rows()
                            final_row = next(
                                (row for row in final_rows if str(row.get("challenge_id", "")) == cid and row.get("status") == "done"),
                                None
                            )
                            if final_row:
                                c = final_row["challenger"]
                                o = final_row["opponent"]
                                cs = safe_int(final_row.get("challenger_score", 0))
                                os_ = safe_int(final_row.get("opponent_score", 0))

                                if cs > os_:
                                    lb_add_xp_and_streak(c, XP_WIN, 0, win_delta=1)
                                    lb_add_xp_and_streak(o, XP_LOSS, 0, loss_delta=1)
                                    mark_sheet_data_stale()
                                    st.success(f"🏆 {c} wins! ({cs} vs {os_})")
                                elif os_ > cs:
                                    lb_add_xp_and_streak(o, XP_WIN, 0, win_delta=1)
                                    lb_add_xp_and_streak(c, XP_LOSS, 0, loss_delta=1)
                                    mark_sheet_data_stale()
                                    st.success(f"🏆 {o} wins! ({os_} vs {cs})")
                                else:
                                    lb_add_xp_and_streak(c, XP_DRAW, 0)
                                    lb_add_xp_and_streak(o, XP_DRAW, 0)
                                    mark_sheet_data_stale()
                                    st.success(f"🤝 Draw! ({cs} vs {os_})")
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

