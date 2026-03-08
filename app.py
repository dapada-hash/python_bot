import os
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
    or "1234"
)

# =================================================
# SETTINGS
# =================================================
MODEL = "gemini-2.5-flash"
BATCH_SIZE = 100
BANK_TARGET = 1000
BANK_CALLS = max(1, BANK_TARGET // BATCH_SIZE)

ALL_DOMAINS_TARGET = 100
ALL_DOMAINS_BATCH_SIZE = 100

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
# DOMAINS
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
# SESSION STATE (PER USER)
# =================================================
st.session_state.setdefault("score", 0)
st.session_state.setdefault("total_answered", 0)
st.session_state.setdefault("answered", False)
st.session_state.setdefault("question", None)
st.session_state.setdefault("is_teacher", False)
st.session_state.setdefault("answer_choice", None)
st.session_state.setdefault("student_name", "")
st.session_state.setdefault("student_period", "Period 1")
st.session_state.setdefault("next_allowed_time", 0.0)
st.session_state.setdefault("gemini_error", "")

# ✅ per-student "seen" tracker so they don't repeat until exhausted
# dict[(topic, difficulty)] -> set[int] indices they've seen
st.session_state.setdefault("seen_by_domain", {})

# =================================================
# SHARED (SERVER-WIDE) BANK — THIS IS THE FIX
# =================================================
@st.cache_resource
def get_shared_bank():
    # bank[(topic, difficulty)] -> list[question dict]
    return {
        "lock": threading.Lock(),
        "bank": {},
        "updated": {},  # (topic, difficulty) -> ISO timestamp
    }

SHARED = get_shared_bank()

def bank_key(topic: str, difficulty: str):
    return (topic, difficulty)

def get_bank_list(topic: str, difficulty: str):
    k = bank_key(topic, difficulty)
    with SHARED["lock"]:
        if k not in SHARED["bank"]:
            SHARED["bank"][k] = []
        return SHARED["bank"][k]

def bank_size(topic: str, difficulty: str) -> int:
    k = bank_key(topic, difficulty)
    with SHARED["lock"]:
        return len(SHARED["bank"].get(k, []))

def bank_last_updated(topic: str, difficulty: str):
    k = bank_key(topic, difficulty)
    with SHARED["lock"]:
        return SHARED["updated"].get(k)

def add_to_bank(topic: str, difficulty: str, questions: list):
    k = bank_key(topic, difficulty)
    with SHARED["lock"]:
        SHARED["bank"].setdefault(k, [])
        SHARED["bank"][k].extend(questions)
        SHARED["updated"][k] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

# =================================================
# GLOBAL LOCK (safe file writes)
# =================================================
@st.cache_resource
def get_file_lock():
    return threading.Lock()

FILE_LOCK = get_file_lock()

# =================================================
# GEMINI ERROR CLASSIFIER
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
# SCORE STORAGE (CSV)
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
# SIDEBAR
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

size_here = bank_size(topic, difficulty)
updated_here = bank_last_updated(topic, difficulty)

st.sidebar.caption(f"✅ Shared bank for THIS Domain: {size_here}")
if updated_here:
    st.sidebar.caption(f"Last teacher refill (UTC): {updated_here}")

# Progress
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

        if st.button(f"✅ Refill {topic} ({difficulty}) +{BATCH_SIZE} questions"):
            with st.spinner(f"Calling Gemini for {topic} ({difficulty})..."):
                qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)
                if qs:
                    add_to_bank(topic, difficulty, qs)
                    st.success(f"Added {len(qs)} to shared bank: {topic} ({difficulty}).")
                else:
                    st.warning("Gemini unavailable. Added fallback instead.")
                    add_to_bank(topic, difficulty, [random.choice(FALLBACK_QUESTIONS) for _ in range(BATCH_SIZE)])
                    if err:
                        st.caption(err)

        if st.button(f"🚀 Build {topic} ({difficulty}) bank (~{BANK_TARGET})"):
            added_total = 0
            with st.spinner(f"Building ~{BANK_TARGET} for {topic} ({difficulty})..."):
                for _ in range(BANK_CALLS):
                    qs, err = fetch_questions_from_gemini(topic, difficulty, BATCH_SIZE)
                    if qs:
                        add_to_bank(topic, difficulty, qs)
                        added_total += len(qs)
                    else:
                        st.warning("Stopped early (Gemini error/quota). Filling remainder with fallback.")
                        add_to_bank(topic, difficulty, [random.choice(FALLBACK_QUESTIONS) for _ in range(BATCH_SIZE)])
                        if err:
                            st.caption(err)
                        break
            st.success(f"Done ✅ Added {added_total} AI questions to shared bank for {topic} ({difficulty}).")

        if st.button(f"🚀 Generate {ALL_DOMAINS_TARGET} questions for EVERY domain ({difficulty})"):
            total_added = 0
            failures = 0
            with st.spinner(f"Generating {ALL_DOMAINS_TARGET} per domain for {difficulty}..."):
                for dom in DOMAINS:
                    qs, err = fetch_questions_from_gemini(dom, difficulty, ALL_DOMAINS_BATCH_SIZE)
                    if qs:
                        add_to_bank(dom, difficulty, qs)
                        total_added += len(qs)
                    else:
                        failures += 1
                        add_to_bank(dom, difficulty, [random.choice(FALLBACK_QUESTIONS) for _ in range(ALL_DOMAINS_BATCH_SIZE)])
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
            st.download_button("⬇️ Download scores.csv", data=csv_bytes, file_name="scores.csv", mime="text/csv")
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
# MAIN: GEMINI STATUS
# =================================================
err = st.session_state.gemini_error
if err:
    t = classify_gemini_error(err)
    if t == "invalid":
        st.warning("Gemini key invalid (teacher calls will fail). Students can still use fallback questions.")
    elif t == "daily_quota":
        st.warning("Gemini daily quota reached (teacher calls will fail today). Students can still practice with fallback.")
    elif t == "rate_limit":
        st.warning("Gemini rate-limited. Try again later.")
    else:
        st.warning("Gemini error. Teacher calls may fail. Students can still practice with fallback.")

# =================================================
# STUDENT: SAVE SCORE
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
# STUDENT: GET NEXT QUESTION FROM SHARED BANK (NO REFILL NEEDED)
# =================================================
now = time.time()
cooldown = int(max(0, st.session_state.next_allowed_time - now))
if cooldown > 0:
    st.caption(f"Cooldown: {cooldown}s")

def pick_question_from_shared_bank(topic: str, difficulty: str):
    """Choose a question from the shared bank WITHOUT removing it, avoiding repeats per student."""
    bank = get_bank_list(topic, difficulty)

    # If no shared bank, fallback
    if len(bank) == 0:
        return random.choice(FALLBACK_QUESTIONS)

    key = (topic, difficulty)
    seen = st.session_state.seen_by_domain.setdefault(key, set())

    # If student has seen everything, reset their seen set
    if len(seen) >= len(bank):
        seen.clear()

    # pick an unseen index
    attempts = 0
    while attempts < 50:
        idx = random.randrange(len(bank))
        if idx not in seen:
            seen.add(idx)
            return bank[idx]
        attempts += 1

    # fallback if too many collisions
    return random.choice(bank)

if st.button("Next Question", disabled=cooldown > 0):
    st.session_state.next_allowed_time = time.time() + COOLDOWN_SECONDS
    st.session_state.question = pick_question_from_shared_bank(topic, difficulty)
    st.session_state.answered = False
    st.session_state.answer_choice = None

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


