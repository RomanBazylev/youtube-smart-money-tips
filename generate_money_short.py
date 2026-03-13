import asyncio
import json
import os
import random
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
from PIL import Image
if not hasattr(Image, "ANTIALIAS"):
    Image.ANTIALIAS = Image.LANCZOS

import edge_tts
import requests
from moviepy.editor import (
    AudioFileClip,
    CompositeAudioClip,
    TextClip,
    VideoFileClip,
    CompositeVideoClip,
    concatenate_audioclips,
    concatenate_videoclips,
    vfx,
    afx,
)

# ── Constants ──────────────────────────────────────────────────────────
TARGET_W, TARGET_H = 1080, 1920
BUILD_DIR = Path("build")
CLIPS_DIR = BUILD_DIR / "clips"
AUDIO_DIR = BUILD_DIR / "audio_parts"
MUSIC_PATH = BUILD_DIR / "music.mp3"
HISTORY_PATH = BUILD_DIR / "topic_history.json"
MAX_HISTORY = 12  # remember last N topics to avoid repeats

# Voice rotation for variety
TTS_VOICES = [
    "en-US-GuyNeural",
    "en-US-AndrewMultilingualNeural",
    "en-US-BrianMultilingualNeural",
]
TTS_RATE_OPTIONS = ["+8%", "+10%", "+12%"]

# Pronunciation fixes for finance-specific terms
TTS_PRONUNCIATION_FIXES = {
    "401k": "four oh one K",
    "401(k)": "four oh one K",
    "403b": "four oh three B",
    "IRA": "I R A",
    "ETF": "E T F",
    "ETFs": "E T Fs",
    "ROI": "R O I",
    "APR": "A P R",
    "APY": "A P Y",
    "FICO": "fie-co",
    "S&P": "S and P",
    "S&P500": "S and P five hundred",
    "REIT": "reet",
    "REITs": "reets",
    "CD": "C D",
    "CDs": "C Ds",
    "HSA": "H S A",
    "FSA": "F S A",
    "FIRE": "F I R E",
    "HYSA": "H Y S A",
    "LLC": "L L C",
    "IPO": "I P O",
    "CEO": "C E O",
    "CFO": "C F O",
    "W-2": "W two",
    "1099": "ten ninety nine",
    "K-1": "K one",
    "FDIC": "F D I C",
    "YoY": "year over year",
    "MoM": "month over month",
    "HELOC": "he-lock",
    "BRRRR": "burr strategy",
}

# Content angles — how the topic is presented
ANGLES = [
    "shocking stat that changes your perspective on money",
    "common money mistake almost everyone makes",
    "hidden trick that saves thousands per year",
    "step-by-step money hack anyone can start today",
    "myth vs reality — what actually works with money",
    "rich vs poor mindset difference",
    "money rule that millionaires follow religiously",
    "controversial money take that's actually smart",
    "beginner mistake that costs thousands over time",
    "one simple change that compounds into wealth",
    "money secret financial advisors won't tell you",
    "side-by-side comparison with real numbers",
]

# Finance topics
MONEY_TOPICS = [
    "Budgeting methods", "Emergency fund", "High-yield savings",
    "Index fund investing", "ETF portfolio", "Credit score hacks",
    "Credit card rewards", "Debt payoff strategies", "Student loan tactics",
    "Side hustle income", "Passive income streams", "Freelance pricing",
    "Salary negotiation", "Tax optimization", "Retirement planning",
    "401k strategies", "Roth IRA", "Real estate investing",
    "Compound interest", "Financial psychology", "Frugal living hacks",
    "Cryptocurrency basics", "Insurance optimization", "Net worth tracking",
    "Dividend investing", "Money automation", "FIRE movement",
]

# Audience level
AUDIENCE_LEVELS = [
    "broke college student",
    "young professional starting out",
    "middle-income earner wanting more",
    "parent saving for family",
    "aspiring investor",
    "someone drowning in debt",
]

# Pexels queries — strictly money/finance/tech visuals, NO PEOPLE
PEXELS_QUERIES = [
    "money cash dollar bills dark",
    "stock market chart screen green",
    "calculator spreadsheet finance dark",
    "credit card payment terminal",
    "piggy bank savings coins",
    "real estate house keys",
    "cryptocurrency bitcoin gold",
    "financial dashboard laptop screen",
    "coins money jar savings",
    "gold bars investment vault",
    "stock trading screen charts",
    "dollar bills counting cash",
    "wallet cash money leather",
    "atm money withdraw screen",
    "financial chart growth green",
    "savings account banking app",
    "money growth plant coins",
    "finance spreadsheet dark mode",
    "budget planner notebook pen",
    "investment portfolio screen dark",
    "tax documents calculator desk",
    "bank vault door gold",
    "shopping cart receipt money",
    "paycheck direct deposit screen",
    "compound interest graph chart",
    "side hustle laptop freelance",
    "retirement savings elderly plan",
    "net worth tracker app screen",
]

# Pixabay queries — money/finance only
PIXABAY_QUERIES = [
    "money finance dollar",
    "stock market trading",
    "savings investment coins",
    "cryptocurrency bitcoin",
    "budget calculator finance",
    "real estate investment",
    "gold bars wealth",
    "financial chart growth",
]

# Blacklist — reject any query or tag containing these words
_QUERY_BLACKLIST_WORDS = {
    # People / interactions
    "meeting", "teamwork", "handshake", "presentation",
    "conference", "whiteboard", "planning", "boardroom",
    "negotiation", "seminar", "hug", "embrace", "couple",
    "friends", "love", "together", "celebrate", "portrait",
    "face", "smile", "happy", "group", "crowd", "party",
    "wedding", "romantic", "family", "children", "kid",
    "student", "classroom", "fashion", "model", "beauty",
    "lifestyle", "yoga", "fitness", "dance", "selfie",
    # Corporate / generic
    "corporate", "suit", "office people", "business team",
    "interview", "handshaking", "coworker", "colleague",
}


@dataclass
class ScriptPart:
    text: str


@dataclass
class VideoMetadata:
    title: str
    description: str
    tags: List[str]


# ── Topic deduplication ────────────────────────────────────────────────

def _load_topic_history() -> list:
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_topic_history(history: list) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")


def _pick_unique_topic() -> str:
    """Pick a topic not recently used."""
    history = _load_topic_history()
    available = [t for t in MONEY_TOPICS if t not in history]
    if not available:
        available = MONEY_TOPICS
    topic = random.choice(available)
    history.append(topic)
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    _save_topic_history(history)
    return topic


def _clean_build_dir() -> None:
    """Remove previous build artifacts to save disk space."""
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR, ignore_errors=True)
        print("  Cleaned previous build directory")


def ensure_dirs() -> None:
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)


FALLBACK_METADATA = VideoMetadata(
    title="5 Money Rules That Changed My Life 💰 #shorts",
    description=(
        "These 5 simple money rules helped me build wealth from zero. "
        "Which one are you starting today? Comment below!\n\n"
        "#money #finance #investing #wealth #shorts #personalfinance"
    ),
    tags=["money", "finance", "investing", "wealth", "shorts", "personalfinance", "budgeting"],
)

_CORE_TAGS = ["money", "finance", "shorts", "investing", "wealth", "personalfinance"]

_DESCRIPTION_FOOTER = (
    "\n\n#money #finance #shorts #investing #wealth #personalfinance"
    "\nFollow for daily money tips that actually work!"
)


def _enrich_metadata(meta: VideoMetadata) -> VideoMetadata:
    """Ensure title has #shorts, tags have core keywords, description has footer."""
    title = meta.title
    if "#shorts" not in title.lower():
        title = title.rstrip() + " #shorts"
    if "#money" not in title.lower():
        title = title.rstrip() + " #money"

    tags = list(meta.tags)
    for t in _CORE_TAGS:
        if t not in tags:
            tags.append(t)

    desc = meta.description
    if "#money" not in desc.lower():
        desc = desc + _DESCRIPTION_FOOTER

    return VideoMetadata(title=title[:100], description=desc, tags=tags)


_FALLBACK_POOL = [
    [
        ScriptPart("Five money rules that separate the rich from everyone else."),
        ScriptPart("Rule one — compound interest. Invest $500 a month at 7% for 30 years and you'll have $567,000."),
        ScriptPart("That's $387,000 in pure interest — money making money while you sleep."),
        ScriptPart("Rule two — the 50/30/20 budget. 50% needs, 30% wants, 20% savings. No exceptions."),
        ScriptPart("Rule three — pay yourself first. Before rent, before food, move 20% into investments automatically."),
        ScriptPart("Set up an auto-transfer every payday to your brokerage account. Remove the temptation entirely."),
        ScriptPart("Rule four — automate everything. Bills, savings, investments — your money should move without you touching it."),
        ScriptPart("Rule five — invest, don't just save. A savings account at 0.01% loses to inflation every single year."),
        ScriptPart("Put your money in a total market index fund with a 0.03% expense ratio. Let it grow for decades."),
        ScriptPart("Which rule are you starting this week? Comment below. Follow for more money tips!"),
    ],
    [
        ScriptPart("Stop using a debit card for everything. Credit cards are actually smarter. Here's why."),
        ScriptPart("Reason one — rewards. A good cashback card gives you 2% back on every purchase. That's $1,000 a year on $50K spending."),
        ScriptPart("Reason two — purchase protection. Your credit card company will fight chargebacks for you. Debit cards? Good luck."),
        ScriptPart("Reason three — building credit. Every on-time payment raises your FICO score. A high score saves you $100,000 on a mortgage."),
        ScriptPart("Reason four — fraud protection. If someone steals your credit card number, it's the bank's money at risk, not yours."),
        ScriptPart("With a debit card, that money is gone from your account instantly while you wait for an investigation."),
        ScriptPart("The golden rule — pay your balance in FULL every single month. Never carry a balance, ever."),
        ScriptPart("Set up autopay for the full statement balance. This way you get all the benefits with zero interest."),
        ScriptPart("Start with a no-annual-fee card like the Citi Double Cash — 2% on everything, no games."),
        ScriptPart("Save this for later. Follow for more money tips that actually work!"),
    ],
    [
        ScriptPart("The 401k mistake that costs you $300,000 by retirement. And almost everyone makes it."),
        ScriptPart("Mistake one — not getting the full employer match. That's literally free money you're leaving on the table."),
        ScriptPart("If your employer matches 4%, contribute at least 4%. A $60K salary means $2,400 free per year."),
        ScriptPart("Mistake two — wrong allocation. If you're under 40, put 90% in stocks and 10% in bonds. Target-date funds work too."),
        ScriptPart("Mistake three — high expense ratio funds. A 1% fee versus 0.03% costs you $200,000 over 30 years on a $500K portfolio."),
        ScriptPart("Check your plan for index funds with fees under 0.10%. Switch out of any fund charging over 0.50%."),
        ScriptPart("Mistake four — early withdrawal. You'll pay income tax PLUS a 10% penalty. $50,000 becomes $32,500 after taxes."),
        ScriptPart("Mistake five — not increasing your contribution. Every time you get a raise, bump your 401k by 1%."),
        ScriptPart("Max out at $23,500 per year if you can. Over 30 years at 7%, that's over $2.3 million."),
        ScriptPart("Which mistake were you making? Comment below. Follow for daily money tips!"),
    ],
    [
        ScriptPart("Three side hustles that actually pay more than minimum wage in 2026. With real numbers."),
        ScriptPart("Side hustle one — freelance writing. Businesses pay $50 to $200 per blog post. Write two a week, that's $400 to $1,600 extra per month."),
        ScriptPart("Start on Upwork or Contently. Build a portfolio with three free samples, then pitch paid clients."),
        ScriptPart("Side hustle two — Etsy digital products. Create budget templates, planners, or resume designs once. Sell them forever."),
        ScriptPart("Top sellers make $2,000 to $10,000 per month. Your cost? A $30 Canva subscription."),
        ScriptPart("Side hustle three — online tutoring or consulting. Charge $50 to $150 per hour on platforms like Wyzant or Clarity."),
        ScriptPart("If you know accounting, marketing, or coding — people will pay premium rates for your knowledge."),
        ScriptPart("The key — pick ONE side hustle and go all in for 90 days. Don't chase three at once."),
        ScriptPart("Reinvest your first $1,000 back into the business. Buy better tools, run small ads, build systems."),
        ScriptPart("Which one are you trying first? Drop a comment. Follow for more money tips!"),
    ],
]

_FALLBACK_META_POOL = [
    FALLBACK_METADATA,
    VideoMetadata(
        title="Stop Using Debit Cards — Here's Why 💳 #shorts",
        description="Credit cards are actually smarter than debit cards if you use them right.\n\n#money #finance #creditcard #shorts",
        tags=["money", "finance", "credit card", "debit card", "cashback", "shorts"],
    ),
    VideoMetadata(
        title="The 401k Mistake Costing You $300K 😱 #shorts",
        description="Almost everyone makes these 401k mistakes. Are you losing $300,000?\n\n#money #finance #401k #retirement #shorts",
        tags=["money", "finance", "401k", "retirement", "investing", "shorts"],
    ),
    VideoMetadata(
        title="3 Side Hustles That Actually Pay in 2026 💸 #shorts",
        description="Real side hustles with real numbers. No dropshipping nonsense.\n\n#money #finance #sidehustle #income #shorts",
        tags=["money", "finance", "side hustle", "income", "freelance", "shorts"],
    ),
]


# Filler phrases that make content weak
_FILLER_PATTERNS = [
    "you won't believe", "this is amazing", "this is incredible", "let me tell you",
    "this changed everything", "trust me on this", "you need to hear this",
    "listen carefully", "here's the thing", "everyone should know",
    "let me explain", "this is so cool", "i was shocked", "you'll be surprised",
]


def _validate_script(parts: List[ScriptPart]) -> bool:
    """Quality gate — rejects weak/generic scripts."""
    if len(parts) < 8:
        print(f"[QUALITY] Rejected: too few parts ({len(parts)}, need >=8)")
        return False

    avg_words = sum(len(p.text.split()) for p in parts) / len(parts)
    if avg_words < 8:
        print(f"[QUALITY] Rejected: avg words too low ({avg_words:.1f}, need >=8)")
        return False

    # Check for filler phrases
    filler_count = 0
    for part in parts:
        text_lower = part.text.lower()
        for filler in _FILLER_PATTERNS:
            if filler in text_lower:
                filler_count += 1
                print(f"[QUALITY] Filler detected: '{part.text}'")
                break
    if filler_count > 2:
        print(f"[QUALITY] Rejected: too many fillers ({filler_count})")
        return False

    # At least 40% of phrases must contain finance-specific or actionable content
    concrete_markers = re.compile(
        r'\d|dollar|save|invest|budget|credit|interest|compound|'
        r'fund|stock|etf|401k|ira|debt|income|passive|tax|'
        r'mortgage|loan|index|portfolio|dividend|percent|'
        r'account|bank|score|rate|yield|wealth|retire|'
        r'pay|earn|spend|frugal|side hustle|automat',
        re.IGNORECASE,
    )
    concrete_count = sum(1 for p in parts if concrete_markers.search(p.text))
    ratio = concrete_count / len(parts)
    if ratio < 0.4:
        print(f"[QUALITY] Rejected: not enough concrete content ({ratio:.0%}, need >=40%)")
        return False

    print(f"[QUALITY] Passed: {len(parts)} parts, avg {avg_words:.1f} words, {ratio:.0%} concrete")
    return True


# ── Fallback script ────────────────────────────────────────────────────
def _fallback_script() -> tuple:
    idx = random.randrange(len(_FALLBACK_POOL))
    parts = _FALLBACK_POOL[idx]
    meta = _FALLBACK_META_POOL[idx]
    print(f"[FALLBACK] Using fallback script #{idx + 1}")
    return parts, meta


def call_groq_for_script() -> tuple:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return _fallback_script()

    angle = random.choice(ANGLES)
    topic = _pick_unique_topic()
    level = random.choice(AUDIENCE_LEVELS)

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    system_prompt = (
        "You are an experienced personal finance expert and viral YouTube Shorts scriptwriter. "
        "You create scripts with REAL, ACTIONABLE money advice — specific dollar amounts, percentages, account types, investment vehicles, and step-by-step financial actions. "
        "EVERY phrase must deliver CONCRETE value: a specific number, dollar amount, percentage, account type, or step-by-step financial action. "
        "NEVER write filler phrases like 'This is amazing' or 'You won't believe this' or 'Trust me on this'. "
        "Every phrase = a specific tip, fact, or action the viewer can use immediately to improve their finances. "
        "Write in a confident, direct tone — like a wealthy friend sharing money secrets over coffee. "
        "Use CONTROVERSY and CONTRARIAN takes when possible — challenge popular money advice. "
        "Respond ONLY with valid JSON, no markdown wrappers or explanations."
    )

    user_prompt = f"""Write a YouTube Shorts script (45–60 seconds) about personal finance.

CONTEXT:
- Topic: {topic}
- Angle: {angle}
- Target audience: {level}

CONTENT REQUIREMENTS:
1. First phrase — SCROLL-STOPPING hook: a shocking money stat, provocative claim, or contrarian take with a SPECIFIC NUMBER. Examples: "Stop saving money", "Your bank is stealing $3,000 from you every year", "90% of millionaires did this one thing".
2. EVERY phrase must contain SPECIFIC value: a dollar amount, percentage, account type, investment name, exact steps, or calculation.
3. NO filler phrases. Banned: "This is amazing", "You won't believe", "Trust me", "This changed everything", "Let me explain", "Here's the thing".
4. Each phrase = 1–2 sentences, 12–25 words. Enough for substance, short enough for dynamics.
5. Use "you" — speak like a wealthy friend sharing secrets, not a lecturer.
6. Include at least ONE specific calculation or comparison (e.g., "$500/month at 7% for 30 years = $567,000").
7. Final phrase — strong call to action: ask which tip was best, ask to comment, follow for more money tips.
8. 10–14 parts total (for 45–60 second video).
9. IMPORTANT: include REAL financial terms, specific account types, actual percentages, and concrete steps — NOT generic "save more money" advice.

EXAMPLE OF GOOD PHRASE: "Open a high-yield savings account at 5% APY. Your regular bank pays 0.01%. On $10,000, that's $500 versus $1 per year."
EXAMPLE OF BAD PHRASE: "Start saving money today!" or "This will change your financial life!"

Format — strictly JSON:
{{
  "title": "Catchy YouTube title (max 70 chars) with emoji and #shorts",
  "description": "YouTube description (2–3 lines) with hashtags",
  "tags": ["money", "finance", "shorts", ...4-7 more topic-specific tags],
  "parts": [
    {{ "text": "Phrase with specific actionable financial tip, 12-25 words" }}
  ]
}}"""

    print(f"  Topic: {topic} | Level: {level} | Angle: {angle}")

    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.85,
        "max_tokens": 2048,
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Groq API attempt 1 failed: {exc}, retrying...")
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=45)
            resp.raise_for_status()
        except Exception as exc2:
            print(f"[WARN] Groq API attempt 2 failed: {exc2}, using fallback")
            return _fallback_script()

    try:
        content = resp.json()["choices"][0]["message"]["content"]
        content = re.sub(r"^```(?:json)?\s*", "", content.strip())
        content = re.sub(r"\s*```$", "", content.strip())
        data = json.loads(content)
        parts = [ScriptPart(p["text"]) for p in data.get("parts", []) if p.get("text")]
        metadata = VideoMetadata(
            title=data.get("title", "")[:100] or "Smart Money Tips #shorts",
            description=data.get("description", "") or "#money #finance #investing #shorts",
            tags=data.get("tags", ["money", "finance", "investing", "shorts"]),
        )
        metadata = _enrich_metadata(metadata)

        if _validate_script(parts):
            return parts, metadata
        print("[WARN] LLM output failed quality check, retrying...")
    except Exception as exc:
        print(f"[WARN] Groq parse error: {exc}, retrying...")

    # ── Retry with reinforced prompt ──
    body["messages"].append({
        "role": "user",
        "content": (
            "IMPORTANT: the previous response failed quality checks. "
            "Make sure:\n"
            "1. At least 10 parts, each 12-25 words.\n"
            "2. Every part has SPECIFIC financial content: dollar amounts, percentages, account types, steps.\n"
            "3. NO filler phrases.\n"
            "Return JSON in the same format."
        ),
    })
    body["temperature"] = 1.0
    try:
        resp2 = requests.post(url, headers=headers, json=body, timeout=45)
        resp2.raise_for_status()
        content2 = resp2.json()["choices"][0]["message"]["content"]
        content2 = re.sub(r"^```(?:json)?\s*", "", content2.strip())
        content2 = re.sub(r"\s*```$", "", content2.strip())
        data2 = json.loads(content2)
        parts2 = [ScriptPart(p["text"]) for p in data2.get("parts", []) if p.get("text")]
        metadata2 = VideoMetadata(
            title=data2.get("title", "")[:100] or "Smart Money Tips #shorts",
            description=data2.get("description", "") or "#money #finance #investing #shorts",
            tags=data2.get("tags", ["money", "finance", "investing", "shorts"]),
        )
        metadata2 = _enrich_metadata(metadata2)
        if _validate_script(parts2):
            return parts2, metadata2
        print("[WARN] Retry also failed quality check, using fallback")
    except Exception as exc:
        print(f"[WARN] Retry failed: {exc}, using fallback")

    return _fallback_script()


# ── Download clips ─────────────────────────────────────────────────────
def _download_file(url: str, dest: Path) -> None:
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=32768):
            if chunk:
                f.write(chunk)


def _pexels_best_file(video_files: list) -> Optional[dict]:
    """Pick the best HD file from Pexels video_files list."""
    hd = [f for f in video_files if (f.get("height") or 0) >= 720]
    if hd:
        return min(hd, key=lambda f: abs((f.get("height") or 0) - 1920))
    if video_files:
        return max(video_files, key=lambda f: f.get("height") or 0)
    return None


def download_pexels_clips(target_count: int = 14) -> List[Path]:
    """Download clips using hardcoded finance queries only — 1 clip per query."""
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        return []

    headers = {"Authorization": api_key}
    queries = list(PEXELS_QUERIES)
    random.shuffle(queries)
    queries = queries[:target_count]
    result_paths: List[Path] = []
    seen_ids: set = set()
    clip_idx = 0

    for query in queries:
        if len(result_paths) >= target_count:
            break
        params = {
            "query": query,
            "per_page": 1,  # only top result — most relevant to query
            "orientation": "portrait",
        }
        try:
            resp = requests.get(
                "https://api.pexels.com/videos/search",
                headers=headers, params=params, timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            print(f"[WARN] Pexels search '{query}' failed: {exc}")
            continue

        for video in resp.json().get("videos", []):
            vid_id = video.get("id")
            if vid_id in seen_ids:
                continue
            seen_ids.add(vid_id)
            best = _pexels_best_file(video.get("video_files", []))
            if not best:
                continue
            clip_idx += 1
            clip_path = CLIPS_DIR / f"pexels_{clip_idx}.mp4"
            try:
                _download_file(best["link"], clip_path)
                result_paths.append(clip_path)
                print(f"    Pexels [{query}] -> clip {clip_idx}")
            except Exception as exc:
                print(f"[WARN] Pexels clip {clip_idx} download failed: {exc}")
            if len(result_paths) >= target_count:
                break

    return result_paths


def download_pixabay_clips(max_clips: int = 3) -> List[Path]:
    api_key = os.getenv("PIXABAY_API_KEY")
    if not api_key:
        return []

    query = random.choice(PIXABAY_QUERIES)
    params = {
        "key": api_key,
        "q": query,
        "per_page": max_clips * 3,  # fetch extra to allow tag filtering
        "safesearch": "true",
        "order": "popular",
    }

    try:
        resp = requests.get(
            "https://pixabay.com/api/videos/",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as exc:
        safe_msg = str(exc)
        if api_key:
            safe_msg = safe_msg.replace(api_key, "***")
        print(f"[WARN] Pixabay API error: {safe_msg}")
        return []

    data = resp.json()
    result_paths: List[Path] = []
    clip_idx = 0

    for hit in data.get("hits", []):
        if len(result_paths) >= max_clips:
            break
        # Tag-based filtering — skip clips with people/corporate content
        hit_tags = hit.get("tags", "").lower()
        if any(bw in hit_tags for bw in _QUERY_BLACKLIST_WORDS):
            print(f"    Pixabay skip (blacklisted tags: {hit_tags})")
            continue
        videos = hit.get("videos") or {}
        cand = videos.get("large") or videos.get("medium") or videos.get("small")
        if not cand or "url" not in cand:
            continue
        clip_idx += 1
        url = cand["url"]
        clip_path = CLIPS_DIR / f"pixabay_{clip_idx}.mp4"
        try:
            _download_file(url, clip_path)
            result_paths.append(clip_path)
            print(f"    Pixabay [{query}] -> clip {clip_idx}")
        except Exception as exc:
            print(f"[WARN] Failed to download Pixabay clip {clip_idx}: {exc}")

    return result_paths


def download_background_music() -> Optional[Path]:
    if os.getenv("DISABLE_BG_MUSIC") == "1":
        return None

    if MUSIC_PATH.is_file():
        return MUSIC_PATH

    candidate_urls = [
        "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/no_curator/Komiku/Its_time_for_adventure/Komiku_-_05_-_Friends.mp3",
        "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/no_curator/Podington_Bear/Daydream/Podington_Bear_-_Daydream.mp3",
        "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/ccCommunity/Chad_Crouch/Arps/Chad_Crouch_-_Shipping_Lanes.mp3",
        "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/no_curator/Lobo_Loco/Folkish_things/Lobo_Loco_-_01_-_Acoustic_Dreams_ID_1199.mp3",
    ]

    for url in random.sample(candidate_urls, len(candidate_urls)):
        try:
            _download_file(url, MUSIC_PATH)
            return MUSIC_PATH
        except Exception:
            continue
    return None


# ── TTS (edge-tts, per-phrase) ─────────────────────────────────────────
def _fix_pronunciation(text: str) -> str:
    """Replace hard-to-pronounce terms with phonetic equivalents."""
    result = text
    for word, replacement in TTS_PRONUNCIATION_FIXES.items():
        result = re.sub(re.escape(word), replacement, result, flags=re.IGNORECASE)
    return result


async def _generate_all_audio(parts: List[ScriptPart]) -> List[Path]:
    """Generate all audio phrases in parallel."""
    voice = random.choice(TTS_VOICES)
    rate = random.choice(TTS_RATE_OPTIONS)
    print(f"  TTS voice: {voice}, rate: {rate}")
    audio_paths: List[Path] = []
    tasks = []
    for i, part in enumerate(parts):
        out = AUDIO_DIR / f"part_{i}.mp3"
        audio_paths.append(out)
        tts_text = _fix_pronunciation(part.text)
        comm = edge_tts.Communicate(tts_text, voice, rate=rate)
        tasks.append(comm.save(str(out)))
    await asyncio.gather(*tasks)
    return audio_paths


def build_tts_per_part(parts: List[ScriptPart]) -> List[Path]:
    """Generate a separate mp3 for each phrase — perfect sync."""
    return asyncio.run(_generate_all_audio(parts))


# ── Video assembly ─────────────────────────────────────────────────────
def _fit_clip_to_frame(clip: VideoFileClip, duration: float) -> VideoFileClip:
    """Trim/loop clip to duration, crop to 9:16."""
    if clip.duration > duration + 0.5:
        max_start = clip.duration - duration
        start = random.uniform(0, max_start)
        segment = clip.subclip(start, start + duration)
    else:
        segment = clip.fx(vfx.loop, duration=duration)

    margin = 1.10
    src_ratio = segment.w / segment.h
    target_ratio = TARGET_W / TARGET_H
    if src_ratio > target_ratio:
        segment = segment.resize(height=int(TARGET_H * margin))
    else:
        segment = segment.resize(width=int(TARGET_W * margin))

    segment = segment.crop(
        x_center=segment.w / 2, y_center=segment.h / 2,
        width=TARGET_W, height=TARGET_H,
    )
    return segment


def _apply_ken_burns(clip, duration: float):
    """Slow zoom-in or zoom-out for visual dynamics."""
    direction = random.choice(["in", "out"])
    start_scale = 1.0
    end_scale = random.uniform(1.06, 1.12)
    if direction == "out":
        start_scale, end_scale = end_scale, start_scale

    def make_frame(get_frame, t):
        progress = t / max(duration, 0.01)
        scale = start_scale + (end_scale - start_scale) * progress
        frame = get_frame(t)
        h, w = frame.shape[:2]
        new_h, new_w = int(h * scale), int(w * scale)
        img = Image.fromarray(frame)
        img = img.resize((new_w, new_h), Image.LANCZOS)
        arr = np.array(img)
        y_off = (new_h - h) // 2
        x_off = (new_w - w) // 2
        return arr[y_off:y_off + h, x_off:x_off + w]

    return clip.fl(make_frame)


def _make_subtitle(text: str, duration: float) -> list:
    """Subtitle with stroke — readable on any background."""
    shadow = (
        TextClip(
            text,
            fontsize=72,
            color="black",
            font="DejaVu-Sans-Bold",
            method="caption",
            size=(TARGET_W - 80, None),
            stroke_color="black",
            stroke_width=5,
        )
        .set_position(("center", 0.70), relative=True)
        .set_duration(duration)
    )
    main_txt = (
        TextClip(
            text,
            fontsize=72,
            color="white",
            font="DejaVu-Sans-Bold",
            method="caption",
            size=(TARGET_W - 80, None),
            stroke_color="black",
            stroke_width=3,
        )
        .set_position(("center", 0.70), relative=True)
        .set_duration(duration)
    )
    return [shadow, main_txt]


def build_video(
    parts: List[ScriptPart],
    clip_paths: List[Path],
    audio_parts: List[Path],
    music_path: Optional[Path],
) -> Path:
    if not clip_paths:
        raise RuntimeError("No video clips downloaded. Provide PEXELS_API_KEY or PIXABAY_API_KEY.")

    part_audios = [AudioFileClip(str(p)) for p in audio_parts]
    durations = [a.duration for a in part_audios]
    total_duration = sum(durations)

    voice = concatenate_audioclips(part_audios)

    if len(clip_paths) >= len(parts):
        chosen_clips = random.sample(clip_paths, len(parts))
    else:
        chosen_clips = clip_paths[:]
        random.shuffle(chosen_clips)
        while len(chosen_clips) < len(parts):
            chosen_clips.append(random.choice(clip_paths))

    source_clips = []
    video_clips = []
    for i, part in enumerate(parts):
        src_path = chosen_clips[i]
        clip = VideoFileClip(str(src_path))
        source_clips.append(clip)
        dur = durations[i]

        fitted = _fit_clip_to_frame(clip, dur)
        fitted = _apply_ken_burns(fitted, dur)

        subtitle_layers = _make_subtitle(part.text, dur)

        composed = CompositeVideoClip(
            [fitted] + subtitle_layers,
            size=(TARGET_W, TARGET_H),
        ).set_duration(dur)
        video_clips.append(composed)

    FADE_DUR = 0.2
    for idx in range(1, len(video_clips)):
        video_clips[idx] = video_clips[idx].crossfadein(FADE_DUR)

    video = concatenate_videoclips(video_clips, method="compose").set_duration(total_duration)

    audio_tracks = [voice]
    bg = None
    if music_path and music_path.is_file():
        bg = AudioFileClip(str(music_path)).volumex(0.10)
        bg = bg.set_duration(total_duration)
        bg = bg.fx(afx.audio_fadeout, min(1.5, total_duration * 0.1))
        audio_tracks.append(bg)

    final_audio = CompositeAudioClip(audio_tracks)
    video = video.set_audio(final_audio).set_duration(total_duration)

    output_path = BUILD_DIR / "output_money_short.mp4"
    video.write_videofile(
        str(output_path),
        fps=30,
        codec="libx264",
        audio_codec="aac",
        preset="medium",
        bitrate="8000k",
        threads=4,
    )

    voice.close()
    if bg is not None:
        bg.close()
    for a in part_audios:
        a.close()
    for vc in video_clips:
        vc.close()
    for sc in source_clips:
        sc.close()
    video.close()

    return output_path


def _save_metadata(meta: VideoMetadata) -> None:
    """Save video metadata to JSON for auto-upload."""
    meta_path = BUILD_DIR / "metadata.json"
    meta_path.write_text(
        json.dumps(
            {"title": meta.title, "description": meta.description, "tags": meta.tags},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"  Metadata saved to {meta_path}")


def main() -> None:
    _clean_build_dir()
    ensure_dirs()
    print("[1/5] Generating script...")
    parts, metadata = call_groq_for_script()
    print(f"  Script: {len(parts)} parts")
    print(f"  Title: {metadata.title}")
    total_words = 0
    for i, p in enumerate(parts, 1):
        wc = len(p.text.split())
        total_words += wc
        print(f"  [{i}] ({wc}w) {p.text}")
    est_duration = total_words / 2.8  # ~2.8 words/sec for English TTS
    print(f"  Estimated duration: ~{est_duration:.0f}s ({total_words} words)")
    _save_metadata(metadata)

    print("[2/5] Downloading video clips...")
    clip_paths = download_pexels_clips()
    clip_paths += download_pixabay_clips()
    print(f"  Downloaded {len(clip_paths)} clips")

    print("[3/5] Generating TTS audio (edge-tts, per-part)...")
    audio_parts = build_tts_per_part(parts)
    for i, ap in enumerate(audio_parts):
        a = AudioFileClip(str(ap))
        print(f"  Part {i+1}: {a.duration:.1f}s")
        a.close()

    print("[4/5] Downloading background music...")
    music_path = download_background_music()

    print("[5/5] Building final video...")
    output = build_video(parts, clip_paths, audio_parts, music_path)
    print(f"Done! Video saved to: {output}")


if __name__ == "__main__":
    main()
