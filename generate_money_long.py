"""
Long-form video generator for Smart Money Tips channel.
Pipeline: RSS feeds → pick article → scrape → extract facts (LLM) →
          generate script (LLM) → edge-tts → Pexels clips → ffmpeg → upload
"""

import asyncio
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import edge_tts
import requests
from bs4 import BeautifulSoup

# ── Constants ──────────────────────────────────────────────────────────
BUILD_DIR = Path("build")
CLIPS_DIR = BUILD_DIR / "clips"
AUDIO_PATH = BUILD_DIR / "voiceover.mp3"
MUSIC_PATH = BUILD_DIR / "music.mp3"
METADATA_PATH = BUILD_DIR / "metadata.json"
OUTPUT_PATH = BUILD_DIR / "output_money_long.mp4"
USED_ARTICLES_PATH = Path("used_articles_long.json")

TARGET_W, TARGET_H = 1280, 720
FPS = 30
FFMPEG_PRESET = "medium"
FFMPEG_CRF = "23"

# RSS feeds — major personal finance sites
RSS_FEEDS = [
    "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_articles",
    "https://www.nerdwallet.com/blog/feed/",
    "https://feeds.feedburner.com/BankrateBlog",
    "https://www.thesimpledollar.com/feed/",
    "https://feeds.feedburner.com/moneyunder30",
]

TTS_VOICES = [
    "en-US-GuyNeural",
    "en-US-AndrewMultilingualNeural",
    "en-US-BrianMultilingualNeural",
]
TTS_RATE = "+3%"

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

PEXELS_QUERIES = [
    "money cash dollar bills", "stock market chart screen",
    "calculator finance desk", "credit card payment",
    "piggy bank savings", "real estate house keys",
    "cryptocurrency bitcoin gold", "financial dashboard laptop",
    "coins money jar", "gold bars investment",
    "stock trading screen", "dollar bills counting",
    "wallet cash leather", "atm money screen",
    "financial chart growth", "savings banking app",
    "money growth plant", "budget planner notebook",
    "investment portfolio screen", "tax documents calculator",
    "bank vault gold", "retirement planning elderly",
    "office desk work laptop", "city skyline business",
    "shopping grocery budget", "insurance documents desk",
    "apartment home interior", "car dealership vehicles",
    "university education books", "hospital health medical",
]

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

MUSIC_URLS = [
    "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/no_curator/Komiku/Its_time_for_adventure/Komiku_-_05_-_Friends.mp3",
    "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/no_curator/Podington_Bear/Daydream/Podington_Bear_-_Daydream.mp3",
    "https://files.freemusicarchive.org/storage-freemusicarchive-org/music/ccCommunity/Chad_Crouch/Arps/Chad_Crouch_-_Shipping_Lanes.mp3",
]

_DESCRIPTION_FOOTER = (
    "\n\n---\n"
    "Subscribe to Smart Money Tips for weekly deep dives into personal finance! 🔔\n"
    "Drop your questions in the comments 👇\n\n"
    "#money #finance #investing #personalfinance #wealth #budgeting"
)

_CORE_TAGS = [
    "money", "finance", "investing", "personalfinance",
    "wealth", "budgeting", "financial freedom",
]

# Fallback topics if RSS fails
FALLBACK_TOPICS = [
    "How to Build an Emergency Fund From Zero",
    "Index Fund Investing: The Complete Beginner's Guide",
    "Credit Score Secrets: How to Go From 500 to 800",
    "The Real Cost of Debt: What Banks Don't Tell You",
    "Roth IRA vs Traditional IRA: Which Is Better For You",
    "How to Negotiate a Higher Salary: Step by Step",
    "Side Hustle Income: 5 Ways to Make $1,000 Extra Per Month",
    "The 50/30/20 Budget Rule Explained With Real Numbers",
    "How Compound Interest Can Make You a Millionaire",
    "Tax Optimization Strategies for Middle-Income Earners",
    "Real Estate Investing for Beginners: REITs vs Rental Property",
    "The FIRE Movement: How to Retire in Your 40s",
    "Credit Card Rewards: How to Earn $2,000 Per Year in Cashback",
    "Student Loan Payoff Strategies: Avalanche vs Snowball",
    "How to Start Investing With Just $100",
    "Insurance Optimization: Save $3,000 Per Year",
    "Dividend Investing: Building a Passive Income Portfolio",
    "How to Build a Net Worth of $100,000 Before 30",
    "The Psychology of Money: Why Smart People Make Bad Decisions",
    "Cryptocurrency Basics: Bitcoin, Ethereum, and Beyond",
]


# ── Helpers ────────────────────────────────────────────────────────────
def _clean_build_dir():
    if BUILD_DIR.is_dir():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)


def _run_ffmpeg(cmd: list):
    print(f"[CMD] {' '.join(cmd[:8])}... ({len(cmd)} args)")
    subprocess.run(cmd, check=True)


def _probe_duration(path: Path) -> float:
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        text=True,
    ).strip()
    return float(out)


def _fix_pronunciation(text: str) -> str:
    result = text
    for word, fix in TTS_PRONUNCIATION_FIXES.items():
        result = re.sub(re.escape(word), fix, result, flags=re.IGNORECASE)
    return result


def _groq_call(messages: list, temperature: float = 0.7, max_tokens: int = 4096, json_mode: bool = False) -> Optional[str]:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    for attempt in range(1, 3):
        try:
            r = requests.post(GROQ_URL, headers=headers, json=body, timeout=90)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except Exception as exc:
            print(f"[WARN] Groq attempt {attempt} failed: {exc}")
            time.sleep(5)
    return None


# ── Article Sourcing (RSS) ────────────────────────────────────────────
def _fetch_rss_articles() -> list[dict]:
    """Fetch articles from RSS feeds. Returns list of {url, title}."""
    articles = []
    for feed_url in RSS_FEEDS:
        try:
            r = requests.get(feed_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.content, "xml")
            items = soup.find_all("item")
            for item in items[:10]:  # top 10 per feed
                link = item.find("link")
                title = item.find("title")
                if link and title:
                    link_text = link.get_text(strip=True) or (link.string or "").strip()
                    if not link_text and link.next_sibling:
                        link_text = str(link.next_sibling).strip()
                    title_text = title.get_text(strip=True)
                    if link_text and title_text:
                        articles.append({"url": link_text, "title": title_text})
        except Exception as exc:
            print(f"[WARN] RSS {feed_url}: {exc}")
    print(f"[RSS] Fetched {len(articles)} articles from {len(RSS_FEEDS)} feeds")
    return articles


def _load_used_articles() -> set:
    if USED_ARTICLES_PATH.is_file():
        try:
            return set(json.loads(USED_ARTICLES_PATH.read_text("utf-8")))
        except Exception:
            pass
    return set()


def _save_used_articles(used: set):
    USED_ARTICLES_PATH.write_text(
        json.dumps(sorted(used), ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _pick_article(articles: list[dict], used: set) -> Optional[dict]:
    available = [a for a in articles if a["url"] not in used]
    if not available:
        print("[WARN] All articles used, resetting history")
        available = articles
        used.clear()
    if not available:
        return None
    return random.choice(available)


def _scrape_article(url: str) -> tuple[str, str]:
    """Scrape article title and text."""
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title = ""
    title_tag = soup.find("h1")
    if title_tag:
        title = title_tag.get_text(strip=True)

    # Try common content containers
    content_div = None
    for selector in ["article", "div.article-body", "div.entry-content",
                     "div.post-content", "div.article-content", "main"]:
        content_div = soup.find(selector.split(".")[-1],
                                class_=selector.split(".")[-1] if "." in selector else None)
        if not content_div and "." in selector:
            tag, cls = selector.split(".")
            content_div = soup.find(tag, class_=cls)
        if content_div:
            break

    if not content_div:
        content_div = soup.find("article") or soup.find("main") or soup.body

    if content_div:
        for tag in content_div.find_all(["script", "style", "nav", "aside", "footer", "header"]):
            tag.decompose()
        text = content_div.get_text(separator="\n", strip=True)
    else:
        text = soup.get_text(separator="\n", strip=True)

    lines = [l.strip() for l in text.split("\n") if l.strip()]
    text = "\n".join(lines)
    return title, text


# ── Two-Step LLM Pipeline ────────────────────────────────────────────
def step1_extract_facts(article_title: str, article_text: str) -> Optional[str]:
    """Step 1: Compress article into 7-10 key facts (~500 words)."""
    words = article_text.split()
    if len(words) > 8000:
        article_text = " ".join(words[:8000])

    messages = [
        {"role": "system", "content": (
            "You are a personal finance expert. "
            "Your task is to read the article and extract 7-10 key facts. "
            "Write ONLY facts, no introductions. Each fact should be 1-2 sentences. "
            "Preserve specifics: numbers, percentages, dollar amounts, account types, deadlines. "
            "Total length: approximately 500 words."
        )},
        {"role": "user", "content": (
            f"Article title: {article_title}\n\n"
            f"Article text:\n{article_text}\n\n"
            "Extract 7-10 key facts from this article."
        )},
    ]
    result = _groq_call(messages, temperature=0.3, max_tokens=2048)
    if result:
        print(f"[STEP1] Extracted facts: {len(result.split())} words")
    return result


def step2_generate_script(facts: str, article_title: str) -> Optional[dict]:
    """Step 2: Generate original YouTube script from extracted facts."""
    messages = [
        {"role": "system", "content": (
            "You are a personal finance YouTube creator with 500K subscribers. "
            "Channel name: Smart Money Tips. "
            "You write scripts for long-form YouTube videos (8-12 minutes). "
            "Style: confident, direct, like a wealthy friend sharing secrets over coffee.\n\n"
            "RULES:\n"
            "- Each sentence should be max 15 words (for TTS voiceover).\n"
            "- Use transition phrases: 'here's the thing', 'now let's talk about', "
            "'and here's where it gets interesting', 'the key takeaway', "
            "'but wait there's more', 'let's break this down'.\n"
            "- Use SPECIFIC numbers: dollar amounts, percentages, timeframes.\n"
            "- DO NOT copy the source text — rewrite in YOUR voice.\n"
            "- Structure: Hook → Main Body (5-7 sections) → Summary → CTA.\n\n"
            "Respond ONLY with valid JSON."
        )},
        {"role": "user", "content": f"""Based on these facts, write a YouTube video script (8-12 minutes).

TOPIC: {article_title}

FACTS:
{facts}

CRITICAL: The "script" field must contain AT LEAST 1200 words.
This is a LONG video, not a Short. If the script is under 800 words, the video cannot be produced.

STRUCTURE (write ALL sections fully, do NOT skip):
1. HOOK (60-80 words): grab the viewer with a provocative question or shocking stat. Promise specific answers.
2. MAIN BODY (5-7 blocks, each 150-200 words):
   - Each block starts with a transition phrase
   - Explain WHAT, WHY, and HOW in detail
   - Give a concrete example or real-life scenario
   - Include specific numbers: dollar amounts, percentages, timeframes
   - End each block with a mini-conclusion
3. SUMMARY (60-80 words): recap 3 main takeaways.
4. CTA (30-40 words): ask viewers to subscribe, like, and comment.

WORD COUNT: Hook (~70) + 6 blocks × 175 words (~1050) + Summary (~70) + CTA (~35) = ~1225 words.
You MUST write at least 1200 words. Count carefully.

JSON FORMAT:
{{
  "title": "Video title, max 90 chars, with emoji",
  "description": "Description 5-8 lines with hashtags",
  "tags": ["money", "finance", ...10-15 more tags],
  "pexels_queries": ["5-8 English queries for stock footage search"],
  "script": "ONE STRING with the full script text (1200-1800 words). Sentences separated by newlines. NOT an array."
}}"""},
    ]
    content = _groq_call(messages, temperature=0.8, max_tokens=16384, json_mode=True)
    if not content:
        return None
    try:
        content = re.sub(r"^```(?:json)?\s*", "", content.strip())
        content = re.sub(r"\s*```$", "", content.strip())
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end > start:
            content = content[start:end + 1]
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            content = re.sub(r'[\x00-\x1f\x7f]', lambda m: f'\\u{ord(m.group()):04x}', content)
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                import ast
                data = ast.literal_eval(content)
        script = data.get("script", "")
        if isinstance(script, list):
            script = "\n".join(str(s) for s in script)
            data["script"] = script
        word_count = len(script.split())
        print(f"[STEP2] Script generated: {word_count} words")
        if word_count < 250:
            print("[WARN] Script too short (< 250 words), skipping")
            return None
        if word_count < 600:
            print(f"[WARN] Script shorter than ideal ({word_count} words), but usable")
        return data
    except Exception as exc:
        print(f"[WARN] JSON parse failed: {exc}")
        return None


def _generate_fallback_script(topic: str) -> Optional[dict]:
    """Generate a script from a fallback topic (no article scraping)."""
    messages = [
        {"role": "system", "content": (
            "You are a personal finance YouTube creator with 500K subscribers. "
            "Channel: Smart Money Tips. Write long-form scripts (8-12 min). "
            "Style: confident, direct, specific numbers and examples. "
            "Each sentence max 15 words. Respond ONLY with valid JSON."
        )},
        {"role": "user", "content": f"""Write a comprehensive YouTube video script about: {topic}

The script must be AT LEAST 1200 words. Structure:
1. HOOK (60-80 words): provocative question or shocking stat
2. MAIN BODY (5-7 sections, each 150-200 words): detailed advice with specific numbers
3. SUMMARY (60-80 words): 3 key takeaways
4. CTA (30-40 words): subscribe, like, comment

JSON FORMAT:
{{
  "title": "Video title, max 90 chars, with emoji",
  "description": "Description 5-8 lines with hashtags",
  "tags": ["money", "finance", ...10-15 more tags],
  "pexels_queries": ["5-8 English queries for stock footage"],
  "script": "ONE STRING with the full script (1200-1800 words). Sentences separated by newlines."
}}"""},
    ]
    content = _groq_call(messages, temperature=0.8, max_tokens=16384, json_mode=True)
    if not content:
        return None
    try:
        content = re.sub(r"^```(?:json)?\s*", "", content.strip())
        content = re.sub(r"\s*```$", "", content.strip())
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end > start:
            content = content[start:end + 1]
        data = json.loads(content)
        script = data.get("script", "")
        if isinstance(script, list):
            script = "\n".join(str(s) for s in script)
            data["script"] = script
        word_count = len(script.split())
        print(f"[FALLBACK SCRIPT] Generated: {word_count} words")
        if word_count < 250:
            return None
        return data
    except Exception as exc:
        print(f"[WARN] Fallback JSON parse failed: {exc}")
        return None


# ── TTS ───────────────────────────────────────────────────────────────
async def _generate_tts(text: str, output_path: Path) -> list[dict]:
    voice = random.choice(TTS_VOICES)
    tts_text = _fix_pronunciation(text)
    comm = edge_tts.Communicate(tts_text, voice, rate=TTS_RATE, boundary="WordBoundary")
    word_events = []
    with open(output_path, "wb") as f:
        async for chunk in comm.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])
            elif chunk["type"] == "WordBoundary":
                word_events.append({
                    "text": chunk["text"],
                    "offset": chunk["offset"] / 10_000_000,
                    "duration": chunk["duration"] / 10_000_000,
                })
    print(f"[TTS] {voice}, {len(word_events)} words, file={output_path}")
    return word_events


def generate_tts(text: str) -> tuple[Path, list[dict]]:
    word_events = asyncio.run(_generate_tts(text, AUDIO_PATH))
    return AUDIO_PATH, word_events


# ── Clip Downloading ─────────────────────────────────────────────────
def _download_file(url: str, dest: Path):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with dest.open("wb") as f:
        for chunk in r.iter_content(32768):
            if chunk:
                f.write(chunk)


def download_clips(extra_queries: list[str] = None, target: int = 35) -> list[Path]:
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        print("[WARN] No PEXELS_API_KEY")
        return []

    queries = list(extra_queries or [])
    base = [q for q in PEXELS_QUERIES if q not in queries]
    random.shuffle(base)
    queries.extend(base)

    headers = {"Authorization": api_key}
    paths = []
    seen_ids = set()
    idx = 0

    for query in queries:
        if len(paths) >= target:
            break
        try:
            resp = requests.get(
                "https://api.pexels.com/videos/search",
                headers=headers,
                params={"query": query, "per_page": 3, "orientation": "landscape"},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            print(f"[WARN] Pexels '{query}': {exc}")
            continue

        for video in resp.json().get("videos", []):
            vid_id = video.get("id")
            if vid_id in seen_ids:
                continue
            seen_ids.add(vid_id)
            hd = [f for f in video.get("video_files", []) if (f.get("height") or 0) >= 720]
            if not hd:
                continue
            best = min(hd, key=lambda f: abs((f.get("height") or 0) - 720))
            idx += 1
            clip_path = CLIPS_DIR / f"clip_{idx:03d}.mp4"
            try:
                _download_file(best["link"], clip_path)
                paths.append(clip_path)
            except Exception:
                pass
            if len(paths) >= target:
                break

    print(f"[CLIPS] Downloaded {len(paths)} clips")
    return paths


def download_music() -> Optional[Path]:
    for url in random.sample(MUSIC_URLS, len(MUSIC_URLS)):
        try:
            _download_file(url, MUSIC_PATH)
            return MUSIC_PATH
        except Exception:
            continue
    return None


# ── FFmpeg Assembly ──────────────────────────────────────────────────
def _prepare_clip(src: Path, dst: Path, duration: int = 5):
    vf = (
        f"scale={TARGET_W}:{TARGET_H}:force_original_aspect_ratio=increase,"
        f"crop={TARGET_W}:{TARGET_H},fps={FPS}"
    )
    _run_ffmpeg([
        "ffmpeg", "-y", "-i", str(src), "-t", str(duration),
        "-vf", vf, "-an", "-c:v", "libx264",
        "-preset", FFMPEG_PRESET, "-crf", FFMPEG_CRF, str(dst),
    ])


def _fmt_ass_time(seconds: float) -> str:
    total_cs = max(0, int(round(seconds * 100)))
    cs = total_cs % 100
    total_s = total_cs // 100
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def _safe_text(raw: str) -> str:
    text = raw.replace("\\", " ").replace("\n", " ")
    text = text.replace(":", " ").replace(";", " ")
    text = text.replace("'", "").replace('"', "")
    text = re.sub(r"\s+", " ", text).strip()
    return text or " "


def _group_words(word_events: list[dict], max_per_line: int = 6) -> list[dict]:
    if not word_events:
        return []
    lines = []
    buf_words, buf_start, buf_end, buf_kara = [], 0.0, 0.0, []
    for ev in word_events:
        start, dur = ev["offset"], ev["duration"]
        end = start + dur
        if buf_words and (len(buf_words) >= max_per_line or (start - buf_end) > 0.6):
            lines.append({"start": buf_start, "end": buf_end, "text": " ".join(buf_words), "words": list(buf_kara)})
            buf_words, buf_kara = [], []
        if not buf_words:
            buf_start = start
        buf_words.append(ev["text"])
        buf_kara.append({"text": ev["text"], "offset": start, "duration": dur})
        buf_end = end
    if buf_words:
        lines.append({"start": buf_start, "end": buf_end, "text": " ".join(buf_words), "words": list(buf_kara)})
    return lines


def _write_ass(word_events: list[dict], ass_path: Path) -> Path:
    font_size = 28
    margin_v = 40
    primary = "&H0000D4FF"     # Yellow-orange (spoken)
    secondary = "&H00FFFFFF"   # White (upcoming)
    outline = "&H00000000"
    shadow = "&H80000000"

    header = (
        "[Script Info]\nScriptType: v4.00+\nWrapStyle: 0\n"
        f"PlayResX: {TARGET_W}\nPlayResY: {TARGET_H}\n"
        "ScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, "
        "Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Kara,DejaVu Sans,{font_size},{primary},{secondary},{outline},{shadow},"
        f"1,0,0,0,100,100,1,0,1,2,1,2,30,30,{margin_v},1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    )

    lines = _group_words(word_events)
    events = []
    for line in lines:
        start = line["start"]
        end = line["end"] + 0.15
        parts = []
        for w in line["words"]:
            dur_cs = max(5, int(w["duration"] * 100))
            safe = _safe_text(w["text"]).upper()
            parts.append(f"{{\\kf{dur_cs}}}{safe}")
        kara_text = " ".join(parts)
        events.append(f"Dialogue: 0,{_fmt_ass_time(start)},{_fmt_ass_time(end)},Kara,,0,0,0,,{kara_text}")

    ass_path.write_text(header + "\n".join(events) + "\n", encoding="utf-8")
    print(f"[SUBS] {len(events)} lines, {len(word_events)} words -> {ass_path}")
    return ass_path


def assemble_video(
    clips: list[Path],
    voiceover: Path,
    word_events: list[dict],
    music: Optional[Path],
) -> Path:
    temp = BUILD_DIR / "temp"
    temp.mkdir(exist_ok=True)

    # Prepare clips
    prepared = []
    for i, clip in enumerate(clips):
        dst = temp / f"prep_{i:03d}.mp4"
        _prepare_clip(clip, dst, duration=5)
        prepared.append(dst)

    # Concatenate
    concat_file = temp / "concat.txt"
    concat_file.write_text(
        "\n".join(f"file '{p.resolve().as_posix()}'" for p in prepared),
        encoding="utf-8",
    )
    silent = temp / "silent.mp4"
    _run_ffmpeg(["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                 "-i", str(concat_file), "-c", "copy", str(silent)])

    voice_dur = _probe_duration(voiceover)
    clip_dur = _probe_duration(silent)
    final_dur = voice_dur + 1.5

    # Loop video if shorter than voice
    if clip_dur < voice_dur:
        looped = temp / "looped.mp4"
        _run_ffmpeg([
            "ffmpeg", "-y", "-stream_loop", "-1",
            "-i", str(silent), "-t", f"{final_dur:.2f}",
            "-c", "copy", str(looped),
        ])
        silent = looped

    # Write ASS subtitles
    ass_path = _write_ass(word_events, temp / "captions.ass")

    # Pass 1: burn subtitles
    graded = temp / "graded.mp4"
    ass_posix = ass_path.resolve().as_posix()
    ass_escaped = (
        ass_posix.replace("\\", "\\\\").replace(":", "\\:")
        .replace("'", "\\'").replace("[", "\\[").replace("]", "\\]")
    )
    _run_ffmpeg([
        "ffmpeg", "-y", "-i", str(silent),
        "-vf", f"subtitles={ass_escaped}",
        "-t", f"{final_dur:.2f}",
        "-c:v", "libx264", "-preset", FFMPEG_PRESET, "-crf", FFMPEG_CRF,
        "-an", str(graded),
    ])

    # Pass 2: mix audio
    voice_pad = f"apad=whole_dur={final_dur:.2f}"
    cmd = ["ffmpeg", "-y", "-i", str(graded), "-i", str(voiceover)]

    if music and music.exists():
        cmd.extend(["-stream_loop", "-1", "-i", str(music)])
        cmd.extend([
            "-filter_complex",
            (
                f"[1:a]acompressor=threshold=-18dB:ratio=2.5:attack=5:release=120,{voice_pad}[va];"
                "[va]asplit=2[va1][va2];"
                "[2:a]highpass=f=80,lowpass=f=14000,volume=0.14[ma];"
                "[ma][va1]sidechaincompress=threshold=0.03:ratio=10:attack=15:release=250[ducked];"
                "[va2][ducked]amix=inputs=2:duration=first:normalize=0[a]"
            ),
            "-map", "0:v", "-map", "[a]",
        ])
    else:
        cmd.extend([
            "-filter_complex", f"[1:a]{voice_pad}[a]",
            "-map", "0:v", "-map", "[a]",
        ])

    cmd.extend([
        "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
        "-t", f"{final_dur:.2f}", "-movflags", "+faststart",
        str(OUTPUT_PATH),
    ])
    _run_ffmpeg(cmd)
    print(f"[VIDEO] voice={voice_dur:.1f}s clips={clip_dur:.1f}s final={final_dur:.1f}s -> {OUTPUT_PATH}")
    return OUTPUT_PATH


# ── YouTube Upload ───────────────────────────────────────────────────
TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = "https://www.googleapis.com/upload/youtube/v3/videos"


def _get_access_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "client_id": os.environ["YOUTUBE_CLIENT_ID"],
        "client_secret": os.environ["YOUTUBE_CLIENT_SECRET"],
        "refresh_token": os.environ["YOUTUBE_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def upload_video(meta: dict) -> str:
    creds = [os.getenv("YOUTUBE_CLIENT_ID"), os.getenv("YOUTUBE_CLIENT_SECRET"),
             os.getenv("YOUTUBE_REFRESH_TOKEN")]
    if not all(creds):
        print("[SKIP] Upload: missing credentials")
        return ""
    if not OUTPUT_PATH.is_file():
        print(f"[ERROR] Video not found: {OUTPUT_PATH}")
        return ""

    privacy = os.getenv("YOUTUBE_PRIVACY", "public")
    if privacy not in ("public", "unlisted", "private"):
        privacy = "public"

    access_token = _get_access_token()
    body = {
        "snippet": {
            "title": meta.get("title", "Smart Money Tips")[:100],
            "description": meta.get("description", ""),
            "tags": meta.get("tags", _CORE_TAGS),
            "categoryId": "27",  # Education
            "defaultLanguage": "en",
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
            "embeddable": True,
        },
    }

    video_data = OUTPUT_PATH.read_bytes()
    init_resp = requests.post(UPLOAD_URL, params={
        "uploadType": "resumable", "part": "snippet,status",
    }, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Length": str(len(video_data)),
        "X-Upload-Content-Type": "video/mp4",
    }, json=body, timeout=30)
    init_resp.raise_for_status()
    upload_url = init_resp.headers["Location"]

    print(f"[UPLOAD] {len(video_data) / 1024 / 1024:.1f} MB...")
    for attempt in range(1, 4):
        try:
            resp = requests.put(upload_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "video/mp4",
                "Content-Length": str(len(video_data)),
            }, data=video_data, timeout=600)
            resp.raise_for_status()
            video_id = resp.json().get("id", "")
            print(f"[UPLOAD] Done! https://youtube.com/watch?v={video_id}")
            try:
                from analytics import log_upload
                log_upload(video_id, meta.get("title", ""), meta.get("topic", ""), meta.get("tags", []))
            except Exception as exc:
                print(f"[WARN] Analytics: {exc}")
            return video_id
        except Exception as exc:
            print(f"[WARN] Upload attempt {attempt}: {exc}")
            if attempt < 3:
                time.sleep(attempt * 15)
    return ""


# ── Main Pipeline ────────────────────────────────────────────────────
def main():
    _clean_build_dir()

    # 1. Fetch articles from RSS
    print("[1/7] Fetching RSS articles...")
    articles = _fetch_rss_articles()
    used = _load_used_articles()

    article = None
    article_title = ""
    article_text = ""
    script_data = None

    if articles:
        article = _pick_article(articles, used)

    if article:
        print(f"  Article: {article['url']}")
        # 2. Scrape article
        print("[2/7] Scraping article...")
        try:
            article_title, article_text = _scrape_article(article["url"])
            print(f"  Title: {article_title}")
            print(f"  Text: {len(article_text.split())} words")
            if len(article_text.split()) < 100:
                print("[WARN] Article too short, trying another...")
                used.add(article["url"])
                article = _pick_article(articles, used)
                if article:
                    article_title, article_text = _scrape_article(article["url"])
        except Exception as exc:
            print(f"[WARN] Scrape failed: {exc}")
            article_text = ""

    if article_text and len(article_text.split()) >= 100:
        # 3. Two-step LLM
        print("[3/7] Extracting facts (Step 1)...")
        facts = step1_extract_facts(article_title, article_text)
        if facts:
            print("[4/7] Generating script (Step 2)...")
            for attempt in range(2):
                script_data = step2_generate_script(facts, article_title)
                if script_data:
                    break
                print(f"[RETRY] Script generation attempt {attempt + 2}...")

    # Fallback if RSS/scraping/LLM failed
    if not script_data:
        print("[FALLBACK] RSS pipeline failed, using fallback topic...")
        random.shuffle(FALLBACK_TOPICS)
        used_topics = _load_used_articles()
        for topic in FALLBACK_TOPICS:
            if topic not in used_topics:
                script_data = _generate_fallback_script(topic)
                if script_data:
                    article_title = topic
                    break
        if not script_data:
            script_data = _generate_fallback_script(FALLBACK_TOPICS[0])
            article_title = FALLBACK_TOPICS[0]
        if not script_data:
            print("[ERROR] All script generation attempts failed")
            sys.exit(1)

    script_text = script_data["script"]
    meta = {
        "title": script_data.get("title", article_title)[:100],
        "description": script_data.get("description", "") + _DESCRIPTION_FOOTER,
        "tags": list(dict.fromkeys(script_data.get("tags", []) + _CORE_TAGS))[:20],
        "topic": article_title,
    }

    # Save metadata
    METADATA_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Title: {meta['title']}")
    print(f"  Script: {len(script_text.split())} words")

    # 5. TTS
    print("[5/7] Generating voiceover (edge-tts)...")
    audio_path, word_events = generate_tts(script_text)
    voice_dur = _probe_duration(audio_path)
    print(f"  Duration: {voice_dur:.1f}s ({voice_dur/60:.1f} min)")

    # 6. Download clips
    print("[6/7] Downloading video clips...")
    pexels_queries = script_data.get("pexels_queries", [])
    clips = download_clips(extra_queries=pexels_queries, target=40)
    if not clips:
        print("[ERROR] No clips downloaded")
        sys.exit(1)

    music = download_music()

    # 7. Assemble video
    print("[7/7] Assembling video with ffmpeg...")
    assemble_video(clips, audio_path, word_events, music)

    # Upload
    print("[UPLOAD] Uploading to YouTube...")
    video_id = upload_video(meta)

    # Track used article
    if article:
        used.add(article["url"])
    elif article_title:
        used.add(article_title)
    _save_used_articles(used)
    print(f"[DONE] Tracked. Total used: {len(used)}")

    # Cleanup temp
    temp = BUILD_DIR / "temp"
    if temp.is_dir():
        shutil.rmtree(temp)


if __name__ == "__main__":
    main()
