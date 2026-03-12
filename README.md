# YouTube Smart Money Tips — Shorts Automation

Automated YouTube Shorts generator for personal finance tips, money hacks, and investing advice. Runs on GitHub Actions every 3 hours.

## What it does

1. **Generates script** via Groq LLM (llama-3.3-70b-versatile) — real finance tips with specific dollar amounts, percentages, account types
2. **Downloads stock video** clips from Pexels + Pixabay (money/finance visuals only)
3. **Generates voice-over** using edge-tts with per-phrase sync
4. **Assembles 9:16 video** — Ken Burns zoom, bold subtitles, background music
5. **Uploads to YouTube** via OAuth2 Data API v3
6. **Quality gate** — validates script for substance (rejects filler content)

## Content Quality

- LLM prompt demands specific dollar amounts, percentages, account types, and step-by-step actions
- Quality validation: min 8 parts, avg 8+ words, filler phrase detection, 40%+ concrete financial content
- Pronunciation fixes for 30+ finance acronyms (401k, ETF, APY, FICO, REIT, etc.)
- 4 battle-tested fallback scripts if LLM output is weak

## Setup

### 1. Add Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Required | Description |
|--------|----------|-------------|
| `GROQ_API_KEY` | Yes | Free API key from [console.groq.com](https://console.groq.com) |
| `PEXELS_API_KEY` | Yes | Free API key from [pexels.com/api](https://www.pexels.com/api/) |
| `PIXABAY_API_KEY` | No | Free API key from [pixabay.com/api](https://pixabay.com/api/docs/) |
| `YOUTUBE_CLIENT_ID` | For upload | Google Cloud OAuth2 client ID |
| `YOUTUBE_CLIENT_SECRET` | For upload | Google Cloud OAuth2 client secret |
| `YOUTUBE_REFRESH_TOKEN` | For upload | Refresh token from OAuth2 flow |
| `YOUTUBE_PRIVACY` | No | `public` (default), `unlisted`, or `private` |

### 2. YouTube OAuth2 Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project, enable **YouTube Data API v3**
3. Create **OAuth 2.0 Client ID** (Desktop app type)
4. Add your Google account as a test user under **OAuth consent screen**
5. Use the [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/) to get a refresh token:
   - Settings gear → check "Use your own OAuth credentials" → paste Client ID & Secret
   - Step 1: authorize `https://www.googleapis.com/auth/youtube.upload`
   - Step 2: exchange for tokens → copy the **refresh_token**
6. Add all three values as GitHub Secrets

### 3. Run

- **Automatic**: runs every 3 hours via cron
- **Manual**: Actions tab → "Generate Money Short" → "Run workflow"

## Project Structure

```
generate_money_short.py       — Main script: LLM → clips → TTS → video
upload_youtube.py             — YouTube OAuth2 resumable upload
requirements.txt              — Python dependencies
.github/workflows/            — GitHub Actions workflow
```

## Topics Covered

The generator randomly combines:
- **27 finance topics**: budgeting, index funds, credit scores, 401k, side hustles, FIRE, etc.
- **12 content angles**: shocking stats, common mistakes, hidden tricks, myth vs reality, etc.
- **6 audience levels**: college student, young professional, parent, aspiring investor, etc.

This gives **1,944 unique combinations** before any repeats.
