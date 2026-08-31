# Deploying the SPX Options Flow Terminal to Streamlit Community Cloud

## What changed in the code

| # | Request | Where |
|---|---------|-------|
| 1 | Default timeframe is **5m** until the user picks another | `dashboard.py` — `st.selectbox(..., index=1)` |
| 2 | All timestamps render in **US Eastern (ET)**, auto DST | new `tz_utils.py`; wired into `dashboard.py`, `flow_engine.py`, `feed_provider.py`, `history_logger.py`, `main.py` |
| 3 | **API keys are never in the repo**; resolved from env / Streamlit encrypted secrets | `config.py` — `get_secret()`, plus `.gitignore`, `.streamlit/secrets.toml.example` |
| 4 | Ready to deploy on Streamlit Cloud | `requirements.txt` (+`tzdata`), `.streamlit/config.toml`, this guide |

---

## 1. Rotate the exposed key first (important)

Your `DATABENTO_API_KEY` currently sits in plaintext in `.env` on disk. Treat it as
compromised: log in to Databento, **revoke the old key, and issue a new one**. Use the
new key everywhere below. Do the same for the Twelve Data key.

---

## 2. How secrets work now

`config.py` resolves each key in this order and **never prints it**:

1. OS environment variable (`DATABENTO_API_KEY`, `TWELVE_DATA_API_KEY`)
2. `.env` file (local dev only — git-ignored)
3. `st.secrets` — the Streamlit Cloud **Secrets** manager (encrypted at rest,
   invisible in the repo and to app viewers)

`.gitignore` now blocks `.env` and `.streamlit/secrets.toml` from ever being committed.

### Local development
```powershell
# option A: keep using .env  (already git-ignored)
# option B: copy the template and fill it in
Copy-Item .streamlit\secrets.toml.example .streamlit\secrets.toml
```

### Run locally
```powershell
pip install -r requirements.txt
python -m streamlit run dashboard.py
```

---

## 3. Push to GitHub (no secrets included)

From this folder (`Ratio_dashboard`):

```powershell
git init
git add .
git status                     # confirm .env and .streamlit/secrets.toml are NOT listed
git commit -m "SPX flow terminal: 5m default, ET timezone, secret-safe config"
git branch -M main
git remote add origin https://github.com/<your-user>/<your-repo>.git
git push -u origin main
```

If `git status` shows `.env`, stop and fix `.gitignore` before committing.

---

## 4. Create the Streamlit Cloud app

1. Go to <https://share.streamlit.io> and sign in with GitHub.
2. **New app** -> pick your repo / branch `main`.
3. **Main file path:** `dashboard.py`
   (if the repo root is the parent folder, use `Ratio_dashboard/dashboard.py`).
4. **Advanced settings -> Python version:** `3.12` (the code also runs on 3.11).
5. **Advanced settings -> Secrets:** paste
   ```toml
   DATABENTO_API_KEY = "your-new-databento-key"
   TWELVE_DATA_API_KEY = "your-new-twelve-data-key"
   ```
6. **Deploy.**

You can edit Secrets any time from **Manage app -> Settings -> Secrets**; the app
restarts automatically and the values stay encrypted.

---

## 5. Notes / gotchas

- **Databento live connection limit.** Streamlit Cloud may run more than one
  instance or restart the app; each live run opens 2 Databento live connections
  (SPY + SPXW). If you hit `connection limit`, close local runs / other tabs and
  use the **Reconnect** button. For a always-on public dashboard consider a
  dedicated host (Fly.io, Render, a VM) instead of Community Cloud.
- **Ephemeral filesystem.** `logs/` is written at runtime but wiped on every
  redeploy/restart on Community Cloud. That is fine for this app; it is not a
  durable store.
- **Timezone.** `tzdata` is in `requirements.txt` so `zoneinfo` works on any
  container. Times display as `HH:MM:SS ET` and follow EST/EDT automatically.
- **`config.toml`** is committed (no secrets in it) and sets the dark theme +
  headless server.
