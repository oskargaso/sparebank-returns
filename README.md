# Sparebank 1 SMN – Total Return Chart

A static website showing total return (with dividend reinvestment) for Sparebank 1 SMN (MING.OL).

Hosted on GitHub Pages. Data fetched from Yahoo Finance via a weekly GitHub Actions job.

---

## One-time setup

### 1. Create a GitHub repository

Go to [github.com](https://github.com) → **New repository**.
- Name it something like `sparebank-returns`
- Set it to **Public** (required for free GitHub Pages)
- Do NOT initialize with a README

Then push this project to it:

```bash
cd sparebank-returns
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/sparebank-returns.git
git push -u origin main
```

### 2. Enable GitHub Pages

In your repo on GitHub:
- Go to **Settings → Pages**
- Under **Source**, select **Deploy from a branch**
- Branch: `main`, folder: `/ (root)`
- Click **Save**

Your site will be live at: `https://YOUR_USERNAME.github.io/sparebank-returns/`

### 3. Enable write permissions for GitHub Actions

- Go to **Settings → Actions → General**
- Scroll to **Workflow permissions**
- Select **Read and write permissions**
- Click **Save**

### 4. Run the data fetch for the first time

- Go to the **Actions** tab in your repo
- Click **Update Stock Data** in the left sidebar
- Click **Run workflow → Run workflow**
- Wait ~30 seconds for it to complete

After this, `data/data.json` will be committed to your repo and the website will show the chart.

---

## Running locally

```bash
pip install -r requirements.txt
python fetch_data.py
```

Then open `index.html` in a browser. Note: some browsers block `fetch()` on local `file://` URLs.
A simple workaround is to use VS Code's Live Server extension, or Python's built-in server:

```bash
python -m http.server 8080
# Open http://localhost:8080 in your browser
```

---

## How it works

| File | Purpose |
|------|---------|
| `fetch_data.py` | Downloads MING.OL price + dividend history from Yahoo Finance, calculates DRIP total return, saves `data/data.json` |
| `index.html` | Loads the JSON, renders an interactive Chart.js chart with stats |
| `.github/workflows/update-data.yml` | Runs every Monday to refresh the data automatically |

**Dividend reinvestment logic:** On each ex-dividend date, the dividend per share is divided by that day's closing price to calculate how many additional shares would have been purchased. This compounds over time.
