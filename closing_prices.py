"""
closing_prices.py
Inscrit chaque jour les prix de clôture Yahoo Finance dans 'Freezed prices'.

Structure de la feuille :
  - Tickers Yahoo  : ligne 3, colonnes C→  (C3, D3, E3 …)
  - Tickers Google : ligne 4, colonnes C→  (C4, D4, E4 …)
  - Dates          : colonne B, lignes 6→  (B6 = 17/05/2021, format DD/MM/YYYY)
  - Prix           : intersection (ligne de la DATE DU COURS, colonne du ticker)

Comportement :
  1. Le prix est écrit à la date réelle du cours (timezone du marché), pas
     à la date d'exécution du workflow.
  2. Forward-fill : si aujourd'hui n'est pas un jour de cotation pour le
     ticker (week-end, férié local…), le dernier cours connu est aussi
     propagé sur les lignes des jours non-ouvrés jusqu'à aujourd'hui.
     Garde-fou : pas de fill si l'écart dépasse MAX_FILL_DAYS jours.
  3. Fill-only : une cellule déjà remplie n'est JAMAIS écrasée. Ça permet
     de relancer le script sans risque, et de chaîner plusieurs passes
     pour boucher d'éventuels trous liés à des glitchs Yahoo.

Robustesse fetch (cascade de fallbacks pour gérer rate limit + NaN Yahoo) :
  a) Clôture daily du jour → si valide, on prend.
  b) Si réponse vide ou Close NaN → retry après 2-3s (souvent rate limit
     qui se relâche).
  c) Si toujours NaN → fallback intraday (dernier tick 5min du jour).
  d) Filet de sécurité ultime → dernière clôture valide dans l'historique.

Entre chaque ticker : sleep(0.3) pour rester à ~60 req/min et éviter le
rate limiting Yahoo (qui se manifeste par des NaN ou des réponses vides
en heures de pointe, typiquement à 23h UTC).

Filtre : seuls les tickers dont le ticker Google (ligne 4) est présent
dans Equities!E7:E sont traités.

Secrets GitHub requis :
  GOOGLE_CREDENTIALS  — contenu du fichier JSON du compte de service
  SHEET_IDS           — IDs séparés par des virgules
"""

import os
import json
import math
import time
import logging
from datetime import datetime, timezone, timedelta

import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(levelname)s │ %(message)s")
log = logging.getLogger(__name__)

# ── Constantes ───────────────────────────────────────────────────────────────

PRICES_SHEET      = "Freezed prices"
EQUITIES_SHEET    = "Equities"
TICKER_ROW        = 3       # ligne des tickers Yahoo (C3, D3, E3 …)
GOOGLE_TICKER_ROW = 4       # ligne des tickers Google (C4, D4, E4 …)
TICKER_COL_START  = 3       # colonne C = index 3 (1-based)
DATE_COL          = 2       # colonne B
DATE_ROW_START    = 6       # première date en B6
EQUITIES_RANGE    = "E7:E"  # tickers actifs au format Google Finance
DATE_FMT          = "%d/%m/%Y"
MAX_FILL_DAYS     = 7       # garde-fou : pas de forward-fill au-delà

# Rate limiting & retry
INTER_TICKER_SLEEP = 0.3    # délai entre tickers (≈ 60 req/min max)
RETRY_SLEEP_EMPTY  = 3      # sleep avant retry si réponse Yahoo vide
RETRY_SLEEP_NAN    = 2      # sleep avant retry si Close NaN

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ── Authentification ─────────────────────────────────────────────────────────

def get_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── Lecture des tickers actifs dans Equities (format Google brut) ─────────────

def read_active_tickers(spreadsheet: gspread.Spreadsheet) -> set[str]:
    """Lit Equities!E7:E et retourne les valeurs brutes, sans conversion."""
    ws = spreadsheet.worksheet(EQUITIES_SHEET)
    values = ws.get(EQUITIES_RANGE)
    result = set()
    for row in values:
        if row and row[0].strip():
            result.add(row[0].strip())
    log.info("Tickers actifs (format Google brut) : %d", len(result))
    return result

# ── Lecture de la feuille Freezed prices ─────────────────────────────────────

def read_prices_sheet(ws: gspread.Worksheet) -> tuple[dict, dict, dict, set]:
    """
    Retourne :
      ticker_to_col  : { 'FQT.VI': 5, 'BRK-B': 7, … }    Yahoo → colonne
      date_to_row    : { '10/04/2026': 1795, … }         date  → ligne
      google_to_yahoo: { 'ETR:FQT': 'FQT.VI', … }        Google → Yahoo
      filled_cells   : { (1795, 5), (1795, 7), … }       cellules NON VIDES
    """
    all_values = ws.get_all_values()

    yahoo_row  = all_values[TICKER_ROW - 1]        if len(all_values) >= TICKER_ROW        else []
    google_row = all_values[GOOGLE_TICKER_ROW - 1] if len(all_values) >= GOOGLE_TICKER_ROW else []

    ticker_to_col   = {}
    google_to_yahoo = {}

    for col_idx, yahoo in enumerate(yahoo_row, start=1):
        if col_idx >= TICKER_COL_START and yahoo.strip():
            ticker_to_col[yahoo.strip()] = col_idx
            google = google_row[col_idx - 1].strip() if col_idx - 1 < len(google_row) else ""
            if google:
                google_to_yahoo[google] = yahoo.strip()

    date_to_row = {
        row[DATE_COL - 1].strip(): row_idx
        for row_idx, row in enumerate(all_values, start=1)
        if row_idx >= DATE_ROW_START
        and len(row) >= DATE_COL
        and row[DATE_COL - 1].strip()
    }

    # Cellules déjà remplies dans la zone des prix (ligne ≥ DATE_ROW_START,
    # colonne ≥ TICKER_COL_START). Utilisées pour le mode fill-only.
    filled_cells = set()
    for row_idx, row in enumerate(all_values, start=1):
        if row_idx < DATE_ROW_START:
            continue
        for col_idx, val in enumerate(row, start=1):
            if col_idx >= TICKER_COL_START and val.strip():
                filled_cells.add((row_idx, col_idx))

    log.info("Tickers dans Freezed prices : %d", len(ticker_to_col))
    log.info("Dates dans Freezed prices   : %d", len(date_to_row))
    log.info("Cellules déjà remplies      : %d", len(filled_cells))
    return ticker_to_col, date_to_row, google_to_yahoo, filled_cells

# ── Fetch du prix avec cascade de fallbacks ──────────────────────────────────

def fetch_close(ticker: str) -> tuple[str, float] | None:
    """
    Retourne (date 'DD/MM/YYYY', prix) selon une cascade de fallbacks :
      1) clôture daily du jour (si non-NaN)
      2) retry après sleep court (rattrape rate limit ponctuel)
      3) fallback intraday (dernier tick 5min du jour)
      4) filet de sécurité : dernière clôture valide dans l'historique

    La date renvoyée est celle fournie par Yahoo dans la timezone du marché,
    donc la date réelle du cours — pas la date d'exécution du script.
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period="5d")

        # Retry si réponse totalement vide (souvent rate limit)
        if hist.empty:
            log.info("  %s : réponse vide Yahoo → retry dans %ds", ticker, RETRY_SLEEP_EMPTY)
            time.sleep(RETRY_SLEEP_EMPTY)
            hist = yf_ticker.history(period="5d")
            if hist.empty:
                log.info("  %s : toujours vide après retry — ignoré", ticker)
                return None

        # ── Tentative 1 : clôture daily du jour, si valide ──────────────
        last_close = hist["Close"].iloc[-1]
        if not math.isnan(last_close):
            last_date = hist.index[-1].strftime(DATE_FMT)
            return last_date, round(float(last_close), 4)

        # ── Tentative 2 : retry sur NaN (rate limit qui se relâche) ──────
        last_daily_date = hist.index[-1].strftime(DATE_FMT)
        log.info("  %s : Close NaN pour %s → retry dans %ds", ticker, last_daily_date, RETRY_SLEEP_NAN)
        time.sleep(RETRY_SLEEP_NAN)
        hist_retry = yf_ticker.history(period="5d")
        if not hist_retry.empty:
            last_close_retry = hist_retry["Close"].iloc[-1]
            if not math.isnan(last_close_retry):
                last_date = hist_retry.index[-1].strftime(DATE_FMT)
                log.info("  %s : retry OK → clôture récupérée", ticker)
                return last_date, round(float(last_close_retry), 4)
            hist = hist_retry  # utilise la version la plus fraîche pour la suite

        # ── Tentative 3 : fallback intraday (dernier tick 5min) ──────────
        log.info("  %s : retry NaN → fallback intraday", ticker)
        try:
            intraday = yf_ticker.history(period="1d", interval="5m")
            intraday = intraday.dropna(subset=["Close"])
            if not intraday.empty:
                price = round(float(intraday["Close"].iloc[-1]), 4)
                log.info("  %s : fallback intraday OK → %s", ticker, price)
                return last_daily_date, price
        except Exception as e:
            log.warning("  %s : intraday KO → %s", ticker, e)

        # ── Tentative 4 : dernière clôture valide (même si ancienne) ─────
        valid_hist = hist.dropna(subset=["Close"])
        if not valid_hist.empty:
            last_date = valid_hist.index[-1].strftime(DATE_FMT)
            price = round(float(valid_hist["Close"].iloc[-1]), 4)
            log.info("  %s : fallback dernière clôture valide (%s) → %s", ticker, last_date, price)
            return last_date, price

        log.info("  %s : aucune donnée exploitable", ticker)
        return None

    except Exception as exc:
        log.warning("  %s : erreur fetch → %s", ticker, exc)
        return None

# ── Forward-fill : dates à remplir pour ce ticker ────────────────────────────

def dates_to_fill(price_date: str, today: str, date_to_row: dict) -> list[str]:
    """
    Retourne les dates (DD/MM/YYYY) présentes dans la feuille, de price_date
    (inclus) à today (inclus). Sécurise contre les écarts anormaux.
    """
    try:
        start = datetime.strptime(price_date, DATE_FMT).date()
        end   = datetime.strptime(today,      DATE_FMT).date()
    except ValueError as exc:
        log.warning("  Parsing date impossible : %s", exc)
        return [price_date] if price_date in date_to_row else []

    # Cours "futur" par rapport à today (timezone asiatique qui déborde) :
    # on écrit uniquement à la date du cours si elle est dans la feuille.
    if end < start:
        return [price_date] if price_date in date_to_row else []

    # Garde-fou : écart anormalement grand → pas de forward-fill.
    if (end - start).days > MAX_FILL_DAYS:
        log.warning(
            "Écart cours %s → today %s > %d jours — forward-fill désactivé",
            price_date, today, MAX_FILL_DAYS,
        )
        return [price_date] if price_date in date_to_row else []

    result = []
    current = start
    while current <= end:
        d_str = current.strftime(DATE_FMT)
        if d_str in date_to_row:
            result.append(d_str)
        current += timedelta(days=1)
    return result

# ── Date du jour (UTC) ───────────────────────────────────────────────────────

def today_str() -> str:
    return datetime.now(timezone.utc).strftime(DATE_FMT)

# ── Traitement d'un fichier Google Sheets ────────────────────────────────────

def process_spreadsheet(client: gspread.Client, sheet_id: str) -> None:
    log.info("═══ Sheet %s ═══", sheet_id)
    spreadsheet = client.open_by_key(sheet_id)
    active      = read_active_tickers(spreadsheet)
    ws          = spreadsheet.worksheet(PRICES_SHEET)
    ticker_to_col, date_to_row, google_to_yahoo, filled_cells = read_prices_sheet(ws)

    today = today_str()
    log.info("Date d'exécution : %s", today)

    # Tickers Yahoo à traiter = ceux dont le ticker Google est dans Equities
    to_process = {
        yahoo
        for google, yahoo in google_to_yahoo.items()
        if google in active
    }

    ignored = {
        yahoo
        for google, yahoo in google_to_yahoo.items()
        if google not in active
    }
    if ignored:
        log.info("Ignorés (Google ticker absent d'Equities) : %s", ", ".join(sorted(ignored)))

    updates      = []
    skipped_full = 0   # compteur de cellules skippées car déjà remplies

    for ticker in sorted(to_process):
        result = fetch_close(ticker)
        time.sleep(INTER_TICKER_SLEEP)   # rate limit safety
        if result is None:
            continue
        price_date, price = result

        # Garde-fou ultime : ne jamais pousser un NaN à Google Sheets
        # (le JSON n'accepte pas NaN et tout le batch se fait rejeter)
        if math.isnan(price):
            log.warning("  %s : prix NaN après tous les fallbacks — skip", ticker)
            continue

        fills = dates_to_fill(price_date, today, date_to_row)
        if not fills:
            log.info("  %s : date cours %s absente — rien à écrire", ticker, price_date)
            continue

        col              = ticker_to_col[ticker]
        written_dates    = []
        skipped_dates    = []

        for date_str in fills:
            target_row = date_to_row[date_str]
            cell_pos   = (target_row, col)
            if cell_pos in filled_cells:
                skipped_dates.append(date_str)
                skipped_full += 1
                continue
            cell = gspread.utils.rowcol_to_a1(target_row, col)
            updates.append({"range": cell, "values": [[price]]})
            written_dates.append(date_str)

        if written_dates:
            extra = ""
            if len(written_dates) > 1:
                extra = f"  (fill sur {len(written_dates)} jours : {', '.join(written_dates)})"
            if skipped_dates:
                extra += f"  [déjà rempli : {', '.join(skipped_dates)}]"
            log.info("  ✓ %-15s = %s  [cours du %s]%s", ticker, price, price_date, extra)
        elif skipped_dates:
            log.info("  = %-15s  déjà rempli sur %s", ticker, ", ".join(skipped_dates))

    if updates:
        ws.batch_update(updates)
        log.info("✓ %d cellules écrites (%d cellules déjà remplies ignorées)",
                 len(updates), skipped_full)
    else:
        log.info("Aucun prix à écrire (%d cellules déjà remplies ignorées).", skipped_full)

# ── Point d'entrée ────────────────────────────────────────────────────────────

def main() -> None:
    sheet_ids = [s.strip() for s in os.environ["SHEET_IDS"].split(",") if s.strip()]
    log.info("%d feuille(s) à traiter", len(sheet_ids))
    client = get_client()
    for sheet_id in sheet_ids:
        try:
            process_spreadsheet(client, sheet_id)
        except Exception as exc:
            log.error("Échec sur sheet %s : %s", sheet_id, exc, exc_info=True)

if __name__ == "__main__":
    main()
