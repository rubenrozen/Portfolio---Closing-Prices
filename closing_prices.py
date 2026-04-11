"""
closing_prices.py
Inscrit chaque jour les prix de clôture Yahoo Finance dans 'Freezed prices'.

Structure de la feuille :
  - Tickers Yahoo  : ligne 3, colonnes C→  (C3, D3, E3 …)
  - Tickers Google : ligne 4, colonnes C→  (C4, D4, E4 …)
  - Dates          : colonne B, lignes 6→  (B6 = 17/05/2021, format DD/MM/YYYY)
  - Prix           : intersection (ligne de la date du jour, colonne du ticker)

Filtre : seuls les tickers dont le ticker Google (ligne 4) est présent
dans Equities!E7:E sont traités. La conversion Google→Yahoo est supprimée :
on compare directement les valeurs brutes.

Secrets GitHub requis :
  GOOGLE_CREDENTIALS  — contenu du fichier JSON du compte de service
  SHEET_IDS           — IDs séparés par des virgules
"""

import os
import json
import logging
from datetime import datetime, timezone

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

def read_prices_sheet(ws: gspread.Worksheet) -> tuple[dict, dict, dict]:
    """
    Retourne :
      ticker_to_col  : { 'FQT.VI': 5, 'BRK-B': 7, … }   Yahoo → colonne
      date_to_row    : { '10/04/2026': 1795, … }           date  → ligne
      google_to_yahoo: { 'ETR:FQT': 'FQT.VI', … }         Google → Yahoo
    """
    all_values = ws.get_all_values()

    yahoo_row  = all_values[TICKER_ROW - 1]        if len(all_values) >= TICKER_ROW        else []
    google_row = all_values[GOOGLE_TICKER_ROW - 1] if len(all_values) >= GOOGLE_TICKER_ROW else []

    ticker_to_col   = {}
    google_to_yahoo = {}

    for col_idx, yahoo in enumerate(yahoo_row, start=1):
        if col_idx >= TICKER_COL_START and yahoo.strip():
            ticker_to_col[yahoo.strip()] = col_idx
            # ticker Google correspondant (même colonne, ligne 4)
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

    log.info("Tickers dans Freezed prices : %d", len(ticker_to_col))
    log.info("Dates dans Freezed prices   : %d", len(date_to_row))
    return ticker_to_col, date_to_row, google_to_yahoo

# ── Date du jour ─────────────────────────────────────────────────────────────

def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%d/%m/%Y")

# ── Fetch du prix de clôture ─────────────────────────────────────────────────

def fetch_close(ticker: str) -> float | None:
    """
    Utilise yf.Ticker().history() — période de 5 jours pour couvrir
    week-ends et jours fériés. Retourne le dernier prix connu.
    """
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if hist.empty:
            log.info("  %s : aucune donnée retournée par Yahoo", ticker)
            return None
        price = hist["Close"].iloc[-1]
        return round(float(price), 4)
    except Exception as exc:
        log.warning("  %s : erreur fetch → %s", ticker, exc)
        return None

# ── Traitement d'un fichier Google Sheets ────────────────────────────────────

def process_spreadsheet(client: gspread.Client, sheet_id: str) -> None:
    log.info("═══ Sheet %s ═══", sheet_id)
    spreadsheet = client.open_by_key(sheet_id)
    active      = read_active_tickers(spreadsheet)           # tickers Google bruts d'Equities
    ws          = spreadsheet.worksheet(PRICES_SHEET)
    ticker_to_col, date_to_row, google_to_yahoo = read_prices_sheet(ws)

    today = today_str()
    if today not in date_to_row:
        log.info("Date '%s' absente de la feuille — rien à faire.", today)
        return

    target_row = date_to_row[today]
    log.info("Ligne cible : %d  (date : %s)", target_row, today)

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

    updates = []
    for ticker in sorted(to_process):
        price = fetch_close(ticker)
        if price is None:
            continue
        col  = ticker_to_col[ticker]
        cell = gspread.utils.rowcol_to_a1(target_row, col)
        updates.append({"range": cell, "values": [[price]]})
        log.info("  ✓ %-15s → %-6s = %s", ticker, cell, price)

    if updates:
        ws.batch_update(updates)
        log.info("✓ %d prix écrits pour le %s", len(updates), today)
    else:
        log.info("Aucun prix à écrire.")

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
