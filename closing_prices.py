"""
closing_prices.py
Inscrit chaque jour les prix de clôture Yahoo Finance dans la feuille 'Freezed prices'.

Structure de la feuille :
  - Tickers  : ligne 3,  colonnes C→  (C3, D3, E3 …)
  - Dates    : colonne B, lignes 6→  (B6, B7, B8 …)
  - Prix     : cellule à l'intersection (ligne de la date, colonne du ticker)

Seuls les tickers présents dans Equities!E7:E sont traités.

Secrets GitHub requis :
  GOOGLE_CREDENTIALS  — JSON du compte de service (une seule ligne)
  SHEET_IDS           — IDs des feuilles séparés par des virgules
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(levelname)s │ %(message)s")
log = logging.getLogger(__name__)

# ── Constantes ───────────────────────────────────────────────────────────────

PRICES_SHEET    = "Freezed prices"
EQUITIES_SHEET  = "Equities"
TICKER_ROW      = 3          # ligne où sont les tickers (C4, D4, E4 …)
TICKER_COL_START = 3         # colonne C = index 3 (1-based)
DATE_COL        = 2          # colonne B = index 2 (1-based)
DATE_ROW_START  = 6          # première date en B6
EQUITIES_RANGE  = "E7:E"     # tickers actifs dans la feuille Equities

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ── Authentification ─────────────────────────────────────────────────────────

def get_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── Lecture des tickers actifs (Equities!E7:E) ───────────────────────────────

def read_active_tickers(spreadsheet: gspread.Spreadsheet) -> set[str]:
    ws = spreadsheet.worksheet(EQUITIES_SHEET)
    values = ws.get(EQUITIES_RANGE)
    tickers = {row[0].strip() for row in values if row and row[0].strip()}
    log.info("Tickers actifs dans Equities : %d", len(tickers))
    return tickers

# ── Lecture de la feuille Freezed prices ─────────────────────────────────────

def read_prices_sheet(ws: gspread.Worksheet) -> tuple[dict, dict]:
    """
    Retourne :
      ticker_to_col : { 'AAPL' : 5, 'MC.PA' : 7, … }   (index colonne 1-based)
      date_to_row   : { '17/05/2021' : 6, … }            (index ligne  1-based)
    """
    all_values = ws.get_all_values()

    # Tickers (ligne 4, colonnes C→)
    ticker_row = all_values[TICKER_ROW - 1] if len(all_values) >= TICKER_ROW else []
    ticker_to_col = {}
    for col_idx, cell in enumerate(ticker_row, start=1):
        if col_idx >= TICKER_COL_START and cell.strip():
            ticker_to_col[cell.strip()] = col_idx

    # Dates (colonne B, lignes 6→)
    date_to_row = {}
    for row_idx, row in enumerate(all_values, start=1):
        if row_idx >= DATE_ROW_START and len(row) >= DATE_COL:
            cell = row[DATE_COL - 1].strip()
            if cell:
                date_to_row[cell] = row_idx

    log.info("Tickers trouvés dans la feuille : %d", len(ticker_to_col))
    log.info("Dates trouvées dans la feuille  : %d", len(date_to_row))
    return ticker_to_col, date_to_row

# ── Formatage de la date du jour ─────────────────────────────────────────────

def today_label() -> str:
    """Retourne la date d'aujourd'hui dans le même format que la feuille : DD/MM/YYYY."""
    return datetime.now(timezone.utc).strftime("%d/%m/%Y")

# ── Fetch du prix de clôture Yahoo Finance ───────────────────────────────────

def fetch_close(ticker: str) -> float | None:
    """Retourne le dernier prix de clôture disponible pour ce ticker."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=5)   # 5 jours pour couvrir les week-ends et jours fériés
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        return None
    return float(df["Close"].iloc[-1])

# ── Traitement d'un fichier Google Sheets ────────────────────────────────────

def process_spreadsheet(client: gspread.Client, sheet_id: str) -> None:
    log.info("═══ Traitement sheet %s ═══", sheet_id)
    spreadsheet   = client.open_by_key(sheet_id)
    active_tickers = read_active_tickers(spreadsheet)
    ws             = spreadsheet.worksheet(PRICES_SHEET)
    ticker_to_col, date_to_row = read_prices_sheet(ws)

    today = today_label()
    if today not in date_to_row:
        log.info("Date '%s' absente de la feuille — rien à faire.", today)
        return

    target_row = date_to_row[today]
    log.info("Ligne cible pour %s : ligne %d", today, target_row)

    # Préparer toutes les mises à jour en une seule requête batch
    updates = []
    for ticker in active_tickers:
        if ticker not in ticker_to_col:
            log.info("  Ticker %s absent de la feuille — ignoré.", ticker)
            continue

        price = fetch_close(ticker)
        if price is None:
            log.info("  %s : aucun prix disponible.", ticker)
            continue

        col = ticker_to_col[ticker]
        cell = gspread.utils.rowcol_to_a1(target_row, col)
        updates.append({"range": cell, "values": [[price]]})
        log.info("  %s → %s = %.4f", ticker, cell, price)

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
        process_spreadsheet(client, sheet_id)

if __name__ == "__main__":
    main()
