"""
closing_prices.py
Inscrit chaque jour les prix de clôture Yahoo Finance dans 'Freezed prices'.

Structure de la feuille :
  - Tickers Yahoo  : ligne 3, colonnes C→  (C3, D3, E3 …)
  - Dates          : colonne B, lignes 6→  (B6 = 17/05/2021, format DD/MM/YYYY)
  - Prix           : intersection (ligne de la date du jour, colonne du ticker)

Filtre : seuls les tickers présents dans Equities!E7:E sont traités.
Ces tickers sont au format Google Finance et convertis en Yahoo avant comparaison.

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

PRICES_SHEET     = "Freezed prices"
EQUITIES_SHEET   = "Equities"
TICKER_ROW       = 3       # ligne des tickers Yahoo (C3, D3, E3 …)
TICKER_COL_START = 3       # colonne C = index 3 (1-based)
DATE_COL         = 2       # colonne B
DATE_ROW_START   = 6       # première date en B6
EQUITIES_RANGE   = "E7:E"  # tickers actifs au format Google Finance

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ── Table de conversion Google Finance → Yahoo Finance ───────────────────────

EXCHANGE_MAP = {
    "NASDAQ": "", "NYSE": "", "NYSEARCA": "", "NYSEAMERICAN": "",
    "BATS": "", "OTC": "",
    "EPA": ".PA", "AMS": ".AS", "EBR": ".BR", "ELI": ".LS",
    "ETR": ".DE", "FRA": ".F",
    "LON": ".L",
    "BIT": ".MI", "MIL": ".MI",
    "BME": ".MC",
    "VIE": ".VI",
    "STO": ".ST", "CPH": ".CO", "HEL": ".HE", "OSL": ".OL",
    "SWX": ".SW",
    "WSE": ".WA", "PRA": ".PR", "BUD": ".BD",
    "TSX": ".TO", "TSXV": ".V", "CVE": ".CN",
    "TYO": ".T", "OSA": ".OS",
    "SHA": ".SS", "SHE": ".SZ",
    "HKG": ".HK",
    "TPE": ".TW",
    "NSE": ".NS", "BSE": ".BO", "BOM": ".BO",
    "SGX": ".SI",
    "KRX": ".KS", "KOSDAQ": ".KQ",
    "ASX": ".AX", "NZX": ".NZ",
    "JSE": ".JO",
    "BVMF": ".SA",
    "TLV": ".TA",
    "IST": ".IS",
    "MCX": ".ME",
}

def to_yahoo(raw: str) -> str:
    """
    Convertit un ticker Google Finance en ticker Yahoo Finance.
    'EPA:MC' → 'MC.PA' | 'NASDAQ:AAPL' → 'AAPL' | 'TYO:285A' → '285A.T'
    Si déjà au format Yahoo (pas de ':'), retourné tel quel.
    Exchange inconnu → symbole brut tenté quand même.
    """
    raw = raw.strip()
    if ":" not in raw:
        return raw
    exchange, symbol = raw.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange.upper())
    if suffix is None:
        log.warning("Exchange inconnu '%s' — symbole '%s' utilisé tel quel", exchange, symbol)
        return symbol
    return symbol + suffix

# ── Authentification ─────────────────────────────────────────────────────────

def get_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── Lecture des tickers actifs dans Equities ─────────────────────────────────

def read_active_tickers(spreadsheet: gspread.Spreadsheet) -> set[str]:
    """Lit Equities!E7:E (format Google) et convertit en Yahoo."""
    ws = spreadsheet.worksheet(EQUITIES_SHEET)
    values = ws.get(EQUITIES_RANGE)
    result = set()
    for row in values:
        if row and row[0].strip():
            result.add(to_yahoo(row[0].strip()))
    log.info("Tickers actifs (format Yahoo) : %d", len(result))
    return result

# ── Lecture de la feuille Freezed prices ─────────────────────────────────────

def read_prices_sheet(ws: gspread.Worksheet) -> tuple[dict, dict]:
    """
    Retourne :
      ticker_to_col : { 'AAPL': 5, 'MC.PA': 7, … }   index colonne 1-based
      date_to_row   : { '10/04/2026': 1795, … }        index ligne  1-based
    """
    all_values = ws.get_all_values()

    # Tickers Yahoo en ligne 3, à partir de la colonne C
    ticker_row = all_values[TICKER_ROW - 1] if len(all_values) >= TICKER_ROW else []
    ticker_to_col = {
        cell.strip(): col_idx
        for col_idx, cell in enumerate(ticker_row, start=1)
        if col_idx >= TICKER_COL_START and cell.strip()
    }

    # Dates en colonne B, à partir de la ligne 6
    date_to_row = {
        row[DATE_COL - 1].strip(): row_idx
        for row_idx, row in enumerate(all_values, start=1)
        if row_idx >= DATE_ROW_START
        and len(row) >= DATE_COL
        and row[DATE_COL - 1].strip()
    }

    log.info("Tickers dans Freezed prices : %d", len(ticker_to_col))
    log.info("Dates dans Freezed prices   : %d", len(date_to_row))
    return ticker_to_col, date_to_row

# ── Date du jour ─────────────────────────────────────────────────────────────

def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%d/%m/%Y")

# ── Fetch du prix de clôture ─────────────────────────────────────────────────

def fetch_close(ticker: str) -> float | None:
    """
    Utilise yf.Ticker().history() qui retourne un DataFrame simple
    sans MultiIndex — compatible avec toutes les versions récentes de yfinance.
    Période de 5 jours pour couvrir week-ends et jours fériés.
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
    spreadsheet    = client.open_by_key(sheet_id)
    active         = read_active_tickers(spreadsheet)
    ws             = spreadsheet.worksheet(PRICES_SHEET)
    ticker_to_col, date_to_row = read_prices_sheet(ws)

    today = today_str()
    if today not in date_to_row:
        log.info("Date '%s' absente de la feuille — rien à faire.", today)
        return

    target_row = date_to_row[today]
    log.info("Ligne cible : %d  (date : %s)", target_row, today)

    # Tickers à traiter = actifs ET présents dans Freezed prices
    to_process = active & set(ticker_to_col.keys())
    ignored    = active - set(ticker_to_col.keys())
    if ignored:
        log.info("Ignorés (absents de Freezed prices) : %s", ", ".join(sorted(ignored)))

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
