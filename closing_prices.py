"""
closing_prices.py
Inscrit chaque jour les prix de clôture Yahoo Finance dans la feuille 'Freezed prices'.

Structure de la feuille :
  - Tickers Yahoo  : ligne 3, colonnes C→  (C3, D3, E3 …)
  - Dates          : colonne B, lignes 6→  (B6, B7, B8 …)
  - Prix           : cellule à l'intersection (ligne de la date, colonne du ticker)

Seuls les tickers présents dans Equities!E7:E sont traités.
Les tickers Equities sont au format Google Finance et sont convertis en Yahoo.

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

PRICES_SHEET     = "Freezed prices"
EQUITIES_SHEET   = "Equities"
TICKER_ROW       = 3       # ligne des tickers Yahoo (C3, D3, E3 …)
TICKER_COL_START = 3       # colonne C = index 3 (1-based)
DATE_COL         = 2       # colonne B = index 2 (1-based)
DATE_ROW_START   = 6       # première date en B6
EQUITIES_RANGE   = "E7:E"  # tickers actifs (format Google Finance)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ── Conversion ticker Google Finance → Yahoo Finance ─────────────────────────

EXCHANGE_MAP = {
    # Américains (pas de suffixe)
    "NASDAQ":       "",
    "NYSE":         "",
    "NYSEARCA":     "",
    "NYSEAMERICAN": "",
    "BATS":         "",
    "OTC":          "",
    # Europe
    "EPA":  ".PA",   # Euronext Paris
    "AMS":  ".AS",   # Euronext Amsterdam
    "EBR":  ".BR",   # Euronext Bruxelles
    "ELI":  ".LS",   # Euronext Lisbonne
    "ETR":  ".DE",   # Xetra / Frankfurt
    "FRA":  ".F",    # Frankfurt (autre code)
    "LON":  ".L",    # London Stock Exchange
    "BIT":  ".MI",   # Borsa Italiana
    "MIL":  ".MI",
    "BME":  ".MC",   # Madrid
    "VIE":  ".VI",   # Vienne
    "STO":  ".ST",   # Stockholm
    "CPH":  ".CO",   # Copenhague
    "HEL":  ".HE",   # Helsinki
    "OSL":  ".OL",   # Oslo
    "SWX":  ".SW",   # Zurich
    "WSE":  ".WA",   # Varsovie
    "PRA":  ".PR",   # Prague
    "BUD":  ".BD",   # Budapest
    # Amérique du Nord
    "TSX":  ".TO",   # Toronto
    "TSXV": ".V",    # Toronto Venture
    "CVE":  ".CN",   # Canadian Securities Exchange
    # Asie-Pacifique
    "TYO":  ".T",    # Tokyo
    "TSE":  ".TO",   # attention : aussi Toronto — contexte Canada vs Japon
    "OSA":  ".OS",   # Osaka
    "SHA":  ".SS",   # Shanghai
    "SHE":  ".SZ",   # Shenzhen
    "HKG":  ".HK",   # Hong Kong
    "TPE":  ".TW",   # Taïwan
    "NSE":  ".NS",   # Inde NSE
    "BSE":  ".BO",   # Inde BSE
    "SGX":  ".SI",   # Singapour
    "KRX":  ".KS",   # Corée du Sud (KOSPI)
    "KOSDAQ": ".KQ", # Corée du Sud (KOSDAQ)
    "ASX":  ".AX",   # Australie
    "NZX":  ".NZ",   # Nouvelle-Zélande
    "BOM":  ".BO",   # Bombay
    # Autres
    "JSE":  ".JO",   # Johannesburg
    "BVMF": ".SA",   # Brésil
    "TLV":  ".TA",   # Tel Aviv
    "IST":  ".IS",   # Istanbul
    "MCX":  ".ME",   # Moscou
}

def google_ticker_to_yahoo(raw: str) -> str | None:
    """
    Convertit un ticker Google Finance en ticker Yahoo Finance.
    Exemples :
      'EPA:MC'        → 'MC.PA'
      'NASDAQ:AAPL'   → 'AAPL'
      'TYO:285A'      → '285A.T'    (Kioxia)
      'LON:RR'        → 'RR.L'      (Rolls-Royce)
      'AAPL'          → 'AAPL'      (déjà au format Yahoo)
    """
    if not raw:
        return None
    raw = raw.strip()
    if ":" not in raw:
        return raw   # déjà au format Yahoo ou ticker brut
    exchange, symbol = raw.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange.upper())
    if suffix is None:
        log.warning("Exchange inconnu : %s — ticker conservé tel quel : %s", exchange, symbol)
        return symbol   # on tente quand même avec le symbole brut
    return symbol + suffix

# ── Authentification Google Sheets ───────────────────────────────────────────

def get_client() -> gspread.Client:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── Lecture des tickers actifs depuis Equities!E7:E ──────────────────────────

def read_active_tickers_yahoo(spreadsheet: gspread.Spreadsheet) -> set[str]:
    """
    Lit les tickers au format Google Finance dans Equities!E7:E
    et les convertit en format Yahoo Finance.
    """
    ws = spreadsheet.worksheet(EQUITIES_SHEET)
    values = ws.get(EQUITIES_RANGE)
    tickers = set()
    for row in values:
        if row and row[0].strip():
            yahoo = google_ticker_to_yahoo(row[0].strip())
            if yahoo:
                tickers.add(yahoo)
    log.info("Tickers actifs (convertis Yahoo) : %d", len(tickers))
    return tickers

# ── Lecture de la feuille Freezed prices ─────────────────────────────────────

def read_prices_sheet(ws: gspread.Worksheet) -> tuple[dict, dict]:
    """
    Retourne :
      ticker_to_col : { 'AAPL' : 5, 'MC.PA' : 7, … }   (index colonne 1-based)
      date_to_row   : { '10/04/2026' : 1795, … }         (index ligne  1-based)
    """
    all_values = ws.get_all_values()

    # Tickers Yahoo — ligne 3, colonnes C→
    ticker_row = all_values[TICKER_ROW - 1] if len(all_values) >= TICKER_ROW else []
    ticker_to_col = {}
    for col_idx, cell in enumerate(ticker_row, start=1):
        if col_idx >= TICKER_COL_START and cell.strip():
            ticker_to_col[cell.strip()] = col_idx

    # Dates — colonne B, lignes 6→
    date_to_row = {}
    for row_idx, row in enumerate(all_values, start=1):
        if row_idx >= DATE_ROW_START and len(row) >= DATE_COL:
            cell = row[DATE_COL - 1].strip()
            if cell:
                date_to_row[cell] = row_idx

    log.info("Tickers dans Freezed prices : %d", len(ticker_to_col))
    log.info("Dates dans Freezed prices   : %d", len(date_to_row))
    return ticker_to_col, date_to_row

# ── Date du jour au format de la feuille ─────────────────────────────────────

def today_label() -> str:
    """Retourne la date du jour au format DD/MM/YYYY (comme dans la feuille)."""
    return datetime.now(timezone.utc).strftime("%d/%m/%Y")

# ── Fetch du prix de clôture Yahoo Finance ───────────────────────────────────

def fetch_close(ticker: str) -> float | None:
    """
    Retourne le dernier prix de clôture disponible.
    Fenêtre de 7 jours pour couvrir week-ends, jours fériés et décalages horaires.
    """
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception as exc:
        log.warning("Erreur fetch %s : %s", ticker, exc)
        return None

# ── Traitement d'un fichier Google Sheets ────────────────────────────────────

def process_spreadsheet(client: gspread.Client, sheet_id: str) -> None:
    log.info("═══ Sheet %s ═══", sheet_id)
    spreadsheet    = client.open_by_key(sheet_id)
    active_tickers = read_active_tickers_yahoo(spreadsheet)
    ws             = spreadsheet.worksheet(PRICES_SHEET)
    ticker_to_col, date_to_row = read_prices_sheet(ws)

    today = today_label()
    if today not in date_to_row:
        log.info("Date '%s' absente de la feuille — rien à faire.", today)
        return

    target_row = date_to_row[today]
    log.info("Ligne cible : %d (date : %s)", target_row, today)

    # Tickers à traiter = présents dans Equities ET dans Freezed prices
    to_process = active_tickers & set(ticker_to_col.keys())
    skipped    = active_tickers - set(ticker_to_col.keys())

    if skipped:
        log.info("Tickers actifs absents de Freezed prices : %s", ", ".join(sorted(skipped)))

    updates = []
    for ticker in sorted(to_process):
        price = fetch_close(ticker)
        if price is None:
            log.info("  %s : aucun prix disponible", ticker)
            continue
        col  = ticker_to_col[ticker]
        cell = gspread.utils.rowcol_to_a1(target_row, col)
        updates.append({"range": cell, "values": [[round(price, 4)]]})
        log.info("  ✓ %s → %s = %.4f", ticker, cell, price)

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
            log.error("Échec sur %s : %s", sheet_id, exc, exc_info=True)

if __name__ == "__main__":
    main()
