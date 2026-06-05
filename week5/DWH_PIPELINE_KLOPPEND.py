# ============================================================
# DWH_PIPELINE.py
# Laadt DWH vanuit SDM, verwerkt SCD1/SCD2 en feiten,
# en logt ALLES naar dwh_log.csv
# ============================================================

import sqlite3
import pandas as pd
import csv
import os
import traceback
from datetime import datetime

# =========================
# PADEN - AANPASSEN ALS NODIG
# =========================

SDM_DB = r"C:\Users\doaaj\Semester4\DEAI_portfolio\sdm_week2\BikeToDriveDatabase.db"

DWH_BASE = r"C:\Users\doaaj\Semester4\DEAI_portfolio\week5"
DWH_DB = os.path.join(DWH_BASE, "DWH_DB.db")
DWH_LOG_FILE = os.path.join(DWH_BASE, "dwh_log.csv")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# =========================
# LOGGING
# =========================

os.makedirs(DWH_BASE, exist_ok=True)

with open(DWH_LOG_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "run_id", "timestamp", "laag", "stap", "tabel", "actie",
        "aantal_sdm", "aantal_dwh_voor", "aantal_insert", "aantal_update",
        "aantal_skipped", "aantal_error", "aantal_dwh_na", "status", "melding"
    ])

def log_dwh(
    stap,
    tabel="",
    actie="",
    aantal_sdm=0,
    aantal_dwh_voor=0,
    aantal_insert=0,
    aantal_update=0,
    aantal_skipped=0,
    aantal_error=0,
    aantal_dwh_na=0,
    status="OK",
    melding=""
):
    with open(DWH_LOG_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            RUN_ID,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "DWH",
            stap,
            tabel,
            actie,
            aantal_sdm,
            aantal_dwh_voor,
            aantal_insert,
            aantal_update,
            aantal_skipped,
            aantal_error,
            aantal_dwh_na,
            status,
            melding
        ])

def table_exists(conn, table_name):
    df = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        conn,
        params=(table_name,)
    )
    return len(df) > 0

def count_rows(conn, table_name):
    if not table_exists(conn, table_name):
        return 0
    return int(pd.read_sql_query(f"SELECT COUNT(*) AS aantal FROM [{table_name}]", conn)["aantal"][0])

def norm(v):
    if pd.isna(v):
        return None
    return str(v)

def row_changed(row, cols):
    for col in cols:
        if norm(row.get(f"{col}_sdm")) != norm(row.get(f"{col}_dwh")):
            return True
    return False

def safe_datekey(series):
    dates = pd.to_datetime(series, errors="coerce")
    return dates.dt.strftime("%Y%m%d").astype("Int64")

# =========================
# DWH RIM / CREATE TABLES
# =========================

DWH_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS Datum(
    datum_key INTEGER PRIMARY KEY,
    dag INTEGER,
    maand INTEGER,
    kwartaal INTEGER,
    jaar INTEGER
);

CREATE TABLE IF NOT EXISTS Filiaal (
    filiaal_key INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT,
    filiaalnr INTEGER,
    naam TEXT,
    adres TEXT,
    provincie TEXT,
    begin_tijd TEXT,
    eind_tijd TEXT
);

CREATE TABLE IF NOT EXISTS InkoopPeriode(
    periode_key INTEGER PRIMARY KEY,
    inkoopmaand INTEGER,
    inkoopjaar INTEGER
);

CREATE TABLE IF NOT EXISTS Klant (
    klant_key INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT,
    source_id INTEGER,
    klantnr INTEGER,
    naam TEXT,
    woonplaats TEXT,
    adres TEXT,
    geslacht TEXT,
    geboortedatum DATE
);

CREATE TABLE IF NOT EXISTS Leverancier (
    lev_key INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT,
    leveranciernr INTEGER,
    naam TEXT,
    adres TEXT,
    woonplaats TEXT
);

CREATE TABLE IF NOT EXISTS Monteur (
    monteur_key INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT,
    monteurnr INTEGER,
    naam TEXT,
    woonplaats TEXT,
    uurloon REAL,
    begin_tijd TEXT,
    eind_tijd TEXT
);

CREATE TABLE IF NOT EXISTS Product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT,
    productnr INTEGER,
    product_type TEXT,
    soort TEXT,
    merk TEXT,
    type TEXT,
    kleur TEXT,
    fabrikant TEXT
);

CREATE TABLE IF NOT EXISTS Inkoop(
    inkoop_key INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    inkoopnr INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    periode_key INTEGER NOT NULL,
    lev_key INTEGER NOT NULL,
    aantal INTEGER,
    inkoopprijs REAL,
    FOREIGN KEY (product_key) REFERENCES Product(product_key),
    FOREIGN KEY (periode_key) REFERENCES InkoopPeriode(periode_key),
    FOREIGN KEY (lev_key) REFERENCES Leverancier(lev_key)
);

CREATE TABLE IF NOT EXISTS Onderhoud (
    onderhoud_key INTEGER PRIMARY KEY AUTOINCREMENT,
    onderhoudnr INTEGER,
    product_key INTEGER NOT NULL,
    monteur_key INTEGER NOT NULL,
    filiaal_key INTEGER NOT NULL,
    datum DATE,
    starttijd TIME,
    eindtijd TIME,
    uurloon REAL,
    FOREIGN KEY (product_key) REFERENCES Product(product_key),
    FOREIGN KEY (monteur_key) REFERENCES Monteur(monteur_key),
    FOREIGN KEY (filiaal_key) REFERENCES Filiaal(filiaal_key)
);

CREATE TABLE IF NOT EXISTS Verkoop(
    verkoop_key INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    verkoopnr INTEGER NOT NULL,
    klant_key INTEGER NOT NULL,
    monteur_key INTEGER,
    product_key INTEGER NOT NULL,
    filiaal_key INTEGER NOT NULL,
    datum_key INTEGER NOT NULL,
    verkoopprijs REAL,
    aantal INTEGER,
    FOREIGN KEY (klant_key) REFERENCES Klant(klant_key),
    FOREIGN KEY (monteur_key) REFERENCES Monteur(monteur_key),
    FOREIGN KEY (product_key) REFERENCES Product(product_key),
    FOREIGN KEY (filiaal_key) REFERENCES Filiaal(filiaal_key),
    FOREIGN KEY (datum_key) REFERENCES Datum(datum_key)
);
"""

DWH_TABLES_RESET_ORDER = [
    "Verkoop", "Onderhoud", "Inkoop",
    "Klant", "Product", "Leverancier", "Monteur", "Filiaal", "Datum", "InkoopPeriode"
]

# =========================
# CONNECTIES
# =========================

sdm_conn = None
dwh_conn = None

try:
    log_dwh("START", actie="start_pipeline", status="OK", melding="DWH pipeline gestart")

    if not os.path.exists(SDM_DB):
        raise FileNotFoundError(f"SDM database bestaat niet: {SDM_DB}")

    sdm_conn = sqlite3.connect(SDM_DB)
    dwh_conn = sqlite3.connect(DWH_DB)
    dwh_conn.execute("PRAGMA foreign_keys = OFF")

    dwh_conn.executescript(DWH_CREATE_SQL)
    dwh_conn.commit()

    log_dwh("CREATE", actie="create_tables", status="OK", melding="DWH tabellen aangemaakt of bestonden al")

    # Full refresh voor feiten en SCD1 dimensies.
    # SCD2-tabellen worden OOK gereset voor een nette demonstratie-run.
    # Als je echte historie over meerdere runs wilt bewaren, verwijder Monteur en Filiaal uit deze reset.
    for table in DWH_TABLES_RESET_ORDER:
        try:
            before = count_rows(dwh_conn, table)
            dwh_conn.execute(f"DELETE FROM [{table}]")
            dwh_conn.commit()
            log_dwh("RESET", table, "delete", aantal_dwh_voor=before, aantal_skipped=0, aantal_dwh_na=0, status="OK", melding="DWH tabel geleegd")
        except Exception as e:
            dwh_conn.rollback()
            log_dwh("RESET", table, "delete", status="ERROR", melding=str(e))

    # =========================
    # DIM KLANT - SCD1
    # =========================
    try:
        before = count_rows(dwh_conn, "Klant")

        df = pd.read_sql_query("""
            SELECT
                'FIETS_VERKOOP_' || klantnr AS business_key,
                1 AS source_id,
                klantnr,
                naam,
                woonplaats,
                adres,
                geslacht,
                geboortedatum
            FROM Fiets_Verkoop_Klant

            UNION ALL

            SELECT
                'ACCESSOIRE_VERKOOP_' || klantnr AS business_key,
                2 AS source_id,
                klantnr,
                naam,
                woonplaats,
                adres,
                geslacht,
                geboortedatum
            FROM Accessoire_Verkoop_Klant
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["business_key"])

        df.to_sql("Klant", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Klant")
        log_dwh("SCD1", "Klant", "insert_full_refresh", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Klant geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("SCD1", "Klant", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # DIM PRODUCT - SCD1
    # =========================
    try:
        before = count_rows(dwh_conn, "Product")

        df = pd.read_sql_query("""
            SELECT
                'FIETS_VERKOOP_' || f.fietsnr AS business_key,
                f.fietsnr AS productnr,
                'fiets' AS product_type,
                f.soort,
                f.merk,
                f.type,
                f.kleur,
                fab.naam AS fabrikant
            FROM Fiets_Verkoop_Fiets f
            LEFT JOIN Fiets_Verkoop_Fabrikant fab ON f.fabrikant = fab.fabrikantnr

            UNION ALL

            SELECT
                'FIETS_INKOOP_' || f.fietsnr AS business_key,
                f.fietsnr AS productnr,
                'fiets' AS product_type,
                f.soort,
                f.merk,
                f.type,
                f.kleur,
                fab.naam AS fabrikant
            FROM Fiets_Inkoop_Fiets f
            LEFT JOIN Fiets_Inkoop_Fabrikant fab ON f.fabrikant = fab.fabrikantnr

            UNION ALL

            SELECT
                'ONDERHOUD_' || f.fietsnr AS business_key,
                f.fietsnr AS productnr,
                'fiets' AS product_type,
                f.soort,
                f.merk,
                f.type,
                f.kleur,
                fab.naam AS fabrikant
            FROM Onderhoud_Fiets f
            LEFT JOIN Onderhoud_Fabrikant fab ON f.fabrikant = fab.fabrikantnr

            UNION ALL

            SELECT
                'ACCESSOIRE_VERKOOP_' || a.accessoirenr AS business_key,
                a.accessoirenr AS productnr,
                'accessoire' AS product_type,
                a.soort,
                a.naam AS merk,
                NULL AS type,
                NULL AS kleur,
                l.naam AS fabrikant
            FROM Accessoire_Verkoop_Accessoire a
            LEFT JOIN Accessoire_Verkoop_Leverancier l ON a.leverancier = l.leveranciernr

            UNION ALL

            SELECT
                'ACCESSOIRE_INKOOP_' || a.accessoirenr AS business_key,
                a.accessoirenr AS productnr,
                'accessoire' AS product_type,
                a.soort,
                a.naam AS merk,
                NULL AS type,
                NULL AS kleur,
                l.naam AS fabrikant
            FROM Accessoire_Inkoop_Accessoire a
            LEFT JOIN Accessoire_Inkoop_Leverancier l ON a.leverancier = l.leveranciernr
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["business_key"])

        df.to_sql("Product", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Product")
        log_dwh("SCD1", "Product", "insert_full_refresh", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Product geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("SCD1", "Product", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # DIM LEVERANCIER - SCD1
    # =========================
    try:
        before = count_rows(dwh_conn, "Leverancier")

        df = pd.read_sql_query("""
            SELECT
                'ACCESSOIRE_VERKOOP_' || leveranciernr AS business_key,
                leveranciernr,
                naam,
                adres,
                woonplaats
            FROM Accessoire_Verkoop_Leverancier

            UNION ALL

            SELECT
                'ACCESSOIRE_INKOOP_' || leveranciernr AS business_key,
                leveranciernr,
                naam,
                adres,
                woonplaats
            FROM Accessoire_Inkoop_Leverancier

            UNION ALL

            SELECT
                'FIETS_VERKOOP_FABRIKANT_' || fabrikantnr AS business_key,
                fabrikantnr AS leveranciernr,
                naam,
                adres,
                plaats AS woonplaats
            FROM Fiets_Verkoop_Fabrikant

            UNION ALL

            SELECT
                'FIETS_INKOOP_FABRIKANT_' || fabrikantnr AS business_key,
                fabrikantnr AS leveranciernr,
                naam,
                adres,
                plaats AS woonplaats
            FROM Fiets_Inkoop_Fabrikant
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["business_key"])

        df.to_sql("Leverancier", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Leverancier")
        log_dwh("SCD1", "Leverancier", "insert_full_refresh", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Leverancier geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("SCD1", "Leverancier", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # DIM MONTEUR - SCD2
    # =========================
    try:
        before = count_rows(dwh_conn, "Monteur")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = pd.read_sql_query("""
            SELECT
                'FIETS_VERKOOP_' || monteurnr AS business_key,
                monteurnr,
                naam,
                woonplaats,
                uurloon
            FROM Fiets_Verkoop_Monteur

            UNION ALL

            SELECT
                'ACCESSOIRE_VERKOOP_' || monteurnr AS business_key,
                monteurnr,
                naam,
                woonplaats,
                uurloon
            FROM Accessoire_Verkoop_Monteur

            UNION ALL

            SELECT
                'ONDERHOUD_' || monteurnr AS business_key,
                monteurnr,
                naam,
                woonplaats,
                uurloon
            FROM Onderhoud_Monteur
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["business_key"])
        df["begin_tijd"] = now
        df["eind_tijd"] = None

        df.to_sql("Monteur", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Monteur")
        log_dwh("SCD2", "Monteur", "insert_current_records", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Monteur SCD2 geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("SCD2", "Monteur", "insert_current_records", status="ERROR", melding=str(e))

    # =========================
    # DIM FILIAAL - SCD2
    # =========================
    try:
        before = count_rows(dwh_conn, "Filiaal")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = pd.read_sql_query("""
            SELECT
                'FIETS_VERKOOP_' || filiaalnr AS business_key,
                filiaalnr,
                naam,
                adres,
                provincie
            FROM Fiets_Verkoop_Filiaal

            UNION ALL

            SELECT
                'ACCESSOIRE_VERKOOP_' || filiaalnr AS business_key,
                filiaalnr,
                naam,
                adres,
                provincie
            FROM Accessoire_Verkoop_Filiaal

            UNION ALL

            SELECT
                'ONDERHOUD_' || filiaalnr AS business_key,
                filiaalnr,
                naam,
                adres,
                provincie
            FROM Onderhoud_Filiaal
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["business_key"])
        df["begin_tijd"] = now
        df["eind_tijd"] = None

        df.to_sql("Filiaal", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Filiaal")
        log_dwh("SCD2", "Filiaal", "insert_current_records", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Filiaal SCD2 geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("SCD2", "Filiaal", "insert_current_records", status="ERROR", melding=str(e))

    # =========================
    # DIM DATUM
    # =========================
    try:
        before = count_rows(dwh_conn, "Datum")

        df = pd.read_sql_query("""
            SELECT datum FROM Fiets_Verkoop
            UNION ALL
            SELECT datum FROM Accessoire_Verkoop
            UNION ALL
            SELECT datum FROM Onderhoud
        """, sdm_conn)

        aantal_sdm = len(df)
        df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
        df = df.dropna(subset=["datum"]).drop_duplicates(subset=["datum"])

        df["datum_key"] = df["datum"].dt.strftime("%Y%m%d").astype(int)
        df["dag"] = df["datum"].dt.day
        df["maand"] = df["datum"].dt.month
        df["kwartaal"] = df["datum"].dt.quarter
        df["jaar"] = df["datum"].dt.year

        df[["datum_key", "dag", "maand", "kwartaal", "jaar"]].to_sql("Datum", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Datum")
        log_dwh("DIM", "Datum", "insert_full_refresh", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "Datum geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("DIM", "Datum", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # DIM INKOOPPERIODE
    # =========================
    try:
        before = count_rows(dwh_conn, "InkoopPeriode")

        df = pd.read_sql_query("""
            SELECT inkoopmaand, inkoopjaar FROM Fiets_Inkoop
            UNION ALL
            SELECT inkoopmaand, inkoopjaar FROM Accessoire_Inkoop
        """, sdm_conn)

        aantal_sdm = len(df)
        df = df.drop_duplicates(subset=["inkoopmaand", "inkoopjaar"])
        df = df.sort_values(["inkoopjaar", "inkoopmaand"]).reset_index(drop=True)
        df["periode_key"] = range(1, len(df) + 1)

        df[["periode_key", "inkoopmaand", "inkoopjaar"]].to_sql("InkoopPeriode", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "InkoopPeriode")
        log_dwh("DIM", "InkoopPeriode", "insert_full_refresh", aantal_sdm, before, after-before, 0, aantal_sdm-(after-before), 0, after, "OK", "InkoopPeriode geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("DIM", "InkoopPeriode", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # FACT VERKOOP
    # =========================
    try:
        before = count_rows(dwh_conn, "Verkoop")

        df = pd.read_sql_query("""
            SELECT
                1 AS source_id,
                fv.fiets_verkoopnr AS verkoopnr,
                'FIETS_VERKOOP_' || fv.klant AS klant_bk,
                'FIETS_VERKOOP_' || fv.monteur AS monteur_bk,
                'FIETS_VERKOOP_' || fv.fiets AS product_bk,
                'FIETS_VERKOOP_' || m.filiaal AS filiaal_bk,
                fv.datum,
                fv.verkoopprijs,
                fv.aantal
            FROM Fiets_Verkoop fv
            JOIN Fiets_Verkoop_Monteur m ON fv.monteur = m.monteurnr

            UNION ALL

            SELECT
                2 AS source_id,
                av.accessoire_verkoopnr AS verkoopnr,
                'ACCESSOIRE_VERKOOP_' || av.klant AS klant_bk,
                'ACCESSOIRE_VERKOOP_' || av.monteur AS monteur_bk,
                'ACCESSOIRE_VERKOOP_' || av.accessoire AS product_bk,
                'ACCESSOIRE_VERKOOP_' || m.filiaal AS filiaal_bk,
                av.datum,
                av.verkoopprijs,
                av.aantal
            FROM Accessoire_Verkoop av
            JOIN Accessoire_Verkoop_Monteur m ON av.monteur = m.monteurnr
        """, sdm_conn)

        aantal_sdm = len(df)
        df["datum_key"] = safe_datekey(df["datum"])

        klant = pd.read_sql_query("SELECT klant_key, business_key FROM Klant", dwh_conn)
        monteur = pd.read_sql_query("SELECT monteur_key, business_key FROM Monteur WHERE eind_tijd IS NULL", dwh_conn)
        product = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
        filiaal = pd.read_sql_query("SELECT filiaal_key, business_key FROM Filiaal WHERE eind_tijd IS NULL", dwh_conn)

        df = df.merge(klant, left_on="klant_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(monteur, left_on="monteur_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(product, left_on="product_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(filiaal, left_on="filiaal_bk", right_on="business_key", how="left").drop(columns=["business_key"])

        fact = df[["source_id", "verkoopnr", "klant_key", "monteur_key", "product_key", "filiaal_key", "datum_key", "verkoopprijs", "aantal"]].copy()

        before_drop = len(fact)
        fact = fact.dropna(subset=["klant_key", "product_key", "filiaal_key", "datum_key"])
        skipped = before_drop - len(fact)

        for col in ["source_id", "verkoopnr", "klant_key", "product_key", "filiaal_key", "datum_key"]:
            fact[col] = fact[col].astype(int)

        fact["monteur_key"] = fact["monteur_key"].astype("Int64")

        fact.to_sql("Verkoop", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Verkoop")
        log_dwh("FACT", "Verkoop", "insert_full_refresh", aantal_sdm, before, after-before, 0, skipped, 0, after, "OK", "Verkoop geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("FACT", "Verkoop", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # FACT ONDERHOUD
    # =========================
    try:
        before = count_rows(dwh_conn, "Onderhoud")

        df = pd.read_sql_query("""
            SELECT
                o.onderhoudnr,
                'ONDERHOUD_' || o.fiets AS product_bk,
                'ONDERHOUD_' || o.monteur AS monteur_bk,
                'ONDERHOUD_' || m.filiaal AS filiaal_bk,
                o.datum,
                o.starttijd,
                o.eindtijd,
                m.uurloon
            FROM Onderhoud o
            JOIN Onderhoud_Monteur m ON o.monteur = m.monteurnr
        """, sdm_conn)

        aantal_sdm = len(df)

        product = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
        monteur = pd.read_sql_query("SELECT monteur_key, business_key FROM Monteur WHERE eind_tijd IS NULL", dwh_conn)
        filiaal = pd.read_sql_query("SELECT filiaal_key, business_key FROM Filiaal WHERE eind_tijd IS NULL", dwh_conn)

        df = df.merge(product, left_on="product_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(monteur, left_on="monteur_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(filiaal, left_on="filiaal_bk", right_on="business_key", how="left").drop(columns=["business_key"])

        fact = df[["onderhoudnr", "product_key", "monteur_key", "filiaal_key", "datum", "starttijd", "eindtijd", "uurloon"]].copy()

        before_drop = len(fact)
        fact = fact.dropna(subset=["product_key", "monteur_key", "filiaal_key"])
        skipped = before_drop - len(fact)

        for col in ["onderhoudnr", "product_key", "monteur_key", "filiaal_key"]:
            fact[col] = fact[col].astype(int)

        fact.to_sql("Onderhoud", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Onderhoud")
        log_dwh("FACT", "Onderhoud", "insert_full_refresh", aantal_sdm, before, after-before, 0, skipped, 0, after, "OK", "Onderhoud geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("FACT", "Onderhoud", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # FACT INKOOP
    # =========================
    try:
        before = count_rows(dwh_conn, "Inkoop")

        df = pd.read_sql_query("""
            SELECT
                1 AS source_id,
                fi.inkoopnr,
                'FIETS_INKOOP_' || fi.fiets AS product_bk,
                fi.inkoopmaand,
                fi.inkoopjaar,
                'FIETS_INKOOP_FABRIKANT_' || f.fabrikant AS leverancier_bk,
                fi.aantal,
                f.inkoopprijs
            FROM Fiets_Inkoop fi
            JOIN Fiets_Inkoop_Fiets f ON fi.fiets = f.fietsnr

            UNION ALL

            SELECT
                2 AS source_id,
                ai.inkoopnr,
                'ACCESSOIRE_INKOOP_' || ai.accessoire AS product_bk,
                ai.inkoopmaand,
                ai.inkoopjaar,
                'ACCESSOIRE_INKOOP_' || a.leverancier AS leverancier_bk,
                ai.aantal,
                a.inkoopprijs
            FROM Accessoire_Inkoop ai
            JOIN Accessoire_Inkoop_Accessoire a ON ai.accessoire = a.accessoirenr
        """, sdm_conn)

        aantal_sdm = len(df)

        product = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
        periode = pd.read_sql_query("SELECT periode_key, inkoopmaand, inkoopjaar FROM InkoopPeriode", dwh_conn)
        leverancier = pd.read_sql_query("SELECT lev_key, business_key FROM Leverancier", dwh_conn)

        df = df.merge(product, left_on="product_bk", right_on="business_key", how="left").drop(columns=["business_key"])
        df = df.merge(periode, on=["inkoopmaand", "inkoopjaar"], how="left")
        df = df.merge(leverancier, left_on="leverancier_bk", right_on="business_key", how="left").drop(columns=["business_key"])

        fact = df[["source_id", "inkoopnr", "product_key", "periode_key", "lev_key", "aantal", "inkoopprijs"]].copy()

        before_drop = len(fact)
        fact = fact.dropna(subset=["product_key", "periode_key", "lev_key"])
        skipped = before_drop - len(fact)

        for col in ["source_id", "inkoopnr", "product_key", "periode_key", "lev_key"]:
            fact[col] = fact[col].astype(int)

        fact.to_sql("Inkoop", dwh_conn, if_exists="append", index=False)
        dwh_conn.commit()

        after = count_rows(dwh_conn, "Inkoop")
        log_dwh("FACT", "Inkoop", "insert_full_refresh", aantal_sdm, before, after-before, 0, skipped, 0, after, "OK", "Inkoop geladen")
    except Exception as e:
        dwh_conn.rollback()
        log_dwh("FACT", "Inkoop", "insert_full_refresh", status="ERROR", melding=str(e))

    # =========================
    # EINDAANTALLEN
    # =========================
    for table in ["Klant", "Product", "Leverancier", "Monteur", "Filiaal", "Datum", "InkoopPeriode", "Verkoop", "Onderhoud", "Inkoop"]:
        try:
            aantal = count_rows(dwh_conn, table)
            status = "OK" if aantal > 0 else "WARNING"
            melding = "eindaantal gecontroleerd" if aantal > 0 else "tabel heeft 0 rijen"
            log_dwh("EINDCONTROLE", table, "count", aantal_dwh_na=aantal, status=status, melding=melding)
        except Exception as e:
            log_dwh("EINDCONTROLE", table, "count", status="ERROR", melding=str(e))

    log_dwh("EINDE", actie="end_pipeline", status="OK", melding="DWH pipeline afgerond")

except Exception:
    log_dwh("PIPELINE", actie="fatal_error", status="ERROR", melding=traceback.format_exc())

finally:
    if sdm_conn is not None:
        sdm_conn.close()
    if dwh_conn is not None:
        dwh_conn.close()

print("DWH klaar.")
print("Database:", DWH_DB)
print("Logfile:", DWH_LOG_FILE)
