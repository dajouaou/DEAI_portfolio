# ============================================================
# SDM_PIPELINE.py
# Maakt SDM-tabellen aan, reset ze, laadt 5 brondatabases in,
# en logt ALLES naar sdm_log.csv
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

BASE = r"C:\Users\doaaj\Semester4\DEAI_portfolio\sdm_week2"

SDM_DB = os.path.join(BASE, "BikeToDriveDatabase.db")

BRONNEN = {
    "Accessoire_Verkoop": os.path.join(BASE, "BikeToDrive_1_Accessoireverkoop.db"),
    "Fiets_Verkoop": os.path.join(BASE, "BikeToDrive_2_Fietsverkoop.db"),
    "Onderhoud": os.path.join(BASE, "BikeToDrive_3_Onderhoud.db"),
    "Accessoire_Inkoop": os.path.join(BASE, "BikeToDrive_4_Accessoire_Inkoop.db"),
    "Fiets_Inkoop": os.path.join(BASE, "BikeToDrive_5_Fiets_Inkoop.db")
}

SDM_LOG_FILE = os.path.join(BASE, "sdm_log.csv")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# =========================
# LOGGING
# =========================

with open(SDM_LOG_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "run_id", "timestamp", "laag", "stap", "bron_database", "bron_tabel",
        "doel_tabel", "actie", "aantal_bron", "aantal_doel_voor",
        "aantal_verwerkt", "aantal_doel_na", "status", "melding"
    ])

def log_sdm(
    stap,
    bron_database="",
    bron_tabel="",
    doel_tabel="",
    actie="",
    aantal_bron=0,
    aantal_doel_voor=0,
    aantal_verwerkt=0,
    aantal_doel_na=0,
    status="OK",
    melding=""
):
    with open(SDM_LOG_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            RUN_ID,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "SDM",
            stap,
            bron_database,
            bron_tabel,
            doel_tabel,
            actie,
            aantal_bron,
            aantal_doel_voor,
            aantal_verwerkt,
            aantal_doel_na,
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

def get_columns(conn, table_name):
    return [row[1] for row in conn.execute(f"PRAGMA table_info([{table_name}])").fetchall()]

# =========================
# SDM RIM / CREATE TABLES
# =========================

SDM_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS Onderhoud_Fabrikant (
    fabrikantnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    plaats TEXT
);

CREATE TABLE IF NOT EXISTS Onderhoud_Filiaal (
    filiaalnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    provincie TEXT
);

CREATE TABLE IF NOT EXISTS Onderhoud_Fiets (
    fietsnr INTEGER PRIMARY KEY,
    soort TEXT,
    merk TEXT,
    type TEXT,
    standaardprijs REAL,
    inkoopprijs REAL,
    kleur TEXT,
    fabrikant INTEGER NOT NULL,
    FOREIGN KEY (fabrikant) REFERENCES Onderhoud_Fabrikant(fabrikantnr)
);

CREATE TABLE IF NOT EXISTS Onderhoud_Monteur (
    monteurnr INTEGER PRIMARY KEY,
    naam TEXT,
    woonplaats TEXT,
    uurloon REAL,
    filiaal INTEGER NOT NULL,
    FOREIGN KEY (filiaal) REFERENCES Onderhoud_Filiaal(filiaalnr)
);

CREATE TABLE IF NOT EXISTS Onderhoud (
    onderhoudnr INTEGER PRIMARY KEY,
    datum TEXT,
    starttijd TEXT,
    eindtijd TEXT,
    fiets INTEGER NOT NULL,
    monteur INTEGER NOT NULL,
    FOREIGN KEY (fiets) REFERENCES Onderhoud_Fiets(fietsnr),
    FOREIGN KEY (monteur) REFERENCES Onderhoud_Monteur(monteurnr)
);

CREATE TABLE IF NOT EXISTS Accessoire_Inkoop_Leverancier (
    leveranciernr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    woonplaats TEXT
);

CREATE TABLE IF NOT EXISTS Accessoire_Inkoop_Accessoire (
    accessoirenr INTEGER PRIMARY KEY,
    naam TEXT,
    standaardprijs REAL,
    inkoopprijs REAL,
    soort TEXT,
    leverancier INTEGER NOT NULL,
    FOREIGN KEY (leverancier) REFERENCES Accessoire_Inkoop_Leverancier(leveranciernr)
);

CREATE TABLE IF NOT EXISTS Accessoire_Inkoop (
    inkoopnr INTEGER PRIMARY KEY,
    inkoopmaand INTEGER,
    inkoopjaar INTEGER,
    aantal INTEGER,
    accessoire INTEGER NOT NULL,
    FOREIGN KEY (accessoire) REFERENCES Accessoire_Inkoop_Accessoire(accessoirenr)
);

CREATE TABLE IF NOT EXISTS Fiets_Inkoop_Fabrikant (
    fabrikantnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    plaats TEXT
);

CREATE TABLE IF NOT EXISTS Fiets_Inkoop_Fiets (
    fietsnr INTEGER PRIMARY KEY,
    soort TEXT,
    merk TEXT,
    type TEXT,
    standaardprijs REAL,
    inkoopprijs REAL,
    kleur TEXT,
    fabrikant INTEGER NOT NULL,
    FOREIGN KEY (fabrikant) REFERENCES Fiets_Inkoop_Fabrikant(fabrikantnr)
);

CREATE TABLE IF NOT EXISTS Fiets_Inkoop (
    inkoopnr INTEGER PRIMARY KEY,
    inkoopmaand INTEGER,
    inkoopjaar INTEGER,
    aantal INTEGER,
    fiets INTEGER NOT NULL,
    FOREIGN KEY (fiets) REFERENCES Fiets_Inkoop_Fiets(fietsnr)
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop_Klant (
    klantnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    woonplaats TEXT,
    geslacht CHAR(1),
    geboortedatum TEXT
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop_Leverancier (
    leveranciernr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    woonplaats TEXT
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop_Accessoire (
    accessoirenr INTEGER PRIMARY KEY,
    naam TEXT,
    standaardprijs REAL,
    inkoopprijs REAL,
    soort TEXT,
    leverancier INTEGER NOT NULL,
    FOREIGN KEY (leverancier) REFERENCES Accessoire_Verkoop_Leverancier(leveranciernr)
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop_Filiaal (
    filiaalnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    provincie TEXT
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop_Monteur (
    monteurnr INTEGER PRIMARY KEY,
    naam TEXT,
    woonplaats TEXT,
    uurloon REAL,
    filiaal INTEGER NOT NULL,
    FOREIGN KEY (filiaal) REFERENCES Accessoire_Verkoop_Filiaal(filiaalnr)
);

CREATE TABLE IF NOT EXISTS Accessoire_Verkoop (
    accessoire_verkoopnr INTEGER PRIMARY KEY,
    datum TEXT,
    aantal INTEGER,
    verkoopprijs REAL,
    klant INTEGER NOT NULL,
    accessoire INTEGER NOT NULL,
    monteur INTEGER NOT NULL,
    FOREIGN KEY (klant) REFERENCES Accessoire_Verkoop_Klant(klantnr),
    FOREIGN KEY (accessoire) REFERENCES Accessoire_Verkoop_Accessoire(accessoirenr),
    FOREIGN KEY (monteur) REFERENCES Accessoire_Verkoop_Monteur(monteurnr)
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop_Klant (
    klantnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    woonplaats TEXT,
    geslacht CHAR(1),
    geboortedatum TEXT
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop_Fabrikant (
    fabrikantnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    plaats TEXT
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop_Fiets (
    fietsnr INTEGER PRIMARY KEY,
    soort TEXT,
    merk TEXT,
    type TEXT,
    standaardprijs REAL,
    inkoopprijs REAL,
    kleur TEXT,
    fabrikant INTEGER NOT NULL,
    FOREIGN KEY (fabrikant) REFERENCES Fiets_Verkoop_Fabrikant(fabrikantnr)
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop_Filiaal (
    filiaalnr INTEGER PRIMARY KEY,
    naam TEXT,
    adres TEXT,
    provincie TEXT
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop_Monteur (
    monteurnr INTEGER PRIMARY KEY,
    naam TEXT,
    woonplaats TEXT,
    uurloon REAL,
    filiaal INTEGER NOT NULL,
    FOREIGN KEY (filiaal) REFERENCES Fiets_Verkoop_Filiaal(filiaalnr)
);

CREATE TABLE IF NOT EXISTS Fiets_Verkoop (
    fiets_verkoopnr INTEGER PRIMARY KEY,
    datum TEXT,
    aantal INTEGER,
    verkoopprijs REAL,
    klant INTEGER NOT NULL,
    fiets INTEGER NOT NULL,
    monteur INTEGER NOT NULL,
    FOREIGN KEY (klant) REFERENCES Fiets_Verkoop_Klant(klantnr),
    FOREIGN KEY (fiets) REFERENCES Fiets_Verkoop_Fiets(fietsnr),
    FOREIGN KEY (monteur) REFERENCES Fiets_Verkoop_Monteur(monteurnr)
);
"""

# parent-tabellen eerst, child-tabellen daarna
LOAD_MAPPING = {
    "Onderhoud": [
        ("Fabrikant", "Onderhoud_Fabrikant"),
        ("Filiaal", "Onderhoud_Filiaal"),
        ("Fiets", "Onderhoud_Fiets"),
        ("Monteur", "Onderhoud_Monteur"),
        ("Onderhoud", "Onderhoud")
    ],
    "Accessoire_Inkoop": [
        ("Leverancier", "Accessoire_Inkoop_Leverancier"),
        ("Accessoire", "Accessoire_Inkoop_Accessoire"),
        ("Accessoire_Inkoop", "Accessoire_Inkoop")
    ],
    "Fiets_Inkoop": [
        ("Fabrikant", "Fiets_Inkoop_Fabrikant"),
        ("Fiets", "Fiets_Inkoop_Fiets"),
        ("Fiets_Inkoop", "Fiets_Inkoop")
    ],
    "Accessoire_Verkoop": [
        ("Klant", "Accessoire_Verkoop_Klant"),
        ("Leverancier", "Accessoire_Verkoop_Leverancier"),
        ("Accessoire", "Accessoire_Verkoop_Accessoire"),
        ("Filiaal", "Accessoire_Verkoop_Filiaal"),
        ("Monteur", "Accessoire_Verkoop_Monteur"),
        ("Accessoire_Verkoop", "Accessoire_Verkoop")
    ],
    "Fiets_Verkoop": [
        ("Klant", "Fiets_Verkoop_Klant"),
        ("Fabrikant", "Fiets_Verkoop_Fabrikant"),
        ("Fiets", "Fiets_Verkoop_Fiets"),
        ("Filiaal", "Fiets_Verkoop_Filiaal"),
        ("Monteur", "Fiets_Verkoop_Monteur"),
        ("Fiets_Verkoop", "Fiets_Verkoop")
    ]
}

RESET_ORDER = [
    "Onderhoud",
    "Accessoire_Inkoop",
    "Fiets_Inkoop",
    "Accessoire_Verkoop",
    "Fiets_Verkoop",

    "Onderhoud_Monteur", "Onderhoud_Fiets", "Onderhoud_Filiaal", "Onderhoud_Fabrikant",
    "Accessoire_Inkoop_Accessoire", "Accessoire_Inkoop_Leverancier",
    "Fiets_Inkoop_Fiets", "Fiets_Inkoop_Fabrikant",
    "Accessoire_Verkoop_Accessoire", "Accessoire_Verkoop_Monteur", "Accessoire_Verkoop_Filiaal",
    "Accessoire_Verkoop_Leverancier", "Accessoire_Verkoop_Klant",
    "Fiets_Verkoop_Fiets", "Fiets_Verkoop_Monteur", "Fiets_Verkoop_Filiaal",
    "Fiets_Verkoop_Fabrikant", "Fiets_Verkoop_Klant"
]

# =========================
# PIPELINE
# =========================

sdm_conn = None

try:
    os.makedirs(BASE, exist_ok=True)

    log_sdm("START", actie="start_pipeline", status="OK", melding="SDM pipeline gestart")

    sdm_conn = sqlite3.connect(SDM_DB)
    sdm_conn.execute("PRAGMA foreign_keys = OFF")

    sdm_conn.executescript(SDM_CREATE_SQL)
    sdm_conn.commit()

    log_sdm("CREATE", doel_tabel="ALLE_SDM_TABELLEN", actie="create_tables", status="OK", melding="SDM tabellen aangemaakt of bestonden al")

    # Reset alle SDM-tabellen
    for table in RESET_ORDER:
        try:
            before = count_rows(sdm_conn, table)
            sdm_conn.execute(f"DELETE FROM [{table}]")
            sdm_conn.commit()
            after = count_rows(sdm_conn, table)

            log_sdm(
                stap="RESET",
                doel_tabel=table,
                actie="delete",
                aantal_doel_voor=before,
                aantal_verwerkt=before,
                aantal_doel_na=after,
                status="OK",
                melding="tabel geleegd"
            )
        except Exception as e:
            sdm_conn.rollback()
            log_sdm("RESET", doel_tabel=table, actie="delete", status="ERROR", melding=str(e))

    # Bronnen laden
    for bron_naam, bron_pad in BRONNEN.items():

        if not os.path.exists(bron_pad):
            log_sdm("CONNECT", bron_database=bron_pad, actie="open_database", status="ERROR", melding="bronbestand bestaat niet")
            continue

        bron_conn = None

        try:
            bron_conn = sqlite3.connect(bron_pad)
            log_sdm("CONNECT", bron_database=bron_pad, actie="open_database", status="OK", melding="bronconnectie geopend")

            for bron_tabel, doel_tabel in LOAD_MAPPING[bron_naam]:
                try:
                    if not table_exists(bron_conn, bron_tabel):
                        log_sdm(
                            stap="EXTRACT",
                            bron_database=bron_pad,
                            bron_tabel=bron_tabel,
                            doel_tabel=doel_tabel,
                            actie="select",
                            status="ERROR",
                            melding="brontabel bestaat niet"
                        )
                        continue

                    df = pd.read_sql_query(f"SELECT * FROM [{bron_tabel}]", bron_conn)
                    aantal_bron = len(df)

                    log_sdm(
                        stap="EXTRACT",
                        bron_database=bron_pad,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="select",
                        aantal_bron=aantal_bron,
                        status="OK",
                        melding="brondata gelezen"
                    )

                    # Kolommen gelijkzetten aan SDM-tabel
                    doel_cols = get_columns(sdm_conn, doel_tabel)
                    df = df[[c for c in df.columns if c in doel_cols]]

                    before = count_rows(sdm_conn, doel_tabel)

                    df.to_sql(doel_tabel, sdm_conn, if_exists="append", index=False)
                    sdm_conn.commit()

                    after = count_rows(sdm_conn, doel_tabel)
                    verwerkt = after - before

                    status = "OK" if verwerkt == aantal_bron else "WARNING"
                    melding = "aantal geladen rijen klopt" if status == "OK" else "aantal geladen rijen wijkt af"

                    log_sdm(
                        stap="LOAD",
                        bron_database=bron_pad,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="insert",
                        aantal_bron=aantal_bron,
                        aantal_doel_voor=before,
                        aantal_verwerkt=verwerkt,
                        aantal_doel_na=after,
                        status=status,
                        melding=melding
                    )

                except Exception as e:
                    sdm_conn.rollback()
                    log_sdm(
                        stap="LOAD",
                        bron_database=bron_pad,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="insert",
                        status="ERROR",
                        melding=str(e)
                    )

        except Exception as e:
            log_sdm("CONNECT", bron_database=bron_pad, actie="open_database", status="ERROR", melding=str(e))

        finally:
            if bron_conn is not None:
                bron_conn.close()
                log_sdm("CONNECT", bron_database=bron_pad, actie="close_database", status="OK", melding="bronconnectie gesloten")

    # Eindcontrole
    for table in RESET_ORDER:
        try:
            aantal = count_rows(sdm_conn, table)
            eind_status = "OK" if aantal > 0 else "WARNING"
            eind_melding = "eindaantal gecontroleerd" if aantal > 0 else "tabel heeft 0 rijen"
            log_sdm("EINDCONTROLE", doel_tabel=table, actie="count", aantal_doel_na=aantal, status=eind_status, melding=eind_melding)
        except Exception as e:
            log_sdm("EINDCONTROLE", doel_tabel=table, actie="count", status="ERROR", melding=str(e))

    log_sdm("EINDE", actie="end_pipeline", status="OK", melding="SDM pipeline afgerond")

except Exception:
    log_sdm("PIPELINE", actie="fatal_error", status="ERROR", melding=traceback.format_exc())

finally:
    if sdm_conn is not None:
        sdm_conn.close()

print("SDM klaar.")
print("Database:", SDM_DB)
print("Logfile:", SDM_LOG_FILE)
