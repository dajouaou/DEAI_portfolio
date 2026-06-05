import sqlite3
import pandas as pd
import csv
import os
import traceback
from datetime import datetime

# =========================
# INSTELLINGEN
# =========================

SDM_DB = "BikeToDriveDatabase.db"

BRONNEN = {
    "Accessoire_Verkoop": "BikeToDrive_1_Accessoireverkoop.db",
    "Fiets_Verkoop": "BikeToDrive_2_Fietsverkoop.db",
    "Onderhoud": "BikeToDrive_3_Onderhoud.db",
    "Accessoire_Inkoop": "BikeToDrive_4_Accessoire_Inkoop.db",
    "Fiets_Inkoop": "BikeToDrive_5_Fiets_Inkoop.db"
}

SDM_LOG_FILE = "sdm_log.csv"

# bron tabel -> SDM tabel
TABLE_MAPPING = {
    "Accessoire_Verkoop": {
        "Klant": "Accessoire_Verkoop_Klant",
        "Accessoire": "Accessoire_Verkoop_Accessoire",
        "Monteur": "Accessoire_Verkoop_Monteur",
        "Filiaal": "Accessoire_Verkoop_Filiaal",
        "Leverancier": "Accessoire_Verkoop_Leverancier",
        "Accessoire_Verkoop": "Accessoire_Verkoop"
    },
    "Fiets_Verkoop": {
        "Klant": "Fiets_Verkoop_Klant",
        "Fiets": "Fiets_Verkoop_Fiets",
        "Monteur": "Fiets_Verkoop_Monteur",
        "Filiaal": "Fiets_Verkoop_Filiaal",
        "Fabrikant": "Fiets_Verkoop_Fabrikant",
        "Fiets_Verkoop": "Fiets_Verkoop"
    },
    "Onderhoud": {
        "Fabrikant": "Onderhoud_Fabrikant",
        "Filiaal": "Onderhoud_Filiaal",
        "Fiets": "Onderhoud_Fiets",
        "Monteur": "Onderhoud_Monteur",
        "Onderhoud": "Onderhoud"
    },
    "Accessoire_Inkoop": {
        "Leverancier": "Accessoire_Inkoop_Leverancier",
        "Accessoire": "Accessoire_Inkoop_Accessoire",
        "Accessoire_Inkoop": "Accessoire_Inkoop"
    },
    "Fiets_Inkoop": {
        "Fabrikant": "Fiets_Inkoop_Fabrikant",
        "Fiets": "Fiets_Inkoop_Fiets",
        "Fiets_Inkoop": "Fiets_Inkoop"
    }
}

# juiste volgorde voor reset/load
SDM_TABLE_ORDER = [
    "Onderhoud",
    "Onderhoud_Monteur", "Onderhoud_Fiets", "Onderhoud_Filiaal", "Onderhoud_Fabrikant",

    "Accessoire_Inkoop",
    "Accessoire_Inkoop_Accessoire", "Accessoire_Inkoop_Leverancier",

    "Fiets_Inkoop",
    "Fiets_Inkoop_Fiets", "Fiets_Inkoop_Fabrikant",

    "Accessoire_Verkoop",
    "Accessoire_Verkoop_Klant", "Accessoire_Verkoop_Accessoire",
    "Accessoire_Verkoop_Monteur", "Accessoire_Verkoop_Filiaal",
    "Accessoire_Verkoop_Leverancier",

    "Fiets_Verkoop",
    "Fiets_Verkoop_Klant", "Fiets_Verkoop_Fiets",
    "Fiets_Verkoop_Monteur", "Fiets_Verkoop_Filiaal",
    "Fiets_Verkoop_Fabrikant"
]

# =========================
# LOGGING
# =========================

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(SDM_LOG_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "run_id",
        "timestamp",
        "laag",
        "stap",
        "bron_database",
        "bron_tabel",
        "doel_tabel",
        "actie",
        "aantal_bron",
        "aantal_doel_voor",
        "aantal_verwerkt",
        "aantal_doel_na",
        "status",
        "melding"
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
    result = pd.read_sql_query("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
    """, conn, params=(table_name,))
    return len(result) > 0

def count_rows(conn, table_name):
    if not table_exists(conn, table_name):
        return 0
    return pd.read_sql_query(f"SELECT COUNT(*) AS aantal FROM [{table_name}]", conn)["aantal"][0]

# =========================
# START PIPELINE
# =========================

sdm_conn = sqlite3.connect(SDM_DB)
sdm_conn.execute("PRAGMA foreign_keys = OFF")

log_sdm(
    stap="START",
    actie="start_pipeline",
    status="OK",
    melding="SDM full refresh gestart"
)

try:
    # =========================
    # RESET SDM
    # =========================
    for table in SDM_TABLE_ORDER:
        try:
            if table_exists(sdm_conn, table):
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
            else:
                log_sdm(
                    stap="RESET",
                    doel_tabel=table,
                    actie="delete",
                    status="WARNING",
                    melding="tabel bestaat niet in SDM"
                )
        except Exception as e:
            log_sdm(
                stap="RESET",
                doel_tabel=table,
                actie="delete",
                status="ERROR",
                melding=str(e)
            )

    # =========================
    # LOAD VAN BRONNEN NAAR SDM
    # =========================
    for bron_naam, bron_db in BRONNEN.items():
        if not os.path.exists(bron_db):
            log_sdm(
                stap="CONNECT",
                bron_database=bron_db,
                actie="open_database",
                status="ERROR",
                melding="bronbestand bestaat niet"
            )
            continue

        try:
            bron_conn = sqlite3.connect(bron_db)

            log_sdm(
                stap="CONNECT",
                bron_database=bron_db,
                actie="open_database",
                status="OK",
                melding="bronconnectie geopend"
            )

            for bron_tabel, doel_tabel in TABLE_MAPPING[bron_naam].items():
                try:
                    if not table_exists(bron_conn, bron_tabel):
                        log_sdm(
                            stap="EXTRACT",
                            bron_database=bron_db,
                            bron_tabel=bron_tabel,
                            doel_tabel=doel_tabel,
                            actie="select",
                            status="ERROR",
                            melding="brontabel bestaat niet"
                        )
                        continue

                    if not table_exists(sdm_conn, doel_tabel):
                        log_sdm(
                            stap="LOAD",
                            bron_database=bron_db,
                            bron_tabel=bron_tabel,
                            doel_tabel=doel_tabel,
                            actie="insert",
                            status="ERROR",
                            melding="doeltabel bestaat niet in SDM"
                        )
                        continue

                    doel_voor = count_rows(sdm_conn, doel_tabel)

                    df = pd.read_sql_query(f"SELECT * FROM [{bron_tabel}]", bron_conn)
                    aantal_bron = len(df)

                    log_sdm(
                        stap="EXTRACT",
                        bron_database=bron_db,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="select",
                        aantal_bron=aantal_bron,
                        status="OK",
                        melding="data uit bron gelezen"
                    )

                    df.to_sql(doel_tabel, sdm_conn, if_exists="append", index=False)
                    sdm_conn.commit()

                    doel_na = count_rows(sdm_conn, doel_tabel)

                    log_sdm(
                        stap="LOAD",
                        bron_database=bron_db,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="insert",
                        aantal_bron=aantal_bron,
                        aantal_doel_voor=doel_voor,
                        aantal_verwerkt=aantal_bron,
                        aantal_doel_na=doel_na,
                        status="OK",
                        melding="data geladen naar SDM"
                    )

                    if doel_na - doel_voor == aantal_bron:
                        check_status = "OK"
                        check_msg = "aantal geladen rijen klopt"
                    else:
                        check_status = "WARNING"
                        check_msg = "aantal geladen rijen wijkt af"

                    log_sdm(
                        stap="CHECK",
                        bron_database=bron_db,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="rowcount_check",
                        aantal_bron=aantal_bron,
                        aantal_doel_voor=doel_voor,
                        aantal_verwerkt=doel_na - doel_voor,
                        aantal_doel_na=doel_na,
                        status=check_status,
                        melding=check_msg
                    )

                except Exception as e:
                    sdm_conn.rollback()
                    log_sdm(
                        stap="LOAD",
                        bron_database=bron_db,
                        bron_tabel=bron_tabel,
                        doel_tabel=doel_tabel,
                        actie="insert",
                        status="ERROR",
                        melding=str(e)
                    )

            bron_conn.close()

            log_sdm(
                stap="CONNECT",
                bron_database=bron_db,
                actie="close_database",
                status="OK",
                melding="bronconnectie gesloten"
            )

        except Exception as e:
            log_sdm(
                stap="CONNECT",
                bron_database=bron_db,
                actie="open_database",
                status="ERROR",
                melding=str(e)
            )

    # =========================
    # EINDAANTALLEN LOGGEN
    # =========================
    for table in SDM_TABLE_ORDER:
        try:
            aantal = count_rows(sdm_conn, table)
            log_sdm(
                stap="EINDCONTROLE",
                doel_tabel=table,
                actie="count",
                aantal_doel_na=aantal,
                status="OK",
                melding="eindaantal gecontroleerd"
            )
        except Exception as e:
            log_sdm(
                stap="EINDCONTROLE",
                doel_tabel=table,
                actie="count",
                status="ERROR",
                melding=str(e)
            )

    log_sdm(
        stap="EINDE",
        actie="end_pipeline",
        status="OK",
        melding="SDM full refresh succesvol afgerond"
    )

except Exception as e:
    log_sdm(
        stap="PIPELINE",
        actie="fatal_error",
        status="ERROR",
        melding=traceback.format_exc()
    )

finally:
    sdm_conn.close()

print("SDM klaar. Logbestand aangemaakt:", SDM_LOG_FILE)