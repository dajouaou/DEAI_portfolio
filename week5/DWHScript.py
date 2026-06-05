import sqlite3
import pandas as pd
import csv
import traceback
from datetime import datetime

# =========================
# INSTELLINGEN
# =========================

SDM_DB = "BikeToDriveDatabase.db"
DWH_DB = "DWH_DB.db"
DWH_LOG_FILE = "dwh_log.csv"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# =========================
# LOGGING
# =========================

with open(DWH_LOG_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "run_id",
        "timestamp",
        "laag",
        "stap",
        "tabel",
        "actie",
        "aantal_sdm",
        "aantal_dwh_voor",
        "aantal_insert",
        "aantal_update",
        "aantal_skipped",
        "aantal_error",
        "aantal_dwh_na",
        "status",
        "melding"
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

def table_exists(conn, table):
    df = pd.read_sql_query("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, conn, params=(table,))
    return len(df) > 0

def count_rows(conn, table):
    if not table_exists(conn, table):
        return 0
    return pd.read_sql_query(f"SELECT COUNT(*) AS aantal FROM [{table}]", conn)["aantal"][0]

def norm(value):
    if pd.isna(value):
        return None
    return value

def changed(row, columns):
    for col in columns:
        sdm_value = norm(row[f"{col}_sdm"])
        dwh_value = norm(row[f"{col}_dwh"])
        if str(sdm_value) != str(dwh_value):
            return True
    return False

# =========================
# CONNECTIES
# =========================

sdm_conn = sqlite3.connect(SDM_DB)
dwh_conn = sqlite3.connect(DWH_DB)

dwh_conn.execute("PRAGMA foreign_keys = ON")

log_dwh("START", actie="start_pipeline", status="OK", melding="DWH pipeline gestart")

# =========================
# DWH TABELLEN AANMAKEN
# =========================

try:
    dwh_conn.executescript("""

    CREATE TABLE IF NOT EXISTS Klant (
        klant_key INTEGER PRIMARY KEY AUTOINCREMENT,
        business_key TEXT UNIQUE,
        source_id INTEGER,
        klantnr INTEGER,
        naam TEXT,
        woonplaats TEXT,
        adres TEXT,
        geslacht TEXT,
        geboortedatum TEXT
    );

    CREATE TABLE IF NOT EXISTS Product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        business_key TEXT UNIQUE,
        productnr INTEGER,
        product_type TEXT,
        soort TEXT,
        merk TEXT,
        type TEXT,
        kleur TEXT,
        fabrikant TEXT
    );

    CREATE TABLE IF NOT EXISTS Leverancier (
        lev_key INTEGER PRIMARY KEY AUTOINCREMENT,
        business_key TEXT UNIQUE,
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

    CREATE TABLE IF NOT EXISTS Datum (
        datum_key INTEGER PRIMARY KEY,
        datum TEXT,
        dag INTEGER,
        maand INTEGER,
        kwartaal INTEGER,
        jaar INTEGER
    );

    CREATE TABLE IF NOT EXISTS InkoopPeriode (
        periode_key INTEGER PRIMARY KEY,
        inkoopmaand INTEGER,
        inkoopjaar INTEGER
    );

    CREATE TABLE IF NOT EXISTS Verkoop (
        verkoop_key INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER,
        verkoopnr INTEGER,
        klant_key INTEGER,
        monteur_key INTEGER,
        product_key INTEGER,
        filiaal_key INTEGER,
        datum_key INTEGER,
        verkoopprijs REAL,
        aantal INTEGER
    );

    CREATE TABLE IF NOT EXISTS Onderhoud (
        onderhoud_key INTEGER PRIMARY KEY AUTOINCREMENT,
        onderhoudnr INTEGER,
        product_key INTEGER,
        monteur_key INTEGER,
        filiaal_key INTEGER,
        datum TEXT,
        starttijd TEXT,
        eindtijd TEXT,
        uurloon REAL
    );

    CREATE TABLE IF NOT EXISTS Inkoop (
        inkoop_key INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER,
        inkoopnr INTEGER,
        product_key INTEGER,
        periode_key INTEGER,
        lev_key INTEGER,
        aantal INTEGER,
        inkoopprijs REAL
    );

    """)

    dwh_conn.commit()

    log_dwh("CREATE", actie="create_tables", status="OK", melding="DWH tabellen bestaan of zijn aangemaakt")

except Exception as e:
    log_dwh("CREATE", actie="create_tables", status="ERROR", melding=str(e))
    raise

# =========================
# DIMENSIE KLANT - SCD TYPE 1
# =========================

try:
    before = count_rows(dwh_conn, "Klant")

    sdm_klant = pd.read_sql_query("""
        SELECT klantnr, naam, woonplaats, adres, geslacht, geboortedatum, 1 AS source_id
        FROM Fiets_Verkoop_Klant

        UNION ALL

        SELECT klantnr, naam, woonplaats, adres, geslacht, geboortedatum, 2 AS source_id
        FROM Accessoire_Verkoop_Klant
    """, sdm_conn)

    sdm_klant["business_key"] = sdm_klant["naam"].astype(str) + "_" + sdm_klant["geboortedatum"].astype(str)
    aantal_sdm = len(sdm_klant)
    sdm_klant = sdm_klant.drop_duplicates(subset=["business_key"])

    dwh_klant = pd.read_sql_query("""
        SELECT klant_key, business_key, source_id, klantnr, naam, woonplaats, adres, geslacht, geboortedatum
        FROM Klant
    """, dwh_conn)

    merged = sdm_klant.merge(dwh_klant, on="business_key", how="left", suffixes=("_sdm", "_dwh"))

    inserts = 0
    updates = 0
    skipped = 0

    for _, row in merged.iterrows():
        if pd.isna(row["klant_key"]):
            dwh_conn.execute("""
                INSERT INTO Klant (business_key, source_id, klantnr, naam, woonplaats, adres, geslacht, geboortedatum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["business_key"], row["source_id_sdm"], row["klantnr_sdm"], row["naam_sdm"],
                row["woonplaats_sdm"], row["adres_sdm"], row["geslacht_sdm"], row["geboortedatum_sdm"]
            ))
            inserts += 1
        else:
            if changed(row, ["source_id", "klantnr", "naam", "woonplaats", "adres", "geslacht", "geboortedatum"]):
                dwh_conn.execute("""
                    UPDATE Klant
                    SET source_id=?, klantnr=?, naam=?, woonplaats=?, adres=?, geslacht=?, geboortedatum=?
                    WHERE klant_key=?
                """, (
                    row["source_id_sdm"], row["klantnr_sdm"], row["naam_sdm"], row["woonplaats_sdm"],
                    row["adres_sdm"], row["geslacht_sdm"], row["geboortedatum_sdm"], int(row["klant_key"])
                ))
                updates += 1
            else:
                skipped += 1

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Klant")

    log_dwh("SCD1", "Klant", "insert_update", aantal_sdm, before, inserts, updates, skipped, 0, after, "OK", "Klant SCD1 verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("SCD1", "Klant", "insert_update", status="ERROR", melding=str(e))

# =========================
# DIMENSIE PRODUCT - SCD TYPE 1
# =========================

try:
    before = count_rows(dwh_conn, "Product")

    sdm_product = pd.read_sql_query("""
        SELECT accessoirenr AS productnr, 'accessoire' AS product_type, soort, naam AS merk, NULL AS type, NULL AS kleur, leverancier AS fabrikant
        FROM Accessoire_Verkoop_Accessoire

        UNION ALL

        SELECT accessoirenr AS productnr, 'accessoire' AS product_type, soort, naam AS merk, NULL AS type, NULL AS kleur, leverancier AS fabrikant
        FROM Accessoire_Inkoop_Accessoire

        UNION ALL

        SELECT fietsnr AS productnr, 'fiets' AS product_type, soort, merk, type, kleur, fabrikant
        FROM Fiets_Verkoop_Fiets

        UNION ALL

        SELECT fietsnr AS productnr, 'fiets' AS product_type, soort, merk, type, kleur, fabrikant
        FROM Fiets_Inkoop_Fiets
    """, sdm_conn)

    sdm_product["business_key"] = sdm_product["product_type"].astype(str) + "_" + sdm_product["productnr"].astype(str)
    aantal_sdm = len(sdm_product)
    sdm_product = sdm_product.drop_duplicates(subset=["business_key"])

    dwh_product = pd.read_sql_query("""
        SELECT product_key, business_key, productnr, product_type, soort, merk, type, kleur, fabrikant
        FROM Product
    """, dwh_conn)

    merged = sdm_product.merge(dwh_product, on="business_key", how="left", suffixes=("_sdm", "_dwh"))

    inserts = 0
    updates = 0
    skipped = 0

    for _, row in merged.iterrows():
        if pd.isna(row["product_key"]):
            dwh_conn.execute("""
                INSERT INTO Product (business_key, productnr, product_type, soort, merk, type, kleur, fabrikant)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["business_key"], row["productnr_sdm"], row["product_type_sdm"], row["soort_sdm"],
                row["merk_sdm"], row["type_sdm"], row["kleur_sdm"], row["fabrikant_sdm"]
            ))
            inserts += 1
        else:
            if changed(row, ["productnr", "product_type", "soort", "merk", "type", "kleur", "fabrikant"]):
                dwh_conn.execute("""
                    UPDATE Product
                    SET productnr=?, product_type=?, soort=?, merk=?, type=?, kleur=?, fabrikant=?
                    WHERE product_key=?
                """, (
                    row["productnr_sdm"], row["product_type_sdm"], row["soort_sdm"], row["merk_sdm"],
                    row["type_sdm"], row["kleur_sdm"], row["fabrikant_sdm"], int(row["product_key"])
                ))
                updates += 1
            else:
                skipped += 1

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Product")

    log_dwh("SCD1", "Product", "insert_update", aantal_sdm, before, inserts, updates, skipped, 0, after, "OK", "Product SCD1 verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("SCD1", "Product", "insert_update", status="ERROR", melding=str(e))

# =========================
# DIMENSIE LEVERANCIER - SCD TYPE 1
# =========================

try:
    before = count_rows(dwh_conn, "Leverancier")

    sdm_lev = pd.read_sql_query("""
        SELECT leveranciernr, naam, adres, woonplaats
        FROM Accessoire_Verkoop_Leverancier

        UNION ALL

        SELECT leveranciernr, naam, adres, woonplaats
        FROM Accessoire_Inkoop_Leverancier

        UNION ALL

        SELECT fabrikantnr AS leveranciernr, naam, adres, plaats AS woonplaats
        FROM Fiets_Verkoop_Fabrikant

        UNION ALL

        SELECT fabrikantnr AS leveranciernr, naam, adres, plaats AS woonplaats
        FROM Fiets_Inkoop_Fabrikant
    """, sdm_conn)

    sdm_lev["business_key"] = sdm_lev["naam"].astype(str) + "_" + sdm_lev["adres"].astype(str)
    aantal_sdm = len(sdm_lev)
    sdm_lev = sdm_lev.drop_duplicates(subset=["business_key"])

    dwh_lev = pd.read_sql_query("""
        SELECT lev_key, business_key, leveranciernr, naam, adres, woonplaats
        FROM Leverancier
    """, dwh_conn)

    merged = sdm_lev.merge(dwh_lev, on="business_key", how="left", suffixes=("_sdm", "_dwh"))

    inserts = 0
    updates = 0
    skipped = 0

    for _, row in merged.iterrows():
        if pd.isna(row["lev_key"]):
            dwh_conn.execute("""
                INSERT INTO Leverancier (business_key, leveranciernr, naam, adres, woonplaats)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row["business_key"], row["leveranciernr_sdm"], row["naam_sdm"], row["adres_sdm"], row["woonplaats_sdm"]
            ))
            inserts += 1
        else:
            if changed(row, ["leveranciernr", "naam", "adres", "woonplaats"]):
                dwh_conn.execute("""
                    UPDATE Leverancier
                    SET leveranciernr=?, naam=?, adres=?, woonplaats=?
                    WHERE lev_key=?
                """, (
                    row["leveranciernr_sdm"], row["naam_sdm"], row["adres_sdm"], row["woonplaats_sdm"], int(row["lev_key"])
                ))
                updates += 1
            else:
                skipped += 1

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Leverancier")

    log_dwh("SCD1", "Leverancier", "insert_update", aantal_sdm, before, inserts, updates, skipped, 0, after, "OK", "Leverancier SCD1 verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("SCD1", "Leverancier", "insert_update", status="ERROR", melding=str(e))

# =========================
# DATUM DIMENSIE
# =========================

try:
    before = count_rows(dwh_conn, "Datum")

    sdm_datum = pd.read_sql_query("""
        SELECT datum FROM Fiets_Verkoop
        UNION ALL
        SELECT datum FROM Accessoire_Verkoop
        UNION ALL
        SELECT datum FROM Onderhoud
    """, sdm_conn)

    aantal_sdm = len(sdm_datum)

    sdm_datum["datum"] = pd.to_datetime(sdm_datum["datum"], errors="coerce")
    sdm_datum = sdm_datum.dropna(subset=["datum"])
    sdm_datum = sdm_datum.drop_duplicates(subset=["datum"])

    sdm_datum["datum_key"] = sdm_datum["datum"].dt.strftime("%Y%m%d").astype(int)
    sdm_datum["dag"] = sdm_datum["datum"].dt.day
    sdm_datum["maand"] = sdm_datum["datum"].dt.month
    sdm_datum["kwartaal"] = sdm_datum["datum"].dt.quarter
    sdm_datum["jaar"] = sdm_datum["datum"].dt.year
    sdm_datum["datum"] = sdm_datum["datum"].dt.strftime("%Y-%m-%d")

    bestaande = pd.read_sql_query("SELECT datum_key FROM Datum", dwh_conn)

    nieuw = sdm_datum[~sdm_datum["datum_key"].isin(bestaande["datum_key"])]

    nieuw[["datum_key", "datum", "dag", "maand", "kwartaal", "jaar"]].to_sql(
        "Datum", dwh_conn, if_exists="append", index=False
    )

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Datum")

    log_dwh("DIM", "Datum", "insert", aantal_sdm, before, len(nieuw), 0, len(sdm_datum)-len(nieuw), 0, after, "OK", "Datum verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("DIM", "Datum", "insert", status="ERROR", melding=str(e))

# =========================
# INKOOPPERIODE DIMENSIE
# =========================

try:
    before = count_rows(dwh_conn, "InkoopPeriode")

    sdm_periode = pd.read_sql_query("""
        SELECT inkoopmaand, inkoopjaar FROM Fiets_Inkoop
        UNION ALL
        SELECT inkoopmaand, inkoopjaar FROM Accessoire_Inkoop
    """, sdm_conn)

    aantal_sdm = len(sdm_periode)
    sdm_periode = sdm_periode.drop_duplicates(subset=["inkoopmaand", "inkoopjaar"])

    bestaande = pd.read_sql_query("SELECT periode_key, inkoopmaand, inkoopjaar FROM InkoopPeriode", dwh_conn)

    nieuw = sdm_periode.merge(
        bestaande,
        on=["inkoopmaand", "inkoopjaar"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"').drop(columns=["_merge", "periode_key"])

    max_key = pd.read_sql_query("SELECT COALESCE(MAX(periode_key), 0) AS max_key FROM InkoopPeriode", dwh_conn)["max_key"][0]

    nieuw = nieuw.sort_values(["inkoopjaar", "inkoopmaand"]).reset_index(drop=True)
    nieuw["periode_key"] = range(int(max_key) + 1, int(max_key) + 1 + len(nieuw))

    nieuw[["periode_key", "inkoopmaand", "inkoopjaar"]].to_sql(
        "InkoopPeriode", dwh_conn, if_exists="append", index=False
    )

    dwh_conn.commit()
    after = count_rows(dwh_conn, "InkoopPeriode")

    log_dwh("DIM", "InkoopPeriode", "insert", aantal_sdm, before, len(nieuw), 0, len(sdm_periode)-len(nieuw), 0, after, "OK", "InkoopPeriode verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("DIM", "InkoopPeriode", "insert", status="ERROR", melding=str(e))

# =========================
# MONTEUR - SCD TYPE 2
# =========================

try:
    before = count_rows(dwh_conn, "Monteur")
    etl_tijd = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sdm_monteur = pd.read_sql_query("""
        SELECT monteurnr, naam, woonplaats, uurloon FROM Accessoire_Verkoop_Monteur
        UNION ALL
        SELECT monteurnr, naam, woonplaats, uurloon FROM Fiets_Verkoop_Monteur
        UNION ALL
        SELECT monteurnr, naam, woonplaats, uurloon FROM Onderhoud_Monteur
    """, sdm_conn)

    sdm_monteur["business_key"] = sdm_monteur["naam"].astype(str) + "_" + sdm_monteur["woonplaats"].astype(str)
    aantal_sdm = len(sdm_monteur)
    sdm_monteur = sdm_monteur.drop_duplicates(subset=["business_key"])

    dwh_monteur = pd.read_sql_query("""
        SELECT monteur_key, business_key, monteurnr, naam, woonplaats, uurloon, begin_tijd, eind_tijd
        FROM Monteur
        WHERE eind_tijd IS NULL
    """, dwh_conn)

    merged = sdm_monteur.merge(dwh_monteur, on="business_key", how="left", suffixes=("_sdm", "_dwh"))

    inserts = 0
    updates = 0
    skipped = 0

    for _, row in merged.iterrows():
        if pd.isna(row["monteur_key"]):
            dwh_conn.execute("""
                INSERT INTO Monteur (business_key, monteurnr, naam, woonplaats, uurloon, begin_tijd, eind_tijd)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
            """, (
                row["business_key"], row["monteurnr_sdm"], row["naam_sdm"], row["woonplaats_sdm"], row["uurloon_sdm"], etl_tijd
            ))
            inserts += 1
        else:
            if changed(row, ["monteurnr", "naam", "woonplaats", "uurloon"]):
                dwh_conn.execute("UPDATE Monteur SET eind_tijd=? WHERE monteur_key=?", (etl_tijd, int(row["monteur_key"])))

                dwh_conn.execute("""
                    INSERT INTO Monteur (business_key, monteurnr, naam, woonplaats, uurloon, begin_tijd, eind_tijd)
                    VALUES (?, ?, ?, ?, ?, ?, NULL)
                """, (
                    row["business_key"], row["monteurnr_sdm"], row["naam_sdm"], row["woonplaats_sdm"], row["uurloon_sdm"], etl_tijd
                ))
                inserts += 1
                updates += 1
            else:
                skipped += 1

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Monteur")

    log_dwh("SCD2", "Monteur", "historiseren", aantal_sdm, before, inserts, updates, skipped, 0, after, "OK", "Monteur SCD2 verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("SCD2", "Monteur", "historiseren", status="ERROR", melding=str(e))

# =========================
# FILIAAL - SCD TYPE 2
# =========================

try:
    before = count_rows(dwh_conn, "Filiaal")
    etl_tijd = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sdm_filiaal = pd.read_sql_query("""
        SELECT filiaalnr, naam, adres, provincie FROM Accessoire_Verkoop_Filiaal
        UNION ALL
        SELECT filiaalnr, naam, adres, provincie FROM Fiets_Verkoop_Filiaal
        UNION ALL
        SELECT filiaalnr, naam, adres, provincie FROM Onderhoud_Filiaal
    """, sdm_conn)

    sdm_filiaal["business_key"] = sdm_filiaal["adres"].astype(str)
    aantal_sdm = len(sdm_filiaal)
    sdm_filiaal = sdm_filiaal.drop_duplicates(subset=["business_key"])

    dwh_filiaal = pd.read_sql_query("""
        SELECT filiaal_key, business_key, filiaalnr, naam, adres, provincie, begin_tijd, eind_tijd
        FROM Filiaal
        WHERE eind_tijd IS NULL
    """, dwh_conn)

    merged = sdm_filiaal.merge(dwh_filiaal, on="business_key", how="left", suffixes=("_sdm", "_dwh"))

    inserts = 0
    updates = 0
    skipped = 0

    for _, row in merged.iterrows():
        if pd.isna(row["filiaal_key"]):
            dwh_conn.execute("""
                INSERT INTO Filiaal (business_key, filiaalnr, naam, adres, provincie, begin_tijd, eind_tijd)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
            """, (
                row["business_key"], row["filiaalnr_sdm"], row["naam_sdm"], row["adres_sdm"], row["provincie_sdm"], etl_tijd
            ))
            inserts += 1
        else:
            if changed(row, ["filiaalnr", "naam", "adres", "provincie"]):
                dwh_conn.execute("UPDATE Filiaal SET eind_tijd=? WHERE filiaal_key=?", (etl_tijd, int(row["filiaal_key"])))

                dwh_conn.execute("""
                    INSERT INTO Filiaal (business_key, filiaalnr, naam, adres, provincie, begin_tijd, eind_tijd)
                    VALUES (?, ?, ?, ?, ?, ?, NULL)
                """, (
                    row["business_key"], row["filiaalnr_sdm"], row["naam_sdm"], row["adres_sdm"], row["provincie_sdm"], etl_tijd
                ))
                inserts += 1
                updates += 1
            else:
                skipped += 1

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Filiaal")

    log_dwh("SCD2", "Filiaal", "historiseren", aantal_sdm, before, inserts, updates, skipped, 0, after, "OK", "Filiaal SCD2 verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("SCD2", "Filiaal", "historiseren", status="ERROR", melding=str(e))

# =========================
# FEIT VERKOOP
# =========================

try:
    before = count_rows(dwh_conn, "Verkoop")

    sdm_verkoop = pd.read_sql_query("""
        SELECT
            fv.fiets_verkoopnr AS verkoopnr,
            1 AS source_id,
            fv.datum,
            fv.aantal,
            fv.verkoopprijs,
            fv.klant,
            fv.fiets AS productnr,
            'fiets' AS product_type,
            fv.monteur,
            k.naam AS klant_naam,
            k.geboortedatum,
            m.naam AS monteur_naam,
            m.woonplaats AS monteur_woonplaats,
            f.adres AS filiaal_adres
        FROM Fiets_Verkoop fv
        JOIN Fiets_Verkoop_Klant k ON fv.klant = k.klantnr
        JOIN Fiets_Verkoop_Monteur m ON fv.monteur = m.monteurnr
        JOIN Fiets_Verkoop_Filiaal f ON m.filiaal = f.filiaalnr

        UNION ALL

        SELECT
            av.accessoire_verkoopnr AS verkoopnr,
            2 AS source_id,
            av.datum,
            av.aantal,
            av.verkoopprijs,
            av.klant,
            av.accessoire AS productnr,
            'accessoire' AS product_type,
            av.monteur,
            k.naam AS klant_naam,
            k.geboortedatum,
            m.naam AS monteur_naam,
            m.woonplaats AS monteur_woonplaats,
            f.adres AS filiaal_adres
        FROM Accessoire_Verkoop av
        JOIN Accessoire_Verkoop_Klant k ON av.klant = k.klantnr
        JOIN Accessoire_Verkoop_Monteur m ON av.monteur = m.monteurnr
        JOIN Accessoire_Verkoop_Filiaal f ON m.filiaal = f.filiaalnr
    """, sdm_conn)

    aantal_sdm = len(sdm_verkoop)

    sdm_verkoop["datum"] = pd.to_datetime(sdm_verkoop["datum"], errors="coerce")
    sdm_verkoop["datum_key"] = sdm_verkoop["datum"].dt.strftime("%Y%m%d").astype("Int64")

    sdm_verkoop["klant_business_key"] = sdm_verkoop["klant_naam"].astype(str) + "_" + sdm_verkoop["geboortedatum"].astype(str)
    sdm_verkoop["monteur_business_key"] = sdm_verkoop["monteur_naam"].astype(str) + "_" + sdm_verkoop["monteur_woonplaats"].astype(str)
    sdm_verkoop["product_business_key"] = sdm_verkoop["product_type"].astype(str) + "_" + sdm_verkoop["productnr"].astype(str)
    sdm_verkoop["filiaal_business_key"] = sdm_verkoop["filiaal_adres"].astype(str)

    klant_dim = pd.read_sql_query("SELECT klant_key, business_key FROM Klant", dwh_conn)
    product_dim = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
    monteur_dim = pd.read_sql_query("SELECT monteur_key, business_key FROM Monteur WHERE eind_tijd IS NULL", dwh_conn)
    filiaal_dim = pd.read_sql_query("SELECT filiaal_key, business_key FROM Filiaal WHERE eind_tijd IS NULL", dwh_conn)

    sdm_verkoop = sdm_verkoop.merge(klant_dim, left_on="klant_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_verkoop = sdm_verkoop.merge(product_dim, left_on="product_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_verkoop = sdm_verkoop.merge(monteur_dim, left_on="monteur_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_verkoop = sdm_verkoop.merge(filiaal_dim, left_on="filiaal_business_key", right_on="business_key", how="left").drop(columns=["business_key"])

    fact = sdm_verkoop[[
        "source_id", "verkoopnr", "klant_key", "monteur_key", "product_key",
        "filiaal_key", "datum_key", "verkoopprijs", "aantal"
    ]].copy()

    before_drop = len(fact)
    fact = fact.dropna(subset=["klant_key", "monteur_key", "product_key", "filiaal_key", "datum_key"])
    skipped_fk = before_drop - len(fact)

    for col in ["source_id", "verkoopnr", "klant_key", "monteur_key", "product_key", "filiaal_key", "datum_key"]:
        fact[col] = fact[col].astype(int)

    bestaand = pd.read_sql_query("SELECT source_id, verkoopnr FROM Verkoop", dwh_conn)

    nieuw = fact.merge(
        bestaand,
        on=["source_id", "verkoopnr"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"').drop(columns=["_merge"])

    nieuw.to_sql("Verkoop", dwh_conn, if_exists="append", index=False)

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Verkoop")

    log_dwh("FACT", "Verkoop", "insert_incremental", aantal_sdm, before, len(nieuw), 0, skipped_fk, 0, after, "OK", "Verkoop feittabel verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("FACT", "Verkoop", "insert_incremental", status="ERROR", melding=str(e))

# =========================
# FEIT ONDERHOUD
# =========================

try:
    before = count_rows(dwh_conn, "Onderhoud")

    sdm_onderhoud = pd.read_sql_query("""
        SELECT
            o.onderhoudnr,
            o.datum,
            o.starttijd,
            o.eindtijd,
            o.fiets AS productnr,
            'fiets' AS product_type,
            m.naam AS monteur_naam,
            m.woonplaats AS monteur_woonplaats,
            m.uurloon,
            f.adres AS filiaal_adres
        FROM Onderhoud o
        JOIN Onderhoud_Monteur m ON o.monteur = m.monteurnr
        JOIN Onderhoud_Filiaal f ON m.filiaal = f.filiaalnr
    """, sdm_conn)

    aantal_sdm = len(sdm_onderhoud)

    sdm_onderhoud["product_business_key"] = sdm_onderhoud["product_type"].astype(str) + "_" + sdm_onderhoud["productnr"].astype(str)
    sdm_onderhoud["monteur_business_key"] = sdm_onderhoud["monteur_naam"].astype(str) + "_" + sdm_onderhoud["monteur_woonplaats"].astype(str)
    sdm_onderhoud["filiaal_business_key"] = sdm_onderhoud["filiaal_adres"].astype(str)

    product_dim = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
    monteur_dim = pd.read_sql_query("SELECT monteur_key, business_key FROM Monteur WHERE eind_tijd IS NULL", dwh_conn)
    filiaal_dim = pd.read_sql_query("SELECT filiaal_key, business_key FROM Filiaal WHERE eind_tijd IS NULL", dwh_conn)

    sdm_onderhoud = sdm_onderhoud.merge(product_dim, left_on="product_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_onderhoud = sdm_onderhoud.merge(monteur_dim, left_on="monteur_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_onderhoud = sdm_onderhoud.merge(filiaal_dim, left_on="filiaal_business_key", right_on="business_key", how="left").drop(columns=["business_key"])

    fact = sdm_onderhoud[[
        "onderhoudnr", "product_key", "monteur_key", "filiaal_key",
        "datum", "starttijd", "eindtijd", "uurloon"
    ]].copy()

    before_drop = len(fact)
    fact = fact.dropna(subset=["product_key", "monteur_key", "filiaal_key"])
    skipped_fk = before_drop - len(fact)

    for col in ["onderhoudnr", "product_key", "monteur_key", "filiaal_key"]:
        fact[col] = fact[col].astype(int)

    bestaand = pd.read_sql_query("SELECT onderhoudnr FROM Onderhoud", dwh_conn)

    nieuw = fact[~fact["onderhoudnr"].isin(bestaand["onderhoudnr"])]

    nieuw.to_sql("Onderhoud", dwh_conn, if_exists="append", index=False)

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Onderhoud")

    log_dwh("FACT", "Onderhoud", "insert_incremental", aantal_sdm, before, len(nieuw), 0, skipped_fk, 0, after, "OK", "Onderhoud feittabel verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("FACT", "Onderhoud", "insert_incremental", status="ERROR", melding=str(e))

# =========================
# FEIT INKOOP
# =========================

try:
    before = count_rows(dwh_conn, "Inkoop")

    sdm_inkoop = pd.read_sql_query("""
        SELECT 
            ai.inkoopnr,
            1 AS source_id,
            ai.inkoopmaand,
            ai.inkoopjaar,
            ai.aantal,
            a.accessoirenr AS productnr,
            'accessoire' AS product_type,
            l.naam AS leverancier_naam,
            l.adres AS leverancier_adres,
            a.inkoopprijs
        FROM Accessoire_Inkoop ai
        JOIN Accessoire_Inkoop_Accessoire a ON ai.accessoire = a.accessoirenr
        JOIN Accessoire_Inkoop_Leverancier l ON a.leverancier = l.leveranciernr

        UNION ALL

        SELECT 
            fi.inkoopnr,
            2 AS source_id,
            fi.inkoopmaand,
            fi.inkoopjaar,
            fi.aantal,
            f.fietsnr AS productnr,
            'fiets' AS product_type,
            fab.naam AS leverancier_naam,
            fab.adres AS leverancier_adres,
            f.inkoopprijs
        FROM Fiets_Inkoop fi
        JOIN Fiets_Inkoop_Fiets f ON fi.fiets = f.fietsnr
        JOIN Fiets_Inkoop_Fabrikant fab ON f.fabrikant = fab.fabrikantnr
    """, sdm_conn)

    aantal_sdm = len(sdm_inkoop)

    sdm_inkoop["product_business_key"] = sdm_inkoop["product_type"].astype(str) + "_" + sdm_inkoop["productnr"].astype(str)
    sdm_inkoop["lev_business_key"] = sdm_inkoop["leverancier_naam"].astype(str) + "_" + sdm_inkoop["leverancier_adres"].astype(str)

    product_dim = pd.read_sql_query("SELECT product_key, business_key FROM Product", dwh_conn)
    periode_dim = pd.read_sql_query("SELECT periode_key, inkoopmaand, inkoopjaar FROM InkoopPeriode", dwh_conn)
    leverancier_dim = pd.read_sql_query("SELECT lev_key, business_key FROM Leverancier", dwh_conn)

    sdm_inkoop = sdm_inkoop.merge(product_dim, left_on="product_business_key", right_on="business_key", how="left").drop(columns=["business_key"])
    sdm_inkoop = sdm_inkoop.merge(periode_dim, on=["inkoopmaand", "inkoopjaar"], how="left")
    sdm_inkoop = sdm_inkoop.merge(leverancier_dim, left_on="lev_business_key", right_on="business_key", how="left").drop(columns=["business_key"])

    fact = sdm_inkoop[[
        "source_id", "inkoopnr", "product_key", "periode_key", "lev_key", "aantal", "inkoopprijs"
    ]].copy()

    before_drop = len(fact)
    fact = fact.dropna(subset=["product_key", "periode_key", "lev_key"])
    skipped_fk = before_drop - len(fact)

    for col in ["source_id", "inkoopnr", "product_key", "periode_key", "lev_key"]:
        fact[col] = fact[col].astype(int)

    bestaand = pd.read_sql_query("SELECT source_id, inkoopnr FROM Inkoop", dwh_conn)

    nieuw = fact.merge(
        bestaand,
        on=["source_id", "inkoopnr"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"').drop(columns=["_merge"])

    nieuw.to_sql("Inkoop", dwh_conn, if_exists="append", index=False)

    dwh_conn.commit()
    after = count_rows(dwh_conn, "Inkoop")

    log_dwh("FACT", "Inkoop", "insert_incremental", aantal_sdm, before, len(nieuw), 0, skipped_fk, 0, after, "OK", "Inkoop feittabel verwerkt")

except Exception as e:
    dwh_conn.rollback()
    log_dwh("FACT", "Inkoop", "insert_incremental", status="ERROR", melding=str(e))

# =========================
# EINDAANTALLEN
# =========================

for tabel in ["Klant", "Product", "Leverancier", "Monteur", "Filiaal", "Datum", "InkoopPeriode", "Verkoop", "Onderhoud", "Inkoop"]:
    try:
        aantal = count_rows(dwh_conn, tabel)
        log_dwh("EINDCONTROLE", tabel, "count", aantal_dwh_na=aantal, status="OK", melding="eindaantal gecontroleerd")
    except Exception as e:
        log_dwh("EINDCONTROLE", tabel, "count", status="ERROR", melding=str(e))

log_dwh("EINDE", actie="end_pipeline", status="OK", melding="DWH pipeline afgerond")

sdm_conn.close()
dwh_conn.close()

print("DWH klaar. Logbestand aangemaakt:", DWH_LOG_FILE)