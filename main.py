from fastapi import FastAPI, Query, HTTPException, Depends, Header
from typing import List, Optional
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import re
from fastapi.responses import JSONResponse
from datetime import date
import os

app = FastAPI()

config = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

def get_connection():
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"]
    )

# ✅ API Key beveiliging
def verify_api_key(x_api_key: str = Header(...)):
    expected_key = os.getenv("API_KEY")
    if x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Ongeldige API sleutel")

class RouteResult(BaseModel):
    inzamelroute: Optional[str] = None
    datum: Optional[date] = None
    postcode: Optional[str] = None
    huisnummer: Optional[str] = None
    huisnummertoevoeging: Optional[str] = None
    melding: Optional[str] = None

@app.get("/api/route", response_model=List[RouteResult])
def get_route(
    postcode: str = Query(..., min_length=6, max_length=7),
    huisnummer: str = Query(...),
    huisnummertoevoeging: Optional[str] = Query(None),
    fracties: str = Query(...),
    _: str = Depends(verify_api_key)
):
    postcode = postcode.replace(" ", "").upper()
    fractie_raw_list = [f.strip().upper() for f in fracties.split("/") if f.strip()]

    # Voeg MIX toe als RST opgegeven is
    fractie_list = []
    for f in fractie_raw_list:
        fractie_list.append(f)
        if f == "RST" and "MIX" not in fractie_list:
            fractie_list.append("MIX")

    huisnummer_int = int(re.match(r"\d+", huisnummer).group()) if huisnummer else 0
    like_clauses = " OR ".join(["A.INZAMELROUTE LIKE %s" for _ in fractie_list])
    base_params = [f"%{f}%" for f in fractie_list] + [postcode]

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"]
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Ophalen van het pakket van het opgegeven adres
        cur.execute("""
            SELECT pakket FROM AANSLUITING_PAKKET
            WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NOT DISTINCT FROM %s
            LIMIT 1
        """, [postcode, huisnummer_int, huisnummertoevoeging])
        pakket_row = cur.fetchone()
        referentie_pakket = pakket_row["pakket"] if pakket_row else None

        offset = 0
        max_offset = 50
        fallback_result = None

        # Reusable functie
        def fetch_and_return(route, pc, hn, toevoeging, melding=None, exact_match=False):
            cur.execute("""
                SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
                FROM INZAMELROUTE I
                LEFT JOIN AANSLUITING_INZAMELROUTE A ON A.INZAMELROUTE_ID = I.ID
                WHERE I.INZAMELROUTE = %s
                  AND I.DATUM > CURRENT_DATE
                  AND REPLACE(A.POSTCODE, ' ', '') = %s
                  AND A.HUISNUMMER::INT = %s
                  AND A.HUISNUMMERTOEVOEGING IS NOT DISTINCT FROM %s
                ORDER BY I.DATUM ASC
                LIMIT 3
            """, [route, pc, hn, toevoeging])
            rows = cur.fetchall()
            return [{
                "inzamelroute": r["inzamelroute"],
                "datum": r["datum"],
                "postcode": r["postcode"],
                "huisnummer": r["huisnummer"],
                "huisnummertoevoeging": r["huisnummertoevoeging"],
                "melding": "Aansluiting aanwezig op route" if exact_match else melding
            } for r in rows]

        # 1. Exacte match
        cur.execute(f"""
            SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
            FROM INZAMELROUTE I
            LEFT JOIN AANSLUITING_INZAMELROUTE A ON A.INZAMELROUTE_ID = I.ID
            WHERE I.DATUM > CURRENT_DATE AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
              AND A.HUISNUMMER::INT = %s
              AND A.HUISNUMMERTOEVOEGING IS NOT DISTINCT FROM %s
            ORDER BY I.DATUM ASC
            LIMIT 1
        """, base_params + [huisnummer_int, huisnummertoevoeging])
        exact = cur.fetchone()
        if exact:
            return fetch_and_return(
                exact["inzamelroute"],
                exact["postcode"],
                exact["huisnummer"],
                exact["huisnummertoevoeging"],
                exact_match=True
            )

        # 2. Zelfde huisnummer, andere toevoeging
        cur.execute(f"""
            SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
            FROM INZAMELROUTE I
            LEFT JOIN AANSLUITING_INZAMELROUTE A ON A.INZAMELROUTE_ID = I.ID
            WHERE I.DATUM > CURRENT_DATE AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
              AND A.HUISNUMMER::INT = %s
            ORDER BY I.DATUM ASC
            LIMIT 1
        """, base_params + [huisnummer_int])
        result = cur.fetchone()
        if result:
            cur.execute("""
                SELECT pakket FROM AANSLUITING_PAKKET
                WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NOT DISTINCT FROM %s
                LIMIT 1
            """, [postcode, int(result["huisnummer"]), result["huisnummertoevoeging"]])
            check = cur.fetchone()
            if check and check["pakket"] == referentie_pakket:
                return fetch_and_return(result["inzamelroute"], result["postcode"], result["huisnummer"], result["huisnummertoevoeging"])

        # 3. Nabije buren
        while offset <= max_offset:
            cur.execute(f"""
                SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
                FROM INZAMELROUTE I
                LEFT JOIN AANSLUITING_INZAMELROUTE A ON A.INZAMELROUTE_ID = I.ID
                WHERE I.DATUM > CURRENT_DATE AND ({like_clauses})
                  AND REPLACE(A.POSTCODE, ' ', '') = %s
                  AND ABS(A.HUISNUMMER::INT - %s) = %s
                ORDER BY I.DATUM ASC
                LIMIT 1
            """, base_params + [huisnummer_int, offset])
            result = cur.fetchone()
            if result:
                cur.execute("""
                    SELECT pakket FROM AANSLUITING_PAKKET
                    WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NOT DISTINCT FROM %s
                    LIMIT 1
                """, [postcode, int(result["huisnummer"]), result["huisnummertoevoeging"]])
                check = cur.fetchone()
                if check and referentie_pakket and check["pakket"] == referentie_pakket:
                    return fetch_and_return(result["inzamelroute"], result["postcode"], result["huisnummer"], result["huisnummertoevoeging"])
                elif not fallback_result:
                    fallback_result = {
                        "inzamelroute": result["inzamelroute"],
                        "postcode": result["postcode"],
                        "huisnummer": result["huisnummer"],
                        "huisnummertoevoeging": result["huisnummertoevoeging"]
                    }
            offset += 1

        # Fallback met melding
        if fallback_result:
            rows = fetch_and_return(
                fallback_result["inzamelroute"],
                fallback_result["postcode"],
                fallback_result["huisnummer"],
                fallback_result["huisnummertoevoeging"],
                melding="Let op: pakket komt niet overeen met uw adres. Mogelijk moet dit worden aangepast."
            )
            cur.close()
            conn.close()
            return rows

        cur.close()
        conn.close()
        return [{
            "inzamelroute": None,
            "datum": None,
            "postcode": postcode,
            "huisnummer": huisnummer,
            "huisnummertoevoeging": huisnummertoevoeging,
            "melding": "Geen inzamelroute gevonden voor dit adres en fractie(s)."
        }]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--

class AfvalCheckResponse(BaseModel):
    status: str

@app.get("/api/check-afval", response_model=AfvalCheckResponse)
def check_afval(
    postcode: str = Query(..., min_length=6, max_length=7),
    huisnummer: str = Query(...),
    huisnummertoevoeging: Optional[str] = Query(None),
    _: str = Depends(verify_api_key)
):
    postcode_clean = postcode.replace(" ", "").upper()
    huisnummer_int = int(re.match(r"\d+", huisnummer).group()) if huisnummer else 0

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT 1 FROM AANSLUITING_INZAMELROUTE
            WHERE REPLACE(postcode, ' ', '') = %s
              AND huisnummer::INT = %s
              AND huisnummertoevoeging IS NOT DISTINCT FROM %s
              AND inzamelroute ILIKE '%%Afval%%'
            LIMIT 1
        """, [postcode_clean, huisnummer_int, huisnummertoevoeging])

        resultaat = cur.fetchone()

        cur.close()
        conn.close()

        if resultaat:
            return {"status": "Aanwezig"}
        else:
            return {"status": "Koppelen aan afvalkalender"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
