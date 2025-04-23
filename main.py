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
    inzamelroute: str
    datum: date
    postcode: Optional[str] = None
    huisnummer: Optional[str] = None
    melding: Optional[str] = None

@app.get("/api/route", response_model=List[RouteResult])
def get_route(
    postcode: str = Query(..., min_length=6, max_length=7),
    huisnummer: str = Query(...),
    fracties: str = Query(...),
    _: str = Depends(verify_api_key)
):
    postcode = postcode.upper().replace(" ", "")
    fractie_raw_list = [f.strip().upper() for f in fracties.split("/") if f.strip()]

    # Voeg 'MIX' toe als 'RST' is opgegeven
    fractie_list = []
    for f in fractie_raw_list:
        fractie_list.append(f)
        if f == "RST" and "MIX" not in fractie_list:
            fractie_list.append("MIX")

    huisnummer_int = int(re.match(r"\d+", huisnummer).group()) if huisnummer else 0

    if not fractie_list:
        raise HTTPException(status_code=400, detail="Minimaal één fractie vereist.")

    like_clauses = " OR ".join(["A.INZAMELROUTE LIKE %s" for _ in fractie_list])
    base_params = [f"%{f}%" for f in fractie_list] + [postcode]

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT pakket FROM AANSLUITING_PAKKET
            WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NULL
            LIMIT 1
        """, [postcode, huisnummer_int])
        pakket_row = cur.fetchone()
        referentie_pakket = pakket_row["pakket"] if pakket_row else None

        offset = 0
        max_offset = 50
        fallback_result = None

        while offset <= max_offset:
            cur.execute(f"""
                SELECT 
                    I.INZAMELROUTE, 
                    I.DATUM,
                    A.POSTCODE,
                    A.HUISNUMMER,
                    A.HUISNUMMERTOEVOEGING
                FROM INZAMELROUTE AS I
                LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE_ID = I.ID
                WHERE I.DATUM > CURRENT_DATE
                    AND ({like_clauses})
                    AND REPLACE(A.POSTCODE, ' ', '') = %s
                    AND ABS(A.HUISNUMMER::INT - %s) = %s
                ORDER BY I.DATUM ASC
                LIMIT 3
            """, base_params + [huisnummer_int, offset])

            result = cur.fetchone()
            if result:
                hn = int(result["huisnummer"])
                toevoeging = result["huisnummertoevoeging"]

                cur.execute("""
                    SELECT pakket FROM AANSLUITING_PAKKET
                    WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NOT DISTINCT FROM %s
                    LIMIT 1
                """, [postcode, hn, toevoeging])

                pakket_check = cur.fetchone()

                if pakket_check and referentie_pakket and pakket_check["pakket"] == referentie_pakket:
                    gevonden_route = result["inzamelroute"]
                    cur.execute("""
                        SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER
                        FROM INZAMELROUTE AS I
                        LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE_ID = I.ID
                        WHERE I.INZAMELROUTE = %s AND REPLACE(A.POSTCODE, ' ', '') = %s AND A.HUISNUMMER::INT = %s
                        ORDER BY I.DATUM ASC
                        LIMIT 3
                    """, [gevonden_route, postcode, hn])
                    rows = cur.fetchall()
                    cur.close()
                    conn.close()
                    return [{"inzamelroute": row["inzamelroute"], "datum": row["datum"], "postcode": row["postcode"], "huisnummer": row["huisnummer"]} for row in rows]
                elif not fallback_result:
                    fallback_result = {
                        "inzamelroute": result["inzamelroute"],
                        "postcode": result["postcode"],
                        "huisnummer": result["huisnummer"]
                    }

            offset += 1

        if fallback_result:
            gevonden_route = fallback_result["inzamelroute"]
            cur.execute("""
                SELECT I.INZAMELROUTE, I.DATUM
                FROM INZAMELROUTE AS I
                WHERE I.INZAMELROUTE = %s AND I.DATUM > CURRENT_DATE
                ORDER BY I.DATUM ASC
                LIMIT 3
            """, [gevonden_route])
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [
                {
                    "inzamelroute": row["inzamelroute"],
                    "datum": row["datum"],
                    "postcode": fallback_result["postcode"],
                    "huisnummer": fallback_result["huisnummer"],
                    "melding": "Let op: pakket komt niet overeen met uw adres. Mogelijk moet dit worden aangepast."
                }
                for row in rows
            ]

        cur.close()
        conn.close()
        return []

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
