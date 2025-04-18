from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import re
from fastapi.responses import JSONResponse
from datetime import date
import os

app = FastAPI()

# Config ophalen uit environment variables (geschikt voor Railway)
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

class RouteResult(BaseModel):
    inzamelroute: str
    datum: date
    postcode: str
    huisnummer: str

@app.get("/api/route", response_model=List[RouteResult])
def get_route(
    postcode: str = Query(..., min_length=6, max_length=7),
    huisnummer: str = Query(...),
    fracties: str = Query(...)
):
    postcode = postcode.upper().replace(" ", "")
    fractie_list = [f.strip().upper() for f in fracties.split("/") if f.strip()]
    huisnummer_int = int(re.match(r"\d+", huisnummer).group()) if huisnummer else 0

    if not fractie_list:
        raise HTTPException(status_code=400, detail="Minimaal één fractie vereist.")

    like_clauses = " OR ".join(["I.INZAMELROUTE LIKE %s" for _ in fractie_list])
    fractie_params = [f"%{f}%" for f in fractie_list]

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Zoek het pakket van het opgegeven adres
        cur.execute("""
            SELECT pakket
            FROM aansluiting_pakket
            WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s
            ORDER BY LENGTH(COALESCE(huisnummertoevoeging, '')), huisnummertoevoeging
            LIMIT 1
        """, [postcode, huisnummer_int])
        pakket_row = cur.fetchone()

        if not pakket_row:
            cur.close()
            conn.close()
            return JSONResponse(content=[], status_code=200)

        pakket = pakket_row["pakket"]

        # Zoek dichtstbijzijnd adres met zelfde pakket
        cur.execute("""
            SELECT a.postcode, a.huisnummer, a.huisnummertoevoeging
            FROM aansluiting_pakket a
            WHERE a.pakket = %s
            ORDER BY ABS(a.huisnummer::INT - %s), LENGTH(COALESCE(a.huisnummertoevoeging, '')), a.huisnummertoevoeging
            LIMIT 1
        """, [pakket, huisnummer_int])
        adres_row = cur.fetchone()

        if not adres_row:
            cur.close()
            conn.close()
            return JSONResponse(content=[], status_code=200)

        match_postcode = adres_row["postcode"].replace(" ", "")
        match_huisnummer = int(adres_row["huisnummer"])

        # Haal de bijbehorende inzamelroutes op
        cur.execute(f"""
            SELECT 
                I.INZAMELROUTE, 
                I.DATUM,
                A.POSTCODE,
                A.HUISNUMMER
            FROM INZAMELROUTE AS I
            LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE_ID = I.ID
            WHERE I.DATUM > CURRENT_DATE
              AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
              AND A.HUISNUMMER::INT = %s
            ORDER BY I.DATUM ASC
            LIMIT 3
        """, fractie_params + [match_postcode, match_huisnummer])

        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return JSONResponse(content=[], status_code=200)

        return [dict(row) for row in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
