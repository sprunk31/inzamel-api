from fastapi import FastAPI, Query, HTTPException
from typing import List
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import re
import json
from fastapi.responses import JSONResponse
from datetime import date
import os
from typing import Optional

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
    huisnummertoevoeging: Optional[str] = None

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

    like_clauses = " OR ".join(["A.INZAMELROUTE LIKE %s" for _ in fractie_list])
    params_base = [f"%{f}%" for f in fractie_list] + [postcode]

    queries = [
        # 1. Exact huisnummer zonder toevoeging
        (
            f"""
            SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
            FROM INZAMELROUTE AS I
            LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE = I.INZAMELROUTE
            WHERE I.DATUM > CURRENT_DATE
              AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
              AND A.HUISNUMMER::INT = %s
              AND A.HUISNUMMERTOEVOEGING IS NULL
            ORDER BY I.DATUM ASC
            LIMIT 3
            """,
            params_base + [huisnummer_int]
        ),

        # 2. Exact huisnummer mét toevoeging
        (
            f"""
            SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
            FROM INZAMELROUTE AS I
            LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE = I.INZAMELROUTE
            WHERE I.DATUM > CURRENT_DATE
              AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
              AND A.HUISNUMMER::INT = %s
              AND A.HUISNUMMERTOEVOEGING IS NOT NULL
            ORDER BY A.HUISNUMMERTOEVOEGING ASC, I.DATUM ASC
            LIMIT 3
            """,
            params_base + [huisnummer_int]
        ),

        # 3. Dichtstbijzijndste andere huisnummers
        (
            f"""
            SELECT I.INZAMELROUTE, I.DATUM, A.POSTCODE, A.HUISNUMMER, A.HUISNUMMERTOEVOEGING
            FROM INZAMELROUTE AS I
            LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE = I.INZAMELROUTE
            WHERE I.DATUM > CURRENT_DATE
              AND ({like_clauses})
              AND REPLACE(A.POSTCODE, ' ', '') = %s
            ORDER BY ABS(A.HUISNUMMER::INT - %s), A.HUISNUMMERTOEVOEGING ASC, I.DATUM ASC
            LIMIT 1
            """,
            params_base + [huisnummer_int]
        )
    ]

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for sql, params in queries:
            cur.execute(sql, params)
            rows = cur.fetchall()
            if rows:
                cur.close()
                conn.close()
                return [dict(row) for row in rows]

        cur.close()
        conn.close()
        return []

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
