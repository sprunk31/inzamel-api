from fastapi import FastAPI, Query, HTTPException
from typing import List
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import re
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
    params = [f"%{f}%" for f in fractie_list] + [postcode]

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        offset = 0
        max_offset = 50  # zoek binnen 50 huisnummers afstand

        while offset <= max_offset:
            query = f"""
                SELECT 
                    I.INZAMELROUTE, 
                    I.DATUM,
                    A.POSTCODE,
                    A.HUISNUMMER,
                    A.HUISNUMMERTOEVOEGING
                FROM
                    INZAMELROUTE AS I
                LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE_ID = I.ID
                WHERE
                    I.DATUM > CURRENT_DATE
                    AND ({like_clauses})
                    AND REPLACE(A.POSTCODE, ' ', '') = %s
                    AND ABS(A.HUISNUMMER::INT - %s) = %s
                ORDER BY I.DATUM ASC
                LIMIT 3
            """

            cur.execute(query, params + [huisnummer_int, offset])
            result = cur.fetchone()

            if result:
                # Controleer pakketovereenkomst
                pakket_query = """
                    SELECT 1 FROM AANSLUITING_PAKKET
                    WHERE REPLACE(POSTCODE, ' ', '') = %s AND HUISNUMMER::INT = %s AND HUISNUMMERTOEVOEGING IS NOT DISTINCT FROM %s
                      AND PAKKET = (
                          SELECT PAKKET FROM AANSLUITING_PAKKET
                          WHERE REPLACE(POSTCODE, ' ', '') = %s AND HUISNUMMER::INT = %s AND HUISNUMMERTOEVOEGING IS NOT DISTINCT FROM %s
                          LIMIT 1
                      )
                """
                cur.execute(
                    pakket_query,
                    [postcode, huisnummer_int, None, postcode, huisnummer_int, None]
                )
                if cur.fetchone():
                    cur.close()
                    conn.close()
                    return [
                        {
                            "inzamelroute": result["inzamelroute"],
                            "datum": result["datum"],
                            "postcode": result["postcode"],
                            "huisnummer": result["huisnummer"]
                        }
                    ]

            offset += 1

        cur.close()
        conn.close()
        return []

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
