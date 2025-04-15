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
    # Normaliseren input
    postcode = postcode.upper().replace(" ", "")
    fractie_list = [f.strip().upper() for f in fracties.split("/") if f.strip()]
    huisnummer_int = int(re.match(r"\d+", huisnummer).group()) if huisnummer else 0

    if not fractie_list:
        raise HTTPException(status_code=400, detail="Minimaal één fractie vereist.")

    like_clauses = " OR ".join(["A.INZAMELROUTE LIKE %s" for _ in fractie_list])
    params = [f"%{f}%" for f in fractie_list] + [postcode, huisnummer_int]

    query = f"""
        WITH filtered AS (
    SELECT 
        I.INZAMELROUTE, 
        I.DATUM, 
        A.POSTCODE, 
        A.HUISNUMMER, 
        ABS(A.HUISNUMMER::INT - %(huisnummer)s) AS diff,
        A.HUISNUMERTOEVOEGING
        FROM INZAMELROUTE AS I
        LEFT JOIN AANSLUITING_INZAMELROUTE AS A 
        ON A.INZAMELROUTE = I.INZAMELROUTE
    WHERE I.DATUM > CURRENT_DATE
      AND (
            -- Stel hier je dynamische LIKE condities in voor de fracties, bijvoorbeeld:
            A.INZAMELROUTE LIKE %(like_clause_1)s
            -- eventueel extra condities met OR voor meerdere fracties
          )
      AND REPLACE(A.POSTCODE, ' ', '') = %(postcode)s
)
SELECT INZAMELROUTE, DATUM, POSTCODE, HUISNUMMER
FROM filtered
WHERE diff = (SELECT MIN(diff) FROM filtered)
ORDER BY A.HUISNUMERTOEVOEGING, DATUM ASC
        LIMIT 3
    """

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return JSONResponse(content=[], status_code=200)

        return [dict(row) for row in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
