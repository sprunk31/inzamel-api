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
    base_params = [f"%{f}%" for f in fractie_list] + [postcode]

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Stap 1: Haal pakket op van opgegeven adres
        cur.execute("""
            SELECT pakket FROM AANSLUITING_PAKKET
            WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NULL
            LIMIT 1
        """, [postcode, huisnummer_int])
        pakket_row = cur.fetchone()
        if not pakket_row:
            cur.close()
            conn.close()
            return []

        referentie_pakket = pakket_row["pakket"]

        # Stap 2: Loop door nabije huisnummers
        offset = 0
        max_offset = 50

        while offset <= max_offset:
            cur.execute(f"""
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
            """, base_params + [huisnummer_int, offset])

            result = cur.fetchone()
            if result:
                hn = int(result["huisnummer"])
                toevoeging = result["huisnummertoevoeging"]

                # Controleer of pakket hetzelfde is
                cur.execute("""
                    SELECT 1 FROM AANSLUITING_PAKKET
                    WHERE REPLACE(postcode, ' ', '') = %s AND huisnummer::INT = %s AND huisnummertoevoeging IS NOT DISTINCT FROM %s
                      AND pakket = %s
                    LIMIT 1
                """, [postcode, hn, toevoeging, referentie_pakket])

                if cur.fetchone():
                    gevonden_route = result["inzamelroute"]

                    # Haal 3 datums op voor de gevonden route
                    cur.execute("""
                        SELECT 
                            I.INZAMELROUTE,
                            I.DATUM,
                            A.POSTCODE,
                            A.HUISNUMMER
                        FROM
                            INZAMELROUTE AS I
                        LEFT JOIN AANSLUITING_INZAMELROUTE AS A ON A.INZAMELROUTE_ID = I.ID
                        WHERE
                            I.INZAMELROUTE = %s
                            AND I.DATUM > CURRENT_DATE
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
                            "postcode": row["postcode"],
                            "huisnummer": row["huisnummer"]
                        }
                        for row in rows
                    ]

            offset += 1

        cur.close()
        conn.close()
        return []

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
