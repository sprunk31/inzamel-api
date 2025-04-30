from fastapi import FastAPI, Query, HTTPException, Depends, Header
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import os
import re

app = FastAPI()

# Databaseconfiguratie via omgevingsvariabelen (bijv. Railway)
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

# API-key beveiliging
def verify_api_key(x_api_key: str = Header(...)):
    expected_key = os.getenv("API_KEY")
    if x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Ongeldige API sleutel")

# Antwoordmodel
class AfvalCheckResponse(BaseModel):
    status: str

@app.get("/api/check-afval", response_model=AfvalCheckResponse)
def check_afval(
    postcode: str = Query(..., min_length=6, max_length=7),
    huisnummer: str = Query(...),
    huisnummertoevoeging: str = Query(None),
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
