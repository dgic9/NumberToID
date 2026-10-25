from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

import duckdb
from huggingface_hub import HfFileSystem


# =========================================================
# APP
# =========================================================

app = FastAPI()


# =========================================================
# DUCKDB + HUGGING FACE PUBLIC BUCKET
# =========================================================

con = duckdb.connect()

# Public / anonymous access
# No HF token required
hf_fs = HfFileSystem(token=False)

con.register_filesystem(hf_fs)


# =========================================================
# ERROR HANDLER
# =========================================================

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(
    request: Request,
    exc: StarletteHTTPException
):

    if exc.status_code == 404:

        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. Use /?number=XXXXXXXXXX",
                "Developer": "@shreeapi"
            }
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "detail": exc.detail,
            "Developer": "@shreepai"
        }
    )


# =========================================================
# SINGLE ROUTE
# /
#
# Example:
# /?number=XXXXXXXXXX
# =========================================================

@app.get("/")
def fetch_data(
    number: str = Query(None)
):

    # -----------------------------------------------------
    # Validate number
    # -----------------------------------------------------

    if (
        not number
        or not number.isdigit()
        or len(number) < 10
        or len(number) > 15
    ):

        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "Invalid parameter. Use /?number=XXXXXXXXXX",
                "Developer": "@shreeapi"
            }
        )


    # -----------------------------------------------------
    # Select shard based on last digit
    # -----------------------------------------------------

    last_digit = number[-1]


    # -----------------------------------------------------
    # Hugging Face Storage Bucket
    # -----------------------------------------------------

    primary_url = (
        "hf://buckets/"
        "CutehackX/hitek-data-bucket/"
        f"final_master_shard_{last_digit}.parquet"
    )

    alt_url = (
        "hf://buckets/"
        "CutehackX/hitek-data-bucket/"
        f"alt_master_shard_{last_digit}.parquet"
    )


    # -----------------------------------------------------
    # Query Parquet
    # -----------------------------------------------------

    try:

        query = """
            SELECT
                *,
                'Main' AS _record_type

            FROM read_parquet(?)

            WHERE CAST(mobile AS VARCHAR) = ?

            UNION ALL

            SELECT
                *,
                'Alt' AS _record_type

            FROM read_parquet(?)

            WHERE CAST(alt AS VARCHAR) = ?
        """


        result = con.execute(
            query,
            [
                primary_url,
                number,
                alt_url,
                number
            ]
        )


        # -------------------------------------------------
        # Convert result to dictionaries
        # -------------------------------------------------

        columns = [
            description[0]
            for description in result.description
        ]

        rows = result.fetchall()

        raw_results = []

        for row in rows:

            raw_results.append(
                dict(
                    zip(columns, row)
                )
            )


        # -------------------------------------------------
        # Separate Main / Alt records
        # -------------------------------------------------

        main_records = []
        alt_records = []


        for row in raw_results:

            rec_type = row.pop(
                "_record_type",
                None
            )

            if rec_type == "Main":

                main_records.append(row)

            elif rec_type == "Alt":

                alt_records.append(row)


        # -------------------------------------------------
        # No result
        # -------------------------------------------------

        if (
            not main_records
            and not alt_records
        ):

            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found",
                    "phone": number,
                    "Developer": "@shreeapi"
                }
            )


        # -------------------------------------------------
        # Success
        # -------------------------------------------------

        return {
            "status": "success",

            "Data": {
                "Main_Records": main_records,
                "Alt_Records": alt_records
            },

            "Developer": "@shreeapi"
        }


    # -----------------------------------------------------
    # Database / DuckDB error
    # -----------------------------------------------------

    except Exception as e:

        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": (
                    "Database processing error: "
                    f"{str(e)}"
                ),
                "Developer": "@shreeapi"
            }
        )


# =========================================================
# START SERVER
# =========================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080
    )
