import os
import pandas as pd
import requests
import numpy as np
import os
from fastapi import FastAPI, HTTPException, Query
from typing import List
from datetime import date
from pydantic import BaseModel
from google.cloud import storage, bigquery

app = FastAPI(
    title="ONS Data Pipeline API",
)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
BIGQUERY_TABLE_ID = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
ONS_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=ear-diario-por-reservatorio"


class IngestRequest(BaseModel):
    start_date: date
    end_date: date

# --- ENDPOINT POST ---
@app.post("/ingest")
async def ingest_data(request: IngestRequest):
    start_year = request.start_date.year
    end_year = request.end_date.year
    
    try:
        # 1. EXTRACTION
        response = requests.get(ONS_API_URL)
        response.raise_for_status()
        package_data = response.json()
        
        file_urls = []
        if package_data.get("success"):
            resources = package_data.get("result", {}).get("resources", [])
            for res in resources:
                if res.get("format", "").upper() == "PARQUET" and "name" in res:
                    try:
                        year = int(res["name"].split('-')[-1])
                        if start_year <= year <= end_year:
                            file_urls.append(res["url"])
                    except (IndexError, ValueError): continue
        if not file_urls:
            raise HTTPException(status_code=404, detail="No Parquet source files found for the specified period.")

        # 2. TRANSFORMATION AND LOAD
        print(f"Step 2: Processing {len(file_urls)} file(s)...")
        storage_client = storage.Client()
        total_records_saved = 0

        for url in file_urls:
            df = pd.read_parquet(url, engine='pyarrow')
            df.columns = df.columns.str.lower().str.strip()


            df['ear_data'] = pd.to_datetime(df['ear_data'])
            df_filtered = df[(df['ear_data'].dt.date >= request.start_date) & (df['ear_data'].dt.date <= request.end_date)].copy()
            
            if df_filtered.empty:
                continue

            df_filtered['ano'] = df_filtered['ear_data'].dt.year
            df_filtered['mes'] = df_filtered['ear_data'].dt.month
            df_filtered['dia'] = df_filtered['ear_data'].dt.day
            
            for (year, month, day), group in df_filtered.groupby(['ano', 'mes', 'dia']):
                path = f"ano={year}/mes={month:02d}/dia={day:02d}/data.parquet"
                full_gcs_path = f"gs://{BUCKET_NAME}/{path}"
                group.to_parquet(full_gcs_path, engine='pyarrow', index=False)
                total_records_saved += len(group)

        return {
            "status": "success",
            "message": "Ingestion process completed successfully.",
            "total_records_saved": total_records_saved
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred during ingestion: {e}")

# --- ENDPOINT GET ---
@app.get("/data")
async def get_data(
    page: int = Query(1, gt=0),
    size: int = Query(50, gt=0, le=1000)
):
    bq_client = bigquery.Client()
    offset = (page - 1) * size

    try:
        # Query to get the total count of records for pagination metadata
        count_query = f"SELECT COUNT(*) as total FROM `{BIGQUERY_TABLE_ID}`"
        count_job = bq_client.query(count_query)
        total_records = next(count_job.result()).total
        
        if total_records == 0:
            return {"total_records": 0, "data": []}

        # Query for the current page data
        query = f"""
            SELECT *
            FROM `{BIGQUERY_TABLE_ID}`
            ORDER BY ear_data DESC
            LIMIT {size} OFFSET {offset}
        """
        results_df = bq_client.query(query).to_dataframe()
        
        # Calculate total pages
        total_pages = (total_records + size - 1) // size

        return {
            "total_records": total_records,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": len(results_df),
            "data": results_df.replace({np.nan: None}).to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying BigQuery: {e}")