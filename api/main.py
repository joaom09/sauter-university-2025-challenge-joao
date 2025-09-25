import os
import pandas as pd
import requests
import numpy as np
from dotenv import load_dotenv
load_dotenv()
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
BIGQUERY_ENA_TABLE = os.getenv("BIGQUERY_ENA_TABLE")
BIGQUERY_RESERVATORIO_TABLE = os.getenv("BIGQUERY_RESERVATORIO_TABLE")
ENA_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=ena-diario-por-bacia"
RESERVATORIO_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=reservatorio"

class IngestRequest(BaseModel):
    start_date: date
    end_date: date

@app.post("/ingest")
async def ingest_data(request: IngestRequest):
    start_year = request.start_date.year
    end_year = request.end_date.year
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        
        reservatorio_blob_path = "dimensao/reservatorios/reservatorio.parquet"
        blob = bucket.blob(reservatorio_blob_path)

        if not blob.exists():
            print("-> Reservoir file not found in the bucket. Downloading from ONS...")
            res_response = requests.get(RESERVATORIO_API_URL)
            res_response.raise_for_status()
            res_package_data = res_response.json()
            
            reservatorio_url = None
            if res_package_data.get("success"):
                for res in res_package_data.get("result", {}).get("resources", []):
                    if res.get("name", "").lower() == "reservatorios" and res.get("format", "").upper() == "PARQUET":
                        reservatorio_url = res["url"]
                        break
            
            if reservatorio_url:
                df_reservatorio = pd.read_parquet(reservatorio_url, engine='pyarrow')
                full_gcs_path = f"gs://{BUCKET_NAME}/{reservatorio_blob_path}"
                df_reservatorio.to_parquet(full_gcs_path, engine='pyarrow', index=False)
                print("-> Reservoir dimension data saved successfully.")
            else:
                print("WARNING: reservatorio.parquet resource not found in ONS metadata.")
        else:
            print("-> Reservoir file already exists in the bucket. Skipping download.")

        ena_response = requests.get(ENA_API_URL)
        ena_response.raise_for_status()
        ena_package_data = ena_response.json()
        
        file_urls = []
        if ena_package_data.get("success"):
            resources = ena_package_data.get("result", {}).get("resources", [])
            for res in resources:
                if res.get("format", "").upper() == "PARQUET" and "name" in res:
                    try:
                        year = int(res["name"].split('-')[-1])
                        if start_year <= year <= end_year:
                            file_urls.append(res["url"])
                    except (IndexError, ValueError): continue
        if not file_urls:
            raise HTTPException(status_code=404, detail="No ENA Parquet source files found.")

        total_records_saved = 0
        for url in file_urls:
            df_ena = pd.read_parquet(url, engine='pyarrow')
            df_ena.columns = df_ena.columns.str.lower().str.strip()
            df_ena.rename(columns={'ena_data': 'measurement_date'}, inplace=True)
            df_ena['measurement_date'] = pd.to_datetime(df_ena['measurement_date'])
            df_filtered = df_ena[(df_ena['measurement_date'].dt.date >= request.start_date) & (df_ena['measurement_date'].dt.date <= request.end_date)].copy()
            
            if df_filtered.empty: continue

            df_filtered['ano'] = df_filtered['measurement_date'].dt.year
            df_filtered['mes'] = df_filtered['measurement_date'].dt.month
            df_filtered['dia'] = df_filtered['measurement_date'].dt.day
            
            for (year, month, day), group in df_filtered.groupby(['ano', 'mes', 'dia']):
                path = f"fatos/ena/ano={year}/mes={month:02d}/dia={day:02d}/data.parquet"
                full_gcs_path = f"gs://{BUCKET_NAME}/{path}"
                group.to_parquet(full_gcs_path, engine='pyarrow', index=False)
                total_records_saved += len(group)
        print(f"-> ENA time-series data saved: {total_records_saved} records.")
        
        return {"status": "success", "message": "Ingestion process completed."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred during ingestion: {e}")

@app.get("/data")
async def get_data(page: int = Query(1, gt=0), size: int = Query(50, gt=0, le=1000)):
    bq_client = bigquery.Client()
    offset = (page - 1) * size
    
    ena_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_ENA_TABLE}"
    reservatorio_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_RESERVATORIO_TABLE}"

    try:
        query = f"""
            SELECT
              ena.*,
              res.id_reservatorio,
              res.nom_reservatorio
            FROM `{ena_table_id}` AS ena
            LEFT JOIN `{reservatorio_table_id}` AS res
              ON ena.nom_bacia = res.nom_bacia
            ORDER BY
              ena.measurement_date DESC
            LIMIT {size} OFFSET {offset}
        """
        results_df = bq_client.query(query).to_dataframe()
        
        count_query = f"SELECT COUNT(*) as total FROM `{ena_table_id}`"
        total_records = next(bq_client.query(count_query).result()).total
        total_pages = (total_records + size - 1) // size

        return {
            "total_records": total_records, "total_pages": total_pages,
            "current_page": page, "page_size": len(results_df),
            "data": results_df.replace({np.nan: None}).to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query error: {e}")