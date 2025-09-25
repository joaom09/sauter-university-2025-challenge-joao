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

PROJECT_ID = os.getenv("PROJECT_ID")
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
            print("-> Downloading Reservoir data...")
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
                print("-> Reservoir data saved.")
        else:
            print("-> Reservoir file already exists. Skipping download.")

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
            
            df_ena['ena_data'] = pd.to_datetime(df_ena['ena_data'])
            
            df_filtered = df_ena[(df_ena['ena_data'].dt.date >= request.start_date) & (df_ena['ena_data'].dt.date <= request.end_date)].copy()
            
            if df_filtered.empty: continue

            df_filtered['ano'] = df_filtered['ena_data'].dt.year
            df_filtered['mes'] = df_filtered['ena_data'].dt.month
            df_filtered['dia'] = df_filtered['ena_data'].dt.day
            
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

    ena_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_ENA_TABLE}"
    reservatorio_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_RESERVATORIO_TABLE}"

    try:
        query = f"""
SELECT
  ena.nom_bacia AS nome_bacia_da_tabela_ena,
  res.* 
FROM
  `rowadi.bq_ml.reservatorios_ena` AS ena
LEFT JOIN
  `rowadi.bq_ml.reservatorios_dimensao` AS res
ON
  ena.nom_bacia = res.nom_bacia 
  
LIMIT 10;
        """

        print("--- DEBUG: Executando a seguinte query no BigQuery ---")
        print(query)
        print("----------------------------------------------------")

        results_df = bq_client.query(query).to_dataframe()

        # Converte datetime para string (ISO) antes de serializar
        if not results_df.empty:
            for col in results_df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
                results_df[col] = results_df[col].astype(str)

        # Corrige o COUNT (pega via dict)
        count_query = f"SELECT COUNT(*) as total FROM `{ena_table_id}`"
        row = next(bq_client.query(count_query).result())
        total_records = row["total"]

        total_pages = (total_records + size - 1) // size

        return {
            "total_records": total_records,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": len(results_df),
            "data": results_df.replace({np.nan: None}).to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query error: {e}")

