import pandas as pd
import requests
import numpy as np
import os
from fastapi import FastAPI, HTTPException, Query
from datetime import date
from pydantic import BaseModel 

app = FastAPI(
    title="ONS Data API (Local)",
)

ONS_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=ear-diario-por-reservatorio"
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True) 

# POST request model
class IngestRequest(BaseModel):
    start_date: date
    end_date: date

# --- ENDPOINT POST ---
@app.post("/ingest")
async def ingest_data(request: IngestRequest):
    start_year = request.start_date.year
    end_year = request.end_date.year
    
    try:
        # Extraction
        response = requests.get(ONS_API_URL)
        response.raise_for_status()
        package_data = response.json()
        csv_urls = []
        if package_data.get("success"):
            resources = package_data.get("result", {}).get("resources", [])
            for res in resources:
                if res.get("format", "").upper() == "CSV" and "name" in res:
                    try:
                        year = int(res["name"].split('-')[-1])
                        if start_year <= year <= end_year:
                            csv_urls.append(res["url"])
                    except (IndexError, ValueError): continue
        if not csv_urls:
            raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado para o período.")

        # Transformation
        list_of_dfs = []
        for url in csv_urls:
            df = pd.read_csv(url, sep=';', encoding='utf-8')
            df.columns = df.columns.str.lower().str.strip()
            list_of_dfs.append(df)
        
        final_df = pd.concat(list_of_dfs, ignore_index=True)
        final_df['ear_data'] = pd.to_datetime(final_df['ear_data']).dt.date
        mask = (final_df['ear_data'] >= request.start_date) & (final_df['ear_data'] <= request.end_date)
        filtered_df = final_df.loc[mask]

        # Loading
        file_name = f"dados_{request.start_date}_a_{request.end_date}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        filtered_df.to_csv(file_path, index=False)
        
        return {
            "status": "sucesso",
            "message": f"Processo de ingestão concluído.",
            "file_saved": file_path,
            "records_saved": len(filtered_df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro interno durante a ingestão: {e}")


# --- ENDPOINT GET ---
@app.get("/data")
async def get_data(
    start_date: date = Query(..., description="Data de início do filtro."),
    end_date: date = Query(..., description="Data de fim do filtro.")
):
    page = 1
    size = 50

    source_file_path = None
    
    
    for filename in os.listdir(OUTPUT_DIR):
        if filename.startswith("dados_") and filename.endswith(".csv"):
            try: 
                parts = filename.replace("dados_", "").replace(".csv", "").split("_a_")
                file_start_date = date.fromisoformat(parts[0])
                file_end_date = date.fromisoformat(parts[1])


                if file_start_date <= start_date and file_end_date >= end_date:
                    source_file_path = os.path.join(OUTPUT_DIR, filename)
                    break 
            except (ValueError, IndexError):
                continue
                
    if not source_file_path:
        raise HTTPException(
            status_code=404, 
            detail=f"Nenhum arquivo de dados contendo o período de {start_date} a {end_date} foi encontrado. "
                   f"Execute a ingestão para um período maior primeiro."
        )

    try:
        df = pd.read_csv(source_file_path)
        
        df['ear_data'] = pd.to_datetime(df['ear_data']).dt.date
        mask = (df['ear_data'] >= start_date) & (df['ear_data'] <= end_date)
        filtered_df = df.loc[mask]

        start_index = (page - 1) * size
        end_index = start_index + size
        paginated_data = filtered_df.iloc[start_index:end_index]

        paginated_data_cleaned = paginated_data.replace({np.nan: None})

        return {
            "source_file": os.path.basename(source_file_path),
            "total_records_in_subset": len(filtered_df),
            "showing_records": len(paginated_data_cleaned),
            "data": paginated_data_cleaned.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro ao ler o arquivo de dados: {e}")
    
