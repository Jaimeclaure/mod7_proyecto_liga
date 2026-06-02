import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


################ CONFIGURACIÓN GENERAL

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
RAW_DIR = ROOT / "data" / "raw"
GCS_BUCKET = "analisis_liga"
RAW_PREFIX = "raw"
GCP_SA_KEY = os.getenv("GCP_SA_KEY")


################ UTILIDADES

# La función slug convierte un texto a formato slug, útil para generar nombres de archivos.
def slug(text: str) -> str:
	return (
		str(text)
		.lower()
		.replace(" ", "_")
		.replace("-", "_")
		.replace("/", "_")
		.replace(".", "")
	)


# Guarda un DataFrame en formato Parquet.
def to_parquet(df: pd.DataFrame, path: Path) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	df.to_parquet(path, index=False, engine="pyarrow")
	print(f"[OK] Archivo generado: {path}")


################ GOOGLE CLOUD STORAGE
# El módulo utils.py también incluye funciones para interactuar con Google Cloud Storage, como gcs_client y upload_raw.
# La función gcs_client crea un cliente autenticado para Google Cloud Storage utilizando las credenciales proporcionadas en el archivo .env.
def gcs_client() -> storage.Client:
	if not GCP_SA_KEY:
		raise ValueError("No se encontró GCP_SA_KEY en las variables de entorno")
	
	credentials_info = json.loads(GCP_SA_KEY)
	credentials = service_account.Credentials.from_service_account_info(credentials_info)
	return storage.Client(project=credentials_info["project_id"], credentials=credentials)