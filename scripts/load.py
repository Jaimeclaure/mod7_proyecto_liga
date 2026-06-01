"""
Sube los parquet de data/raw/ a GCS.

Estructura en el bucket:
  {RAW_PREFIX}/{dataset}/{filename}.parquet

Ejemplos:
  raw/matches/matches_pl.parquet
  raw/players/players_pl.parquet
  raw/scorers/scorers_pl.parquet
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from utils import GCS_BUCKET, RAW_DIR, RAW_PREFIX, gcs_client

load_dotenv()


################ ARMADO DE RUTA GCS
# La función _gcs_path genera la ruta destino dentro del bucket, manteniendo la estructura por carpetas.
def _gcs_path(path: Path) -> str:
	"""
	Genera la ruta destino dentro del bucket.
	Ejemplo:
	data/raw/matches/matches_pl.parquet →   raw/matches/matches_pl.parquet
	"""

	relative = path.relative_to(RAW_DIR)

	if len(relative.parts) == 1:
		return f"{RAW_PREFIX}/consolidated/{path.name}"

	dataset = relative.parts[0]

	return f"{RAW_PREFIX}/{dataset}/{path.name}"


################ CARGA A GCS
# La función upload_raw busca todos los archivos parquet dentro de data/raw/ y los sube a GCS manteniendo la estructura por carpetas.
def upload_raw() -> None:
	"""
	Sube todos los archivos parquet encontrados dentro de data/raw/.

	Mantiene la estructura por carpetas:
	- matches
	- players
	- scorers
	- standings
	- teams
	"""

	parquets = sorted(RAW_DIR.rglob("*.parquet"))

	if not parquets:
		print("No hay archivos parquet en data/raw/")
		return

	client = gcs_client()
	bucket = client.bucket(GCS_BUCKET)

	for path in parquets:
		gcs_path = _gcs_path(path)

		blob = bucket.blob(gcs_path)
		blob.upload_from_filename(str(path))

		print(f"  subido: gs://{GCS_BUCKET}/{gcs_path}")

	print(f"\n{len(parquets)} archivo(s) subido(s).")


################ MAIN
if __name__ == "__main__":
	upload_raw()