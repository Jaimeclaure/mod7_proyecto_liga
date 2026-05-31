import pandas as pd
from pathlib import Path

#slug es una función que convierte un texto a minúsculas y reemplaza espacios y caracteres especiales por guiones bajos, para crear identificadores legibles y consistentes.
def slug(text: str) -> str:
	return (
		str(text)
		.lower()
		.replace(" ", "_")
		.replace("-", "_")
		.replace("/", "_")
		.replace(".", "")
	)

# Función para guardar DataFrame en formato Parquet
def to_parquet(df: pd.DataFrame, path: Path) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	df.to_parquet(path, index=False, engine="pyarrow")
	print(f"[OK] Archivo generado: {path}")