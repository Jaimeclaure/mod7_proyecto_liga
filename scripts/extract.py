"""
Módulo de extracción de datos desde football-data.org.

Descarga datos de ligas europeas y los guarda en formato Parquet.

Estructura generada:
- data/raw/matches/matches_<liga>.parquet
- data/raw/standings/standings_<liga>.parquet
- data/raw/teams/teams_<liga>.parquet
- data/raw/players/players_<liga>.parquet
- data/raw/scorers/scorers_<liga>.parquet

También genera archivos consolidados:
- data/raw/all_matches.parquet
- data/raw/all_standings.parquet
- data/raw/all_teams.parquet
- data/raw/all_players.parquet
- data/raw/all_scorers.parquet
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils import slug, to_parquet


################ CONFIGURACIÓN BASE

# Se carga el archivo .env desde la raíz del proyecto.
load_dotenv(ROOT / ".env")

API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")

if not API_KEY:
	raise ValueError("No se encontró FOOTBALL_DATA_API_KEY en el archivo .env")

BASE_URL = "https://api.football-data.org/v4/competitions"

RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
	"X-Auth-Token": API_KEY
}

# Ligas consultadas en el proceso de extracción.
LIGAS_INTERESADAS = [
	"PL",   # Premier League
	"CL",   # UEFA Champions League
	"PD",   # LaLiga
	"SA",   # Serie A
	"BL1",  # Bundesliga
	"FL1",  # Ligue 1
	"DED",  # Eredivisie
	"PPL",  # Primeira Liga
]

# Pausa entre ligas para reducir el riesgo de bloqueo por límite de peticiones.
SLEEP_SECONDS = 6


################ CONEXIÓN A LA API
# La función get_json maneja la lógica de conexión, incluyendo reintentos en caso de alcanzar el límite de peticiones (429).
def get_json(url: str) -> dict | None:
	try:
		response = requests.get(url, headers=HEADERS, timeout=30)

		if response.status_code == 200:
			return response.json()

		if response.status_code == 429:
			print("[WARNING] Límite de API alcanzado. Esperando 60 segundos...")
			time.sleep(60)

			response = requests.get(
				url,
				headers=HEADERS,
				timeout=30
			)

			if response.status_code == 200:
				return response.json()

		print(f"[ERROR] Código {response.status_code}: {url}")
		print(response.text)
		return None

	except requests.RequestException as e:
		print(f"[ERROR] Fallo de conexión: {e}")
		return None


################ EXTRACCIONES
# Cada función de extracción se encarga de una entidad (matches, standings, teams, players, scorers) y sigue un patrón similar:
def extract_matches(liga: str) -> pd.DataFrame:
	print(f"\n[matches] Extrayendo partidos de {liga}...")

	url = f"{BASE_URL}/{liga}/matches"
	data = get_json(url)

	if not data:
		return pd.DataFrame()

	matches = data.get("matches", [])

	if not matches:
		print(f"[matches] Sin datos para {liga}")
		return pd.DataFrame()

	# Convierte el JSON anidado en una tabla plana.
	df = pd.json_normalize(matches, sep="_")

	# Permite identificar de qué liga proviene cada registro.
	df["liga_codigo"] = liga

	if "competition_name" not in df.columns:
		df["competition_name"] = data.get("competition", {}).get("name", liga)

	# La API entrega los árbitros como una lista.
	# Para análisis tabular se extrae el primer árbitro como principal.
	if "referees" in df.columns:
		df["arbitro_principal"] = df["referees"].apply(
			lambda x: x[0].get("name") if isinstance(x, list) and len(x) > 0 else None
		)
		df["arbitro_nacionalidad"] = df["referees"].apply(
			lambda x: x[0].get("nationality") if isinstance(x, list) and len(x) > 0 else None
		)
		df = df.drop(columns=["referees"])

	to_parquet(df, RAW_DIR / "matches" / f"matches_{slug(liga)}.parquet")

	print(f"[matches] {liga}: {len(df)} registros")
	return df

# Las siguientes funciones siguen la misma lógica: extraen datos específicos, los normalizan, agregan información de la liga y guardan el resultado en Parquet.
def extract_standings(liga: str) -> pd.DataFrame:
	print(f"\n[standings] Extrayendo posiciones de {liga}...")

	url = f"{BASE_URL}/{liga}/standings"
	data = get_json(url)

	if not data:
		return pd.DataFrame()

	nombre_liga = data.get("competition", {}).get("name", liga)
	standings = data.get("standings", [])

	if not standings:
		print(f"[standings] Sin tabla para {liga}")
		return pd.DataFrame()

	# Normalmente el primer elemento contiene la tabla general.
	rows = standings[0].get("table", [])

	for row in rows:
		row["liga_codigo"] = liga
		row["liga_nombre"] = nombre_liga

	df = pd.json_normalize(rows, sep="_")

	to_parquet(df, RAW_DIR / "standings" / f"standings_{slug(liga)}.parquet")

	print(f"[standings] {liga}: {len(df)} registros")
	return df

# La función extract_teams extrae los equipos de una liga, agrega información de la liga a cada equipo y guarda el resultado en Parquet.
def extract_teams(liga: str) -> pd.DataFrame:
	print(f"\n[teams] Extrayendo equipos de {liga}...")

	url = f"{BASE_URL}/{liga}/teams"
	data = get_json(url)

	if not data:
		return pd.DataFrame()

	teams = data.get("teams", [])
	nombre_liga = data.get("competition", {}).get("name", liga)

	if not teams:
		print(f"[teams] Sin equipos para {liga}")
		return pd.DataFrame()

	for team in teams:
		team["liga_codigo"] = liga
		team["liga_nombre"] = nombre_liga

	df = pd.json_normalize(teams, sep="_")

	to_parquet(df, RAW_DIR / "teams" / f"teams_{slug(liga)}.parquet")

	print(f"[teams] {liga}: {len(df)} registros")
	return df

# La función extract_players extrae los jugadores de cada equipo en una liga, agrega información del club y la liga a cada jugador, y guarda el resultado en Parquet.
def extract_players(liga: str) -> pd.DataFrame:
	print(f"\n[players] Extrayendo jugadores de {liga}...")

	url = f"{BASE_URL}/{liga}/teams"
	data = get_json(url)

	if not data:
		return pd.DataFrame()

	teams = data.get("teams", [])
	nombre_liga = data.get("competition", {}).get("name", liga)

	players = []

	for team in teams:
		team_id = team.get("id")
		team_name = team.get("name")
		team_short = team.get("shortName")
		squad = team.get("squad", [])

		for player in squad:
			# Se agregan datos del club al jugador para conservar la relación.
			player["club_id"] = team_id
			player["club_name"] = team_name
			player["club_short"] = team_short
			player["liga_codigo"] = liga
			player["liga_nombre"] = nombre_liga
			players.append(player)

	if not players:
		print(f"[players] Sin jugadores para {liga}")
		return pd.DataFrame()

	df = pd.json_normalize(players, sep="_")

	to_parquet(df, RAW_DIR / "players" / f"players_{slug(liga)}.parquet")

	print(f"[players] {liga}: {len(df)} registros")
	return df

# La función extract_scorers extrae los goleadores de una liga, agrega información de la liga a cada goleador y guarda el resultado en Parquet.
def extract_scorers(liga: str) -> pd.DataFrame:
	print(f"\n[scorers] Extrayendo goleadores de {liga}...")

	url = f"{BASE_URL}/{liga}/scorers"
	data = get_json(url)

	if not data:
		return pd.DataFrame()

	scorers = data.get("scorers", [])
	nombre_liga = data.get("competition", {}).get("name", liga)

	if not scorers:
		print(f"[scorers] Sin goleadores para {liga}")
		return pd.DataFrame()

	for scorer in scorers:
		scorer["liga_codigo"] = liga
		scorer["liga_nombre"] = nombre_liga

	df = pd.json_normalize(scorers, sep="_")

	to_parquet(df, RAW_DIR / "scorers" / f"scorers_{slug(liga)}.parquet")

	print(f"[scorers] {liga}: {len(df)} registros")
	return df


################ MAIN

def main() -> None:
	print("Iniciando extracción de datos de football-data.org...")

	all_matches = []
	all_standings = []
	all_teams = []
	all_players = []
	all_scorers = []

	for liga in LIGAS_INTERESADAS:
		matches = extract_matches(liga)
		standings = extract_standings(liga)
		teams = extract_teams(liga)
		players = extract_players(liga)
		scorers = extract_scorers(liga)

		if not matches.empty:
			all_matches.append(matches)

		if not standings.empty:
			all_standings.append(standings)

		if not teams.empty:
			all_teams.append(teams)

		if not players.empty:
			all_players.append(players)

		if not scorers.empty:
			all_scorers.append(scorers)

		time.sleep(SLEEP_SECONDS)

	# Archivos consolidados generales.
	if all_matches:
		to_parquet(pd.concat(all_matches, ignore_index=True), RAW_DIR / "all_matches.parquet")

	if all_standings:
		to_parquet(pd.concat(all_standings, ignore_index=True), RAW_DIR / "all_standings.parquet")

	if all_teams:
		to_parquet(pd.concat(all_teams, ignore_index=True), RAW_DIR / "all_teams.parquet")

	if all_players:
		to_parquet(pd.concat(all_players, ignore_index=True), RAW_DIR / "all_players.parquet")

	if all_scorers:
		to_parquet(pd.concat(all_scorers, ignore_index=True), RAW_DIR / "all_scorers.parquet")

	print("\nExtracción completada correctamente.")


if __name__ == "__main__":
	main()