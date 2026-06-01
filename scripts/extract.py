"""
Módulo de extracción. Consume la API de football-data.org
y descarga los datos de Partidos y Posiciones a un directorio temporal.
"""
import requests
import time
import pandas as pd
import os
from typing import List, Tuple
from utils import setup_logger

logger = setup_logger()

def extract_datasets(temp_dir: str = "football_data") -> List[Tuple[str, str]]:
    """
    Extrae partidos y posiciones desde api.football-data.org.
    Retorna una lista de tuplas (nombre_tabla, ruta_archivo_local).
    """
    api_key = os.environ.get("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("La variable FOOTBALL_API_KEY no está definida.")

    headers = {"X-Auth-Token": api_key}
    base_url = "https://api.football-data.org/v4/competitions"
    
    # Extraeremos una muestra de ligas para no saturar el rate limit en el pipeline de CI/CD
    ligas = ["PL", "CL", "PD", "SA"] 

    try:
        os.makedirs(temp_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"Error al crear el directorio temporal: {e}")
        raise

    extracted_files = []
    
    # 1. Extracción de Partidos (Matches)
    logger.info("Iniciando extracción de partidos...")
    partidos_crudos = []
    for liga in ligas:
        url = f"{base_url}/{liga}/matches"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                partidos_crudos.extend(resp.json().get("matches", []))
                logger.info(f"Partidos de {liga} extraídos.")
            elif resp.status_code == 429:
                logger.warning("Rate limit alcanzado. Esperando 15s...")
                time.sleep(15)
                # Reintento simple
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    partidos_crudos.extend(resp.json().get("matches", []))
            time.sleep(2) # Pausa de cortesía
        except Exception as e:
            logger.error(f"Error extrayendo partidos de {liga}: {e}")

    if partidos_crudos:
        df_matches = pd.json_normalize(partidos_crudos, sep='_')
        columnas_deseadas = ['id', 'competition_name', 'utcDate', 'status', 'homeTeam_name', 'awayTeam_name', 'score_fullTime_home', 'score_fullTime_away', 'score_winner']
        df_matches = df_matches[[col for col in columnas_deseadas if col in df_matches.columns]].copy()
        
        # Estandarización de nombres para BigQuery
        df_matches.columns = ['match_id', 'liga', 'fecha', 'estado', 'equipo_local', 'equipo_visitante', 'goles_local', 'goles_visitante', 'ganador']
        df_matches['fecha'] = df_matches['fecha'].str[:10] # Solo fecha
        
        local_path = os.path.join(temp_dir, "matches.csv")
        df_matches.to_csv(local_path, index=False)
        extracted_files.append(("matches", local_path))

    # 2. Extracción de Posiciones (Standings)
    logger.info("Iniciando extracción de tabla de posiciones...")
    posiciones_crudos = []
    for liga in ligas:
        url = f"{base_url}/{liga}/standings"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                datos = resp.json()
                nombre_liga = datos.get('competition', {}).get('name', liga)
                standings_list = datos.get("standings", [])
                if standings_list:
                    tabla_json = standings_list[0].get("table", [])
                    for fila in tabla_json:
                        fila['liga_nombre'] = nombre_liga
                    posiciones_crudos.extend(tabla_json)
                logger.info(f"Posiciones de {liga} extraídas.")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error extrayendo posiciones de {liga}: {e}")

    if posiciones_crudos:
        df_standings = pd.json_normalize(posiciones_crudos, sep='_')
        columnas_standings = ['liga_nombre', 'position', 'team_name', 'playedGames', 'won', 'draw', 'lost', 'points', 'goalsFor', 'goalsAgainst', 'goalDifference']
        df_standings = df_standings[[col for col in columnas_standings if col in df_standings.columns]].copy()
        
        df_standings.columns = ['liga', 'posicion', 'equipo', 'partidos_jugados', 'victorias', 'empates', 'derrotas', 'puntos', 'goles_favor', 'goles_contra', 'diferencia_goles']
        local_path = os.path.join(temp_dir, "standings.csv")
        df_standings.to_csv(local_path, index=False)
        extracted_files.append(("standings", local_path))
            
    return extracted_files