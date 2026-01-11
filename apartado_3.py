# José Herrera, Mateo Gómez-Acebo, Gonzalo Crespo y Diego Bertolín
# Adquisición de Datos - PROYECTO FINAL
# Ingeniería Matemática e Inteligenica Artificial
# ETSI ICAI
# apartado_3.py

import pandas as pd
from pathlib import Path

def cargar_resultados(season_path):
    season = int(season_path.name)
    archivos = sorted(season_path.glob("*.csv"))
    resultados = []

    race_number = 1
    for archivo in archivos:
        if "pitstops" in archivo.name:
            continue

        try:
            df = pd.read_csv(archivo)

            if "Driver" not in df.columns or "No." not in df.columns:
                continue

            # Filtrar filas basura
            df = df[df["Driver"].apply(lambda x: isinstance(x, str) and "Source" not in x and "107%" not in x)]

            # Convertir No. a string SIEMPRE
            df["No."] = df["No."].astype(str).str.extract(r"(\d+)")  # extrae solo números
            df = df.dropna(subset=["No."])  # elimina filas sin número válido
            df["No."] = df["No."].astype(str)

            df["Season"] = season
            df["RaceNumber"] = race_number
            race_number += 1

            resultados.append(df)

        except Exception as e:
            print(f"[ERROR] leyendo {archivo.name}: {e}")

    if not resultados:
        print(f"[AVISO] No hay resultados válidos en {season_path}")
        return pd.DataFrame()

    return pd.concat(resultados, ignore_index=True)


def cargar_pitstops(season_path):
    archivos = sorted(season_path.glob("race_*_pitstops.csv"))
    pitstops = []

    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            df["DriverNumber"] = df["DriverNumber"].astype(str)
            pitstops.append(df)
        except Exception as e:
            print(f"[ERROR] leyendo {archivo.name}: {e}")

    if not pitstops:
        print(f"[AVISO] No hay pitstops en {season_path}")
        return pd.DataFrame()

    return pd.concat(pitstops, ignore_index=True)


def merge_datos(resultados, pitstops):
    if resultados.empty:
        return pd.DataFrame()

    if pitstops.empty:
        resultados["DriverId"] = ""
        resultados["DriverNumber"] = ""
        resultados["NPitstops"] = 0
        resultados["MedianPitStopDuration"] = None
        return resultados

    # Asegurar tipos homogéneos
    resultados["No."] = resultados["No."].astype(str)
    pitstops["DriverNumber"] = pitstops["DriverNumber"].astype(str)

    merged = resultados.merge(
        pitstops,
        left_on=["Season", "RaceNumber", "No."],
        right_on=["Season", "RaceNumber", "DriverNumber"],
        how="left"
    )
    return merged


def run_part_iii(data_dir="data", output_file="data/final_merged.csv"):
    base_path = Path(data_dir)
    temporadas = [p for p in base_path.iterdir() if p.is_dir() and p.name.isdigit()]

    todos = []

    for temporada in temporadas:
        print(f"[CARGANDO] Temporada {temporada.name}")

        resultados = cargar_resultados(temporada)
        pitstops = cargar_pitstops(temporada)

        merged = merge_datos(resultados, pitstops)

        if not merged.empty:
            todos.append(merged)

    if not todos:
        raise RuntimeError("No se pudo generar ningún DataFrame final. Revisa los CSV.")

    final_df = pd.concat(todos, ignore_index=True)
    final_df.to_csv(output_file, index=False)

    print(f"[FINALIZADO] CSV exportado a {output_file} con {len(final_df)} filas")
