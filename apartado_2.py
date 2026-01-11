# José Herrera, Mateo Gómez-Acebo, Gonzalo Crespo y Diego Bertolín
# Adquisición de Datos - PROYECTO FINAL
# Ingeniería Matemática e Inteligenica Artificial
# ETSI ICAI
# apartado_2.py

import pandas as pd
import requests 
import time #para hacer pausas en los requests y evitar hacer demasiadas peticiones al servidor en poco tiempo
import os #permite crear carpetas y rutas

BASE = "https://api.jolpi.ca/ergast/f1" #url base de Jolpica (que replica el esquema de Ergast)


def peticion_json(url, params, max_reintentos=6, sleep_base=1.0):
    """
    Hemos tenido que implementar esta función para hacer las peticiones
    que hemos obtenido con ayuda de la inteligencia artificial ya que 
    generábamos demasiadas peticiones y daba error por lo que manejamos
    aquí los erroes y añadimos esperas para evitar que nuestro código
    termine antes de generar todo.
    
    Nuestro código tardará un poco más pero nos aseguraremos que tenemos
    todos los archivos creados para los años pedidos con este manejo de 
    las peticiones
    """
    for intento in range(max_reintentos):
        try:
            response = requests.get(url, params=params, timeout=60) #realizamos la peitción
            
            if response.status_code == 429: #en caso de rate limiting añadimos esperas y reintentamos
                time.sleep(sleep_base * (2 ** intento))
                continue

            response.raise_for_status()
            return response.json() #devolvemos el archivo json

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(sleep_base * (2 ** intento))
        except requests.exceptions.HTTPError:

            time.sleep(sleep_base * (2 ** intento))

    raise RuntimeError(f"No se pudo obtener respuesta tras {max_reintentos} intentos: {url}")

def rounds_por_temporadas(temporada):
    """
    Nos permite obtener una lista con el round de todas las carreras
    de la temporada pasada por argumento haciendo la petición con la url
    respectiva a ese año.
     
    Con la respuesta json obtenida gracias a la función anterior, navegamos
    por el diccionario hasta extraer la lista de carrera de ese año.
    
    Una vez tengamos todo listo simplemente itera y va guardando en formato
    entero el round de las carreras y los devuelve en una lista
    
    nota: limitamos los elementos máximos que devuelve una respuesta usando limit en parametros
    """
    url = f"{BASE}/{temporada}.json" #nos construimos la url para pedir el calendario de la temporada 
    data = peticion_json(url,params={"limit":1000})
    carreras = data["MRData"]["RaceTable"]["Races"] #extraemos usando la estructura del json la lista de carreras
    
    lista_rounds  = []
    for carrera in carreras:
        p = int(carrera["round"]) #de cada carrera guardamos el round y lo pasamos a entero (ya que estaba en string)
        lista_rounds.append(p)
    
    return lista_rounds

def obtener_numero_por_piloto_en_carrera(temporada, round_number):
    """
    Mapea driverId -> number (número usado en ESA carrera), para capturar cambios como el #1.
    """
    url = f"{BASE}/{temporada}/{round_number}/results.json"
    datos = peticion_json(url, params={"limit": 1000})
    races = datos["MRData"]["RaceTable"]["Races"]
    if not races:
        return {}

    results = races[0].get("Results", [])
    mapping = {}
    for r in results:
        driver_id = r["Driver"]["driverId"]
        number = r.get("number", "")  # número del coche en esa carrera
        mapping[driver_id] = number
    return mapping

def descargar_pitstops_carrera(temporada, numero_ronda, limit = 1000, sleep = 1.2):
    """
    Nos permite descargar todos los pit-stops de una carrera.
    
    nota:
        - limit es el máximo de filas que te devuelve en una respuesta
        - offset nos permite medir desde qué fila empezamos a devolver
    
    Devolvemos una lista de diccionarios y cada diccionario representa un 
    pit-stop.
     
    Ejemplo:
    [
        {"driverId": "max verstappen", "stop": "...", "duration":"...",...}
        ...
    ]
    
    Importante: usamos sleep para hacer pausas entre las peticiones. De esta manera 
    aseguramos ser respuetuosos con el rate limit. Habíamos hablado en clase de la 
    importancia de estos tiempos de espera para evitar sobrecargas en el servidor y 
    en nuestro código.
    
    """
    url = f"{BASE}/{temporada}/{numero_ronda}/pitstops.json" #creamos la url para obtener los pit-stops de una determinada carrera en una temporada concreta.
    
    lista_pitstops = [] #vamos guardando aquí los datos de todos los pitstops
    offset = 0 #para empezar a leer desde el princio
    
    while True: #no sabemos cúantas páginas habrá
        datos = peticion_json(url, params={"limit":limit,"offset":offset}) #hacemos la petición con la url bien definida y limitando la respuesta pero también usando offset para medir por donde retomamos en la siguiente vuelta
        carreras = datos["MRData"]["RaceTable"]["Races"] #extraemos la lista de carreras (solo hay 1 ya que fijamos temporada y carrera en la url)
        if not carreras: #en caso de no haber carreras, paramos y nos salimos del bucle infinito
            break
        
        #como la lista de carrera solo tiene 1, accedemos a esa carrera
        carrera = carreras[0]
        
        #carrera ahora es un diccionario que almacena los pit-stops de esa carrera con la clave PitStops
        pitstops = carrera.get("PitStops",[]) #en caso de no haber también evitamos error usando get en vez de acceder normalmente al diccionario
        #pitstops ahora es una lista de diccionarios y cada diccionario es un pit-stop
        
        if len(pitstops) == 0: #si no hay más pit-stops terminamos el bucle
            break
        
        lista_pitstops.extend(pitstops) #añadimos los diccionarios que tenemos dentro de esa lista (no la lista en sí, por ello no hacemos append)
        
        total = int(datos["MRData"].get("total", "0"))
        offset += limit #actualizamos para leer en la próxima petición desde donde nos hemos quedado
        
        # Si ya hemos leído todo, paramos
        if offset >= total:
            break
        
        time.sleep(sleep) #para evitar muchas peticiones en corto periodo de tiempo
    return lista_pitstops #devolvemos la lista con los diccionarios representando cada pitstop de esa carrera en esa temporada

def construir_dataframe_pitstops(temporada, round, num_piloto_dict):
    """
    Nos permite construir el dataframe final pedido por el enunciado.
    Tendremos una fila por piloto conteniendo las siguientes columnas
    [ DrivedId, DriverNumber, NPitstops, MedianPitsStopsDuration]
    
    Añadimos dos columnas adicionales muy útiles para poder identificar
    rápidamente la carrera: [Season, RaceNumber]
    
    Primero descargamos pitstops (la lista de diccionarios)
    
    Luego los convertimos a DataFrame y convertimos durationa float
    
    Agrupamos por driverId y calculamos NPitstops = número de filas
    y MedianPitStopDuration = mediana de duration
    
    Luego añadimos DriverNumber gracias al diccionario que asocia cada 
    driverId a su número por carrera
    
    Finalmente añadimos las columnas Season y RaceNumber 
    
    >>Devolvemos un dataframe con todas las columnas pedidas
    
    """
    
    filas_pitstops = descargar_pitstops_carrera(temporada,round) #usamos la función creada previamente para obtener la lista de diccionarios con la info de los pitstops
    
    if len(filas_pitstops) == 0: #en caso de no tener pit-stops devolvemos un dataframe vacío con las mismas columnas (importante para que luego funcione todo más adelante en el proyecto)
        return pd.DataFrame(columns=["Season","RaceNumber","DriverId","DriverNumber","NPitstops","MedianPitStopDuration"]) 
    
    dataframe_pitstops = pd.DataFrame(filas_pitstops)
    
    #pasamos la duración a float ya que viene como string
    dataframe_pitstops["duration"] = pd.to_numeric(dataframe_pitstops["duration"],errors="coerce")
    
    df_nuevo = dataframe_pitstops.groupby("driverId")["duration"].agg(NPitstops = "count", MedianPitStopDuration = "median").reset_index() #agrupamos por piloto y generamos las dos columnas que tenemos que añadir con la cuenta y la media de la duración
    df_nuevo.rename(columns={"driverId":"DriverId"},inplace=True) #ajustamos el nombre como lo pide el enunciado exactamente
    
    df_nuevo["DriverNumber"] = df_nuevo["DriverId"].map(num_piloto_dict).fillna("") #usamos el diccionario que une el id del piloto y su número para crear la columna DriverNumber con esos números por piloto
    
    #añadimos un identificador de la carrera y la temporada que nos permitirá cruzar más fácilmente luego
    df_nuevo["Season"] = int(temporada)
    df_nuevo["RaceNumber"] = int(round)
    
    df_nuevo = df_nuevo[["Season", "RaceNumber",
        "DriverId", "DriverNumber",
        "NPitstops", "MedianPitStopDuration"]]
    
    return df_nuevo



def save_race_df(df, out_dir, season, round_number):
    """
    Nos permite guardar el dataframe de una carrera como csv en su carpeta del 
    año correspondiente. 
    
    Nos crea una ruta, por ejemplo:
    data/2021/race_05_pitstops.csv
    
    No devuelve nada, solo escribe un archivo
    """
    filename = f"race_{int(round_number):02d}_pitstops.csv" #creamos los nombres que tendrán los csv que irán cambiando según la carrera y temporada
    path = os.path.join(out_dir, str(season), filename) #creamos la ruta completa

    os.makedirs(os.path.dirname(path), exist_ok=True) #manejamos directorio
    df.to_csv(path, index=False) #pasamos a csv


def run_part_ii(seasons, out_dir="data"):
    """
    Ejecutamos toda la parte II del proyecto.
    -Para cada temporada extraemos los rounds, construímos el diccionario
    que conecta driverId con su número respectivo
    -descargamos los pitstops de cada carrera
    -los resumimos por piloto
    -guardamos en csv
    
    """
    for season in seasons: #recorre las temporadas que le pasemos
        rounds = rounds_por_temporadas(season) #obtenemos la lista de rounds de la temporada
        

        for rnd in rounds: #recorremos cada rounds
            driver_map = obtener_numero_por_piloto_en_carrera(season,rnd) #generamos el diccionario que conecta piloto y número
            race_df = construir_dataframe_pitstops(season, rnd, driver_map) #construímos el dataframe de los pitstops
            save_race_df(race_df, out_dir, season, rnd) #guardamos en csv
            print(f"[TERMINADO] season={season} round={rnd} rows={len(race_df)}")
            time.sleep(1.2) #añadimos espera
            

if __name__ == "__main__":
    run_part_ii(seasons=[2019, 2020, 2021, 2022, 2023, 2024], out_dir="data")
