# José Herrera, Mateo Gómez-Acebo, Gonzalo Crespo y Diego Bertolín
# Adquisición de Datos - PROYECTO FINAL
# Ingeniería Matemática e Inteligenica Artificial
# ETSI ICAI
# apartado_1.py

import scrapy
import pandas as pd
from pathlib import Path
from io import StringIO
from scrapy.crawler import CrawlerProcess

class QuoteSpyder(scrapy.Spider):
    name = "mi_arañita"
    start_urls = ["https://en.wikipedia.org/wiki/2023_Formula_One_World_Championship"] 
    #    Esta variable le hace una peticion al crawler de que debe empezar por ejecutar esa url, pero es el crawler quien decide cuando lo hace.
    #    Yield entrega la peticion a Scrapy y le deja el trabajo pendiente

    start_urls = [f"https://en.wikipedia.org/wiki/{year}_Formula_One_World_Championship" for year in range(2012, 2025)]

    def parse(self, response):
        tables = response.css("table.wikitable")    # obtenemos una lista de todas las tablas de la pagina web
        tabla = tables[3] # la cuarta tabla en la pagina web es la que nos interesa
        
        year = response.url.split("/")[-1].split("_")[0]
        
        rows = tabla.css("tr")[1:]    # quitamos cabecera

        for row in rows:    # por cada fila en la tabla
            cells = row.css("td")    # cogemos las celdas en esa fila
            if not cells:
                continue
            
            report_cell = cells[-1]    # la ultima celda es la del report
            link = report_cell.css("a::attr(href)").get()    # se coge el link de esa celda
            if not link:
                continue

            yield response.follow(link, callback=self.parse2, meta={"year": year})    # pedirle a scrpay que recorra esta web cuando pueda

    def parse2(self, response):
        dfs = pd.read_html(response.text)     # cogemos todas las tablas de la pagina web
        year = response.meta["year"]    # el año que estamos analizando lo cogemos de meta, que nos lo pasa la funcion parse

        race_df = None

        # quedarnos con la primera tabla que tenga columna Driver, porque en una pagina es la segunda pero en otras la cuarta
        for df in dfs:
            cols = [str(c) for c in df.columns]
            if "Driver" in cols and "Constructor" in cols:
                race_df = df
                break
        
        # si no encontramos tabla válida, salimos sin romper nada
        if race_df is None:
            return

        base_path = Path("data") / year     # si no existe la carpeta data, lo crea, pero no da error si ya existe
        base_path.mkdir(parents=True, exist_ok=True)

        race_name = response.url.split("/wiki/")[1].split("#")[0]     # para ponerle el nombre correspondiente al csv que se va a crear
        race_df.to_csv(base_path / f"{race_name}.csv", index=False)
        
        
def run_part_i():
    process = CrawlerProcess()
    process.crawl(QuoteSpyder)
    process.start()