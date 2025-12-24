
import requests
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import gzip
from io import BytesIO
import json
from warcio.archiveiterator import ArchiveIterator
import pandas as pd
import re

app = Flask(__name__)


CC_INDICES = [
    "CC-MAIN-2008-2009", "CC-MAIN-2009-2010",
    "CC-MAIN-2012", "CC-MAIN-2013-20", "CC-MAIN-2013-48",
    "CC-MAIN-2014-10", "CC-MAIN-2014-15", "CC-MAIN-2014-23",
    "CC-MAIN-2014-35", "CC-MAIN-2014-41", "CC-MAIN-2014-42",
    "CC-MAIN-2014-49", "CC-MAIN-2014-52",
    "CC-MAIN-2015-06", "CC-MAIN-2015-11", "CC-MAIN-2015-14",
    "CC-MAIN-2015-18", "CC-MAIN-2015-22", "CC-MAIN-2015-27",
    "CC-MAIN-2015-35", "CC-MAIN-2015-40", "CC-MAIN-2015-48",
    "CC-MAIN-2016-07", "CC-MAIN-2016-18", "CC-MAIN-2016-22",
    "CC-MAIN-2016-26", "CC-MAIN-2016-30", "CC-MAIN-2016-36",
    "CC-MAIN-2016-40", "CC-MAIN-2016-44", "CC-MAIN-2016-50",
    "CC-MAIN-2017-04", "CC-MAIN-2017-09", "CC-MAIN-2017-13",
    "CC-MAIN-2017-17", "CC-MAIN-2017-22", "CC-MAIN-2017-26",
    "CC-MAIN-2017-30", "CC-MAIN-2017-34", "CC-MAIN-2017-39",
    "CC-MAIN-2017-43", "CC-MAIN-2017-47", "CC-MAIN-2017-51",
    "CC-MAIN-2018-05", "CC-MAIN-2018-09", "CC-MAIN-2018-13",
    "CC-MAIN-2018-17", "CC-MAIN-2018-22", "CC-MAIN-2018-26",
    "CC-MAIN-2018-30", "CC-MAIN-2018-34", "CC-MAIN-2018-39",
    "CC-MAIN-2018-43", "CC-MAIN-2018-47", "CC-MAIN-2018-51",
    "CC-MAIN-2019-04", "CC-MAIN-2019-09", "CC-MAIN-2019-13",
    "CC-MAIN-2019-18", "CC-MAIN-2019-22", "CC-MAIN-2019-26",
    "CC-MAIN-2019-30", "CC-MAIN-2019-35", "CC-MAIN-2019-39",
    "CC-MAIN-2019-43", "CC-MAIN-2019-47", "CC-MAIN-2019-51",
    "CC-MAIN-2020-05", "CC-MAIN-2020-10", "CC-MAIN-2020-16",
    "CC-MAIN-2020-24", "CC-MAIN-2020-29", "CC-MAIN-2020-34",
    "CC-MAIN-2020-40", "CC-MAIN-2020-45", "CC-MAIN-2020-50",
    "CC-MAIN-2021-04", "CC-MAIN-2021-10", "CC-MAIN-2021-17",
    "CC-MAIN-2021-21", "CC-MAIN-2021-25", "CC-MAIN-2021-31",
    "CC-MAIN-2021-39", "CC-MAIN-2021-43", "CC-MAIN-2021-49",
    "CC-MAIN-2022-05", "CC-MAIN-2022-21", "CC-MAIN-2022-27",
    "CC-MAIN-2022-33", "CC-MAIN-2022-40", "CC-MAIN-2022-49",
    "CC-MAIN-2023-06", "CC-MAIN-2023-14", "CC-MAIN-2023-23",
    "CC-MAIN-2023-40", "CC-MAIN-2023-50",
    "CC-MAIN-2024-10", "CC-MAIN-2024-18", "CC-MAIN-2024-22",
    "CC-MAIN-2024-26", "CC-MAIN-2024-30", "CC-MAIN-2024-33",
    "CC-MAIN-2024-38", "CC-MAIN-2024-42", "CC-MAIN-2024-46",
    "CC-MAIN-2024-51",
    "CC-MAIN-2025-05", "CC-MAIN-2025-08", "CC-MAIN-2025-13",
    "CC-MAIN-2025-18", "CC-MAIN-2025-21", "CC-MAIN-2025-26",
    "CC-MAIN-2025-30", "CC-MAIN-2025-33", "CC-MAIN-2025-38",
    "CC-MAIN-2025-43", "CC-MAIN-2025-47", "CC-MAIN-2025-51"
]

@app.route("/process", methods=["GET"])
def process():
    print("=== INICIO DE PROCESS ===", flush=True)
    domain = request.args.get("term")
    index = request.args.get("index")
    print(f"[DEBUG] Parámetro index recibido: '{index}'", flush=True)
    keyword = request.args.get("keyword")
    print(f"[DEBUG] Parámetro keyword recibido: '{keyword}'", flush=True)
    start_date = request.args.get("start")
    print(f"[DEBUG] Parámetro start_date recibido: '{start_date}'", flush=True)
    end_date = request.args.get("end")
    print(f"[DEBUG] Parámetro end_date recibido: '{end_date}'", flush=True)
    frequency = request.args.get("freq")
    print(f"[DEBUG] Parámetro frequency recibido: '{frequency}'", flush=True)

    #Crear los rangos entre start y end segun la frequency:
    date_ranges = []
    if frequency=="daily":     
        #Crear los rangos diarios entre start y end
        for date in pd.date_range(start=start_date, end=end_date, freq='D'):
            date_ranges.append(date.strftime('%Y-%m-%d'))
    elif frequency=="weekly":
        #Crear los rangos semanales entre start y end
        for date in pd.date_range(start=start_date, end=end_date, freq='W'):
            date_ranges.append(date.strftime('%Y-%m-%d'))
    elif frequency=="monthly":
        print("hello")
        #Crear los rangos mensuales entre start y end
        for date in pd.date_range(start=start_date, end=end_date, freq='ME'):
            date_ranges.append(date.strftime('%Y-%m-%d'))
    
    print(f"[DEBUG] Rangos de fechas creados: {date_ranges}", flush=True)

    if not domain:
        print("[DEBUG] Falta el parámetro term, devolviendo 400", flush=True)
        return jsonify({"error": "Missing term"}), 400
        if not index:
            print("[DEBUG] Falta el parámetro index, devolviendo 400")
            return jsonify({"error": "Missing index"}), 400
        indices_to_search = [index.strip()]

    # Permitir múltiples índices separados por coma y buscar solo en esos
    if index:
        indices_to_search = [i.strip() for i in index.split(',') if i.strip()]
        print(f"[DEBUG] Indices a buscar: {indices_to_search}", flush=True)
    else:
        indices_to_search = CC_INDICES
        print(f"[DEBUG] Usando todos los índices predefinidos", flush=True)

    # Si se pasa keyword, buscar páginas que la contengan usando warcio
    if keyword:
        max_results = 5  # Limitar para evitar sobrecarga
        matching_urls = []
        for idx in indices_to_search:
            url = (
                f"https://index.commoncrawl.org/{idx}-index"
                f"?url={domain}/*&output=json"
            )
            print(f"Consultando: {url}", flush=True)
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    lines = r.text.splitlines()
                    print(f"Líneas recibidas: {len(lines)}", flush=True)
                    for i, line in enumerate(lines):
                        if len(matching_urls) >= max_results:
                            break
                        record = None
                        try:
                            record = json.loads(line)
                        except Exception as e:
                            print(f"Error parseando línea JSON: {e}", flush=True)
                            continue
                        warc_filename = record.get("filename")
                        offset = int(record.get("offset", 0))
                        length = int(record.get("length", 0))
                        page_url = record.get("url", "")
                        # Descargar fragmento WARC
                        warc_url = f"https://data.commoncrawl.org/{warc_filename}"
                        headers = {"Range": f"bytes={offset}-{offset+length-1}"}
                        try:
                            warc_resp = requests.get(warc_url, headers=headers, timeout=20)
                            if warc_resp.status_code == 206:
                                with gzip.GzipFile(fileobj=BytesIO(warc_resp.content)) as gz:
                                    warc_stream = BytesIO(gz.read())
                                    for warc_record in ArchiveIterator(warc_stream):
                                        if warc_record.rec_type == 'response' and 'html' in warc_record.http_headers.get('Content-Type', '').lower():
                                            html_content = warc_record.content_stream().read().decode('utf-8', errors='ignore')
                                            soup = BeautifulSoup(html_content, 'html.parser')
                                            for script in soup(["script", "style"]):
                                                script.extract()
                                            text = soup.get_text(separator=' ', strip=True).lower()
                                            #Hallar la fecha en el texto
                                            # Pattern matches: "DD MMM YYYY"
                                            date_pattern = r'(\d{1,2}\s+\w{3}\s+\d{4}\s)'
                                            date_match = re.search(date_pattern, text)
                                            #print("Texto: ", text, flush=True)
                                            if date_match:
                                                fecha_encontrada = date_match.group(1)
                                                print("Fecha encontrada: ", fecha_encontrada, flush=True)
                                            else:
                                                print("No se encontró fecha en el formato esperado", flush=True)
                                            if keyword.lower() in text:
                                                matching_urls.append(page_url)
                                                print(f"Coincidencia encontrada: {page_url}", flush=True)
                                                break
                        except Exception as e:
                            print(f"Error al descargar/procesar WARC: {e}", flush=True)
            except Exception as e:
                print(f"Error al consultar {url}: {e}", flush=True)
            if len(matching_urls) >= max_results:
                break
        return jsonify({
            "domain": domain,
            "indices_searched": len(indices_to_search),
            "keyword": keyword,
            "matching_urls": matching_urls,
            "count": len(matching_urls)
        })

    # Si no hay keyword, solo contar
    total = 0
    for idx in indices_to_search:
        url = (
            f"https://index.commoncrawl.org/{idx}-index"
            f"?url={domain}&output=json"
        )
        print(f"Consultando: {url}", flush=True)
        try:
            r = requests.get(url, timeout=15)
            print(f"Status code: {r.status_code}", flush=True)
            print(f"Primeros 500 caracteres de respuesta:\n{r.text[:500]}", flush=True)
            if r.status_code == 200:
                lineas = r.text.splitlines()
                print(f"Líneas recibidas: {len(lineas)}", flush=True)
                total += len(lineas)
        except Exception as e:
            print(f"Error al consultar {url}: {e}", flush=True)

    print(f"Total news_count: {total}", flush=True)
    return jsonify({
        "domain": domain,
        "indices_searched": len(indices_to_search),
        "news_count": total
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
