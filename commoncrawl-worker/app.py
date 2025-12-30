
import requests
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import gzip
from io import BytesIO
import json
import tempfile
from warcio.archiveiterator import ArchiveIterator
import pandas as pd
import re
from datetime import datetime

app = Flask(__name__)

# Set to track seen titles and avoid duplicates
seen_titles = set()


def normalize_date(date_str: str) -> str | None:
    """
    Convert date string to ISO 8601 format for comparisons.
    Handles: DD/MM/YYYY HH:MM:SS and 2020-11-23T23:09:52.631Z formats.
    Returns: YYYY-MM-DDTHH:MM:SS or None if parsing fails.
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    
    # List of formats to try
    formats = [
        '%d/%m/%Y %H:%M:%S',    # 23/10/2020 13:45:00
        '%d/%m/%Y %H:%M',       # 23/10/2020 13:45
        '%Y-%m-%dT%H:%M:%S.%fZ', # 2020-11-23T23:09:52.631Z
        '%Y-%m-%dT%H:%M:%SZ',    # 2020-11-23T23:09:52Z
        '%Y-%m-%d %H:%M:%S',     # 2020-11-23 23:09:52
        '%Y-%m-%d',              # 2020-11-23
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    
    # If no format matches, return original string
    return date_str


def extract_date_from_soup(soup):
    """
    Extract publication date from BeautifulSoup object using multiple fallback methods.
    Returns: normalized date string or None
    """
    date_news = None
    
    # Method 1: JSON-LD (NewsArticle/Article/BlogPosting)
    try:
        for ld in soup.find_all('script', type='application/ld+json'):
            raw = ld.string or ld.get_text(strip=True)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and obj.get("@type") in ("NewsArticle", "Article", "BlogPosting"):
                    dp = obj.get("datePublished")
                    if dp:
                        date_news = dp
                        print(f"[DEBUG] datePublished (JSON-LD): {date_news}", flush=True)
                        return normalize_date(date_news)
    except Exception as e:
        print(f"[DEBUG] Error extrayendo JSON-LD: {e}", flush=True)

    # Method 2: Extract first_publish_date from Fusion.globalContent JavaScript object
    try:
        scripts = soup.find_all('script', type='application/javascript')
        for script in scripts:
            content = script.string or script.get_text()
            if content and 'Fusion.globalContent' in content:
                # Extract JSON from Fusion.globalContent=...;
                start = content.find('Fusion.globalContent=')
                if start != -1:
                    start += len('Fusion.globalContent=')
                    end = content.find(';', start)
                    if end != -1:
                        json_str = content[start:end].strip()
                        try:
                            data = json.loads(json_str)
                            first_pub = data.get('first_publish_date')
                            if first_pub:
                                date_news = first_pub
                                print(f"[DEBUG] first_publish_date (Fusion): {date_news}", flush=True)
                                return normalize_date(date_news)
                        except Exception as e:
                            print(f"[DEBUG] Error parsing Fusion.globalContent: {e}", flush=True)
    except Exception as e:
        print(f"[DEBUG] Error extrayendo first_publish_date: {e}", flush=True)
    
    print("[DEBUG] No se encontró la fecha en la página usando ningún método.", flush=True)
    return None


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
    start_date = request.args.get("start_date")
    print(f"[DEBUG] Parámetro start_date recibido: '{start_date}'", flush=True)
    end_date = request.args.get("end_date")
    print(f"[DEBUG] Parámetro end_date recibido: '{end_date}'", flush=True)

    #Crear los rangos entre start y end con frequency MS (month start):
    date_ranges = []
        #Crear los rangos mensuales entre start y end (primer día de cada mes)
    for date in pd.date_range(start=start_date, end=end_date, freq='MS'):
        print("Date: ", date, flush=True)
        date_ranges.append([date.strftime('%Y-%m-%d'), 0])
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
        max_results = 10  # Limitar para evitar sobrecarga
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

                                            #text = soup.get_text(separator=' ', strip=True).lower()

                                            if keyword.lower() in page_url:
                                                print(f"Coincidencia encontrada: {page_url}", flush=True)
                                                
                                                # Extract title
                                                title_tag = soup.find('title')
                                                title = title_tag.get_text(strip=True) if title_tag else "No title"
                                                print(f"[DEBUG] Title: {title}", flush=True)
                                                
                                                # Check if title already seen
                                                if title in seen_titles:
                                                    print(f"[DEBUG] Título duplicado, saltando: {title}", flush=True)
                                                    continue
                                                
                                                seen_titles.add(title)
                                                matching_urls.append(page_url)
                                                print(f"[DEBUG] Nuevo título agregado al conjunto. Total títulos únicos: {len(seen_titles)}", flush=True)
                                                

                                                # Intentar extraer datePublished desde JSON-LD (NewsArticle/Article/BlogPosting)
                                                date_news = extract_date_from_soup(soup)
                                                
                                                # Limpiar scripts y estilos para el resto del parseo
                                                for tag in soup(["script", "style"]):
                                                    tag.extract()
                                                
                                                if not date_news:
                                                    print("No se encontró la fecha en la página.", flush=True)
                                                else:
                                                    # Normalizar fecha para comparaciones
                                                    #normalized_date = normalize_date(date_news)
                                                    print(f"[DEBUG] Original date: {date_news}", flush=True)
                                                    #print(f"[DEBUG] Normalized date: {normalized_date}", flush=True)
                                                    #date_news = normalized_date
                                                
                                                # Contar el numero de noticias por rango de fecha
                                                for i in range(len(date_ranges)-1):
                                                    if date_news<=date_ranges[i][0]:
                                                        if i==0:
                                                            print(f"[DEBUG] La noticia del {date_news} cae antes del primer rango {date_ranges[i][0]}", flush=True)
                                                            
                                                        else:
                                                            if date_news>date_ranges[i-1][0]:
                                                                date_ranges[i-1][1] += 1
                                                                print(f"[DEBUG] Incrementando contador para rango {date_ranges[i-1]}", flush=True)
                                                    else:
                                                        print(f"[DEBUG] La noticia del {date_news} cae después del rango {date_ranges[i][0]}", flush=True)
                
                                                
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
            "count": len(matching_urls),
            "date_ranges_counts": date_ranges
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
