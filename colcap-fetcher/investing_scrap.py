import json
import cloudscraper

# Create a cloudscraper session that can bypass Cloudflare
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'mobile': False
    }
)

start_date = "2025-09-04"
end_date = "2025-12-19"
time_frame = "Daily"

url = f"https://api.investing.com/api/financialdata/historical/49642?start-date={start_date}&end-date={end_date}&time-frame={time_frame}&add-missing-rows=false"

headers = {
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'es-419,es;q=0.9',
  'Domain-id': 'es',
  'Origin': 'https://es.investing.com',
  'Referer': 'https://es.investing.com/',
}

response = scraper.get(url, headers=headers)

print(f"Status Code: {response.status_code}")
#print(f"Response Headers: {response.headers}")
print("\nResponse Body:")

datos=json.loads(response.text)
#print(datos['data'])
for item in datos['data']:
    print(f"Fecha: {item['rowDate']}, Valor: {item['last_close']}")

