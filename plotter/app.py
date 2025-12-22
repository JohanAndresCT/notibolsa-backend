import requests
import matplotlib.pyplot as plt
import sys



term = sys.argv[1] if len(sys.argv) > 1 else "inflacion"
DATA_URL = f"http://aggregator:5000/aggregate?term={term}"
resp = requests.get(DATA_URL)
try:
	data = resp.json()
except Exception as e:
	print(f"Error al decodificar JSON. Código de estado: {resp.status_code}")
	print(f"Respuesta cruda: {resp.text}")
	raise

dates = [d["date"] for d in data]
news = [d["news"] for d in data]
colcap = [d["colcap"] for d in data]

plt.figure()
plt.plot(dates, news)
plt.plot(dates, colcap)
plt.title(f"Noticias vs COLCAP ({term})")
plt.xlabel("Fecha")
plt.ylabel("Valor")
plt.tight_layout()

plt.savefig(f"/tmp/{term}.png")
print(f"Grafico generado /tmp/{term}.png")
