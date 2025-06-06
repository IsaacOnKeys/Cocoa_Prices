import requests
from bs4 import BeautifulSoup, FeatureNotFound

URL = "https://www.icco.org/home/"

def latest_icco_daily_price_eur() -> float:
    html = requests.get(URL, timeout=10).text
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")

    cell = soup.select_one("table#table_1 tbody tr[id^=table_]:last-of-type td:nth-of-type(6)")
    if not cell:
        raise RuntimeError("Price cell not found – table markup may have changed.")

    return float(cell.text.replace(",", ""))

if __name__ == "__main__":
    print(latest_icco_daily_price_eur())
