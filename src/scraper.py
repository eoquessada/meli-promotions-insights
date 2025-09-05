import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd

# Configurações
URL = "https://www.mercadolivre.com.br/ofertas?container_id=MLB779362-1&page="
MAX_PAGES = 20
DB_PATH = Path("../scraper.db")


def get_response_text(url: str) -> str | None:
    """Fetch the content of the given URL and return the response text"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def beautify_text(response_text: str) -> BeautifulSoup | None:
    """Parse the response text with BeautifulSoup and return the BeautifulSoup object"""
    return BeautifulSoup(response_text, "html.parser") if response_text else None


def get_items_title(soup: BeautifulSoup) -> list[str]:
    """Extract and return the titles of promotion items"""
    if not soup:
        return []
    tags = soup.find_all("a", class_="poly-component__title")
    return [tag.get_text(strip=True) for tag in tags]


def get_items_prices(soup: BeautifulSoup) -> list[str]:
    """Extract and return the current prices of promotion items"""
    if not soup:
        return []
    prices = []
    divs = soup.find_all("div", class_="poly-price__current")
    for div in divs:
        reais = div.find("span", class_="andes-money-amount__fraction")
        centavos = div.find("span", class_="andes-money-amount__cents")
        reais_text = reais.get_text(strip=True) if reais else "0"
        centavos_text = centavos.get_text(strip=True) if centavos else "00"
        prices.append(f"R$ {reais_text},{centavos_text}")
    return prices


def get_items_discounts(soup: BeautifulSoup) -> list[str]:
    """Extract and return the discounts of promotion items"""
    if not soup:
        return []
    tags = soup.find_all("span", class_="andes-money-amount__discount")
    return [tag.get_text(strip=True) for tag in tags]


def get_timestamp(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Add a timestamp column to the DataFrame"""
    df[column] = datetime.now().isoformat()
    return df


def scrap_all_pages(base_url: str, max_pages: int, column_names: list[str]) -> pd.DataFrame:
    """Scrape multiple pages of offer products and return as DataFrame"""
    titles, prices, discounts = [], [], []

    for i in range(1, max_pages + 1):
        page_url = f"{base_url}{i}"
        response_text = get_response_text(page_url)
        soup = beautify_text(response_text)

        page_titles = get_items_title(soup)
        page_prices = get_items_prices(soup)
        page_discounts = get_items_discounts(soup)

        if not page_titles:
            break

        titles.extend(page_titles)
        prices.extend(page_prices)
        discounts.extend(page_discounts)

    all_products = zip(titles, prices, discounts)
    df = pd.DataFrame(all_products, columns=column_names)
    return get_timestamp(df, "included_in")

def save_to_db(df: pd.DataFrame, db_path: Path, table_name: str = "offers"):
    """Save the DataFrame to a SQLite database"""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.close()

if __name__ == "__main__":
    offers = scrap_all_pages(URL, MAX_PAGES, ["product", "price", "discount"])
    save_to_db(offers, DB_PATH, "offers")
    print(f"Scraped {len(offers)} items and saved to database.")

