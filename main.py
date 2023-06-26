import os
from requests_html import HTMLSession
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))



def parse_data(url: str, session: HTMLSession, name: str) -> None:
    r = session.get(url)

    table = r.html.find("table", first=True)

    table_data = [[cell.text for cell in row.find("td")] for row in table.find("tr")][1:]
    table_header = [[cell.text for cell in row.find("th")] for row in table.find("tr")][0]

    data = [dict(zip(table_header, t)) for t in table_data]

    df = pd.DataFrame(data)
    print(df.head(10))
    path = os.path.join(BASE_DIR, "data")
    os.makedirs(path, exist_ok=True)
    filename = os.path.join("data", f"movie_{name}.csv")
    df.to_csv(filename, index=False)
    print("Saved to CSV")

    return



def download_data(session: HTMLSession, start_year: int | None = None, years_ago: int = 0) -> None:
    if start_year is None:
        now = datetime.now()
        start_year = now.year

    assert len(f'{start_year}') == 4
    assert start_year <= datetime.now().year

    for _ in range(0, years_ago+1):
        url = f"https://www.boxofficemojo.com/year/world/{start_year}/"
        parse_data(url=url, session=session, name=start_year)
        print(f'Downloading {start_year} data...')
        start_year -= 1
    return


if __name__ == "__main__":
    s = HTMLSession()
    download_data(session=s, years_ago=5)
