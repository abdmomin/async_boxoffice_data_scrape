import aiohttp
import asyncio
import pandas as pd
import os
from datetime import datetime
from bs4 import BeautifulSoup


BASE_DIR = os.path.dirname(os.path.abspath(__file__))



async def get_page(url: str, session: aiohttp.ClientSession):
    async with session.get(url) as response:
        return await response.text()



async def get_data(start_year: int | None = None, years_ago: int = 10):
    if start_year is None:
        start_year = datetime.now().year

    tasks = []

    async with aiohttp.ClientSession() as session:
        
        url = f'https://www.boxofficemojo.com/year/world/{start_year}/'
        for i in range(0, years_ago+1):
            task = asyncio.create_task(get_page(url=url, session=session))
            start_year -= i
            
            url = f'https://www.boxofficemojo.com/year/world/{start_year}/'
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results



def parse_data(data) -> bool:

    if len(data) == 0:
        return False

    for i, html in enumerate(data):
        if html is None:
            return False

        soup = BeautifulSoup(html, 'html.parser')

        table = soup.find("table")

        table_data = [[cell.text for cell in row.find_all("td")] for row in table.find_all("tr")][1:]
        table_header = [[cell.text for cell in row.find_all("th")] for row in table.find_all("tr")][0]

        data = [dict(zip(table_header, t)) for t in table_data]

        df = pd.DataFrame(data)
        print(df.head(10))
        path = os.path.join(BASE_DIR, "async_data")
        os.makedirs(path, exist_ok=True)
        filename = os.path.join("async_data", f"movie_{i}.csv")
        df.to_csv(filename, index=False)
        print("Saved to CSV")

    return True


if __name__ == "__main__":
    data = asyncio.run(get_data())
    parsed = parse_data(data)
    if parsed:
        print('Download Complete.')
    else:
        print('Download Failed!')
