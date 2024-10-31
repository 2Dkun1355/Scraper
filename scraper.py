import os
import re
import sys
import csv
import json
import random
import asyncio

import aiohttp
from bs4 import BeautifulSoup
from slugify import slugify


class Scraper:
    BASE_URL = 'https://store.igefa.de'
    INTER_DATASET_PATH = 'intermediate_data.json'
    FINAL_DATASET_PATH = 'products.csv'
    CSV_FIELDNAMES = [
        'Product Name',
        'Original Data Column 1 (Breadcrumb)',
        'Original Data Column 2 (Ausführung)',
        'Supplier Article Number',
        'EAN/GTIN',
        'Article Number',
        'Product Description',
        'Supplier',
        'Supplier-URL',
        'Product Image URL',
        'Manufacturer',
        'Original Data Column 3 (Add. Description)',
    ]
    DELAY = 0.01

    def __init__(self, *args, **kwargs):
        self._setup()
        self.urls_to_scrape = []

    def _setup(self, *args, **kwargs):
        if not os.path.isfile(self.INTER_DATASET_PATH):
            with open(self.INTER_DATASET_PATH, 'w') as f:
                json.dump({}, f, indent=4)

    @staticmethod
    def _get_proxy(*args, **kwargs):
        with open("proxies.txt") as f:
            proxies = [line.strip() for line in f.readlines()]
            return random.choice(proxies)

    @staticmethod
    def get_value(tag, *args, **kwargs):
        try:
            return tag.text if tag else None
        except Exception:
            return None

    async def scrape(self, session, *args, **kwargs):
        """
        Start scraping process.
        Forms a list of products urls for scraping.
        Run scraping products
        """

        tasks = []
        for page_id in range(1, 5):  # count pages == 500
            url = f'{self.BASE_URL}/c/kategorien/f4SXre6ovVohkGNrAvh3zR?page={page_id}'
            task = asyncio.create_task(self.get_products_urls(session, url))
            tasks.append(task)
            if self.DELAY:
                await asyncio.sleep(self.DELAY)
        urls_to_scrape = await asyncio.gather(*tasks, return_exceptions=True)
        self.urls_to_scrape = [l for ls in urls_to_scrape for l in ls if isinstance(ls, set)]
        print(f'* SCRAPED  {len(self.urls_to_scrape)} PRODUCT URLS')
        await self.scrape_products(session)

    async def get_products_urls(self, session, url: str, *args, **kwargs):
        """ Return a list of products urls for scraping. """

        urls = []
        response = await session.get(url)
        if response.status == 200:
            html = await response.text()
            items = re.findall(r'"mainVariant":\s*{.*?"id":"(.*?)".*?"slug":"(.*?)".*?}', html, re.DOTALL)
            for product_id, slug in items:
                url = f"{self.BASE_URL}/p/{slug}/{product_id}/"
                urls.append(url)
            urls_to_scrape = set(urls) - set(self._get_processed_urls())
            return urls_to_scrape
        return []

    async def scrape_products(self, session, *args, **kwargs):
        """ Scraping detail products urls. """

        tasks = []
        for product_url in self.urls_to_scrape:
            task = asyncio.create_task(self.parse_product(session, product_url))
            tasks.append(task)
            if self.DELAY:
                await asyncio.sleep(self.DELAY)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def parse_product(self, session, url: str, *args, **kwargs):
        """ Parse product html and forms data """

        data = {}
        response = await session.get(url)
        if response.status == 200:
            try:
                html = await response.text()
                soup = BeautifulSoup(html, features='html.parser')

                product_name = self.get_value(soup.find('h1', {'data-testid': 'pdp-product-info-product-name'}))
                product_description = self.get_value(soup.find('div', {'class': 'ProductDescription_description__4e5b7'}))
                supplier = "igefa Handelsgesellschaft"
                product_image = soup.find('img', {'class': 'image-gallery-image'})['src']
                original_data_column = self.get_value(soup.find('div', {'class': re.compile(r'ProductBenefits_productBenefits__*')}))

                breadcrumbs_raw = soup.find_all('span', {'class': re.compile(r'ant-typography CategoryBreadcrumbs_breadcrumb__*')})
                breadcrumbs = '/'.join([slugify(b.text) for b in breadcrumbs_raw])

                version_raw = soup.find('div', {'class': re.compile(r'ant-typography ant-typography-secondary ProductCard_paragraph__*')})
                version = version_raw.text.replace('Ausführung:', '').strip() if version_raw else None

                supplier_number_raw = soup.find('div', {'class': re.compile(r'ant-typography ant-typography-secondary ProductCard_paragraph__*'), 'data-testid': 'product-information-sku'})
                supplier_number = supplier_number_raw.text.replace('Artikelnummer:', '').strip() if supplier_number_raw else None

                ean_gtin_raw = soup.find('div', {'class': re.compile(r'ant-typography ant-typography-secondary ProductCard_paragraph__*'), 'data-testid': 'product-information-gtin'})
                ean_gtin = re.search(r'\d+', ean_gtin_raw.text).group() if ean_gtin_raw else None

                article_number_raw = soup.find('div', {'class': re.compile(r'ProductInformation_variantInfo__*')})
                article_number_raw = re.search(r'Herstellernummer:\s*(\d+)', article_number_raw.text) if article_number_raw else None
                article_number = article_number_raw.group(1).strip() if article_number_raw else None

                manufacturer_raw = soup.find('td', string="Hersteller")
                manufacturer_raw = manufacturer_raw.find_next_sibling('td')
                manufacturer = manufacturer_raw.text.strip() if manufacturer_raw else None

                data.update({
                    'Product Name': product_name,
                    'Original Data Column 1 (Breadcrumb)': breadcrumbs,
                    'Original Data Column 2 (Ausführung)': version,
                    'Supplier Article Number': supplier_number,
                    'EAN/GTIN': ean_gtin,
                    'Article Number': article_number,
                    'Product Description': product_description,
                    'Supplier': supplier,
                    'Supplier-URL': url,
                    'Product Image URL': product_image,
                    'Manufacturer': manufacturer,
                    'Original Data Column 3 (Add. Description)': original_data_column,
                })
                self._save_product_to_inter_dataset(url, data)

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                # print(f"Error: {exc_type}, {fname}, {exc_tb.tb_lineno}!")

    def _get_processed_urls(self, *args, **kwargs):
        with open(self.INTER_DATASET_PATH, 'r') as f:
            processed_urls = json.load(f).keys()
            return processed_urls

    def _save_product_to_inter_dataset(self, url: str, data: dict, *args, **kwargs):
        """ Save product data to intermediate json dataset """

        with open(self.INTER_DATASET_PATH, 'r') as fr:
            json_data = json.load(fr)

        with open(self.INTER_DATASET_PATH, 'w') as fw:
            json_data.update({url: data})
            json.dump(json_data, fw, ensure_ascii=False, indent=4)
            print(f'* SAVED  {url} TO INTERMEDIATE DATASET')

    def create_final_dataset(self, *args, **kwargs):
        """ Create csv dataset from intermadiate json dataset. """

        with open(self.INTER_DATASET_PATH, 'r') as fr:
            json_data = json.load(fr)

        with open(f'{self.FINAL_DATASET_PATH}', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.CSV_FIELDNAMES)
            writer.writeheader()
            for product_data in json_data.values():
                writer.writerow(product_data)
            print(f'* SAVED FINAL DATASET')


    async def run(self, *args, **kwargs):
        async with aiohttp.ClientSession() as session:
            print('* START SCRAPING')
            await self.scrape(session)
        self.create_final_dataset()




if __name__ == "__main__":
    asyncio.run(Scraper().run())
