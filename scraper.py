import os
import re
import sys
import csv
import json
import asyncio
from time import time
from unicodedata import category
from xml.etree import ElementTree

import aiohttp
from bs4 import BeautifulSoup
from slugify import slugify
import xmltodict


class Scraper(object):
    BASE_URL = 'https://store.igefa.de'
    INTER_DATASET_PATH = 'intermediate_data.json'
    FINAL_DATASET_PATH = 'products.csv'
    PAGES_URLS_PATH = 'pages_urls.txt'
    PAGES_URLS_PROCESSED_PATH = 'pages_urls_processed.txt'
    PRODUCT_URLS_PATH = 'products_urls.txt'
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
    DELAY = 0.1

    def __init__(self, *args, **kwargs):
        self._setup()
        self.category_index = 0
        self.page_index = 0
        self.product_index = 0

    def _setup(self, *args, **kwargs):
        if not os.path.isfile(self.INTER_DATASET_PATH):
            with open(self.INTER_DATASET_PATH, 'w') as f:
                json.dump({}, f, indent=4)

    @staticmethod
    def get_value(tag, *args, **kwargs):
        try:
            return tag.text if tag else None
        except Exception:
            return None

    async def scrape_categories(self, session, *args, **kwargs):
        categories_xml_url = f'{self.BASE_URL}/sitemap-taxonomies.xml'
        response = await session.get(categories_xml_url)
        xml_string = await response.text()
        xml_dict = xmltodict.parse(xml_string, process_namespaces=True)
        categories_urls_dict = xml_dict.get('http://www.sitemaps.org/schemas/sitemap/0.9:urlset', {}).get('http://www.sitemaps.org/schemas/sitemap/0.9:url', [])
        categories_urls = [d.get('http://www.sitemaps.org/schemas/sitemap/0.9:loc') for d in categories_urls_dict]

        tasks = []
        for category_url in categories_urls:
            task = asyncio.create_task(self.get_page_urls(session, category_url))
            tasks.append(task)
            if self.DELAY:
                await asyncio.sleep(self.DELAY)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        page_urls = [l for ls in results if isinstance(ls, list) for l in ls]
        with open(self.PAGES_URLS_PATH, 'w') as f:
            f.write('\n'.join(page_urls))

        return page_urls

    async def get_page_urls(self, session, category_url, *args, **kwargs):
        pages_urls = []
        response = await session.get(category_url)
        html = await response.text()
        soup = BeautifulSoup(html, features='html.parser')
        try:
            json_data = soup.find('script', {'id': '__NEXT_DATA__'})
            product_count = json.loads(json_data.text)['props']['initialProps']['pageProps']['initialProductData']['total']
            page_count = product_count // 20 + 1 if product_count else 0
        except KeyError:
            page_count = 0

        self.category_index += 1
        print(f'*** SCRAPED CATEGORY {self.category_index}: {category_url} - {page_count}')
        if page_count:
            pages_urls = [f'{category_url}?page={page}' for page in range(1, page_count+1)]
        return pages_urls

    async def scrape_pages(self, session, pages_urls, *args, **kwargs):
        urls = set(pages_urls) - set(self._get_processed_page_urls())
        tasks = []
        for page_urls in urls:
            task = asyncio.create_task(self.get_product_urls(session, page_urls))
            tasks.append(task)
            if self.DELAY:
                await asyncio.sleep(self.DELAY)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        product_urls = [l for ls in results if isinstance(ls, list) for l in ls]
        with open(self.PRODUCT_URLS_PATH, 'a') as f:
            f.write('\n'.join(product_urls))

        return product_urls

    async def get_product_urls(self, session, page_url, *args, **kwargs):
        product_urls = []
        try:
            response = await session.get(page_url)
            html = await response.text()
            items = re.findall(r'"mainVariant":\s*{.*?"id":"(.*?)".*?"slug":"(.*?)".*?}', html, re.DOTALL)

            self.page_index += 1
            print(f'*** SCRAPED PAGE: {self.page_index}: {page_url}')

            if items:
                self._save_processed_page_url(page_url)
                for product_id, slug in items:
                    url = f"{self.BASE_URL}/p/{slug}/{product_id}/"
                    product_urls.append(url)
            return product_urls

        except Exception as e:
            print(e, page_url)
            return product_urls

    async def scrape_products(self, session, products_urls, *args, **kwargs):
        tasks = []
        urls = set(products_urls) - set(self._get_processed_product_urls())
        print(f'** RUN {len(urls)} PRODUCT URLS...')
        for product_url in urls:
            task = asyncio.create_task(self.parse_product(session, product_url))
            tasks.append(task)
            if self.DELAY:
                await asyncio.sleep(self.DELAY)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        saved_products_count = sum(results)
        return saved_products_count

    async def parse_product(self, session, product_url: str, *args, **kwargs):
        data = {}
        try:
            response = await session.get(product_url)
            if response.status == 200:
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
                    'Supplier-URL': product_url,
                    'Product Image URL': product_image,
                    'Manufacturer': manufacturer,
                    'Original Data Column 3 (Add. Description)': original_data_column,
                })
                self._save_product_to_inter_dataset(product_url, data)
                return 1
        except Exception as e:
            print(product_url)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(f"Error: {exc_type}, {fname}, {exc_tb.tb_lineno}!")
            return 0

    def _get_processed_page_urls(self, *args, **kwargs):
        try:
            with open(self.PAGES_URLS_PROCESSED_PATH, 'r') as f:
                return [line.strip() for line in f.readlines()]
        except:
            return []

    def _get_processed_product_urls(self, *args, **kwargs):
        with open(self.INTER_DATASET_PATH, 'r') as f:
            return json.load(f).keys()

    def _save_processed_page_url(self, page_url, *args, **kwargs):
        with open(self.PAGES_URLS_PROCESSED_PATH, 'a') as f:
            f.write(f'{page_url}\n')

    def _get_pages_urls_from_file(self, *args, **kwargs):
        with open(self.PAGES_URLS_PATH, 'r') as f:
            return [line.strip() for line in f.readlines()]

    def _get_product_urls_from_file(self, *args, **kwargs):
        with open(self.PRODUCT_URLS_PATH, 'r') as f:
            return [line.strip() for line in f.readlines()]

    def _save_product_to_inter_dataset(self, url: str, data: dict, *args, **kwargs):
        """ Save product data to intermediate json dataset """
        self.product_index += 1

        with open(self.INTER_DATASET_PATH, 'r') as fr:
            json_data = json.load(fr)

        with open(self.INTER_DATASET_PATH, 'w') as fw:
            json_data.update({url: data})
            json.dump(json_data, fw, ensure_ascii=False, indent=4)
            print(f'* SAVED {self.product_index} - {url} - TO INTERMEDIATE DATASET')

    def create_final_dataset(self, *args, **kwargs):
        """ Create csv dataset from intermediate json dataset. """

        with open(self.INTER_DATASET_PATH, 'r') as fr:
            json_data = json.load(fr)

        with open(f'{self.FINAL_DATASET_PATH}', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.CSV_FIELDNAMES)
            writer.writeheader()
            for product_data in json_data.values():
                writer.writerow(product_data)

    async def run(self, *args, **kwargs):
        async with aiohttp.ClientSession() as session:
            print('* START SCRAPING...')
            t1 = time()

            # 1. Scrape categories form xml and get list of page_url (12400 pages - 300sec)
            # pages_urls = await self.scrape_categories(session)
            pages_urls = self._get_pages_urls_from_file()
            print(f'* SCRAPED: {len(pages_urls)} PAGES URLS.')

            # 2. Scrape page_url and get list or product_url( 76000 product unique )
            # products_urls = await self.scrape_pages(session, pages_urls)
            products_urls = self._get_product_urls_from_file()
            print(f'* SCRAPED: {len(products_urls)} PRODUCTS URLS.')

            # 3. Scrape product_urls, parse data and save to inter dataset
            saved_count = await self.scrape_products(session, products_urls)
            print(f'* SAVED: {saved_count} PRODUCTS TO INTER DATASET.')

            # 4. Save final dataset form inter dataset
            self.create_final_dataset()
            print(f'* SAVED FINAL DATASET.')
            t2 = time()
            print(f'* TIME: {t2-t1}')
            print('* FINISH SCRAPING PRODUCTS')



if __name__ == "__main__":
    asyncio.run(Scraper().run())