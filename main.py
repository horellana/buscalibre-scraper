import re
import sys
import csv
import asyncio

import aiohttp
from aiologger import Logger
from bs4 import BeautifulSoup

HTTP_TIMEOUT = 600
BASE_URL = 'https://www.buscalibre.cl/libros-envio-express-chile_t.html'

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
}

logger = Logger.with_default_handlers(name=__name__)


async def export_to_csv(books):
    fieldnames = ['title', 'author', 'discount_percentage', 'discount', 'original_price', 'price_with_discount', 'url']
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, delimiter=';')

    writer.writeheader()

    for book in books:
        try:
            row = {
                'title': book['title'],
                'author': book['author'],
                'discount_percentage': book['price']['discount_percentage'],
                'discount': book['price']['discount'],
                'original_price': book['price']['original'],
                'price_with_discount': book['price']['with_discount'],
                'url': book['url']
            }

            writer.writerow(row)

        except:
            await logger.error(f'Error with book : {book}')
            continue


def get_book_publisher(book_div):
    return None


def get_book_url(book_div):
    children = list(book_div.children)
    a = children[0]

    return a['href']


def get_book_price(book_div):
    discount_text = book_div.find('div', class_='box-dcto col-xs-5').text
    original_price_text = list(book_div.find('h5', class_='precio-antes hide-on-hover margin-0 color-dark-gray font-weight-normal'))[0].text

    if len(discount_text) < 1:
        return 0

    discount = int(discount_text.split('%')[0]) / 100

    original_price_text = original_price_text.split(' ')[1]
    original_price = int(''.join(original_price_text.split('.')))

    return {
        'discount_percentage': discount,
        'discount': int(original_price * discount),
        'original': original_price,
        'with_discount': int(original_price * (1 - discount))
    }


def get_books(soup):
    selector = 'div.producto'
    book_divs = soup.select(selector)

    result = list()

    for div in book_divs:
        children = list(list(div.children)[0].children)

        title = children[1].text
        author = children[2].text

        result.append({
            'title': title,
            'author': author,
            # 'publisher': get_book_publisher(div),
            'price': get_book_price(div),
            'url': get_book_url(div)
        })

    return result


def get_number_of_pages(number_of_books):
    books_per_row = 7
    rows = 21

    return int(number_of_books / (books_per_row * rows)) + 10


async def get_number_of_books(soup):
    try:
        selector = '.cantidadProductos'
        div = soup.select_one(selector)
        match = re.search('Encontramos (.+\..*) resultados', div.text)
        number_of_books = float(''.join(match.group(1).split('.')))
        return number_of_books
    except Exception as e:
        msg = f'Error when getting total number of available books: {e}'
        await logger.error(msg)
        raise RuntimeError(msg)


async def get_page(http_session, n):
    url = f'{BASE_URL}?page={n}'

    response = await http_session.get(url, timeout=HTTP_TIMEOUT, headers=HTTP_HEADERS)
    response_html = await response.text()

    soup = BeautifulSoup(response_html, 'html.parser')

    await logger.debug(f'GET {url}')
    return soup


async def main():
    async with aiohttp.ClientSession() as session:
        url = f'{BASE_URL}'

        await logger.info('Downloading first page to get total number of available books')
        response = await session.get(url, timeout=HTTP_TIMEOUT, headers=HTTP_HEADERS)
        response_html = await response.text()
        soup = BeautifulSoup(response_html, 'html.parser')
        number_of_books = await get_number_of_books(soup)

        await logger.info('Calculating total number of pages to download')
        pages = get_number_of_pages(number_of_books)

        await logger.info('Downloading all pages concurrently')
        tasks = [get_page(session, i + 1) for i in range(pages)]
        pages = await asyncio.gather(*tasks)

        await logger.info('Getting books data')
        flatten = lambda l: [item for sublist in l for item in sublist]
        books = flatten(get_books(page) for page in pages)

        await export_to_csv(books)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
