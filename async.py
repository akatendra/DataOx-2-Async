import aiohttp
import asyncio
import time
from datetime import datetime
import scraper_async
import database_async
import logging.config
# from selenium.webdriver.common.by import By

"""fix yelling at me error"""
# Monkey Patch:
# https://pythonalgos.com/runtimeerror-event-loop-is-closed-asyncio-fix/
from functools import wraps

from asyncio.proactor_events import _ProactorBasePipeTransport


def silence_event_loop_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise

    return wrapper


_ProactorBasePipeTransport.__del__ = silence_event_loop_closed(
    _ProactorBasePipeTransport.__del__)
"""fix yelling at me error end"""


def spent_time():
    global start_time
    sec_all = time.time() - start_time
    if sec_all > 60:
        minutes = sec_all // 60
        sec = sec_all % 60
        time_str = f'| {int(minutes)} min {round(sec, 1)} sec'
    else:
        time_str = f'| {round(sec_all, 1)} sec'
    start_time = time.time()
    return time_str


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            logger.debug(f'url: {url}')
            logger.debug(f'Status: {response.status}')
            logger.debug(
                f'Content-type: {response.headers["content-type"]}')
            html = await response.text()
            return html
    except Exception as error:
        logger.debug(f'{str(error)}')


async def main():
    global urls
    tasks = []

    # Start sessions
    headers = {
        "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}

    async with aiohttp.ClientSession(headers=headers) as session:
        for url in urls:
            tasks.append(fetch(session, url))
        htmls = await asyncio.gather(*tasks)
    return htmls


if __name__ == "__main__":
    # Set up logging
    logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # Set the variables values
    time_begin = start_time = time.time()
    url = 'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273'
    url1 = 'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/'
    url2 = 'c37l1700273'
    output_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # End of variables values setting

    logger.info('Start...')

    # Getting urls
    urls = []
    for page_number in range(1, 101):
        logger.debug(f'Take in work page: {page_number}')
        if page_number == 1:
            page_url = url1 + url2
        else:
            page_url = f'{url1}page-{page_number}/{url2}'
        logger.debug(f'page_url: {page_url}')
        urls.append(page_url)
    logger.debug((f'urls count: {len(urls)}'))

    htmls = asyncio.run(main())

    # asyncio.set_event_loop(asyncio.ProactorEventLoop())
    #
    # loop = asyncio.get_event_loop()
    # htmls = loop.run_until_complete(main())
    # loop.close()

    # Storing the raw HTML data.
    for html in htmls:
        if html is not None:
            output_data = scraper_async.parse_html(html)
            database_async.write_to_db(output_data)
        else:
            continue

    time_end = time.time()
    elapsed_time = time_end - time_begin
    if elapsed_time > 60:
        elapsed_minutes = elapsed_time // 60
        elapsed_sec = elapsed_time % 60
        elapsed_time_str = f'| {int(elapsed_minutes)} min {round(elapsed_sec, 1)} sec'
    else:
        elapsed_time_str = f'| {round(elapsed_time, 1)} sec'
    logger.info(
        f'Elapsed run time: {elapsed_time_str} seconds | New items: {scraper_async.counter}')
