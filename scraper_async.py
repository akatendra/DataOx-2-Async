from datetime import timedelta, datetime
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import logging
import logging.config

import database_async

# Global variables
counter = 0
# End Global variables

# Set up logging
logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
# Remove from output to the log information from Firefox, where a lot
# of space is taken up by the server response with the html content
# of the entire page. Outputting this information to the log greatly increases
# the size of the log file.
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(
    logging.WARNING)


def get_chrome_browser():
    # LOCAL
    options = webdriver.ChromeOptions()
    # Unable to hide "Chrome is being controlled by automated software" infobar
    options.add_experimental_option("excludeSwitches", ['enable-automation'])
    # Open Chrome for full size of screen
    options.add_argument("--start-maximized")
    #  Will launch browser without UI(headless)
    # options.add_argument("--headless")

    # Below disabled options in local version
    # chrome_options.add_argument('--incognito')
    # chrome_options.add_argument('--ignore-certificate-errors')
    # chrome_options.add_argument('--disable-extensions') #  ?

    # инициализируем драйвер с нужными опциями
    CHROME_PATH = 'd:\\Python\\chromedriver_win32\\chromedriver.exe'
    service = Service(CHROME_PATH)
    browser = webdriver.Chrome(service=service, options=options)
    # LOCAL

    return browser


def splitlines(string):
    line_list = string.splitlines()
    logger.debug(f'line_list: {line_list}')
    new_string = ''
    for line in line_list:
        new_string += line.strip() + ' '
    return new_string


def convert_date(date):
    if 'sec' in date:
        date_list = date.split()
        sec_str = date_list[1]
        if sec_str.isdigit():
            sec = int(date_list[1])
            converted_date = datetime.now() - timedelta(seconds=sec)
        else:
            converted_date = None
    elif 'min' in date:
        date_list = date.split()
        minutes_str = date_list[1]
        if minutes_str.isdigit():
            minutes = int(date_list[1])
            converted_date = datetime.now() - timedelta(minutes=minutes)
        else:
            converted_date = None
    elif 'hour' in date:
        date_list = date.split()
        hours_str = date_list[1]
        if hours_str.isdigit():
            hours = int(date_list[1])
            converted_date = datetime.now() - timedelta(hours=hours)
        else:
            converted_date = None
    elif 'Yesterday' in date:
        converted_date = datetime.now() - timedelta(days=1)
    elif 'day' in date:
        date_list = date.split()
        days_str = date_list[1]
        if days_str.isdigit():
            days = int(date_list[1])
            converted_date = datetime.now() - timedelta(days=days)
        else:
            converted_date = None
    elif 'week' in date:
        date_list = date.split()
        weeks_str = date_list[1]
        if weeks_str.isdigit():
            weeks = int(date_list[1])
            converted_date = datetime.now() - timedelta(weeks=weeks)
        else:
            converted_date = None
    elif 'month' in date:
        date_list = date.split()
        month_str = date_list[0]
        if month_str.isdigit():
            month = int(date_list[0])
            converted_date = datetime.now() - timedelta(days=30 * month)
        else:
            converted_date = None
    else:
        converted_date = None
    return converted_date


def parse_html(html):
    global counter
    logger.info(f'Hi from parse_html func!')
    BASE = 'https://www.kijiji.ca'
    soup = BeautifulSoup(html, 'lxml')
    items = soup.select('div[class*="search-item"]')
    logger.debug(
        '##################################################################')
    logger.debug(f'Number of items founded on page:  {len(items)}')
    logger.debug(
        '##################################################################')
    data = {}
    # Get items_ids which are in database already
    data_listing_id_from_db = database_async.get_item_ids()
    logger.debug(
        f'Number of item_ids are already exist in database: {len(data_listing_id_from_db)}')
    for item in items:
        # We intercept the error in case some fields are not filled while
        # parsing. An error during parsing causes the whole process to stop.
        # In case of an error, we move on to parsing the next item.
        try:
            data_listing_id = int(item['data-listing-id'])
            logger.debug(f'Detected data_listing_id:  {data_listing_id}')

            if data_listing_id in data_listing_id_from_db:
                logger.debug(
                    f'Detected data_listing_id is already exist in database: {data_listing_id} | Skipped...')
                logger.debug(
                    '##############################################################')
                continue
            else:
                counter += 1
                logger.debug(
                    f'counter: {counter}')
                logger.debug(
                    f'Detected data_listing_id is taken in work: {data_listing_id}')
                logger.debug(
                    '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                # Working with a title.
                item_a = item.select_one('a[class="title"]')
                logger.debug(f'item_a:  {item_a}')

                # Getting an item url.
                item_url = BASE + item_a['href']
                logger.debug(f'item_url | len: {len(item_url)} | {item_url}')

                # Getting an item url.
                item_title = item_a.text.strip()
                logger.debug(f'item_title: {item_title}')

                # Getting an image url.
                item_image_url = item.select_one('img')['data-src']
                logger.debug(
                    f'item_image_url | len: {len(item_image_url)} | {item_image_url}')

                # Getting a description_min.
                item_description_min = item.select_one(
                    'div[class="description"]').text
                logger.debug(
                    f'item_description_min original | len: {len(item_description_min)} | {item_description_min}')
                if '...' in item_description_min:
                    item_description_min = item_description_min.split('...')[0] + '...'
                logger.debug(
                    f'item_description_min before space strip | len: {len(item_description_min)} | {item_description_min}')
                item_description_min = splitlines(item_description_min)

                logger.debug(
                    f'item_description_min | len: {len(item_description_min)} | {item_description_min}')

                # Getting a description tagline.
                item_description_tagline = item.select_one(
                    'div[class="tagline"]')
                if item_description_tagline:
                    item_description_tagline = item_description_tagline.text.strip()
                    item_description_tagline_len = len(item_description_tagline)
                else:
                    item_description_tagline = None
                    item_description_tagline_len = None
                logger.debug(
                    f'item_description_tagline | len: {item_description_tagline_len} | {item_description_tagline}')

                # Getting a description.
                item_description = None

                # Getting an item price.
                item_price_str = item.select_one(
                    'div[class="price"]').text.strip()
                logger.debug(f'item_price_str: {item_price_str}')

                # Getting an item price currency.
                if item_price_str[0] == '$':
                    item_currency = '$'
                else:
                    item_currency = None
                    item_price = None
                logger.debug(f'item_currency:  {item_currency}')

                if item_currency is not None:
                    item_price_str = item_price_str.split('.')[0]
                    logger.debug(f'item_price_str: {item_price_str}')
                    item_price = int(
                        ''.join(
                            char for char in item_price_str if
                            char.isdecimal()))
                logger.debug(f'item_price: {item_price}')

                # Getting an item nearest intersection.
                item_intersections = item.select_one(
                    'span[class="nearest-intersection"]')
                if item_intersections:
                    item_intersections_list = item_intersections.select(
                        'span[class="intersection"]')
                    logger.debug(
                        f'item_intersections_list: {item_intersections_list}')
                    item_intersections = item_intersections_list[0].text + ' / ' + item_intersections_list[1].text
                else:
                    item_intersections = None
                logger.debug(
                    f'item_intersections | len: {len(item_intersections)} | {item_intersections}')

                # Getting beds.
                item_beds = item.select_one(
                    'span[class="bedrooms"]').text.strip()
                logger.debug(f'item_beds:  {item_beds}')
                item_beds = item_beds.replace('Beds:', '').strip()
                logger.debug(f'item_beds final: {item_beds}')

                # Getting an item city.
                item_city_publish_date = item.select_one(
                    'div[class="location"]')
                logger.debug(
                    f'item_city_publish_date: {item_city_publish_date}')
                if item_city_publish_date:
                    item_city = item_city_publish_date.find('span').text
                    item_city = item_city.replace('\n', '').strip()
                else:
                    item_city = None
                logger.debug(f'item_city: {item_city}')

                # Getting an item publishing date.
                item_date_data = item.select_one(
                    'span[class="date-posted"]').text
                if '/' in item_date_data:
                    item_date_list = item_date_data.split('/')
                    item_date = datetime(int(item_date_list[2]),
                                         int(item_date_list[1]),
                                         int(item_date_list[0]))
                else:
                    # Convert '< 10 hours ago' or '< 52 minutes ago' in normal calendar date.
                    item_date = convert_date(item_date_data)
                logger.debug(
                    f'item_date:  {item_date.strftime("%d-%m-%Y %H:%M")}')
                item_add_date = datetime.now()
        except Exception as err:
            logging.exception('Exception occurred during parsing!')
            continue
        # data writing into a dictionary.
        item_dict = {'data_listing_id': data_listing_id,
                     'data_vip_url': item_url,
                     'image_url': item_image_url,
                     'title': item_title,
                     'description_min': item_description_min,
                     'description_tagline': item_description_tagline,
                     'description': item_description,
                     'beds': item_beds,
                     'price': item_price,
                     'currency': item_currency,
                     'city': item_city,
                     'intersections': item_intersections,
                     'rental_type': 'Long Term Rentals',
                     'publish_date': item_date,
                     'add_date': item_add_date
                     }
        # Put data dictionary as 'value' in new dictionary
        # with item_id as 'key'.
        data[data_listing_id] = item_dict
        logger.debug(
            '##############################################################')
    logger.debug(f'New items detected during parse: {len(data)}')
    logger.debug(
        '##############################################################')
    return data
