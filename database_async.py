import psycopg2
from psycopg2 import Error, extensions
import logging.config
import scraper_async

# Set up logging
logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def create_connection():
    # Connection to local PostgreSQL DB DataOx
    connection = psycopg2.connect(database='dataox_async',
                                  user='dataox_async7396',
                                  password='jiWkLUYZMK',
                                  host='127.0.0.1',
                                  port='5432'
                                  )
    logger.info(f'Connection established!')
    return connection


def execute_sql_query(sql, fetch=True, data=None):
    try:
        # Connection to local PostgreSQL DB
        connection = create_connection()
        with connection:
            # Курсор для выполнения операций с базой данных
            with connection.cursor() as cursor:
                if cursor:
                    logger.info('PostgreSQL connected!')
                if data is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, data)
                if fetch is True:
                    return cursor.fetchall()
    except (Exception, Error) as error:
        logger.debug("Error during work with PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.info("Connection with PostgreSQL closed!")


def get_item_ids():
    table = 'items'
    sql_get_items_ids = f'SELECT data_listing_id FROM {table};'
    item_ids = execute_sql_query(sql_get_items_ids)
    item_ids_tuple = set((item[0] for item in item_ids))
    logger.debug(f'Items_ids tuple received from DB: {len(item_ids_tuple)}')
    return item_ids_tuple


def write_to_db(data):
    table = 'items'
    sql_put_data = f'''
                   INSERT INTO {table}
                   (data_listing_id,
                    data_vip_url,
                    image_url,
                    title,
                    description_min,
                    description_tagline,
                    description,
                    beds,
                    price,
                    currency,
                    city,
                    intersections,
                    rental_type,
                    publish_date,
                    add_date ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                   '''
    item_ids = set(data.keys())
    logger.debug(
        f'item_ids from parsed data: {type(item_ids)}, {len(item_ids)}, {item_ids}')
    item_ids_database = get_item_ids()
    item_ids_to_write = item_ids.difference(item_ids_database)
    logger.debug(f'item_ids_to_write: {item_ids_to_write}')
    logger.debug(f'counter: {scraper_async.counter}')
    for item_id in item_ids_to_write:
        logger.debug(f'item_id: {item_id}')
        data_tuple = tuple(
            (item_data for item_data in data[item_id].values()))
        logger.debug(f'{type(data_tuple)}, {data_tuple}')
        execute_sql_query(sql_put_data, fetch=False, data=data_tuple)
    logger.info('Data saved into table items!')


###############################################################################
######################### INITIATE DB CREATION ################################
###############################################################################
def create_dataox_db():
    user_name = 'dataox_async7396'
    user_password = 'jiWkLUYZMK'
    db_name = 'dataox_async'
    try:
        # Create connection
        connection = psycopg2.connect(database='postgres',
                                      user='postgres',
                                      password='qw96mi1c',
                                      host='127.0.0.1',
                                      port='5432',
                                      )

        # Set autocommit
        autocommit = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        connection.set_isolation_level(autocommit)

        # Create cursor for working with database
        cursor = connection.cursor()

        # Create user
        sql_create_user = f'''
                            CREATE ROLE {user_name} WITH
                            LOGIN
                            NOSUPERUSER
                            CREATEDB
                            NOCREATEROLE
                            INHERIT
                            NOREPLICATION
                            CONNECTION LIMIT -1
                            PASSWORD '{user_password}';
                           '''

        logger.info('Database user has been created successfully!!!')
        cursor.execute(sql_create_user)

        # Create database
        sql_create_database = f'''
                               CREATE DATABASE {db_name}
                               WITH OWNER {user_name}
                               ENCODING 'UTF8';
                              '''
        cursor.execute(sql_create_database)

        # Set privileges to user
        sql_grant_privileges_to_user = f'''
                                        GRANT ALL ON DATABASE {db_name}
                                        TO {user_name};
                                        '''
        cursor.execute(sql_grant_privileges_to_user)

    except (Exception, Error) as error:
        logger.debug("Error during work with PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

    logger.info('Database has been created successfully!!!')


def create_table():
    # Create table
    table_name = 'items'
    sql_create_table = f'''
                            CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY, 
                            data_listing_id BIGINT NOT NULL,
                            data_vip_url VARCHAR(512),
                            image_url VARCHAR(512),
                            title VARCHAR(255),
                            description_min VARCHAR(1024),
                            description_tagline VARCHAR(512),
                            description TEXT,
                            beds VARCHAR(255),
                            price INTEGER,
                            currency VARCHAR(255),
                            city VARCHAR(255),
                            intersections VARCHAR(512),
                            rental_type VARCHAR(255) NOT NULL,
                            publish_date TIMESTAMP NOT NULL,
                            add_date TIMESTAMP NOT NULL 
                            );
                        '''
    execute_sql_query(sql_create_table, fetch=False)
    logger.info('Table has been created successfully!!!')


if __name__ == '__main__':
    create_dataox_db()
    create_table()
