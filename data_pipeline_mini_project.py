import mysql.connector
from mysql.connector import errorcode
import configparser

c = configparser.ConfigParser()
c.read('pipeline_config.ini')

# connection variables
host = c['MySQL']['host']
user_name = c['MySQL']['user']
user_password = c['MySQL']['pass']
port = c['MySQL']['port']
database = c['MySQL']['db']

# global variable
DB_NAME = database

create_database_query = """
    CREATE DATABASE IF NOT EXISTS `tickets` DEFAULT CHARACTER SET 'utf8'
"""

create_table_query = """
    CREATE TABLE `ticket_sales` (
        `ticket_id` INT,
        `trans_date` INT,
        `event_id` INT,
        `event_name` VARCHAR(50),
        `event_date` DATE,
        `event_type` VARCHAR(10),
        `event_city` VARCHAR(20),
        `customer_id` INT,
        `price` DECIMAL,
        `num_tickets` INT
    )
"""


def create_server_connection(host, user, password, port):
        """Inital connection to server to create database"""
        connection = None

        try:
            connection = mysql.connector.connect(
                host=host,
                user=user_name,
                password=user_password,
                port=port
                # database=database
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Incorrect login credentials")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(f"Error: {err}")

        return connection


def create_database(connection, query=create_database_query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            print("Database created successfully")
        except mysql.connector.Error as err:
            print(f"Error: '{err}'")
        finally:
            cursor.close()
            connection.close()

db_connection = create_server_connection(host, user_name, user_password, port)
create_database(db_connection)

def create_db_connection(db_name='tickets'):
        connection = None
        try:
                connection = mysql.connector.connect(
                    host=host,
                    user=user_name,
                    password=user_password,
                    port=port,
                    database=db_name
                )
                # cursor = connection.cursor()
                print("Database connection successful")
        except mysql.connector.Error as err:
                print(f"Error: '{err}'")

        return connection


def execute_query(connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print("Query successful")
        except mysql.connector.Error as err:
            print(f"Error: '{err}'")

def read_query(connection, query):
        connection = connection
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except mysql.connector.Error as err:
            print(f"Error: '{err}'")

#connect = 
connection = create_db_connection(DB_NAME)

execute_query(connection, create_table_query)

def load_csv(connection, file=str):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            LOAD DATA INFILE '{}'
            INTO TABLE `ticket_sales`
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS;
            """
        ).format(file)  
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)
    
    finally:
        connection.commit()
        cursor.close()
    
    return

connection = create_db_connection('tickets')
# cursor = connection.cursor()
file = '/Users/Shared/my_sql_cvs/third_party_sales_1.csv'

load_csv(connection, file)

def query_popular_tickets(connection):
    query = """
        SELECT * FROM `ticket_sales`
        WHERE `trans_date` > NOW() - INTERVAL 30 DAY
        ORDER BY `num_tickets`
        LIMIT 5;
    """
    cursor = connection.cursor(named_tuple=True)
    try:
        # execute query
        cursor.execute(query)
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

    finally:
        records = cursor.fetchall()
        print("Most popular events in the past month:")
        # show results
        for row in records:
            print(f" - {row.event_name}")
    
    cursor.close()


popular_tickets = query_popular_tickets((connection))

