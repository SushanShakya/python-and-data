import requests
import json
import sqlite3

url = 'https://raw.githubusercontent.com/SushanShakya/python-and-data/main/data.json'

DROP_YEAR_TABLE = '''
DROP TABLE IF EXISTS YEAR;
'''

DROP_PRODUCT_TABLE = '''
DROP TABLE IF EXISTS PRODUCT;
'''

DROP_COUNTRY_TABLE = '''
DROP TABLE IF EXISTS COUNTRY;
'''

DROP_SALES_TABLE = '''
DROP TABLE IF EXISTS SALES;
'''

CREATE_YEAR_TABLE = '''
CREATE TABLE YEAR(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL UNIQUE
);
'''

CREATE_PRODUCT_TABLE = '''
CREATE TABLE PRODUCT(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
'''

CREATE_COUNTRY_TABLE = '''
CREATE TABLE COUNTRY(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
'''

CREATE_SALES_TABLE = '''
CREATE TABLE SALES(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    country_id INTEGER NOT NULL,
    sales INTEGER NOT NULL
);
'''

INSERT_YEAR = "INSERT OR IGNORE INTO YEAR(year) VALUES(?);"
INSERT_PRODUCT = "INSERT OR IGNORE INTO PRODUCT(name) VALUES(?);"
INSERT_COUNTRY = "INSERT OR IGNORE INTO COUNTRY(name) VALUES(?);"
INSERT_SALES = "INSERT OR IGNORE INTO SALES(year_id, product_id, country_id, sales) VALUES(?,?,?,?);"

class Sales:
    def __init__(self,year, product, country, sales):
        self.year = year
        self.product = product
        self.country = country
        self.sales = sales

    @staticmethod
    def fromJson(x):
        return Sales(x['year'], x['petroleum_product'], x['country'], x['sale'])

    def __insertYear(self, conn):
        conn.execute(INSERT_YEAR,(self.year,))
        conn.execute("SELECT id FROM YEAR WHERE year = ?;", (self.year,))
        return conn.fetchone()[0]

    def __insertProduct(self, conn):
        conn.execute(INSERT_PRODUCT,(self.product,))
        conn.execute('SELECT id FROM PRODUCT WHERE name = ?',(self.product,))
        return conn.fetchone()[0]

    def __insertCountry(self, conn):
        conn.execute(INSERT_COUNTRY,(self.country,))
        conn.execute('SELECT id FROM COUNTRY WHERE name = ?',(self.country,))
        return conn.fetchone()[0]

    def __insertSales(self,conn, year_id, product_id, country_id):
        conn.execute(INSERT_SALES,(year_id, product_id, country_id, self.sales,))

    def save(self, conn):
        year_id = self.__insertYear(conn)
        product_id = self.__insertProduct(conn)
        country_id = self.__insertCountry(conn)
        self.__insertSales(conn, year_id, product_id, country_id)

    def __str__(self):
        return f'Sales({self.year}, {self.product},{self.country}, {self.sales})'

response = requests.get(url)

if(response.ok):
    sales = json.loads(response.content, object_hook=Sales.fromJson) 
    
    cur = sqlite3.connect('report.db')
    conn = cur.cursor()
    conn.execute(DROP_YEAR_TABLE)
    conn.execute(DROP_PRODUCT_TABLE)
    conn.execute(DROP_COUNTRY_TABLE)
    conn.execute(DROP_SALES_TABLE)
    conn.execute(CREATE_YEAR_TABLE)
    conn.execute(CREATE_PRODUCT_TABLE)
    conn.execute(CREATE_COUNTRY_TABLE)
    conn.execute(CREATE_SALES_TABLE)

    for sale in sales:
        sale.save(conn)
        print("Save {}".format(sale.product))

    cur.commit()
    cur.close()
