import os
import pandas as pd
from dotenv import load_dotenv
import psycopg

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

df = pd.read_csv(r"../data/raw/olistIT_orders.csv")
df.isnull().sum()
df = df.fillna(0)

with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
    with conn.cursor() as cur:
        # Creazione tabella per le categorie
        sql = """
        CREATE TABLE IF NOT EXISTS orders (
            pk_order_id VARCHAR PRIMARY KEY,
            fk_customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_delivered_customer_date VARCHAR,
            order_estimated_delivery_date DATE,
            FOREIGN KEY (fk_customer_id) REFERENCES customers (pk_customer_id)
        );
        """
        cur.execute(sql)

        # Inserimento dati nella tabella evitando duplicati
        sql = """
            INSERT INTO orders
        (pk_order_id, fk_customer_id, order_status, order_purchase_timestamp, 
        order_delivered_customer_date, order_estimated_delivery_date) 
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())

        conn.commit()


