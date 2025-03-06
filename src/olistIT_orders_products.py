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

df = pd.read_csv(r"../data/raw/olistIT_orders_products.csv")


print(df)
with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
    with conn.cursor() as cur:
        # Creazione tabella per le categorie
        sql = """
        CREATE TABLE IF NOT EXISTS orders_products (
            pk_orders_products SERIAL PRIMARY KEY,
            fk_order_id VARCHAR,
            order_item VARCHAR,
            product_id VARCHAR,
            seller_id VARCHAR,
            price NUMERIC,
            freight NUMERIC,
            FOREIGN KEY (fk_order_id) REFERENCES orders (pk_order_id)
        );
        """
        cur.execute(sql)

        # Inserimento dati nella tabella evitando duplicati
        sql = """
        INSERT INTO orders_products
        (fk_order_id, order_item, product_id, seller_id, price, freight) 
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())

        conn.commit()


#order_id,order_item,product_id,seller_id,price,freight