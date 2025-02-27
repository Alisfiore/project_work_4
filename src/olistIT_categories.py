import os
import pandas as pd
from dotenv import load_dotenv
import psycopg


#va a cercare le variabili dell'ambiente
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

df = pd.read_csv(r"../data/raw/olistIT_categories.csv")
print(df)

print(df.nunique())

print(df.isnull().sum())


with psycopg.connect(host=host, dbname = dbname, user = user, password= password, port = port) as conn:
    with conn.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS categories (
        product_category_name_english character varying PRIMARY KEY
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO categories
        (product_category_name_english) 
        VALUES (%s);
        """

        for index, row in df.iterrows():
            cur.execute(sql, (row["product_category_name_english"],))

        conn.commit()

