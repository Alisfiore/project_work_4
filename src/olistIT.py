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

df = pd.read_csv(r"../data/raw/olistIT_customers.csv")
print(df)

print(df.nunique())

print(df.isnull().sum())
df["cap"] = df["cap"].astype(str).str.zfill(5)
print(df["cap"])
df = df.sort_values(by=["region","city"])
print(df)

with psycopg.connect(host=host, dbname = dbname, user = user, password= password, port = port) as conn:
    with conn.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS customers (
        pk_customer_id character varying PRIMARY KEY,
        region character varying,
        city character varying,
        cap character varying 
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO customers
        (pk_customer_id, region, city, cap) 
        VALUES (%s, %s, %s, %s);
        """

        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())

        conn.commit()