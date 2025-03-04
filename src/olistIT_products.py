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

df = pd.read_csv(r"../data/raw/olistIT_products.csv")
print(df)

print(df.nunique())

print(df.isnull().sum())

print(df.info())

df["category"] = df["category"].str.lower().str.strip()

old_categories = [
    "agro_industry_and_commerce", "air_conditioning", "art", "arts_and_craftmanship", "audio", "auto", "baby",
    "bed_bath_table", "books_general_interest", "books_imported", "books_technical", "cds_dvds_musicals",
    "christmas_supplies", "computers", "computers_accessories", "construction_tools_construction",
    "construction_tools_lights", "cool_stuff", "costruction_tools_garden", "costruction_tools_tools",
    "diapers_and_hygiene", "drinks", "dvds_blu_ray", "electronics", "fashio_female_clothing",
    "fashion_bags_accessories", "fashion_childrens_clothes", "fashion_male_clothing", "fashion_shoes", "fashion_sport",
    "fashion_underwear_beach", "fixed_telephony", "flowers", "food", "food_drink", "furniture_bedroom",
    "furniture_decor", "furniture_living_room", "furniture_mattress_and_upholstery", "garden_tools", "health_beauty",
    "home_appliances", "home_appliances_2", "home_confort", "home_confort_2", "home_construction", "housewares",
    "industry_commerce_and_business", "kitchen_dining_laundry_garden_furniture", "la_cuisine", "luggage_accessories",
    "music", "musical_instruments", "NaN", "office_furniture", "party_supplies", "pc_gamer", "perfumery", "pet_shop",
    "security_and_services", "signaling_and_security", "small_appliances", "small_appliances_home_oven_and_coffee",
    "sports_leisure", "stationery", "tablets_printing_image", "telephony", "toys", "video_photo", "watches_gifts"
]

new_categories_map = {
    "Agribusiness and Trade": ["agro_industry_and_commerce", "industry_commerce_and_business"],
    "Food and Beverages": ["food", "food_drink", "drinks", "la_cuisine"],
    "Arts and Crafts": ["art", "arts_and_craftsmanship", "arts_and_craftmanship"],
    "General Items": ["cool_stuff", "party_supplies", "stationery", "office_furniture"],
    "Tools and Construction": ["construction_tools_construction", "construction_tools_lights",
                               "construction_tools_garden", "construction_tools_tools", "garden_tools", "costruction_tools_garden",
                               "costruction_tools_tools"],
    "Music": ["music", "musical_instruments", "cds_dvds_musicals", "dvds_blu_ray"],
    "Home": ["furniture_bedroom", "furniture_decor", "furniture_living_room", "furniture_mattress_and_upholstery",
             "home_confort", "home_confort_2", "home_construction", "housewares",
             "kitchen_dining_laundry_garden_furniture", "christmas_supplies"],
    "Electronics and Telecommunications": ["electronics", "computers", "computers_accessories",
                                           "pc_gamer", "audio", "fixed_telephony", "telephony",
                                           "tablets_printing_image", "video_photo", "air_conditioning"],
    "Books": ["books_general_interest", "books_imported", "books_technical"],
    "Clothing and Fashion": ["fashio_female_clothing", "fashion_bags_accessories", "fashion_childrens_clothes",
                             "fashion_male_clothing", "fashion_shoes", "fashion_sport", "fashion_underwear_beach", "luggage_accessories"],
    "Pets": ["pet_shop"],
    "Baby Products": ["baby", "diapers_and_hygiene", "toys"],
    "Home Appliances": ["home_appliances", "home_appliances_2", "small_appliances",
                        "small_appliances_home_oven_and_coffee"],
    "Safety Items": ["security_and_services", "signaling_and_security"],
    "Health and Beauty": ["health_beauty", "perfumery"],
    "Sports": ["sports_leisure", "auto"],
    "Miscellaneous": ["watches_gifts", "flowers", "furniture_mattress_and_upholstery", "bed_bath_table"],
    "Other": ["NaN"]
}

# Creazione DataFrame con ID seriale
df_old = pd.DataFrame({"category_id": range(1, len(old_categories) + 1), "old_category": old_categories})

# Creare mapping tra vecchie e nuove categorie
mapping_list = []
for new_cat, old_cats in new_categories_map.items():
    for old_cat in old_cats:
        mapping_list.append({"old_category": old_cat, "category": new_cat})

df_mapping = pd.DataFrame(mapping_list)

# Merge per collegare le vecchie categorie con le nuove
df_final = df_old.merge(df_mapping, on="old_category", how="left")

with psycopg.connect(host=host, dbname = dbname, user = user, password= password, port = port) as conn:
    with conn.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS products (
        pk_product_id character varying PRIMARY KEY,
        category character varying,
        product_name_lenght character varying,
        product_description_lenght character varying,
        product_photos_qty character varying);
        """
        cur.execute(sql)
        sql = """
        INSERT INTO products
        (pk_product_id, category, product_name_lenght, product_description_lenght, product_photos_qty) 
        VALUES (%s, %s, %s, %s, %s);
        """

        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())

        conn.commit()
