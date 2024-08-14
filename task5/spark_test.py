from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum as _sum, count, when
from dotenv import load_dotenv
import os

# Load credentials from .env
load_dotenv()

# Params for DB in .env
db_url = os.getenv("DB_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_driver = os.getenv("DB_DRIVER")

# Initial SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Analysis with PySpark") \
    .config("spark.jars", "/home/jovyan/work/task5/postgresql-42.7.3.jar") \
    .getOrCreate()

# Function for load data from tables
def load_table(table_name):
    return spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", db_driver) \
        .load()

# Load table
film_df = load_table("film")
category_df = load_table("category")
film_category_df = load_table("film_category")
actor_df = load_table("actor")
film_actor_df = load_table("film_actor")
inventory_df = load_table("inventory")
rental_df = load_table("rental")
payment_df = load_table("payment")
customer_df = load_table("customer")
address_df = load_table("address")
city_df = load_table("city")

# task1: Quantity movies in each category, sorted DESC
film_category_count = film_category_df.join(category_df, "category_id") \
    .groupBy("name") \
    .agg(count("film_id").alias("film_count")) \
    .orderBy(desc("film_count"))

film_category_count.show()

# task2: 10 actors, which movies was most rented, sorted DESC
top_actors = film_actor_df.join(rental_df.join(inventory_df, "inventory_id"), "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("rental_id").alias("rental_count")) \
    .orderBy(desc("rental_count")) \
    .limit(10)

top_actors.show()

# task3: movies category , which spend most el then else money
top_spent_category = payment_df \
    .join(rental_df, "rental_id") \
    .join(inventory_df, "inventory_id") \
    .join(film_df, "film_id") \
    .join(film_category_df, "film_id") \
    .join(category_df, "category_id") \
    .groupBy("name") \
    .agg(_sum("amount").alias("total_spent")) \
    .orderBy(desc("total_spent")) \
    .limit(1)

top_spent_category.show()

# task4: movies which not in inventory
films_not_in_inventory = film_df.join(inventory_df, "film_id", "left_anti").select("title")

films_not_in_inventory.show()

#task5: top3 actors whos was most from all in movies in category “Children”
children_category_id = category_df.filter(col("name") == "Children").select("category_id").first()[0]

top_actors_in_children = film_actor_df.join(film_category_df.filter(col("category_id") == children_category_id), "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("film_id").alias("film_count")) \
    .orderBy(desc("film_count")) \
    .limit(3)

top_actors_in_children.show()

# task6: cities with number of active and inactive clients, sorted by number of inactive clients in descending order
active_inactive_customers = customer_df.join(address_df, "address_id") \
    .join(city_df, "city_id") \
    .groupBy("city") \
    .agg(
        count(when(col("active") == 1, True)).alias("active_customers"),
        count(when(col("active") == 0, True)).alias("inactive_customers")
    ) \
    .orderBy(desc("inactive_customers"))

active_inactive_customers.show()

# task7: the category of films with the most rental hours in cities that start with "a" or contain "-"
top_category_a_cities = rental_df.join(inventory_df, "inventory_id") \
    .join(film_df, "film_id") \
    .join(film_category_df, "film_id") \
    .join(category_df, "category_id") \
    .join(customer_df, "customer_id") \
    .join(address_df, "address_id") \
    .join(city_df.filter(col("city").like("a%") | col("city").like("%-%")), "city_id") \
    .groupBy("name") \
    .agg(_sum("length").alias("total_hours")) \
    .orderBy(desc("total_hours")) \
    .limit(1)

top_category_a_cities.show()

# stop SparkSession
spark.stop()