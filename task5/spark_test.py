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

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Analysis with PySpark") \
    .config("spark.jars", "/home/jovyan/work/task5/postgresql-42.7.3.jar") \
    .getOrCreate()

# Функция для загрузки данных из таблицы
def load_table(table_name):
    return spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", db_driver) \
        .load()

# Загрузка данных из таблиц
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

# Задача 1: Количество фильмов в каждой категории, отсортировано по убыванию
film_category_count = film_category_df.join(category_df, "category_id") \
    .groupBy("name") \
    .agg(count("film_id").alias("film_count")) \
    .orderBy(desc("film_count"))

film_category_count.show()

# Задача 2: 10 актеров, чьи фильмы больше всего арендовали, отсортировано по убыванию
top_actors = film_actor_df.join(rental_df.join(inventory_df, "inventory_id"), "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("rental_id").alias("rental_count")) \
    .orderBy(desc("rental_count")) \
    .limit(10)

top_actors.show()

# Задача 3: Категория фильмов, на которую потратили больше всего денег
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

# Задача 4: Названия фильмов, которых нет в inventory
films_not_in_inventory = film_df.join(inventory_df, "film_id", "left_anti").select("title")

films_not_in_inventory.show()

# Задача 5: Топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”
children_category_id = category_df.filter(col("name") == "Children").select("category_id").first()[0]

top_actors_in_children = film_actor_df.join(film_category_df.filter(col("category_id") == children_category_id), "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("film_id").alias("film_count")) \
    .orderBy(desc("film_count")) \
    .limit(3)

top_actors_in_children.show()

# Задача 6: Города с количеством активных и неактивных клиентов, отсортировано по количеству неактивных клиентов по убыванию
active_inactive_customers = customer_df.join(address_df, "address_id") \
    .join(city_df, "city_id") \
    .groupBy("city") \
    .agg(
        count(when(col("active") == 1, True)).alias("active_customers"),
        count(when(col("active") == 0, True)).alias("inactive_customers")
    ) \
    .orderBy(desc("inactive_customers"))

active_inactive_customers.show()

# Задача 7: Категория фильмов с наибольшим количеством часов аренды в городах, которые начинаются на "a" или содержат "-"
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

# Остановка SparkSession
spark.stop()

#psql -h task1-db-1 -U alexv -d task3SQL