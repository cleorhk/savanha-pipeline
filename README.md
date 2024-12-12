Documentation for ETL Pipeline with DBT (Savanna Project)

Overview

This documentation describes the components and functionality of an ETL pipeline integrated with DBT models, which is designed to extract, transform, and load data while also supporting detailed data analysis using SQL joins and aggregations. The ETL pipeline consists of the following stages:

Extract: Retrieves raw data from the source.

Transform: Processes the data to make it analysis-ready.

Load: Loads the processed data into a target database.

Each stage can run independently via dedicated scripts (extract.py, transform.py, and load.py) or collectively through a main driver script (main.py).

Additionally, the pipeline uses DBT to perform advanced data manipulations, including joins and aggregations, on three main tables: users, carts, and products.

Key Scripts and Their Functions

main.py

Orchestrates the entire ETL process.

Executes extract.py, transform.py, and load.py sequentially.

Logs all activities and errors for monitoring.

extract.py

Extracts raw data from the source systems or files.

transform.py

Cleans and processes raw data.

Prepares data for loading into the target database.

load.py

Loads transformed data into the database.

Requirements

The requirements.txt file includes necessary dependencies:

requests

psycopg2-binary

python-dotenv

pandas

DBT (Savanna Project)

The DBT project, named Savanna, is used for complex data transformations and joins. DBT models are stored in the models directory.

Tables and Their Columns

carts:

cart_id

user_id

product_id

quantity

price

total_cart_value

products:

product_id

price

name

category

brand

users:

user_id

age

last_name

gender

postal_code

street

city

first_name

SQL Queries for Joins and Aggregations

Users and Carts:

Purpose: Combine demographic data with transaction details.

SELECT
    u.user_id,
    u.age,
    u.last_name,
    u.gender,
    u.postal_code,
    u.street,
    u.city,
    u.first_name,
    c.cart_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value
FROM
    users u
INNER JOIN
    carts c ON u.user_id = c.user_id;

Carts and Products:

Purpose: Enrich transaction data with product details.

SELECT
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value,
    p.name AS product_name,
    p.category,
    p.brand
FROM
    carts c
INNER JOIN
    products p ON c.product_id = p.product_id;

Generated Datasets

User Summary:

Fields: user_id, first_name, total_spent, total_items, age, city

Insights: Total spending and number of purchases per user.

SELECT
    u.user_id,
    u.first_name,
    SUM(c.total_cart_value) AS total_spent,
    SUM(c.quantity) AS total_items,
    u.age,
    u.city
FROM
    users u
LEFT JOIN
    carts c ON u.user_id = c.user_id
GROUP BY
    u.user_id, u.first_name, u.age, u.city;

Category Summary:

Fields: category, total_sales, total_items_sold

Insights: Aggregate sales by product category.

SELECT
    p.category,
    SUM(c.quantity * c.price) AS total_sales,
    SUM(c.quantity) AS total_items_sold
FROM
    carts c
INNER JOIN
    products p ON c.product_id = p.product_id
GROUP BY
    p.category;

Cart Details:

Fields: cart_id, user_id, product_id, quantity, price, total_cart_value

Insights: Transaction-level details enriched with user and product data.

SELECT
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value,
    u.first_name,
    u.last_name,
    u.city,
    p.name AS product_name,
    p.category,
    p.brand
FROM
    carts c
INNER JOIN
    users u ON c.user_id = u.user_id
INNER JOIN
    products p ON c.product_id = p.product_id;

Notes

The main.py script is ideal for running the entire pipeline with a single command.

Each ETL stage (extract.py, transform.py, load.py) can be executed separately if needed.

DBT models provide flexibility for advanced data transformations and aggregations.
