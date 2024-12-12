import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()

# Database connection settings from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# SQL table creation queries
CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(50),
    age INT,
    street VARCHAR(255),
    city VARCHAR(255),
    postal_code VARCHAR(50)
);
"""

CREATE_PRODUCTS_TABLE = """
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    price NUMERIC
);
"""

CREATE_CARTS_TABLE = """
CREATE TABLE IF NOT EXISTS carts (
    cart_id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    price NUMERIC,
    total_cart_value NUMERIC
);
"""

# Insert queries
INSERT_USERS = """
INSERT INTO users (user_id, first_name, last_name, gender, age, street, city, postal_code) 
VALUES %s ON CONFLICT (user_id) DO NOTHING;
"""
INSERT_PRODUCTS = """
INSERT INTO products (product_id, name, category, brand, price) 
VALUES %s ON CONFLICT (product_id) DO NOTHING;
"""
INSERT_CARTS = """
INSERT INTO carts (cart_id, user_id, product_id, quantity, price, total_cart_value) 
VALUES %s ON CONFLICT (cart_id) DO NOTHING;
"""

def load_data_to_postgres(data, table_name, create_query, insert_query):
    """Load data into a PostgreSQL table."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Execute table creation query
        cursor.execute(create_query)

        # Insert data
        if data:
            execute_values(cursor, insert_query, data)
            conn.commit()
            print(f"Data successfully loaded into {table_name} table.")
        else:
            print(f"No data to load into {table_name} table.")

    except Exception as e:
        print(f"Error loading data into {table_name} table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def process_and_load_all():
    """Process and load all transformed data into the database."""
    datasets = {
        "users": ("cleaned_users.csv", CREATE_USERS_TABLE, INSERT_USERS),
        "products": ("cleaned_products.csv", CREATE_PRODUCTS_TABLE, INSERT_PRODUCTS),
        "carts": ("cleaned_carts.csv", CREATE_CARTS_TABLE, INSERT_CARTS),
    }

    for table_name, (filepath, create_query, insert_query) in datasets.items():
        try:
            # Read CSV file
            if not os.path.exists(filepath):
                print(f"File {filepath} not found. Skipping {table_name} table.")
                continue

            df = pd.read_csv(filepath)

            # Convert DataFrame to list of tuples and ensure native Python types
            records = df.astype(object).where(pd.notnull(df), None).to_records(index=False)
            records = [tuple(record) for record in records]

            # Load data into the PostgreSQL database
            load_data_to_postgres(records, table_name, create_query, insert_query)
        except Exception as e:
            print(f"Error processing {table_name} table: {e}")

if __name__ == "__main__":
    print("Starting data loading process...")
    process_and_load_all()
    print("Data loading process completed.")
