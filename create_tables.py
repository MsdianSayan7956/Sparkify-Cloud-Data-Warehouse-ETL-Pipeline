import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all existing tables to ensure a clean slate.
    
    Args:
        cur: Database cursor object
        conn: Database connection object
    """
    print("Dropping existing tables...")
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error dropping table: {e}")
            conn.rollback()
    print("Tables dropped successfully.")


def create_tables(cur, conn):
    """
    Create all staging and analytics tables based on the defined schema.
    
    Args:
        cur: Database cursor object
        conn: Database connection object
    """
    print("Creating tables...")
    for i, query in enumerate(create_table_queries):
        try:
            cur.execute(query)
            conn.commit()
            print(f"Table {i+1}/{len(create_table_queries)} created successfully.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            conn.rollback()
    print("All tables created successfully.")


def main():
    """
    Main function to establish database connection and create tables.
    
    Reads configuration from dwh.cfg file, connects to Redshift cluster,
    drops existing tables, and creates new ones.
    """
    # Read configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        # Connect to Redshift cluster
        print("Connecting to Redshift cluster...")
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()
            )
        )
        cur = conn.cursor()
        print("Connection established successfully.")

        # Drop and create tables
        drop_tables(cur, conn)
        create_tables(cur, conn)

        print("Database setup completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up connection
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()