import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load raw data from S3 into staging tables.
    
    Copies JSON files from S3 buckets into staging_events and staging_songs tables.
    Uses COPY command for optimal bulk loading performance.
    
    Args:
        cur: Database cursor object
        conn: Database connection object
    """
    print("Loading data from S3 to staging tables...")
    
    for i, query in enumerate(copy_table_queries):
        try:
            print(f"Executing COPY command {i+1}/{len(copy_table_queries)}...")
            cur.execute(query)
            conn.commit()
            print(f"COPY command {i+1} completed successfully.")
        except psycopg2.Error as e:
            print(f"Error loading staging table: {e}")
            conn.rollback()
            raise e
    
    print("All staging tables loaded successfully.")



def insert_tables(cur, conn):
    """
    Transform and load data from staging tables into analytics tables.
    
    Processes data from staging tables and inserts into the star schema:
    - songplays (fact table)
    - users, songs, artists, time (dimension tables)
    
    Args:
        cur: Database cursor object  
        conn: Database connection object
    """
    print("Transforming data from staging to analytics tables...")
    
    table_names = ['songplays', 'users', 'songs', 'artists', 'time']
    
    for i, query in enumerate(insert_table_queries):
        try:
            print(f"Inserting data into {table_names[i]} table...")
            cur.execute(query)
            conn.commit()
            
            # Get row count for validation
            cur.execute(f"SELECT COUNT(*) FROM {table_names[i]}")
            count = cur.fetchone()[0]
            print(f"{table_names[i]} table: {count:,} rows inserted.")
            
        except psycopg2.Error as e:
            print(f"Error inserting into {table_names[i]} table: {e}")
            conn.rollback()
            raise e
    
    print("All analytics tables populated successfully.")



def main():
    """
    Main ETL pipeline execution function.
    
    Establishes database connection, loads staging tables from S3,
    and transforms data into analytics tables.
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
        
        # Execute ETL pipeline
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        
        # Final validation
        print("\nETL Pipeline Summary:")
        print("=" * 40)
        
        tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"{table:<15}: {count:>8,} rows")
        
        print("=" * 40)
        print("ETL pipeline completed successfully!")

    except Exception as e:
        print(f"ETL pipeline failed with error: {e}")
        raise e
    finally:
        # Clean up connection
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()