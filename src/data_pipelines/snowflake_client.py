import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
"""
This module provides a SnowflakeClient class to interact with Snowflake data warehouse.
It loads connection parameters from environment variables and allows executing SQL queries.

"""

class SnowflakeClient:
    def __init__(self,table=None):
        load_dotenv()  # Load environment variables from .env file
        if table is None:
            table = os.getenv("SNOWFLAKE_TABLE")
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            table=table,
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        

    def execute_query(self, query):
        """Execute a SQL query and return the results. takes a SQL query as input and 
           returns the results as a list of tuples.
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def close(self):
        """Close the Snowflake connection."""
        self.conn.close()
        
    def get_all_data(self) -> dict[str,pd.DataFrame]:
        query="show tables"
        with self.conn.cursor() as cur:
            cur.execute(query)
            tables = cur.fetchall()
            dataframes = {}
            for table in tables:
                table_name=table[1]
                query=f"SELECT * FROM {table_name}"
                cur.execute(query)
                df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
                dataframes[table_name] = df
            return dataframes
    
    def save_to_csv(self, dataframes: dict[str,pd.DataFrame], output_dir: str):
        """Save the dataframes to CSV files in the specified output directory."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        for table_name, df in dataframes.items():
            output_path = os.path.join(output_dir, f"{table_name}.csv")
            df.to_csv(output_path, index=False)
if __name__ == "__main__":
    # Example usage
    client = SnowflakeClient()
    #results = client.execute_query("SELECT * FROM fact_order_line LIMIT 10;")
    results=client.get_all_data()
    client.save_to_csv(results, "data/raw/")
    #results = client.execute_query("SELECT CURRENT_REGION(), CURRENT_VERSION();")
    
    client.close()