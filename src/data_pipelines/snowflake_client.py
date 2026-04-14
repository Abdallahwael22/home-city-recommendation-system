import os
from dotenv import load_dotenv
import snowflake.connector
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
        
        

if __name__ == "__main__":
    # Example usage
    client = SnowflakeClient()
    #results = client.execute_query("SELECT * FROM fact_order_line LIMIT 10;")
    results=client.execute_query("SELECT * FROM fact_order_line LIMIT 10;")
    #results = client.execute_query("SELECT CURRENT_REGION(), CURRENT_VERSION();")
    print(results)
    client.close()