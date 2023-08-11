# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 12:38:27 2023

@author: ayusman
"""

from sqlalchemy import create_engine
import pandas as pd

def load(host, database, user, password, df, table_name):

    """
    Load data from a DataFrame into a PostgreSQL table.

    This function establishes a connection to a redshift database using the provided
    host, database, user, and password information. It then appends the data from the
    provided DataFrame (`df`) into the specified table (`table_name`) within the database.

    Parameters:
    - host (str): The hostname or IP address of the redshift server.
    - database (str): The name of the redshift database.
    - user (str): The username for connecting to the redshift server.
    - password (str): The password associated with the given username.
    - df (pandas.DataFrame): The DataFrame containing the data to be loaded.
    - table_name (str): The name of the target table in the database.
    
    Returns:
    - None

    Note:
    - The function uses the `sqlalchemy` library to create a redshift engine and
      establish a connection to the database.
    - The data in the provided DataFrame (`df`) will be appended to the specified
      table (`table_name`) in the database using the `to_sql` method with the
      `if_exists='append'` option.
    - The `chunksize` parameter specifies the number of rows to be inserted in a single
      batch. This can help manage memory usage when inserting a large amount of data.

    Example:
    host = "localhost"
    database = "mydb"
    user = "myuser"
    password = "mypassword"
    df = pd.DataFrame(...)  # Create or load the DataFrame
    table_name = "my_table"
    load(host, database, user, password, df, table_name)


    """
    
    host=host
    database= database
    user= user
    password= password
    engine = create_engine('postgresql://%s:%s@%s:5439/%s'%(user, password, host,  database))
    conn = engine.connect()

    #sql_stmnt = "select * from table_name"
    # Reading the DataFiles
    #cursor = conn.execute(sql_stmnt)
    #df = pd.DataFrame(cursor.fetchall())
    
    df.to_sql(table_name, conn, index = False, if_exists = 'append', chunksize = 500)
    
    