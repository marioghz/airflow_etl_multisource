# dependencies
import requests
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser
import sys
from airflow.exceptions import AirflowSkipException


# reading the configuration file containing the postgres credentials
config = ConfigParser()
config.read("pg_creds.cfg")


def connect_source():
     # attempt the connection to data source postgr
     dbconnect = pg.connect(
          database=config['postgres']['DATABASE'],
          user=config['postgres']['USERNAME'],
          password=config['postgres']['PASSWORD'],
          host=config['postgres']['HOST']
     )
     return dbconnect

#############################################################################
# Extract
#############################################################################


def fetchDataToLocal():

    
    # fetching the request
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)

    # convert the response to a pandas dataframe, then save as csv to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("date_of_interest")
    
    # for integrity reasons, let's attach the current date to the filename
    df.to_csv("files/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))

    print('####### Extract >> FINISHED #####')

#############################################################################
# Transform
#############################################################################


def sqlTransform():
    
    # attempt the connection to postgres
    conn = connect_source()
    
    # create the table if it does not already exist
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aux_covid_data (
            date date,
            case_count int,
            hospitalized_count int,
            death_count int
        );
    """
    )
    conn.commit()
    
    # insert each csv row as a record in our database
    with open("files/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d"))) as f:
        next(f) # skip the first row (header)
        for row in f:
            cursor.execute("""
                INSERT INTO aux_covid_data
                VALUES ('{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3])
            )
    conn.commit()

    cursor.execute("""
    insert into stg_covid_data
    select date, case_count, hospitalized_count
    from aux_covid_data a
     where not exists 
    	(select 1 from stg_covid_data b 
    	where a.date=b.date);
    """)
    conn.commit()
    print(cursor.rowcount, "Records inserted successfully into stg_covid_data table")

       #drop aux table
    cursor.execute("""drop table if exists aux_covid_data;""")
    conn.commit()
    conn.close()


    print('####### Transform >> FINISHED #####')


#############################################################################
# Load
#############################################################################


def sqlLoad():

    #########  source connection  ##############
    conn_source = connect_source()
    cursor_source = conn_source.cursor()

    v_today = date.today()
    v_end_date = '99991231'
    v_iscurrent = True

    v_query = """

    select * into aux_dim_covid_data from (
    select date, case_count, hospitalized_count, death_count 
    from stg_covid_data a
    where not exists 
    	(select 1 from dim_covid_data b 
    	where a.date=b.date)
    ) a;


    insert into dim_covid_data
    select date, case_count, hospitalized_count, death_count, '{}', '{}', '{}'
    from aux_dim_covid_data;
    """.format(v_today, v_end_date, v_iscurrent)

        ##############################################
        ############# SCD VALIDATION WIP #############
        ##############################################


    cursor_source.execute(v_query)
    conn_source.commit()
    print(cursor_source.rowcount, "Records inserted successfully into dim_olympics table")


    #drop aux table
    cursor_source.execute("""drop table if exists aux_dim_covid_data;""")
    conn_source.commit()
    conn_source.close()


    print('####### Extract >> FINISHED #####')





def hello():
    print('####################   Hello!!!!    ##########')