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


def connect_target():
    # attempt the connection to my local postgres
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


def extract():

    conn_source = connect_source()
    conn_target = connect_target()


    #########  source connection  ##############
    
    cursor_source = conn_source.cursor()
    cursor_source.execute("""
        drop table if exists aux_prd_olympics;

        select id, name, sex, age, height, 
        weight, team, noc, games, year, 
        season, city, sport, event, medal
        into aux_prd_olympics
        FROM prd_olympics a
        where not exists 
    	(select 1 from raw_olympics b 
    	where a.id=b.id and a.name=b.name and a.event=b.event);

        select id, name, sex, age, height, 
        weight, team, noc, games, year, 
        season, city, sport, event, medal
        FROM aux_prd_olympics;
    """
    )
    conn_source.commit()
    records = cursor_source.fetchall()
    print(cursor_source.rowcount, "Records extracted successfully from prd_olympics table")

    #########  target connection  ##############
    
    v_query = """ insert into raw_olympics 
        (id, name, sex, age, height, 
        weight, team, noc, games, year, 
        season, city, sport, event, medal)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    
    cursor_target = conn_target.cursor()
    cursor_target.executemany(v_query, records)
    conn_target.commit()
    print(cursor_target.rowcount, "Records inserted successfully into raw_olympics table")

    #drop aux table
    cursor_source.execute("""drop table if exists aux_prd_olympics;""")
    conn_source.commit()
    conn_source.close()
    conn_target.close()

    
    print('####### Extract >> FINISHED #####')

#############################################################################
# Transform
#############################################################################


def sqlTransform():
    
    # attempt the connection to postgres
    conn = connect_target()
    
    # create the table if it does not already exist
    # removing dups using aux table
    cursor = conn.cursor()
    cursor.execute("""
        drop table if exists aux_dups;
        
        select * into aux_dups from (
        select  distinct a.id, a.name, a.sex, a.age, a.height, 
        a.weight, a.team, a.noc, a.games, a.year, 
        a.season, a.city, a.sport, a.event, a.medal 
        from raw_olympics a 
        inner join (
        select id, name, event, count(1) as cnt
		from raw_olympics
		group by id, name, event
		having count(1)>1
        ) b on (a.id=b.id and a.name=b.name and a.event=b.event)
        ) c;   
    """
    )
    conn.commit()

    # delete the dups using the aux table
    cursor.execute("""
        delete from stg_olympics a
        using aux_dups b where (a.id=b.id and a.name=b.name and a.event=b.event)
    """
    )
    conn.commit()
    print(cursor.rowcount, "Records deleted successfully")

    #inserting the distinct values into the original table 
    cursor.execute("""
        	insert into stg_olympics
            select id, name, sex, age, height, 
            weight, team, noc, games, year, 
            season, city, sport, event, medal
            from aux_dups;
    """
    )
    conn.commit()
    print(cursor.rowcount, "Records re-inserted successfully into olympics")


    #drop aux table
    cursor.execute("""drop table if exists aux_dups;""")
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
    select * into aux_dim_olympics from(
    select id, name, sex, age, height, 
    weight, team, noc, games, year, 
    season, city, sport, event, medal from stg_olympics a
    where not exists 
    	(select 1 from dim_olympics b 
    	where a.id=b.id and a.name=b.name and a.event=b.event)
    ) a;
    

    insert into dim_olympics
    select id, name, sex, age, height, 
    weight, team, noc, games, year, 
    season, city, sport, event, medal, '{}', '{}', '{}'
    from aux_dim_olympics;
    """.format(v_today, v_end_date, v_iscurrent)

        ##############################################
        ############# SCD VALIDATION WIP #############
        ##############################################


    cursor_source.execute(v_query)
    conn_source.commit()
    print(cursor_source.rowcount, "Records inserted successfully into dim_olympics table")


    #drop aux table
    cursor_source.execute("""drop table if exists aux_dim_olympics;""")
    conn_source.commit()
    conn_source.close()

    print('####### Load >> FINISHED #####')