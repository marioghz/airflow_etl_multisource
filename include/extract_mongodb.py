import pymongo
import pandas as pd
from pymongo import MongoClient
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser

# reading the configuration file containing the postgres credentials
config = ConfigParser()
config.read("pg_creds.cfg")


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


def extact_mongodb_student():
    client = MongoClient('mongodb://marioghz:marioghz@host.docker.internal:27017/admin')
    result = client['bd_school']['coll_grade'].aggregate([
        {
            '$lookup': {
                'from': 'coll_student', 
                'localField': 'student_id', 
                'foreignField': '_id', 
                'as': 'students'
            }
        }, {
            '$project': {
                'student_id': 1, 
                'class_id': 1, 
                'scores.type': 1, 
                'scores.score': 1, 
                'students.name': 1
            }
        }
    ])
    list_cursor = list(result)
    df = pd.DataFrame(list_cursor)
    df.drop('_id', axis=1, inplace=True)
    
    # for integrity reasons, let's attach the current date to the filename
    df.to_csv("files/school_{}.csv".format(date.today().strftime("%Y%m%d")))

    print('####### Extract >> FINISHED #####')



def sqlTransform():

    conn_source = connect_source()

    # create the table if it does not already exist
    cursor = conn_source.cursor()
    
    #truncate raw_data
    cursor.execute("""truncate table raw_student;""")
    conn_source.commit()
    
    # insert each csv row as a record in our database
    with open("files/school_{}.csv".format(date.today().strftime("%Y%m%d"))) as f:
        next(f) # skip the first row (header)
        v_count = 0
        for row in f:

            v_replace_quote = str(row.replace("'",""))
            cursor.execute("""
                INSERT INTO raw_student
                VALUES ('{}')
            """.format(v_replace_quote)
            )
            v_count = v_count+1
    print('########' + str(v_count) + ' Rows inserted into raw_student ########')
    
    conn_source.commit()
    conn_source.close()




def sqlLoad():
    

    #########  source connection  ##############
    conn_source = connect_source()
    cursor_source = conn_source.cursor()


    df = pd.read_csv("files/school_{}.csv".format(date.today().strftime("%Y%m%d")))

    df_students = df[['student_id','students']].drop_duplicates()
    
    v_today = date.today()
    v_end_date = '99991231'
    v_iscurrent = True
    v_count = 0
    for i, row in df_students.iterrows():
        v_student = str(row.students).replace("[{'name': '","")
        v_student = v_student.replace("'}]","")

        cursor_source.execute("""

            INSERT INTO dim_student (ID, STUDENT_NAME, START_DATE, END_DATE, IS_CURRENT)
            SELECT '{}', '{}', '{}', '{}', '{}'
            WHERE NOT EXISTS (SELECT 1 FROM dim_student WHERE ID = '{}')
        """.format(row.student_id, v_student, v_today, v_end_date, v_iscurrent, row.student_id)
        )
        v_count = v_count+1

    print('########' + str(v_count) + ' Rows inserted into dim_student ########')


    ##############################################
    ############# SCD VALIDATION WIP #############
    ##############################################

    conn_source.commit()
    conn_source.close()


   