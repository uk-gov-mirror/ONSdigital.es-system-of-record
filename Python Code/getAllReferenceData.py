from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql
import os
import sqlalchemy as db


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    try:
        engine = db.create_engine(database)
        connection = engine.connect()
        metadata = db.MetaData()

    except:
        return json.loads('{"QueryTypes":"Failed To Connect To Database."}')

    try:
        query_type_table = db.Table('query_type', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
        vet_table = db.Table('vet', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
        survey_table = db.Table('survey', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
        gor_region_table = db.Table('gor_regions', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
        ssr_region_table = db.Table('ssr_old_regions', metadata, schema='es_db_test', autoload=True, autoload_with=engine)

        query_type_statement = db.select([query_type_table])
        vet_statement = db.select([vet_table])
        survey_statement = db.select([survey_table])
        gor_region_statement = db.select([gor_region_table])
        ssr_region_statement = db.select([ssr_region_table])

        query_type_return = connection.execute(query_type_statement).fetchall()
        vet_return = connection.execute(vet_statement).fetchall()
        survey_return = connection.execute(survey_statement).fetchall()
        gor_region_return = connection.execute(gor_region_statement).fetchall()
        ssr_region_return = connection.execute(ssr_region_statement).fetchall()

    except:
        return json.loads('{"QueryTypes":"Failed To Retrieve Data."}')

    try:
        connection.close()
    except:
        return json.loads('{"QueryTypes":"Connection To Database Closed Badly."}')

    outJSON = '{"QueryTypes":'
    outJSON += json.dumps([(dict(row.items())) for row in query_type_return])
    outJSON += ',"VETsCodes":'
    outJSON += json.dumps([(dict(row.items())) for row in vet_return])
    outJSON += ',"Surveys":'
    outJSON += json.dumps([(dict(row.items())) for row in survey_return])
    outJSON += ',"GovRegions":'
    outJSON += json.dumps([(dict(row.items())) for row in gor_region_return])
    outJSON += ',"SSRoldRegions":'
    outJSON += json.dumps([(dict(row.items())) for row in ssr_region_return])
    outJSON += '}'

    return json.loads(outJSON)


x = lambda_handler('', '')
print(x)
