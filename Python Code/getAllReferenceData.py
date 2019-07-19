from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import os


def lambda_handler(event, context):
    usr = os.environ['Username']
    pas = os.environ['Password']

    try:
        connection = psycopg2.connect(host="", database="es_results_db", user=usr, password=pas)
    except:
        return json.loads('{"QueryTypes":"Failed To Connect To Database."}')

    try:
        query_types = psql.read_sql("SELECT * FROM es_db_test.Query_Type;", connection)
        vets = psql.read_sql("SELECT * FROM es_db_test.VET;", connection)
        surveys = psql.read_sql("SELECT * FROM es_db_test.Survey;", connection)
        gorregion = psql.read_sql("SELECT * FROM es_db_test.GOR_Regions;", connection)
        ssrregion = psql.read_sql("SELECT * FROM es_db_test.SSR_Old_Regions;", connection)

    except:
        return json.loads('{"QueryTypes":"Failed To Retrieve Data."}')

    try:
        connection.close()
    except:
        return json.loads('{"QueryTypes":"Connection To Database Closed Badly."}')

    outJSON = '{"QueryTypes":'
    outJSON += json.dumps(query_types.to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"VETsCodes":'
    outJSON += json.dumps(vets.to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"Surveys":'
    outJSON += json.dumps(surveys.to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"GovRegions":'
    outJSON += json.dumps(gorregion.to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"SSRoldRegions":'
    outJSON += json.dumps(ssrregion.to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += '}'

    return json.loads(outJSON)
