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
        result = ioValidation.FindSurvey(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    search_list = ['SurveyPeriod',
                   'SurveyOutputCode']
    all_query_sql = "SELECT * FROM es_db_test.Survey_Period WHERE"
    added_query_sql = ""
    for criteria in search_list:
        if event[criteria] is None:
            continue

        if event[criteria] == "":
            continue

        if added_query_sql == "":
            added_query_sql += " " + criteria + " = %(" + criteria + ")s"
        else:
            added_query_sql += " AND " + criteria + " = %(" + criteria + ")s"

    if added_query_sql == "":
        all_query_sql = all_query_sql[:-5]
        # return json.loads('{"surveyperiod":"No Search Data Provided."}')

    all_query_sql += added_query_sql + ";"

    try:
        connection = psycopg2.connect(host="es-results-db.cyjaepzpx1tk.eu-west-2.rds.amazonaws.com", database="es_results_db", user=usr, password=pas)

        survey_period = psql.read_sql(all_query_sql, connection, params={'SurveyPeriod': event['SurveyPeriod'], 'SurveyOutputCode': event['SurveyOutputCode']})

        connection.close()
    except:
        return json.loads('{"surveyperiod":"Something Has Gone Wrong"}')

    outJSON = json.dumps(survey_period.to_dict(orient='records'), sort_keys=True, default=str)

    try:
        result = ioValidation.SurveyPeriod(strict=True, many=True).loads(outJSON)
    except ValidationError as err:
        return err.messages

    return json.loads(outJSON)
