from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import pandas
import os
from datetime import datetime


def lambda_handler(event, context):
    usr = os.environ['Username']
    pas = os.environ['Password']

    try:
        result = ioValidation.ContributorUpdate(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    try:
        connection = psycopg2.connect(host="",
                                      database="es_results_db", user=usr, password=pas)
    except:
        return json.loads('{"ContributorData":"Failed To Connect To Database."}')

    currentTime = str(datetime.now())

    try:
        contributorSQL = ("UPDATE es_db_test.Contributor_Survey_Period SET"
                          + " AdditionalComments = %(adcoms)s"
                          + ", ContributorComments = %(contribcoms)s"
                          + ", LastUpdated = '" + currentTime
                          + "'  WHERE SurveyPeriod = %(surper)s"
                          + "  AND RUReference = %(ref)s"
                          + "  AND SurveyOutputCode = %(surcode)s"
                          + ";")
        psql.execute(contributorSQL, connection, params={"adcoms": event["additionalcomments"], "contribcoms": event["contributorcomments"],
                                                         "surper": event["surveyperiod"], "ref": event["rureference"],
                                                         "surcode": event["surveyoutputcode"]})
    except:
        return json.loads('{"ContributorData":"Failed To Update Contributor_Survey_Period."}')

    try:
        connection.commit()
    except:
        return json.loads('{"ContributorData":"Failed To Commit Changes To The Database."}')

    try:
        connection.close()
    except:
        return json.loads('{"ContributorData":"Connection To Database Closed Badly."}')

    return json.loads('{"ContributorData":"Successfully Updated The Table."}')
