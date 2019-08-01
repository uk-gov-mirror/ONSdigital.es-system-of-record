import json
import psycopg2
import pandas.io.sql as psql
import os


def lambda_handler(event, context):
    usr = os.environ['Username']
    pas = os.environ['Password']

    try:
        connection = psycopg2.connect(host="",
                                      database="es_results_db", user=usr, password=pas)
    except:
        return json.loads('{"SurveyPeriod":"Failed To Connect To Database."}')

    try:
        contributorSQL = ("UPDATE es_db_test.Survey_Period SET "
                          + "ActivePeriod = %(activeperiod)s, "
                          + "NoOfResponses = %(noofresponses)s, "
                          + "NumberCleared = %(numbercleared)s, "
                          + "NumberClearedFirstTime = %(numberclearedfirsttime)s, "
                          + "SampleSize = %(samplesize)s "
                          + "WHERE SurveyOutputCode = %(surveyoutputcode)s "
                          + "AND SurveyPeriod = %(surveyperiod)s;")

        psql.execute(contributorSQL, connection, params={"activeperiod": event["activeperiod"], "noofresponses": event["noofresponses"],
                                                         "surveyperiod": event["surveyperiod"], "numbercleared": event["numbercleared"],
                                                         "numberclearedfirsttime": event["numberclearedfirsttime"], "samplesize": event["samplesize"],
                                                         "surveyoutputcode": event["surveyoutputcode"]})
    except:
        return json.loads('{"SurveyPeriod":"Failed To Update Survey_Period."}')

    # contributorSQL = "UPDATE es_db_test.Survey_Period SET "
    # if event['FirstTime'] == True:
    #     contributorSQL += "NumberClearedFirstTime = %(cleared)s, "

    # contributorSQL += ( "NumberCleared = %(cleared)s"
    #                   + "  WHERE SurveyPeriod = %(surper)s"
    #                   + "  AND SurveyOutputCode = %(surcode)s"
    #                   + ";")

    # psql.execute(contributorSQL, connection, params={"cleared": event["cleared"],
    #                                                  "surper": event["surveyperiod"],
    #                                                  "surcode": event["surveyoutputcode"]})

    try:
        connection.commit()
    except:
        return json.loads('{"SurveyPeriod":"Failed To Commit Changes To The Database."}')

    try:
        connection.close()
    except:
        return json.loads('{"SurveyPeriod":"Connection To Database Closed Badly."}')

    return json.loads('{"SurveyPeriod":"Successfully Updated The Table."}')
