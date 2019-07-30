from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas as pd
import os
import sqlalchemy as db


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    # try:
    #     result = ioValidation.ContributorSearch(strict=True).load(event)
    # except ValidationError as err:
    #     return err.messages

    Ref = event['RURef']

    try:
        engine = db.create_engine(database)
        connection = engine.connect()
        metadata = db.MetaData()
    except:
        return json.loads('{"rureference":"' + Ref + '","contributorname":"Failed To Connect To Database."}')

    try:
        contributor_table = db.Table('contributor', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
#        contributor = psql.read_sql("SELECT * FROM es_db_test.Contributor WHERE RUReference = %(Ref)s;", connection, params={'Ref': Ref})
#        survey_enrolment = psql.read_sql("SELECT * FROM es_db_test.Survey_Enrolment WHERE RUReference = %(Ref)s;", connection, params={'Ref': Ref})
#        survey_contact = psql.read_sql("SELECT a.RUReference, a.SurveyOutputCode, a.EffectiveEndDate, a.EffectiveStartDate, b.* FROM es_db_test.Survey_Contact a, es_db_test.Contact b WHERE a.RUReference = %(Ref)s AND a.ContactReference = b.ContactReference;", connection, params={'Ref': Ref})
#        contributor_sp = psql.read_sql("SELECT * FROM es_db_test.Contributor_Survey_Period WHERE RUReference = %(Ref)s;", connection, params={'Ref': Ref})
    except:
        return json.loads('{"rureference":"' + Ref + '","contributorname":"Failed To Retrieve Data."}')

    contributor_statement = db.select([contributor_table]).where(contributor_table.columns.ru_reference == Ref)

    contributor_return = connection.execute(contributor_statement).fetchall()
    try:
        connection.close()
    except:
        return json.loads('{"rureference":"' + Ref + '","contributorname":"Connection To Database Closed Badly."}')


    contributor = pd.DataFrame(contributor_return)
    if not contributor.empty:
        contributor.columns = contributor_return[0].keys()
    else:
        return json.loads('{"rureference":"' + Ref + '","contributorname":"RU Reference does not exist."}')
    outJSON = json.dumps(contributor.to_dict(orient='records'), sort_keys=True, default=str)
    #outJSON = json.dumps([(dict(row.items())) for row in contributor_return])
    outJSON = outJSON[1:-2]
    outJSON += ',"Surveys":[ '

    # for index, row in survey_enrolment.iterrows():
    #     curr_row = survey_enrolment[survey_enrolment['surveyoutputcode'] == row['surveyoutputcode']]
    #     curr_row = json.dumps(curr_row.to_dict(orient='records'), sort_keys=True, default=str)
    #     curr_row = curr_row[2:-2]
    #
    #     outJSON = outJSON + "{" + curr_row + ',"Contacts":'
    #     curr_con = survey_contact[(survey_contact['surveyoutputcode'] == row['surveyoutputcode'])]
    #     curr_con = json.dumps(curr_con.to_dict(orient='records'), sort_keys=True, default=str)
    #     outJSON += curr_con
    #
    #     outJSON = outJSON + ',"Periods":'
    #     curr_per = contributor_sp[(contributor_sp['surveyoutputcode'] == row['surveyoutputcode'])]
    #     curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
    #     outJSON += curr_per + '},'

    outJSON = outJSON[:-1]
    outJSON += ']}'
    print(outJSON)
    return json.loads(outJSON)


x = lambda_handler({"RURef":"12334547474"}, '')
print(x)
