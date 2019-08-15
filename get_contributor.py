import json
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    # try:
    #     ioValidation.ContributorSearch(strict=True).load(test_data.txt)
    # except ValidationError as err:
    #     return err.messages

    ref = event['ru_ref']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Failed To Connect To Database."}')

    try:
        table_list = {'contributor': None,
                      'survey_enrolment': None,
                      'survey_contact': None,
                      'contributor_survey_period': None}

        for current_table in table_list:
            table_model = alchemy_functions.table_model(engine, metadata, current_table)

            statement = db.select([table_model]).where(table_model.columns.ru_reference == ref)

            if current_table == "survey_contact":
                other_model = alchemy_functions.table_model(engine, metadata, "contact")
                joined = table_model.join(other_model)
                statement = statement.select_from(joined)

            table_data = alchemy_functions.select(statement, session)
            table_list[current_table] = table_data

    except:
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Failed To Retrieve Data."}')

    try:
        session.close()
    except:
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Database Session Closed Badly."}')

    out_json = json.dumps(table_list["contributor"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json = out_json[1:-2]
    out_json += ',"Surveys":[ '

    for index, row in table_list['survey_enrolment'].iterrows():
        curr_row = table_list['survey_enrolment'][(table_list['survey_enrolment']['survey_code']
                                                   == row['survey_code'])]
        curr_row = json.dumps(curr_row.to_dict(orient='records'), sort_keys=True, default=str)
        curr_row = curr_row[2:-2]

        out_json = out_json + "{" + curr_row + ',"Contacts":'
        curr_con = table_list['survey_contact'][(table_list['survey_contact']['survey_code']
                                                 == row['survey_code'])]
        curr_con = json.dumps(curr_con.to_dict(orient='records'), sort_keys=True, default=str)
        out_json += curr_con

        out_json = out_json + ',"Periods":'
        curr_per = table_list['contributor_survey_period'][(table_list['contributor_survey_period']['survey_code']
                                                            == row['survey_code'])]
        curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
        out_json += curr_per + '},'

    out_json = out_json[:-1]
    out_json += ']}'
    return json.loads(out_json)


x = lambda_handler({"ru_ref": "77700000001"}, '')
print(x)
