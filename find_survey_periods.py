import json
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    try:
        io_validation.SurveySearch(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    search_list = ['survey_period',
                   'survey_code']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"contributor_name":"Failed To Connect To Database."}')

    table_model = alchemy_functions.table_model(engine, metadata, "survey_period")
    all_query_sql = db.select([table_model])

    for criteria in search_list:
        if criteria not in event.keys():
            continue

        all_query_sql = all_query_sql.where(getattr(table_model.columns, criteria) == event[criteria])

    query = alchemy_functions.select(all_query_sql, session)

    out_json = json.dumps(query.to_dict(orient='records'), sort_keys=True, default=str)

    try:
        io_validation.SurveyPeriod(strict=True, many=True).loads(out_json)
    except ValidationError as err:
        return err.messages

    return json.loads(out_json)


x = lambda_handler({'survey_code': '066'}, '')
print(x)
