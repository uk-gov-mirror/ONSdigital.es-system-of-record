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
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()

    except:
        return json.loads('{"QueryTypes":"Failed To Connect To Database."}')

    try:
        table_list = {'query_type': None,
                      'vet': None,
                      'survey': None,
                      'gor_regions': None,
                      'ssr_old_regions': None}

        for current_table in table_list:
            table_model = alchemy_functions.table_model(engine, metadata, current_table)

            statement = db.select([table_model])

            table_data = alchemy_functions.select(statement, session)
            table_list[current_table] = table_data

    except:
        return json.loads('{"QueryTypes":"Failed To Retrieve Data."}')

    try:
        session.close()
    except:
        return json.loads('{"QueryTypes":"Connection To Database Closed Badly."}')

    out_json = '{"QueryTypes":'
    out_json += json.dumps(table_list["query_type"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json += ',"VETsCodes":'
    out_json += json.dumps(table_list["vet"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json += ',"Surveys":'
    out_json += json.dumps(table_list["survey"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json += ',"GorRegions":'
    out_json += json.dumps(table_list["gor_regions"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json += '}'

    try:
        io_validation.AllReferenceData(strict=True).loads(out_json)
    except ValidationError as err:
        return err.messages

    return json.loads(out_json)


x = lambda_handler('', '')
print(x)
