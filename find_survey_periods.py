import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session
from sqlalchemy.exc import DatabaseError

import alchemy_functions
import io_validation

logger = logging.getLogger("find_survey_periods")

def lambda_handler(event, context):
    """Collects data on a passed in Reference from a tablea and returns a single Json.
    Parameters:
      event (Dict):Two key value pairs used in the search.
    Returns:
      out_json (Json):Json responce of the table data.
    """
    database = os.environ['Database_Location']

    logger.info("INPUT DATA: {}".format(event))

#    try:
#        ioValidation.FindSurvey(strict=True).load(test_data.txt)
#    except ValidationError as err:
#        return err.messages

    search_list = ['survey_period',
                   'survey_code']

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return json.loads('{"contributor_name":"Failed To Connect To Database."}')

    table_model = alchemy_functions.table_model(engine, metadata, "survey_period")
    all_query_sql = db.select([table_model])

    for criteria in search_list:
        if event[criteria] is None:
            logger.info("No parameters have been passed")
            continue

        if event[criteria] == "":
            logger.info("No parameters have been passed")
            continue

        all_query_sql = all_query_sql.where(getattr(table_model.columns, criteria) == event[criteria])

    query = alchemy_functions.select(all_query_sql, session)

    out_json = json.dumps(query.to_dict(orient='records'), sort_keys=True, default=str)

    try:
        session.close()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to close the database session: {}".format(exc))
        return json.loads('{"query_reference":"Connection To Database Closed Badly."}')

#    try:
#        ioValidation.SurveyPeriod(strict=True, many=True).loads(out_json)
#    except ValidationError as err:
#        return err.messages

    return json.loads(out_json)


x = lambda_handler({'survey_period': '',
                   'survey_code': '066'}, '')
print(x)
