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
    try:
        io_validation.SurveySearch(strict=True).load(event)
    except ValidationError as err:
        logger.error("Failed to validate input: {}".format(err.messages))
        return {"statusCode": 500, "body": {err.messages}}

    search_list = ['survey_period',
                   'survey_code']

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as exc:
        logger.error("Error: Failed to connect to the database(driver error): {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Failed To Connect To Database." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Failed To Connect To Database." + str(type(exc))}}
    except Exception as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Failed To Connect To Database." + str(type(exc))}}

    table_model = alchemy_functions.table_model(engine, metadata, "survey_period")
    all_query_sql = db.select([table_model])

    for criteria in search_list:
        if criteria not in event.keys():
            logger.info("No parameters have been passed for {}.".format(criteria))
            continue

        all_query_sql = all_query_sql.where(getattr(table_model.columns, criteria) == event[criteria])

    try:
        query = alchemy_functions.select(all_query_sql, session)
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed To select data: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Failed To select data." + str(type(exc))}}
    except Exception as exc:
        logger.error("Error: Failed To select data: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Failed To select data." + str(type(exc))}}

    out_json = json.dumps(query.to_dict(orient='records'), sort_keys=True, default=str)

    try:
        session.close()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to close the database session: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Database Session Closed Badly."}}
    except Exception as exc:
        logger.error("Error: Failed to close the database session: {}".format(exc))
        return {"statusCode": 500, "body": {"contributor_name": "Database Session Closed Badly."}}

    try:
        io_validation.SurveyPeriod(strict=True, many=True).loads(out_json)
    except ValidationError as err:
        logger.error("Failed to validate output: {}".format(err.messages))
        return {"statusCode": 500, "body": {err.messages}}
    logger.info("Successfully completed get_query")
    return {"statusCode": 200, "body": {out_json}}


x = lambda_handler({'survey_code': '066'}, '')
print(x)
