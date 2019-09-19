import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy import func
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("get_survey_periods")


def lambda_handler(event, context):
    """Collects data on a passed in Reference from a table and returns a single
    Json.
    Parameters:
      event (Dict):Two key value pairs used in the search.
    Returns:
      out_json (Json):Json responce of the table data.
    """

    logger.info("get_survey_periods Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.SurveySearch(strict=True).load(event)
    except ValidationError as exc:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

    search_list = ['survey_period',
                   'survey_code']

    try:
        logger.info("Connecting To The Database.")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as exc:
        logger.error("Driver Error, Failed To Connect: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Driver Error, Failed To Connect."}}
    except db.exc.OperationalError as exc:
        logger.error("Operational Error Encountered: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, Failed To Connect."}}
    except Exception as exc:
        logger.error("Failed To Connect To The Database: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Connect To The Database."}}

    try:
        logger.info("Fetching Table Model: {}".format("survey_period"))
        table_model = alchemy_functions.table_model(engine, metadata,
                                                    "survey_period")

        logger.info("Fetching Table Data: {}".format("survey_period"))
        all_query_sql = session.query(table_model)

        added_query_sql = 0

        for criteria in search_list:
            if criteria not in event.keys():
                logger.info("No parameters have been passed for {}."
                            .format(criteria))
                continue

            added_query_sql += 1
            all_query_sql = all_query_sql.filter(getattr(table_model.columns,
                                                        criteria) ==
                                                event[criteria])

        if added_query_sql == 0:
            all_query_sql = all_query_sql\
                .filter(table_model.columns.survey_period == session.query(
                       func.max(table_model.columns.survey_period)))

        logger.info("Converting Data: {}".format("survey_period"))
        query = alchemy_functions.to_df(all_query_sql)
    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Retrieving Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Retrieve Data."}}
    except Exception as exc:
        logger.error("Problem Retrieving Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Retrieve Data."}}

    logger.info("Creating JSON.")
    out_json = json.dumps(query.to_dict(orient='records'), sort_keys=True,
                          default=str)

    try:
        logger.info("Closing Session.")
        session.close()
    except db.exc.OperationalError as exc:
        logger.error(
            "Operational Error, Failed To Close The Session: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Database Session Closed Badly."}}
    except Exception as exc:
        logger.error("Failed To Close The Session: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Database Session Closed Badly."}}

    try:
        io_validation.SurveyPeriod(strict=True, many=True).loads(out_json)
    except ValidationError as exc:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

    logger.info("get_survey_periods Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}
