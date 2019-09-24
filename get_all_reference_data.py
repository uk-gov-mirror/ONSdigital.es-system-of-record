import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("get_all_reference_data")


def lambda_handler(event, context):
    """Pull back from a database all reference data from five tables."""

    logger.info("get_all_reference_data Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

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
        table_list = {'query_type': None,
                      'vet': None,
                      'survey': None,
                      'gor_regions': None,
                      'ssr_old_regions': None}

        for current_table in table_list:
            logger.info("Fetching Table Model: {}".format(current_table))
            table_model = alchemy_functions.table_model(engine, metadata,
                                                        current_table)

            logger.info("Fetching Table Data: {}".format(current_table))
            statement = session.query(table_model).all()

            logger.info("Converting Data: {}".format(current_table))
            table_data = alchemy_functions.to_df(statement)
            table_list[current_table] = table_data
    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Retrieving Data: {} {}"
            .format(current_table, exc))
        return {"statusCode": 500,
                "body": {"Error":
                         "Operation Error, Failed To Retrieve Data: {}"
                         .format(current_table)}}
    except Exception as exc:
        logger.error("Problem Retrieving Data From The Table: {} {}"
                     .format(current_table, exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Retrieve Data: {}"
                         .format(current_table)}}

    logger.info("Creating JSON.")
    out_json = '{"QueryTypes":'
    out_json += json.dumps(table_list["query_type"].to_dict(orient='records'),
                           sort_keys=True, default=str)
    out_json += ',"VETsCodes":'
    out_json += json.dumps(table_list["vet"].to_dict(orient='records'),
                           sort_keys=True, default=str)
    out_json += ',"Surveys":'
    out_json += json.dumps(table_list["survey"].to_dict(orient='records'),
                           sort_keys=True, default=str)
    out_json += ',"GorRegions":'
    out_json += json.dumps(table_list["gor_regions"].to_dict(orient='records'),
                           sort_keys=True, default=str)
    out_json += '}'

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
        io_validation.AllReferenceData(strict=True).loads(out_json)
    except ValidationError as exc:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

    logger.info("get_all_reference_data Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}
