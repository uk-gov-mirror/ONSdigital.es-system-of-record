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

    try:
        database, error = io_validation.Database(strict=True).load(os.environ)
    except ValidationError as e:
        logger.error(
            "Database_Location Environment Variable" +
            "Has Not Been Set Correctly: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": "Configuration Error: {}"
                .format(e)}}

    try:
        logger.info("Connecting To The Database.")
        engine = db.create_engine(database['Database_Location'])
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as e:
        logger.error("Driver Error, Failed To Connect: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Driver Error, Failed To Connect."}}
    except db.exc.OperationalError as e:
        logger.error("Operational Error Encountered: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, Failed To Connect."}}
    except Exception as e:
        logger.error(
            "General Error, Failed To Connect To The Database: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, " +
                                  "Failed To Connect To The Database."}}

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
    except db.exc.OperationalError as e:
        logger.error(
            "Operational Error, When Retrieving Data: {} {}"
            .format(current_table, e))
        return {"statusCode": 500,
                "body": {"Error":
                         "Operational Error, Failed To Retrieve Data: {}"
                         .format(current_table)}}
    except Exception as e:
        logger.error(
            "General Error, Problem Retrieving Data From The Table: {} {}"
            .format(current_table, e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, Failed To Retrieve Data: {}"
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
    except db.exc.OperationalError as e:
        logger.error(
            "Operational Error, Failed To Close The Session: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, " +
                                  "Database Session Closed Badly."}}
    except Exception as e:
        logger.error("General Error, Failed To Close The Session: {}"
                     .format(e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, " +
                                  "Database Session Closed Badly."}}

    try:
        io_validation.AllReferenceData(strict=True).loads(out_json)
    except ValidationError as e:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": e.messages}}

    logger.info("get_all_reference_data Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}
