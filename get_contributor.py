import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("get_contributor")


def lambda_handler(event, context):
    """Collects data on a passed in Reference from six tables and combines
    them into a single Json.
    Parameters:
      event (Dict):A single key value pair of ru_reference and a string number.
    Returns:
      out_json (Json):Nested Json responce of the six tables data.
    """

    logger.info("get_contributor Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.ContributorSearch(strict=True).load(event)
    except ValidationError as exc:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

    ref = event['ru_reference']

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
        table_list = {'contributor': None,
                      'survey_enrolment': None,
                      'survey_contact': None,
                      'contributor_survey_period': None}

        for current_table in table_list:
            logger.info("Fetching Table Model: {}".format(current_table))
            table_model = alchemy_functions.table_model(engine, metadata,
                                                        current_table)

            logger.info("Building SQL Statement: {}".format(current_table))
            statement = session.query(table_model)\
                .filter(table_model.ru_reference == ref).all()

            if current_table == "survey_contact":
                statement = session.query(table_model).join("contact")\
                    .filter(table_model.ru_reference == ref).all()
            elif current_table == "contributor_survey_period":
                statement = session.query(table_model).join("survey_period1")\
                    .filter(table_model.ru_reference == ref).all()

            logger.info("Fetching Table Data: {}".format(current_table))
            table_data = alchemy_functions.to_df(statement)
            table_list[current_table] = table_data
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
    out_json = json.dumps(table_list["contributor"].to_dict(orient='records'),
                          sort_keys=True, default=str)
    if not out_json == "[]":
        out_json = out_json[1:-2]
        out_json += ',"Surveys":[ '

        for index, row in table_list['survey_enrolment'].iterrows():
            curr_row = table_list['survey_enrolment'][(
                    table_list['survey_enrolment']['survey_code'] ==
                    row['survey_code'])]
            curr_row = json.dumps(curr_row.to_dict(orient='records'),
                                  sort_keys=True, default=str)
            curr_row = curr_row[2:-2]

            out_json = out_json + "{" + curr_row + ',"Contacts":'
            curr_con = table_list['survey_contact'][(
                    table_list['survey_contact']['survey_code'] ==
                    row['survey_code'])]
            curr_con = json.dumps(curr_con.to_dict(orient='records'),
                                  sort_keys=True, default=str)
            out_json += curr_con

            out_json = out_json + ',"Periods":'
            curr_per = table_list['contributor_survey_period'][(
                    table_list['contributor_survey_period']['survey_code'] ==
                    row['survey_code'])]
            curr_per = json.dumps(curr_per.to_dict(orient='records'),
                                  sort_keys=True, default=str)
            out_json += curr_per + '},'

        out_json = out_json[:-1]
        out_json += ']}'

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
        io_validation.Contributor(strict=True).loads(out_json)
    except ValidationError as exc:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

    logger.info("get_contributor Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}
