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

    try:
        database, error = io_validation.Database(strict=True).load(os.environ)
    except ValidationError as e:
        logger.error(
            "Database_Location Environment Variable" +
            "Has Not Been Set Correctly: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": "Configuration Error: {}"
                .format(e)}}

    try:
        io_validation.ContributorSearch(strict=True).load(event)
    except ValidationError as e:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(e.messages))
        return {"statusCode": 400, "body": {"Error": e.messages}}

    ref = event['ru_reference']

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
        logger.error("Operational Error, Encountered: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, Failed To Connect."}}
    except Exception as e:
        logger.error("General Error, Failed To Connect To The Database: {}"
                     .format(e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, " +
                                  "Failed To Connect To The Database."}}

    try:
        table_list = {'contributor': None,
                      'survey_enrolment': None,
                      'survey_contact': "contact",
                      'contributor_survey_period': "survey_period"}

        for current_table in table_list:
            logger.info("Fetching Table Model: {}".format(current_table))
            table_model = alchemy_functions.table_model(engine, metadata,
                                                        current_table)

            logger.info("Fetching Table Data: {}".format(current_table))
            if current_table in ["survey_contact",
                                 "contributor_survey_period"]:
                other_model = alchemy_functions\
                    .table_model(engine, metadata, table_list[current_table])
                statement = session.query(table_model, other_model)\
                    .join(other_model)\
                    .filter(table_model.columns.ru_reference == ref).all()
            else:
                statement = session.query(table_model)\
                    .filter(table_model.columns.ru_reference == ref).all()

            logger.info("Converting Data: {}".format(current_table))
            table_list[current_table] = alchemy_functions.to_df(statement)
    except db.exc.OperationalError as e:
        logger.error(
            "Operational Error, When Retrieving Data: {} {}"
            .format(current_table, e))
        return {"statusCode": 500,
                "body": {"Error":
                         "Operational Error, Failed To Retrieve Data: {}"
                         .format(current_table)}}
    except Exception as e:
        logger.error("General Error, Problem Retrieving " +
                     "Data From The Table: {} {}".format(current_table, e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, Failed To Retrieve Data: {}"
                         .format(current_table)}}

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
        io_validation.Contributor(strict=True).loads(out_json)
    except ValidationError as e:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": e.messages}}

    logger.info("get_contributor Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}


lambda_handler({"ru_reference": "77700000001"}, "")
