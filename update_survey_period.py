import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("update_survey_period")


def lambda_handler(event, context):
    """Takes a survey_period dictonary and updates the MI information based
    in the new run.
    Parameters:
      event (Dict):A series of key value pairs.
    Returns:
      Json message reporting the success of the update.
    """

    logger.info("update_survey_period Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.SurveyPeriod(strict=True).load(event)
    except ValidationError as exc:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": exc.messages}}

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
        table_model = alchemy_functions.table_model(
            engine, metadata, 'survey_period')

        logger.info("Updating Table: {}".format("survey_period"))
        session.query(table_model).filter(db.and_(
            table_model.columns.survey_period == event['survey_period'],
            table_model.columns.survey_code == event['survey_code']))\
            .update({
                table_model.columns.active_period: event['active_period'],
                table_model.columns.number_of_responses:
                    event['number_of_responses'],
                table_model.columns.number_cleared: event['number_cleared'],
                table_model.columns.number_cleared_first_time:
                    event['number_cleared_first_time'],
                table_model.columns.sample_size: event['sample_size']},
                    synchronize_session=False)

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {} {}"
            .format("survey_period", exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("survey_period")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {} {}"
                     .format("survey_period", exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}"
                         .format("survey_period")}}

    try:
        logger.info("Commit Session.")
        session.commit()
    except db.exc.OperationalError as exc:
        logger.error("Operation Error, Failed To Commit Changes: {}"
                     .format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Commit Changes."}}
    except Exception as exc:
        logger.error("Failed To Commit Changes To Database: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Commit Changes To The Database."}}

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

    logger.info("update_survey_period Has Successfully Run.")
    return {"statusCode": 200,
            "body": {"Success": "Successfully Updated The Tables."}}
