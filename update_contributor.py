import os
from datetime import datetime
import logging
import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session
import alchemy_functions
import io_validation

logger = logging.getLogger("update_contributor")


def lambda_handler(event, context):
    """Takes a contributor dictonary and updates the comments.
    Parameters:
      event (Dict):A series of key value pairs with the related comments to be
      added.
    Returns:
      Json message reporting the success of the update.
    """

    logger.info("update_contributor Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.ContributorUpdate(strict=True).load(event)
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

    current_time = str(datetime.now())

    try:
        logger.info("Fetching Table Model: {}"
                    .format("contributor_survey_period"))
        table_model = alchemy_functions.table_model(
            engine, metadata, 'contributor_survey_period')

        logger.info("Updating Table: {}"
                    .format("contributor_survey_period"))
        session.query(table_model)\
            .filter(db.and_(
             table_model.columns.survey_period == event['survey_period'],
             table_model.columns.survey_code == event['survey_code'],
             table_model.columns.ru_reference == event['ru_reference']))\
            .update({table_model.columns.additional_comments:
                     event['additional_comments'],
                     table_model.columns.contributor_comments:
                     event['contributor_comments'],
                     table_model.columns.last_updated: current_time},
                    synchronize_session=False)

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("contributor_survey_period")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}"
                         .format("contributor_survey_period")}}

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

    logger.info("update_contributor Has Successfully Run.")
    return {"statusCode": 200,
            "body": {"Success": "Successfully Updated The Tables."}}
