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
      event (Dict):A series of key value pairs with the related comments to be added.
    Returns:
      Json message reporting the success of the update.
    """

    try:
        io_validation.ContributorUpdate(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    database = os.environ['Database_Location']

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()

    except db.exc.NoSuchModuleError as exc:
        logger.error("Error: Failed to connect to the database(driver error): {}".format(exc))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Connect To Database." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Connect To Database." + str(type(exc))}}
    except Exception as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Connect To Database." + str(type(exc))}}

    current_time = str(datetime.now())

    try:
        logger.info("Retrieving table model")
        table_model = alchemy_functions.table_model(engine, metadata, 'contributor_survey_period')
        logger.info("Updating Table")
        statement = db.update(table_model).\
            values(additional_comments=event['additional_comments'],
                   contributor_comments=event['contributor_comments'],
                   last_updated=current_time).\
            where(db.and_(table_model.columns.survey_period == event['survey_period'],
                          table_model.columns.survey_code == event['survey_code'],
                          table_model.columns.ru_reference == event['ru_reference']))

        alchemy_functions.update(statement, session)

    except db.exc.OperationalError as exc:
        logger.error("Error updating the database.{}".format(type(exc)))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Update The Database."}}
    except Exception as exc:
        logger.error("Error updating the database." + str(type(exc)))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Update The Database."}}

    try:
        session.commit()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to commit changes to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Commit Changes To The Database."}}
    except Exception as exc:
        logger.error("Error updating the database." + str(type(exc)))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Update The Database."}}

    try:
        session.close()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to close connection to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"ContributorData": "Connection To Database Closed Badly."}}
    except Exception as exc:
        logger.error("Error updating the database." + str(type(exc)))
        return {"statusCode": 500, "body": {"ContributorData": "Failed To Update The Database."}}
    logger.info("Successfully completed contributor update")
    return {"statusCode": 200, "body": {"ContributorData": "Successfully Updated The Table."}}


x = lambda_handler({"additional_comments": "6",  # "Hello",
                    "contributor_comments": "666",  # "Contributor says hello!",
                    "survey_period": "201712",  # "201712",
                    "survey_code": "066",  # "066",
                    "ru_reference": "77700000001"}, "")
print(x)
