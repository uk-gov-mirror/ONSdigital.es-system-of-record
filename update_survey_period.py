import json
import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("updateSurveyPeriod")
def lambda_handler(event, context):
    """Takes a survey_period dictonary and updates the MI information based in the new run.
    Parameters:
      event (Dict):A series of key value pairs.
    Returns:
      Json message reporting the success of the update.
    """
    database = os.environ['Database_Location']

    try:
        io_validation.SurveyPeriod(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as exc:
        logger.error("Error: Failed to connect to the database(driver error): {}".format(exc))
        return {"statusCode": 500, "body":{"ContributorData":"Failed To Connect To Database." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"QueryTypes":"Failed To Connect To Database."}}
    except Exception as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"QueryTypes":"Failed To Connect To Database."}}

    try:
        logger.info("Retrieving table model(survey_period)")
        table_model = alchemy_functions.table_model(engine, metadata, 'survey_period')
        logger.info("Updating Table(survey_period)")
        statement = db.update(table_model).\
            values(active_period=event['active_period'],
                   number_of_responses=event['number_of_responses'],
                   number_cleared=event['number_cleared'],
                   number_cleared_first_time=event['number_cleared_first_time'],
                   sample_size=event['sample_size']).\
            where(db.and_(table_model.columns.survey_period ==
                  event['survey_period'],
                  table_model.columns.survey_code ==
                  event['survey_code']))
        alchemy_functions.update(statement, session)
    except db.exc.OperationalError as exc:
        logger.error("Error updating the database." + str(type(exc)))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Failed To Update Survey_Period."}}
    except Exception as exc:
        logger.error("Error updating the database." + str(type(exc)))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Failed To Update Survey_Period."}}


    try:
        session.commit()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to commit changes to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Failed To Commit Changes To The Database."}}
    except Exception as exc:
        logger.error("Error: Failed to commit changes to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Failed To Commit Changes To The Database."}}

    try:
        session.close()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to close connection to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Connection To Database Closed Badly."}}
    except Exception as exc:
        logger.error("Error: Failed to close connection to the database: {}".format(exc))
        return {"statusCode": 500, "body":{"SurveyPeriod":"Connection To Database Closed Badly."}}
    logger.info("Successfully survey_period update")
    return {"statusCode": 200, "body":{"SurveyPeriod":"Successfully Updated The Table."}}


x = lambda_handler({'active_period': True, 'number_of_responses': 2, 'number_cleared': 2,
                    'number_cleared_first_time': 1, 'sample_size': 2, 'survey_period': '201712',
                    'survey_code': '066'}, '')
print(x)
