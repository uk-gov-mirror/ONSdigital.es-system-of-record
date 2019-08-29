import json
import logging
import os

import sqlalchemy as db

from marshmallow import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("create_query")


def lambda_handler(event, context):
    """Takes a query dictionary and inserts new data into the query tables with the run information.
    Parameters:
      event (Dict):A series of key value pairs nested to match table structure.
    Returns:
      Json message reporting the success of the update alongside the new queries reference number.
    """
    logger.info("create_query Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error("Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error":"Configuration error"}}

    try:
        io_validation.Query(strict=True).load(event)
    except ValidationError as err:
        logger.info("INPUT DATA: {}".format(event))
        logger.error("Error: Failed to validate input: {}".format(err.messages))
        return {"statusCode": 500, "body": err.messages}

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as exc:
        logger.error("Driver Error, Failed To Connect: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Driver Error, Failed To Connect."}}
    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Failed To Connect."}}
    except Exception as exc:
        logger.error("Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Connect To Database."}}

    try:

        logger.info("Retrieving table model(query)")
        table_model = alchemy_functions.table_model(engine, metadata, 'query')
        logger.info("Inserting into table (query)")
        statement = insert(table_model).values(query_type=event['query_type'],
                                               ru_reference=event['ru_reference'],
                                               survey_code=event['survey_code'],
                                               survey_period=event['survey_period'],
                                               current_period=event['current_period'],
                                               date_raised=event['date_raised'],
                                               general_specific_flag=event['general_specific_flag'],
                                               industry_group=event['industry_group'],
                                               last_query_update=event['last_query_update'],
                                               query_active=event['query_active'],
                                               query_description=event['query_description'],
                                               query_status=event['query_status'],
                                               raised_by=event['raised_by'],
                                               results_state=event['results_state'],
                                               target_resolution_date=event['target_resolution_date']).\
            returning(table_model.columns.query_reference).on_conflict_do_nothing(constraint=table_model.primary_key)
        new_query = alchemy_functions.update(statement, session).fetchone()[0]

    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to insert into query table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Failed to insert into query table." + str(type(exc))}}
    except Exception as exc:
        logger.error("Failed to insert into query table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed to insert into query table." + str(type(exc))}}

    try:
        if "Exceptions" in event.keys():
            logger.info("Inserting into step_exceptions table {}")
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.info("Inserting step_exception {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'step_exception')
                statement = insert(table_model).values(query_reference=new_query,
                                                       survey_period=exception['survey_period'],
                                                       run_id=exception['run_id'],
                                                       ru_reference=exception['ru_reference'],
                                                       step=exception['step'],
                                                       survey_code=exception['survey_code'],
                                                       error_code=exception['error_code'],
                                                       error_description=exception['error_description']).\
                    on_conflict_do_nothing(constraint=table_model.primary_key)
                alchemy_functions.update(statement, session)
                try:
                    if "Anomalies" in exception.keys():
                        logger.info("Inserting into question_anomaly table {}")
                        anomalies = exception["Anomalies"]
                        for count, anomaly in enumerate(anomalies):
                            logging.info("Inserting question_anomaly {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'question_anomaly')
                            statement = insert(table_model).values(survey_period=anomaly['survey_period'],
                                                                   question_number=anomaly['question_number'],
                                                                   run_id=anomaly['run_id'],
                                                                   ru_reference=anomaly['ru_reference'],
                                                                   step=anomaly['step'],
                                                                   survey_code=anomaly['survey_code'],
                                                                   anomaly_description=anomaly['anomaly_description']).\
                                on_conflict_do_nothing(constraint=table_model.primary_key)
                            alchemy_functions.update(statement, session)

                            try:
                                if "FailedVETs" in anomaly.keys():
                                    logger.info("Inserting into failed_vet table {}")
                                    failed_vets = anomaly["FailedVETs"]
                                    for count, vets in enumerate(failed_vets):
                                        logging.warning("Inserting failed_vet {}".format(count))
                                        table_model = alchemy_functions.table_model(engine, metadata, 'failed_vet')
                                        statement = insert(table_model).values(failed_vet=vets['failed_vet'],
                                                                               survey_period=vets['survey_period'],
                                                                               question_number=vets['question_number'],
                                                                               run_id=vets['run_id'],
                                                                               ru_reference=vets['ru_reference'],
                                                                               step=vets['step'],
                                                                               survey_code=vets['survey_code']).\
                                            on_conflict_do_nothing(constraint=table_model.primary_key)
                                        alchemy_functions.update(statement, session)
                            except db.exc.OperationalError as exc:
                                logger.error("Operational Error, Failed to insert into failed_VET table: {}".format(exc))
                                return {"statusCode": 500, "body": {"Error": "Operational Error, Failed to insert into failed_VET table." + str(type(exc))}}
                            except Exception as exc:
                                logger.error("Failed to insert into failed_VET table: {}".format(exc))
                                return {"statusCode": 500, "body": {"Error": "Failed to insert into failed_VET table." + str(type(exc))}}
                except db.exc.OperationalError as exc:
                    logger.error("Operational Error, Failed to insert into Question_Anomaly table: {}".format(exc))
                    return {"statusCode": 500, "body": {"Error": "Operational Error, Failed to insert into Question_Anomaly table." + str(type(exc))}}
                except Exception as exc:
                    logger.error("Failed to insert into Question_Anomaly table: {}".format(exc))
                    return {"statusCode": 500, "body": {"Error": "Failed to insert into Question_Anomaly table." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to insert into Step_Exception table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Failed to insert into Step_Exception table." + str(type(exc))}}
    except Exception as exc:
        logger.error("Failed to insert into Step_Exception table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed to insert into Step_Exception table." + str(type(exc))}}

    try:
        if "QueryTasks" in event.keys():
            tasks = event["QueryTasks"]
            for count, task in enumerate(tasks):
                logger.info("Inserting into query_task {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'query_task')
                statement = insert(table_model).values(task_sequence_number=task['task_sequence_number'],
                                                       query_reference=new_query,
                                                       response_required_by=task['response_required_by'],
                                                       task_description=task['task_description'],
                                                       task_responsibility=task['task_responsibility'],
                                                       task_status=task['task_status'],
                                                       next_planned_action=task['next_planned_action'],
                                                       when_action_required=task['when_action_required']).\
                    on_conflict_do_nothing(constraint=table_model.primary_key)
                alchemy_functions.update(statement, session)

                try:
                    if "QueryTaskUpdates" in task.keys():
                        update_task = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(update_task):
                            logger.info("Inserting into query task updates {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'query_task_update')
                            statement = insert(table_model).values(task_sequence_number=query_task
                                                                   ['task_sequence_number'],
                                                                   query_reference=new_query,
                                                                   last_updated=query_task['last_updated'],
                                                                   task_update_description=query_task
                                                                   ['task_update_description'],
                                                                   updated_by=query_task['updated_by']).\
                                on_conflict_do_nothing(constraint=table_model.primary_key)
                            alchemy_functions.update(statement, session)

                except db.exc.OperationalError as exc:
                    logger.error("Operational Error, Failed to insert into Query_Task_Update table: {}".format(exc))
                    return {"statusCode": 500, "body": {"Error": "Failed to insert into Query_Task_Update table." + str(type(exc))}}
                except Exception as exc:
                    logger.error("Failed to insert into Query_Task_Update table: {}".format(exc))
                    return {"statusCode": 500, "body": {"Error": "Failed to insert into Query_Task_Update table." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to insert into Query_Task_Update table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Failed to insert into Query_Task_Update table." + str(type(exc))}}
    except Exception as exc:
        logger.error("Failed to insert into Query_Task_Update table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed to insert into Query_Task_Update table." + str(type(exc))}}

    try:
        session.commit()
    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to commit changes to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Failed To Commit Changes To The Database."}}
    except Exception as exc:
        logger.error("Failed to commit changes to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Commit Changes To The Database."}}

    try:
        session.close()
    except db.exc.OperationalError as exc:
        logger.error("Operational Error, Failed to close the database session: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Operational Error, Database Session Closed Badly."}}
    except Exception as exc:
        logger.error("Operational Error, Failed to close the database session: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Database Session Closed Badly."}}
    logger.info("Successfully completed create query")
    return {"statusCode": 201, "body": {"query_type": "Query created successfully"}}


# with open('test_data.txt') as infile:
#     test_data = json.load(infile)
# x = lambda_handler(test_data, '')
# print(x)
