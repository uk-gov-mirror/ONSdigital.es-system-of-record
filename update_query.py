import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("update_query")


def lambda_handler(event, context):
    """Takes a query dictionary and updates the related query tables and
    inserts the new run information.
    Parameters:
      event (Dict):A series of key value pairs nested to match table structure.
    Returns:
      Json message reporting the success of the update.
    """

    logger.info("update_query Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.QueryReference(strict=True).load(event)
        io_validation.Query(strict=True).load(event)

    except ValidationError as err:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(exc.messages))
        return {"statusCode": 500, "body": {"Error": err.messages}}

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
        logger.info("Fetching Table Model: {}".format("query"))
        table_model = alchemy_functions.table_model(engine, metadata, 'query')

        logger.info("Building SQL Statement: {}".format("query"))
        statement = db.update(table_model)\
            .values(query_type=event['query_type'],
                    current_period=event['current_period'],
                    general_specific_flag=event['general_specific_flag'],
                    industry_group=event['industry_group'],
                    query_active=event['query_active'],
                    query_description=event['query_description'],
                    query_status=event['query_status'],
                    results_state=event['results_state'],
                    target_resolution_date=event['target_resolution_date'])\
            .where(table_model.columns.query_reference ==
                   event['query_reference'])

        logger.info("Updating Table Data: {}".format("query"))
        alchemy_functions.update(statement, session)

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("query")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}".format("query")}}

    try:
        if "Exceptions" in event.keys():
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.info("Inserting step_exception row {}".format(count))
                logger.info("Fetching Table Model: {}"
                            .format("step_exception"))
                table_model = alchemy_functions.table_model(
                    engine, metadata, 'step_exception')

                logger.info("Building SQL Statement: {}"
                            .format("step_exception"))
                statement = insert(table_model)\
                    .values(query_reference=exception['query_reference'],
                            survey_period=exception['survey_period'],
                            run_id=exception['run_id'],
                            ru_reference=exception['ru_reference'],
                            step=exception['step'],
                            survey_code=exception['survey_code'],
                            error_code=exception['error_code'],
                            error_description=exception['error_description'])\
                    .on_conflict_do_nothing(constraint=table_model.primary_key)

                logger.info("Updating Table Data: {}".format("step_exception"))
                alchemy_functions.update(statement, session)

                try:
                    if "Anomalies" in exception.keys():
                        anomalies = exception["Anomalies"]
                        for count, anomaly in enumerate(anomalies):
                            logger.info("Inserting question_anomaly row {}"
                                        .format(count))
                            logger.info("Fetching Table Model: {}"
                                        .format("question_anomaly"))
                            table_model = alchemy_functions.table_model(
                                engine, metadata, 'question_anomaly')

                            logger.info("Building SQL Statement: {}"
                                        .format("question_anomaly"))
                            statement = insert(table_model)\
                                .values(survey_period=anomaly['survey_period'],
                                        question_number=anomaly[
                                            'question_number'],
                                        run_id=anomaly['run_id'],
                                        ru_reference=anomaly['ru_reference'],
                                        step=anomaly['step'],
                                        survey_code=anomaly['survey_code'],
                                        anomaly_description=anomaly[
                                            'anomaly_description'])\
                                .on_conflict_do_nothing(
                                constraint=table_model.primary_key)

                            logger.info("Updating Table Data: {}".format(
                                "question_anomaly"))
                            alchemy_functions.update(statement, session)

                            try:
                                if "FailedVETs" in anomaly.keys():
                                    failed_vets = anomaly["FailedVETs"]
                                    for count, vets in enumerate(failed_vets):
                                        logger.info(
                                            "Inserting failed_vet row {}"
                                            .format(count))
                                        logger.info("Fetching Table Model: {}"
                                                    .format("failed_vet"))
                                        table_model = alchemy_functions\
                                            .table_model(engine, metadata,
                                                         'failed_vet')

                                        logger.info(
                                            "Building SQL Statement: {}"
                                            .format("failed_vet"))
                                        statement = insert(table_model)\
                                            .values(failed_vet=anomaly[
                                                        'FailedVETs'],
                                                    survey_period=anomaly[
                                                        'survey_period'],
                                                    question_number=anomaly[
                                                         'question_number'],
                                                    run_id=anomaly['run_id'],
                                                    ru_reference=anomaly[
                                                         'ru_reference'],
                                                    step=anomaly['step'],
                                                    survey_code=anomaly[
                                                         'survey_code'])\
                                            .on_conflict_do_nothing(
                                            constraint=table_model.primary_key)

                                        logger.info(
                                            "Updating Table Data: {}".format(
                                                "failed_vet"))
                                        alchemy_functions.update(
                                            statement, session)
                            except db.exc.OperationalError as exc:
                                logger.error(
                                    "Alchemy Operational Error " +
                                    "When Updating Data: {}".format(exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Operation Error, " +
                                                   "Failed To Update Data: {}"
                                                   .format("failed_vet")}}
                            except Exception as exc:
                                logger.error(
                                    "Problem Updating Data From The Table: {}"
                                    .format(exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Failed To Update Data: {}"
                                          .format("failed_vet")}}

                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {}"
                        .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Update Data: {}"
                                              .format("question_anomaly")}}
                except Exception as exc:
                    logger.error("Problem Updating Data From The Table: {}"
                                 .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Failed To Update Data: {}"
                                     .format("question_anomaly")}}

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("step_exception")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}"
                         .format("step_exception")}}

    try:
        if "QueryTasks" in event.keys():
            tasks = event["QueryTasks"]
            for count, task in enumerate(tasks):
                logger.info("Inserting query_task row {}".format(count))
                logger.info("Fetching Table Model: {}".format("query_task"))
                table_model = alchemy_functions.table_model(
                    engine, metadata, 'query_task')

                logger.info("Building SQL Statement: {}".format("query_task"))
                statement = db.update(table_model)\
                    .values(response_required_by=task['response_required_by'],
                            task_description=task['task_description'],
                            task_responsibility=task['task_responsibility'],
                            task_status=task['task_status'],
                            next_planned_action=task['next_planned_action'],
                            when_action_required=task['when_action_required'])\
                    .where(db.and_(table_model.columns.task_sequence_number ==
                                   task['task_sequence_number'],
                           table_model.columns.query_reference ==
                                   task['query_reference']))

                logger.info("Updating Table Data: {}".format("query_task"))
                alchemy_functions.update(statement, session)

                try:
                    if "QueryTaskUpdates" in task.keys():
                        update_task = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(update_task):
                            logger.info(
                                "Inserting query_task_update row {}"
                                .format(count))
                            logger.info("Fetching Table Model: {}".format(
                                "query_task_update"))
                            table_model = alchemy_functions.table_model(
                                engine, metadata, 'query_task_update')

                            logger.info("Building SQL Statement: {}"
                                        .format("query_task_update"))
                            statement = insert(table_model).values(
                                task_sequence_number=query_task[
                                    'task_sequence_number'],
                                query_reference=query_task['query_reference'],
                                last_updated=query_task['last_updated'],
                                task_update_description=query_task[
                                    'task_update_description'],
                                updated_by=query_task['updated_by'])\
                                .on_conflict_do_nothing(
                                constraint=table_model.primary_key)

                            logger.info("Updating Table Data: {}"
                                        .format("query_task_update"))
                            alchemy_functions.update(statement, session)
                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {}"
                        .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Update Data: {}"
                                              .format("query_task_update")}}
                except Exception as exc:
                    logger.error(
                        "Problem Updating Data From The Table: {}".format(exc))
                return {"statusCode": 500,
                        "body": {"Error": "Failed To Update Data: {}"
                                 .format("query_task_update")}}
    except db.exc.OperationalError as exc:
        logger.error("Alchemy Operational Error When Updating Data: {}"
                     .format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("step_exception")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}"
                         .format("step_exception")}}

    try:
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

    logger.info("update_query Has Successfully Run.")
    return {"statusCode": 200,
            "body": {"Success": "Successfully Updated The Tables."}}
