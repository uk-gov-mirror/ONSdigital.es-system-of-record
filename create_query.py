import logging
import os

import sqlalchemy as db

from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("create_query")


def lambda_handler(event, context):
    """Takes a query dictionary and inserts new data into the query tables with
    the run information.
    Parameters:
      event (Dict):A series of key value pairs nested to match table structure.
    Returns:
      Json message reporting the success of the insert alongside the
      new queries reference number.
    """

    logger.info("create_query Has Started Running.")

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error(
            "Database_Location Environment Variable Has Not Been Set.")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.Query(strict=True).load(event)
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
        logger.info("Fetching Table Model: {}".format("query"))
        table_model = alchemy_functions.table_model(engine, metadata, 'query')

        logger.info("Inserting Table Data: {}".format("query"))
        new_query = session.query(table_model).insert({
            table_model.columns.query_type: event['query_type'],
            table_model.columns.ru_reference: event['ru_reference'],
            table_model.columns.survey_code: event['survey_code'],
            table_model.columns.survey_period: event['survey_period'],
            table_model.columns.current_period: event['current_period'],
            table_model.columns.date_raised: event['date_raised'],
            table_model.columns.general_specific_flag:
                event['general_specific_flag'],
            table_model.columns.industry_group: event['industry_group'],
            table_model.columns.last_query_update: event['last_query_update'],
            table_model.columns.query_active: event['query_active'],
            table_model.columns.query_description: event['query_description'],
            table_model.columns.query_status: event['query_status'],
            table_model.columns.raised_by: event['raised_by'],
            table_model.columns.results_state: event['results_state'],
            table_model.columns.target_resolution_date:
                event['target_resolution_date']},
            synchronize_session=False)\
            .returning(table_model.columns.query_reference)\
            .on_conflict_do_nothing(constraint=table_model.primary_key)

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Insert Data: {}"
                         .format("query")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Insert Data: {}".format("query")}}

    try:
        if "Exceptions" in event.keys():
            logger.info("Inserting into step_exceptions table {}")
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.info("Inserting step_exception row {}".format(count))
                logger.info("Fetching Table Model: {}"
                            .format("step_exception"))
                table_model = alchemy_functions.table_model(
                    engine, metadata, 'step_exception')

                logger.info("Inserting Table Data: {}"
                            .format("step_exception"))
                session.query(table_model).insert({
                    table_model.columns.query_reference: new_query,
                    table_model.columns.survey_period:
                        exception['survey_period'],
                    table_model.columns.run_id: exception['run_id'],
                    table_model.columns.ru_reference:
                        exception['ru_reference'],
                    table_model.columns.step: exception['step'],
                    table_model.columns.survey_code: exception['survey_code'],
                    table_model.columns.error_code: exception['error_code'],
                    table_model.columns.error_description:
                        exception['error_description']},
                    synchronize_session=False).\
                    on_conflict_do_nothing(constraint=table_model.primary_key)

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

                            logger.info("Inserting Table Data: {}"
                                        .format("question_anomaly"))
                            session.query(table_model).insert({
                                table_model.columns.survey_period:
                                    anomaly['survey_period'],
                                table_model.columns.question_number:
                                    anomaly['question_number'],
                                table_model.columns.run_id:
                                    anomaly['run_id'],
                                table_model.columns.ru_reference:
                                    anomaly['ru_reference'],
                                table_model.columns.step:
                                    anomaly['step'],
                                table_model.columns.survey_code:
                                    anomaly['survey_code'],
                                table_model.columns.anomaly_description:
                                    anomaly['anomaly_description']},
                                synchronize_session=False)\
                                .on_conflict_do_nothing(
                                constraint=table_model.primary_key)

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

                                        logger.info("Inserting Table Data: {}"
                                                    .format("failed_vet"))
                                        session.query(table_model).insert({
                                            table_model.columns.failed_vet:
                                                vets['failed_vet'],
                                            table_model.columns.survey_period:
                                                vets['survey_period'],
                                            table_model.columns
                                                .question_number:
                                                vets['question_number'],
                                            table_model.columns.run_id:
                                                vets['run_id'],
                                            table_model.columns.ru_reference:
                                                vets['ru_reference'],
                                            table_model.columns.step:
                                                vets['step'],
                                            table_model.columns.survey_code:
                                                vets['survey_code']},
                                            synchronize_session=False)\
                                            .on_conflict_do_nothing(
                                            constraint=table_model.primary_key)

                            except db.exc.OperationalError as exc:
                                logger.error(
                                    "Alchemy Operational Error " +
                                    "When Updating Data: {}".format(exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Operation Error, " +
                                                   "Failed To Insert Data: {}"
                                                   .format("failed_vet")}}
                            except Exception as exc:
                                logger.error(
                                    "Problem Updating Data From The Table: {}"
                                    .format(exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Failed To Insert Data: {}"
                                          .format("failed_vet")}}

                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {}"
                        .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Insert Data: {}"
                                              .format("question_anomaly")}}
                except Exception as exc:
                    logger.error("Problem Updating Data From The Table: {}"
                                 .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Failed To Insert Data: {}"
                                     .format("question_anomaly")}}

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Insert Data: {}"
                         .format("step_exception")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Insert Data: {}"
                         .format("step_exception")}}

    try:
        if "QueryTasks" in event.keys():
            tasks = event["QueryTasks"]
            for count, task in enumerate(tasks):
                logger.info("Inserting query_task row {}".format(count))
                logger.info("Fetching Table Model: {}".format("query_task"))
                table_model = alchemy_functions.table_model(
                    engine, metadata, 'query_task')

                logger.info("Inserting Table Data: {}".format("query_task"))
                session.query(table_model).insert({
                    table_model.columns.task_sequence_number:
                        task['task_sequence_number'],
                    table_model.columns.query_reference: new_query,
                    table_model.columns.response_required_by:
                        task['response_required_by'],
                    table_model.columns.task_description:
                        task['task_description'],
                    table_model.columns.task_responsibility:
                        task['task_responsibility'],
                    table_model.columns.task_status: task['task_status'],
                    table_model.columns.next_planned_action:
                        task['next_planned_action'],
                    table_model.columns.when_action_required:
                        task['when_action_required']},
                    synchronize_session=False)\
                    .on_conflict_do_nothing(constraint=table_model.primary_key)

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

                            logger.info("Inserting Table Data: {}"
                                        .format("query_task_update"))
                            session.query(table_model).insert({
                                table_model.columns.task_sequence_number:
                                    query_task['task_sequence_number'],
                                table_model.columns.query_reference:
                                    query_task['query_reference'],
                                table_model.columns.last_updated:
                                    query_task['last_updated'],
                                table_model.columns.task_update_description:
                                    query_task['task_update_description'],
                                table_model.columns.updated_by:
                                    query_task['updated_by']},
                                synchronize_session=False)\
                                .on_conflict_do_nothing(
                                constraint=table_model.primary_key)

                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {}"
                        .format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Insert Data: {}"
                                              .format("query_task_update")}}
                except Exception as exc:
                    logger.error(
                        "Problem Updating Data From The Table: {}".format(exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Failed To Insert Data: {}"
                                     .format("query_task_update")}}
    except db.exc.OperationalError as exc:
        logger.error("Alchemy Operational Error When Updating Data: {}"
                     .format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Insert Data: {}"
                         .format("query_task")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {}".format(exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Insert Data: {}"
                         .format("query_task")}}

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

    logger.info("create_query Has Successfully Run.")
    return {"statusCode": 201,
            "body": {"Success": "Successfully Updated The Tables."}}
