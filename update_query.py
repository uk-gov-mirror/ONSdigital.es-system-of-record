import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation
import db_model

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

    except ValidationError as exc:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(exc.messages))
        return {"statusCode": 400, "body": {"Error": exc.messages}}

    try:
        logger.info("Connecting To The Database.")
        engine = db.create_engine(database)
        session = Session(engine, autoflush=False)
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

        logger.info("Updating Table Data: {}".format("query"))
        session.query(table_model)\
            .filter(table_model.columns.query_reference ==
                    event['query_reference'])\
            .update({table_model.columns.query_type:
                    event['query_type'],
                    table_model.columns.current_period:
                    event['current_period'],
                    table_model.columns.general_specific_flag:
                    event['general_specific_flag'],
                    table_model.columns.industry_group:
                    event['industry_group'],
                    table_model.columns.query_active:
                    event['query_active'],
                    table_model.columns.query_description:
                    event['query_description'],
                    table_model.columns.query_status:
                    event['query_status'],
                    table_model.columns.results_state:
                    event['results_state'],
                    table_model.columns.target_resolution_date:
                    event['target_resolution_date']},
                    synchronize_session=False)

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {} {}"
            .format("query", exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("query")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {} {}"
                     .format("query", exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}".format("query")}}

    try:
        if "Exceptions" in event.keys():
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.info("Inserting step_exception row {}".format(count))
                logger.info("Fetching Table Model: {}"
                            .format("step_exception"))
                table_model = alchemy_functions.table_model(engine, metadata,
                                                            'step_exception')
                is_it_here = session.query(table_model)\
                    .filter(db.and_(
                        table_model.columns.query_reference ==
                        exception['query_reference'],
                        table_model.columns.run_id == exception['run_id'],
                        table_model.columns.ru_reference ==
                        exception['ru_reference'],
                        table_model.columns.survey_period ==
                        exception['survey_period'],
                        table_model.columns.survey_code ==
                        exception['survey_code'],
                        table_model.columns.step == exception['step']))\
                    .scalar()
                if is_it_here is not None:
                    continue
                table_model = db_model.StepException

                logger.info("Updating Table Data: {}".format("step_exception"))
                session.add(table_model(
                    query_reference=exception['query_reference'],
                    survey_period=exception['survey_period'],
                    run_id=exception['run_id'],
                    ru_reference=exception['ru_reference'],
                    step=exception['step'],
                    survey_code=exception['survey_code'],
                    error_code=exception['error_code'],
                    error_description=exception['error_description']))

                try:
                    if "Anomalies" in exception.keys():
                        anomalies = exception["Anomalies"]
                        for count, anomaly in enumerate(anomalies):
                            logger.info("Inserting question_anomaly row {}"
                                        .format(count))
                            logger.info("Fetching Table Model: {}"
                                        .format("question_anomaly"))

                            table_model = alchemy_functions\
                                .table_model(engine, metadata,
                                             'question_anomaly')
                            is_it_here = session.query(table_model) \
                                .filter(db.and_(
                                    table_model.columns.query_reference ==
                                    anomaly['question_number'],
                                    table_model.columns.run_id == anomaly[
                                        'run_id'],
                                    table_model.columns.ru_reference ==
                                    anomaly['ru_reference'],
                                    table_model.columns.survey_period ==
                                    anomaly['survey_period'],
                                    table_model.columns.survey_code ==
                                    anomaly['survey_code'],
                                    table_model.columns.step == anomaly[
                                        'step'])).scalar()
                            if is_it_here is not None:
                                continue

                            table_model = db_model.QuestionAnomaly

                            logger.info("Inserting Table Data: {}".format(
                                "question_anomaly"))
                            session.add(table_model(
                                survey_period=anomaly['survey_period'],
                                question_number=anomaly['question_number'],
                                run_id=anomaly['run_id'],
                                ru_reference=anomaly['ru_reference'],
                                step=anomaly['step'],
                                survey_code=anomaly['survey_code'],
                                anomaly_description=anomaly[
                                    'anomaly_description']))

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
                                        is_it_here = session\
                                            .query(table_model).filter(db.and_(
                                                table_model.columns
                                                .query_reference ==
                                                vets['question_number'],
                                                table_model.columns.run_id ==
                                                vets['run_id'],
                                                table_model.columns
                                                .ru_reference ==
                                                vets['ru_reference'],
                                                table_model.columns
                                                .survey_period ==
                                                vets['survey_period'],
                                                table_model.columns
                                                .survey_code ==
                                                vets['survey_code'],
                                                table_model.columns.step ==
                                                vets['step'])).scalar()
                                        if is_it_here is not None:
                                            continue

                                        table_model = db_model.FailedVET

                                        logger.info(
                                            "Inserting Table Data: {}".format(
                                                "failed_vet"))
                                        session.add(table_model(
                                            failed_vet=vets['failed_vet'],
                                            survey_period=vets[
                                                'survey_period'],
                                            question_number=vets[
                                                'question_number'],
                                            run_id=vets['run_id'],
                                            ru_reference=vets['ru_reference'],
                                            step=vets['step'],
                                            survey_code=vets['survey_code']))

                            except db.exc.OperationalError as exc:
                                logger.error(
                                    "Alchemy Operational Error " +
                                    "When Updating Data: {} {}"
                                    .format("failed_vet", exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Operation Error, " +
                                                   "Failed To Update Data: {}"
                                                   .format("failed_vet")}}
                            except Exception as exc:
                                logger.error(
                                    "Problem Updating Data " +
                                    "From The Table: {} {}"
                                    .format("failed_vet", exc))
                                return {"statusCode": 500,
                                        "body": {
                                          "Error": "Failed To Update Data: {}"
                                          .format("failed_vet")}}

                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {} {}"
                        .format("question_anomaly", exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Update Data: {}"
                                              .format("question_anomaly")}}
                except Exception as exc:
                    logger.error("Problem Updating Data From The Table: {} {}"
                                 .format("question_anomaly", exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Failed To Update Data: {}"
                                     .format("question_anomaly")}}

    except db.exc.OperationalError as exc:
        logger.error(
            "Alchemy Operational Error When Updating Data: {} {}"
            .format("step_exception", exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("step_exception")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {} {}"
                     .format("step_exception", exc))
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

                logger.info("Updating Table Data: {}".format("query_task"))
                session.query(table_model)\
                    .filter(db.and_(table_model.columns.task_sequence_number ==
                                    task['task_sequence_number'],
                                    table_model.columns.query_reference ==
                                    task['query_reference']))\
                    .update({table_model.columns.response_required_by:
                            task['response_required_by'],
                            table_model.columns.task_description:
                            task['task_description'],
                            table_model.columns.task_responsibility:
                            task['task_responsibility'],
                            table_model.columns.task_status:
                            task['task_status'],
                            table_model.columns.next_planned_action:
                            task['next_planned_action'],
                            table_model.columns.when_action_required:
                            task['when_action_required']},
                            synchronize_session=False)

                try:
                    if "QueryTaskUpdates" in task.keys():
                        update_task = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(update_task):
                            logger.info(
                                "Inserting query_task_update row {}"
                                .format(count))
                            logger.info("Fetching Table Model: {}".format(
                                "query_task_update"))

                            table_model = alchemy_functions\
                                .table_model(engine, metadata,
                                             'query_task_update')

                            is_it_here = session.query(table_model)\
                                .filter(db.and_(
                                    table_model.columns.query_reference ==
                                    query_task['query_reference'],
                                    table_model.columns.task_sequence_number ==
                                    query_task['task_sequence_number'],
                                    table_model.columns.last_updated ==
                                    query_task['last_updated'])).scalar()

                            if is_it_here is not None:
                                continue

                            table_model = db_model.QueryTaskUpdate

                            logger.info("Inserting Table Data: {}"
                                        .format("query_task_update"))
                            session.add(table_model(
                                task_sequence_number=query_task[
                                    'task_sequence_number'],
                                query_reference=query_task['query_reference'],
                                last_updated=query_task['last_updated'],
                                task_update_description=query_task[
                                    'task_update_description'],
                                updated_by=query_task['updated_by']))

                except db.exc.OperationalError as exc:
                    logger.error(
                        "Alchemy Operational Error When Updating Data: {} {}"
                        .format("query_task_update", exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Operation Error, " +
                                              "Failed To Update Data: {}"
                                              .format("query_task_update")}}
                except Exception as exc:
                    logger.error(
                        "Problem Updating Data From The Table: {} {}"
                        .format("query_task_update", exc))
                    return {"statusCode": 500,
                            "body": {"Error": "Failed To Update Data: {}"
                                     .format("query_task_update")}}
    except db.exc.OperationalError as exc:
        logger.error("Alchemy Operational Error When Updating Data: {} {}"
                     .format("query_task", exc))
        return {"statusCode": 500,
                "body": {"Error": "Operation Error, Failed To Update Data: {}"
                         .format("query_task")}}
    except Exception as exc:
        logger.error("Problem Updating Data From The Table: {} {}"
                     .format("query_task", exc))
        return {"statusCode": 500,
                "body": {"Error": "Failed To Update Data: {}"
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

    logger.info("update_query Has Successfully Run.")
    return {"statusCode": 200,
            "body": {"Success": "Successfully Updated The Tables."}}
