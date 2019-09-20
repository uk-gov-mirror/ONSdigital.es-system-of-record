import logging
import os

import sqlalchemy as db

from marshmallow import ValidationError
from sqlalchemy.orm import Session

import io_validation
import db_model

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
        table_model = db_model.Query
        logger.info("Inserting Table Data: {}".format("query"))
        new_query = session.add(
            table_model(query_type=event['query_type'],
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
                        target_resolution_date=
                        event['target_resolution_date']))
        session.flush()
        print(new_query)
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
                table_model = db_model.StepException

                logger.info("Inserting Table Data: {}"
                            .format("step_exception"))
                session.add(table_model(
                    query_reference=new_query,
                    survey_period=exception['survey_period'],
                    run_id=exception['run_id'],
                    ru_reference=exception['ru_reference'],
                    step=exception['step'],
                    survey_code=exception['survey_code'],
                    error_code=exception['error_code'],
                    error_description=exception['error_description']))
                session.flush()
                try:
                    if "Anomalies" in exception.keys():
                        anomalies = exception["Anomalies"]
                        for count, anomaly in enumerate(anomalies):
                            logger.info("Inserting question_anomaly row {}"
                                        .format(count))
                            logger.info("Fetching Table Model: {}"
                                        .format("question_anomaly"))
                            table_model = db_model.QuestionAnomaly

                            logger.info("Inserting Table Data: {}"
                                        .format("question_anomaly"))
                            session.add(table_model(
                                survey_period=anomaly['survey_period'],
                                question_number=anomaly['question_number'],
                                run_id=anomaly['run_id'],
                                ru_reference=anomaly['ru_reference'],
                                step=anomaly['step'],
                                survey_code=anomaly['survey_code'],
                                anomaly_description=
                                anomaly['anomaly_description']))
                            session.flush()
                            try:
                                if "FailedVETs" in anomaly.keys():
                                    failed_vets = anomaly["FailedVETs"]
                                    for count, vets in enumerate(failed_vets):
                                        logger.info(
                                            "Inserting failed_vet row {}"
                                            .format(count))
                                        logger.info("Fetching Table Model: {}"
                                                    .format("failed_vet"))
                                        table_model = db_model.FailedVET

                                        logger.info("Inserting Table Data: {}"
                                                    .format("failed_vet"))
                                        session.add(table_model(
                                            failed_vet=vets['failed_vet'],
                                            survey_period=
                                            vets['survey_period'],
                                            question_number=
                                            vets['question_number'],
                                            run_id=vets['run_id'],
                                            ru_reference=vets['ru_reference'],
                                            step=vets['step'],
                                            survey_code=vets['survey_code']))
                                session.flush()
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
                table_model = db_model.QueryTask

                logger.info("Inserting Table Data: {}".format("query_task"))
                session.add(table_model(
                    task_sequence_number=task['task_sequence_number'],
                    query_reference= new_query,
                    response_required_by=task['response_required_by'],
                    task_description=task['task_description'],
                    task_responsibility=task['task_responsibility'],
                    task_status= task['task_status'],
                    next_planned_action=task['next_planned_action'],
                    when_action_required=task['when_action_required']))
                session.flush()
                try:
                    if "QueryTaskUpdates" in task.keys():
                        update_task = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(update_task):
                            logger.info(
                                "Inserting query_task_update row {}"
                                .format(count))
                            logger.info("Fetching Table Model: {}".format(
                                "query_task_update"))
                            table_model = db_model.QueryTaskUpdate

                            logger.info("Inserting Table Data: {}"
                                        .format("query_task_update"))
                            session.add(table_model(
                                task_sequence_number=
                                query_task['task_sequence_number'],
                                query_reference=
                                query_task['query_reference'],
                                last_updated=
                                query_task['last_updated'],
                                task_update_description=
                                query_task['task_update_description'],
                                updated_by=
                                query_task['updated_by']))
                    session.flush()
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
    print(new_query)
    logger.info("create_query Has Successfully Run.")
    return {"statusCode": 201,
            "body": {"Success": "Successfully Created Query: {}"
                     .format(new_query)}}


lambda_handler({
  "industry_group": "Con",
  "query_active": False,
  "query_description": "Validation Exception Test",
  "current_period": "200001",
  "survey_period": "201803",
  "ru_reference": "77700000002",
  "query_reference": 5,
  "last_query_update": "2018-01-01",
  "QueryTasks": [
    {
      "task_sequence_number": 1,
      "task_description": "Clerical Investigation",
      "task_status": "Open",
      "query_reference": 5,
      "response_required_by": "2019-07-11",
      "QueryTaskUpdates": [
        {
          "task_sequence_number": 1,
          "query_reference": 5,
          "updated_by": "Us",
          "last_updated": "2019-07-11",
          "task_update_description": "Test"
        }
      ],
      "when_action_required": "2019-07-11",
      "next_planned_action": "2017-03-01",
      "task_responsibility": "BMI Survey Team"
    }
  ],
  "results_state": "Not Run",
  "date_raised": "2019-07-11",
  "query_status": "open",
  "target_resolution_date": "2019-07-11",
  "survey_code": "066",
  "Exceptions": [
    {
      "error_description": "Desc",
      "query_reference": 5,
      "survey_period": "201803",
      "survey_code": "066",
      "step": "Vets",
      "run_id": 1,
      "ru_reference": "77700000002",
      "error_code": "01",
      "Anomalies": [
        {
          "survey_period": "201803",
          "question_number": "601",
          "survey_code": "066",
          "anomaly_description": "Desc Question",
          "FailedVETs": [
            {
              "survey_period": "201803",
              "question_number": "601",
              "failed_vet": 1,
              "survey_code": "066",
              "step": "Vets",
              "run_id": 1,
              "ru_reference": "77700000002"
            }
          ],
          "step": "Vets",
          "run_id": 1,
          "ru_reference": "77700000002"
        }
      ]
    }
  ],
  "general_specific_flag": False,
  "raised_by": "System Generated",
  "query_type": "Data Cleaning"
}, "")
