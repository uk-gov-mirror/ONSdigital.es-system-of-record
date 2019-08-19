import json
import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("updateQuery")

with open('test_data.txt') as infile:
    test_data = json.load(infile)


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    logger.warning("Input {}".format(event))

    try:
        io_validation.QueryReference(strict=True).load(event)
        io_validation.Query(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"Update_Data":"Failed To Connect To Database."}')

    try:
        table_model = alchemy_functions.table_model(engine, metadata, 'query')
        statement = db.update(table_model).values(query_type=event['query_type'],
                                                  current_period=event['current_period'],
                                                  general_specific_flag=event['general_specific_flag'],
                                                  industry_group=event['industry_group'],
                                                  query_active=event['query_active'],
                                                  query_description=event['query_description'],
                                                  query_status=event['query_status'],
                                                  results_state=event['results_state'],
                                                  target_resolution_date=event['target_resolution_date']).\
            where(table_model.columns.query_reference == event['query_reference'])
        alchemy_functions.update(statement, session)

    except Exception as exc:
        logging.warning("Error inserting into table: {}".format(exc))
        return json.loads('{"query_type":"Failed To Create Query in Query Table."}')

    try:
        if "Exceptions" in event.keys():
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.warning("Inserting step_exception {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'step_exception')
                statement = insert(table_model).values(query_reference=exception['query_reference'],
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
                        anomalies = exception["Anomalies"]
                        for count, anomaly in enumerate(anomalies):
                            logging.warning("inserting into question_anomaly {}".format(count))
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
                                if "Failed_VETs" in anomaly.keys():
                                    failed_vets = anomaly["Failed_VETs"]
                                    for count, vets in enumerate(failed_vets):
                                        logging.warning("inserting into failed_vet {}".format(count))
                                        table_model = alchemy_functions.table_model(engine, metadata, 'failed_vet')
                                        statement = insert(table_model).values(failed_vet=anomaly['failed_vet'],
                                                                               survey_period=anomaly[
                                                                                      'survey_period'],
                                                                               question_number=anomaly[
                                                                                      'question_number'],
                                                                               run_id=anomaly['run_id'],
                                                                               ru_reference=anomaly[
                                                                                      'ru_reference'],
                                                                               step=anomaly['step'],
                                                                               survey_code=anomaly[
                                                                                      'survey_code']).\
                                            on_conflict_do_nothing(constraint=table_model.primary_key)
                                        alchemy_functions.update(statement, session)

                            except:
                                return json.loads('{"UpdateData":"Failed To Update Query in Failed_VET Table."}')

                except:
                    return json.loads('{"UpdateData":"Failed To Update Query in Question_Anomaly Table."}')

    except:
        return json.loads('{"UpdateData":"Failed To Update Query in Step_Exception Table."}')

    try:
        if "QueryTasks" in event.keys():
            tasks = event["QueryTasks"]
            for count, task in enumerate(tasks):
                logging.warning("Updating query_tasks {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'query_task')
                statement = db.update(table_model).values(response_required_by=task['response_required_by'],
                                                          task_description=task['task_description'],
                                                          task_responsibility=task['task_responsibility'],
                                                          task_status=task['task_status'],
                                                          next_planned_action=task['next_planned_action'],
                                                          when_action_required=task['when_action_required'])\
                    .where(db.and_(table_model.columns.task_sequence_number == task['task_sequence_number'],
                           table_model.columns.query_reference ==
                           task['query_reference']))
                alchemy_functions.update(statement, session)

                try:
                    if "QueryTaskUpdates" in task.keys():
                        update_task = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(update_task):
                            logging.warning("inserting into query task updates {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'query_task_update')
                            statement = insert(table_model).values(
                                task_sequence_number=query_task['task_sequence_number'],
                                query_reference=query_task['query_reference'],
                                last_updated=query_task['last_updated'],
                                task_update_description=query_task['task_update_description'],
                                updated_by=query_task['updated_by']).\
                                on_conflict_do_nothing(constraint=table_model.primary_key)
                            alchemy_functions.update(statement, session)

                except:
                    return json.loads('{"querytype":"Failed To Create Query in Query_Task_Update Table."}')

    except:
        return json.loads('{"querytype":"Failed To Create Query in Query_Task Table."}')

    try:
        session.commit()
    except:
        return json.loads('{"UpdateData":"Failed To Commit Changes To The Database."}')

    try:
        session.commit()
    except:
        return json.loads('{"UpdateData.":"Failed To Commit."}')

    try:
        session.close()
    except:
        return json.loads('{"UpdateData":"Connection To Database Closed Badly."}')

    return json.loads('{"UpdateData":"Successfully Updated The Tables."}')


x = lambda_handler(test_data, '')
print(x)
