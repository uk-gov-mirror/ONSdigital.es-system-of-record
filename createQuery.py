from marshmallow import ValidationError
import ioValidation
import json
import logging
import os
import sqlalchemy as db
from sqlalchemy.orm import Session
import alchemy_functions
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger("createQuery")


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    logger.warning("INPUT DATA: {}".format(event))

    #try:
    #    result = ioValidation.Query(strict=True).load(test_data.txt)
    #except ValidationError as err:
    #    return err.messages

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"contributor_name":"Failed To Connect To Database."}')

    try:
        table_model = alchemy_functions.table_model(engine, metadata, 'query')
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
                                on_conflict_do_nothing(constraint=table_model.primary_key)
        newQuery = alchemy_functions.update(statement, session)
    except Exception as exc:
        logging.warning("Error inserting into table: {}".format(exc))
        return json.loads('{"querytype":"Failed To Create Query in Query Table."}')

    newQuery = newQuery.fetchone()[0]
    try:
        if "Exceptions" in event.keys():
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.warning("Inserting excepton {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'query')
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
                        Anomaly = exception["Anomalies"]
                        for count, anomaly in enumerate(Anomaly):
                            logging.warning("inserting into anomaly {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'query')
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
                                    Vets = anomaly["FailedVETs"]
                                    for count, vets in enumerate(Vets):
                                        logging.warning("inserting into failedvet {}".format(count))
                                        table_model = alchemy_functions.table_model(engine, metadata, 'query')
                                        statement = insert(table_model).values(failed_vet=anomaly['failed_vet'],
                                                                                  survey_period=anomaly['survey_period'],
                                                                                  question_number=anomaly['question_number'],
                                                                                  run_id=anomaly['run_id'],
                                                                                  ru_reference=anomaly['ru_reference'],
                                                                                  step=anomaly['step'],
                                                                                  survey_code=anomaly['survey_code']).\
                                on_conflict_do_nothing(constraint=table_model.primary_key)
                                        alchemy_functions.update(statement, session)

                            except:
                                return json.loads('{"query_type":"Failed To Create Query in Failed_VET Table."}')
                except:
                    return json.loads('{"query_type":"Failed To Create Query in Question_Anomaly Table."}')
    except:
        return json.loads('{"query_type":"Failed To Update Query in Step_Exception Table."}')

    try:
        if "QueryTasks" in event.keys():
            Tasks = event["QueryTasks"]
            for count, task in enumerate(Tasks):
                logging.warning("inserting into tasks {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'query')
                statement = insert(table_model).values(task_sequence_number=task['task_sequence_number'],
                                                          query_reference=task['query_reference'],
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
                        updateTask = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(updateTask):
                            logging.warning("inserting into query task updates {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'query')
                            statement = insert(table_model).values(task_sequence_number=query_task['task_sequence_number'],
                                                                      query_reference=query_task['query_reference'],
                                                                      last_updated=query_task['last_updated'],
                                                                      task_update_description=query_task['task_update_description'],
                                                                      updated_by=query_task['updated_by']).\
                                on_conflict_do_nothing(constraint=table_model.primary_key)
                            alchemy_functions.update(statement, session)

                except:
                    return json.loads('{"query_type":"Failed To Create Query in Query_Task_Update Table."}')
    except:
        return json.loads('{"query_type":"Failed To Create Query in Query_Task Table."}')
    try:
        session.commit()
    except:
        return json.loads('{"query_type":"Failed To Commit Changes To The Database."}')

    try:
        session.close()
    except:
        return json.loads('{"query_type":"Connection To Database Closed Badly."}')

    return json.loads('{"query_type":"Query created successfully", "query_reference": ' + str(newQuery) + '}')


x = lambda_handler('', '')
print(x)
