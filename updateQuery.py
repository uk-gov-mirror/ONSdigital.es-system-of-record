from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import os
import logging
import sqlalchemy as db
from sqlalchemy.orm import Session
import alchemy_functions

logger = logging.getLogger("UpdateQuery")


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    logger.warning("Input {}".format(event))

#    try:
#        result = ioValidation.Query(strict=True).load(event)
#    except ValidationError as err:
#        return err.messages

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"UpdateData":"Failed To Connect To Database."}')

    try:
        table_model = alchemy_functions.table_model(engine, metadata, 'query')
        statement = db.insert(table_model).values(query_type=event['query_type'], ru_reference=event['ru_reference'],
                                      survey_code=event['survey_code'], survey_period=event['survey_period'],
                                      current_period=event['current_period'], date_raised=event['date_raised'],
                                      general_specific_flag=event['general_specific_flag'],
                                      industry_group=event['industry_group'], last_query_update=event['last_query_update'],
                                      query_active=event['query_active'], query_description=event['query_description'],
                                      query_status=event['query_status'], raised_by=event['raised_by'],
                                      results_state=event['results_state'],
                                      target_resolution_date=event['target_resolution_date'])
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
                    table_model = alchemy_functions.table_model(engine, metadata, 'step_exception')
                    statement = db.insert(table_model).values(query_reference=exception['query_reference'],
                                                              survey_period=exception['survey_period'], run_id=exception['run_id'],
                                                              ru_reference=exception['ru_reference'], step=exception['step'],
                                                              survey_code=exception['survey_code'], error_code=exception['error_code'],
                                                              error_description=exception['error_description'])
                    alchemy_functions.update(statement, session)

                    try:
                        if "Anomalies" in exception.keys():
                            Anomaly = exception["Anomalies"]
                            for count, anomaly in enumerate(Anomaly):
                                logging.warning("inserting into anomaly {}".format(count))
                                table_model = alchemy_functions.table_model(engine, metadata, 'anomalies')
                                statement = db.insert(table_model).values(survey_period=anomaly['survey_period'],
                                                                          question_number=anomaly['question_number'], run_id=anomaly['run_id'],
                                                                          ru_reference=anomaly['ru_reference'], step=anomaly['step'],
                                                                          survey_code=anomaly['survey_code'],
                                                                          anomaly_description=anomaly['anomaly_description'])
                                alchemy_functions.update(statement, session)

                                try:
                                    if "FailedVETs" in anomaly.keys():
                                        Vets = anomaly["FailedVETs"]
                                        for count, vets in enumerate(Vets):
                                            logging.warning("inserting into failedvet {}".format(count))
                                            table_model = alchemy_functions.table_model(engine, metadata, 'failed_vets')
                                            statement = db.insert(table_model).values(failed_vet=anomaly['failed_vet'],
                                                                                      survey_period=anomaly[
                                                                                          'survey_period'],
                                                                                      question_number=anomaly[
                                                                                          'question_number'],
                                                                                      run_id=anomaly['run_id'],
                                                                                      ru_reference=anomaly[
                                                                                          'ru_reference'],
                                                                                      step=anomaly['step'],
                                                                                      survey_code=anomaly[
                                                                                          'survey_code'])
                                            alchemy_functions.update(statement, session)

                                except:
                                    return json.loads('{"UpdateData":"Failed To Update Query in Failed_VET Table."}')

                    except:
                        return json.loads('{"UpdateData":"Failed To Update Query in Question_Anomaly Table."}')

    except:
        return json.loads('{"UpdateData":"Failed To Update Query in Step_Exception Table."}')

    try:
        if "QueryTasks" in event.keys():
            Tasks = event["QueryTasks"]
            for count, task in enumerate(Tasks):
                logging.warning("Updating query_tasks {}".format(count))
                table_model = alchemy_functions.table_model(engine, metadata, 'query_tasks')
                statement = db.update(table_model).values(response_required_by=task['response_required_by'],
                                                          task_description=task['task_description'],
                                                          task_responsibility=task['task_responsibility'],
                                                          task_status=task['task_status'],
                                                          next_planned_action=task['next_planned_action'],
                                                          when_action_required=task['when_action_required']).\
                        where(db.and_(table_model.columns.task_sequence_number == task['task_sequence_number'],
                              table_model.columns.query_reference ==
                              task['query_reference']))
                alchemy_functions.update(statement, session)

                try:
                    if "QueryTaskUpdates" in task.keys():
                        updateTask = task["QueryTaskUpdates"]
                        for count, query_task in enumerate(updateTask):
                            logging.warning("inserting into query task updates {}".format(count))
                            table_model = alchemy_functions.table_model(engine, metadata, 'query_task_updates')
                            statement = db.insert(table_model).values(
                                task_sequence_number=query_task['task_sequence_number'],
                                query_reference=query_task['query_reference'],
                                last_updated=query_task['last_updated'],
                                task_update_description=query_task['task_update_description'],
                                updated_by=query_task['updated_by'])
                            alchemy_functions.update(statement, session)

                except:
                    return json.loads('{"querytype":"Failed To Create Query in Query_Task_Update Table."}')

    except:
        return json.loads('{"querytype":"Failed To Create Query in Query_Task Table."}')

    # try:
    #     querySQL = ("UPDATE es_db_test.Query SET"
    #                 + "  QueryType = %(querytype)s"
    #                 + ", GeneralSpecificFlag = %(generalspecificflag)s"
    #                 + ", IndustryGroup = %(industrygroup)s"
    #                 + ", LastQueryUpdate = %(lastqueryupdate)s"
    #                 + ", QueryActive = %(queryactive)s"
    #                 + ", QueryDescription = %(querydescription)s"
    #                 + ", QueryStatus = %(querystatus)s"
    #                 + ", ResultsState = %(resultsstate)s"
    #                 + ", TargetResolutionDate = %(targetresolutiondate)s"
    #                 + ", CurrentPeriod = %(currentperiod)s"
    #                 + "  WHERE QueryReference = %(queryreference)s"
    #                 + ";")
    #     psql.execute(querySQL, connection, params={"querytype": event["querytype"], "generalspecificflag": event["generalspecificflag"],
    #                                                "industrygroup": event["industrygroup"], "lastqueryupdate": event["lastqueryupdate"],
    #                                                "queryactive": event["queryactive"], "querydescription": event["querydescription"],
    #                                                "querystatus": event["querystatus"], "resultsstate": event["resultsstate"],
    #                                                "targetresolutiondate": event["targetresolutiondate"], "queryreference": event["queryreference"],
    #                                                "currentperiod": event["currentperiod"]
    #                                                })
    # except Exception as exc:
    #     logger.warning("Failed to update query {}", str(exc))
    #     return json.loads('{"UpdateData":"Failed To Update Query."}')

    # try:
    #     if "Exceptions" in event.keys():
    #         Exception = event["Exceptions"]
    #         for exception in Exception:
    #             exceptionSQL = ("INSERT INTO es_db_test.Step_Exception "
    #                             + "(queryreference,"
    #                             + "surveyperiod,"
    #                             + "runreference,"
    #                             + "rureference,"
    #                             + "step,"
    #                             + "surveyoutputcode,"
    #                             + "errorcode,"
    #                             + "errordescription)"
    #                             + "VALUES ( %(queryreference)s"
    #                             + ", %(surveyperiod)s"
    #                             + ", %(runreference)s"
    #                             + ", %(rureference)s"
    #                             + ", %(step)s"
    #                             + ", %(surveyoutputcode)s"
    #                             + ", %(errorcode)s"
    #                             + ", %(errordescription)s ) "
    #                             + "ON CONFLICT (surveyperiod, runreference, rureference, step, surveyoutputcode) "
    #                             + "DO NOTHING;")
    #
    #             psql.execute(exceptionSQL, connection, params={"queryreference": exception["queryreference"], "surveyperiod": exception["surveyperiod"],
    #                                                            "runreference": exception["runreference"],
    #                                                            "rureference": exception["rureference"],
    #                                                            "step": exception["step"], "surveyoutputcode": exception["surveyoutputcode"],
    #                                                            "errorcode": exception["errorcode"], "errordescription": exception["errordescription"]})

                # try:
                #     if "Anomalies" in exception.keys():
                #         Anomaly = exception["Anomalies"]
                #         for anomaly in Anomaly:
                #             anomalySQL = ("INSERT INTO es_db_test.Question_Anomaly "
                #                           + "(surveyperiod,"
                #                           + "questionno,"
                #                           + "runreference,"
                #                           + "rureference,"
                #                           + "step,"
                #                           + "surveyoutputcode,"
                #                           + "description)"
                #                           + " VALUES ("
                #                           + "  %(surveyperiod)s"
                #                           + ", %(questionno)s"
                #                           + ", %(runreference)s"
                #                           + ", %(rureference)s"
                #                           + ", %(step)s"
                #                           + ", %(surveyoutputcode)s"
                #                           + ", %(description)s ) "
                #                           + "ON CONFLICT (surveyperiod, questionno, runreference, rureference, step, surveyoutputcode) "
                #                           + "DO NOTHING;")
                #
                #             psql.execute(anomalySQL, connection, params={"surveyperiod": anomaly["surveyperiod"], "questionno": anomaly["questionno"],
                #                                                          "runreference": anomaly["runreference"], "rureference": anomaly["rureference"],
                #                                                          "step": anomaly["step"], "surveyoutputcode": anomaly["surveyoutputcode"],
                #                                                          "description": anomaly["description"]})

                            # try:
                            #     if "FailedVETs" in anomaly.keys():
                            #         Vets = anomaly["FailedVETs"]
                            #         for vets in Vets:
                            #             vetsSQL = ("INSERT INTO es_db_test.Failed_VET "
                            #                        + "(failedvet,"
                            #                        + "surveyperiod,"
                            #                        + "questionno,"
                            #                        + "runreference,"
                            #                        + "rureference,"
                            #                        + "step,"
                            #                        + "surveyoutputcode)"
                            #                        + "VALUES (%(failedvet)s"
                            #                        + ", %(surveyperiod)s"
                            #                        + ", %(questionno)s"
                            #                        + ", %(runreference)s"
                            #                        + ", %(rureference)s"
                            #                        + ", %(step)s"
                            #                        + ", %(surveyoutputcode)s ) "
                            #                        + "ON CONFLICT (failedvet, surveyperiod, questionno, runreference, rureference, step, surveyoutputcode) "
                            #                        + "DO NOTHING;")
                            #
                            #             psql.execute(vetsSQL, connection, params={"surveyperiod": vets["surveyperiod"], "questionno": vets["questionno"],
                            #                                                       "runreference": vets["runreference"], "failedvet": vets["failedvet"],
                            #                                                       "rureference": vets["rureference"], "step": vets["step"],
                            #                                                       "surveyoutputcode": vets["surveyoutputcode"]})

    #                         except:
    #                             return json.loads('{"UpdateData":"Failed To Update Query in Failed_VET Table."}')
    #             except:
    #                 return json.loads('{"UpdateData":"Failed To Update Query in Question_Anomaly Table."}')
    # except:
    #     return json.loads('{"UpdateData":"Failed To Update Query in Step_Exception Table."}')
    # # In future QueryTask May Need An Insert On Conflict Update.
    # try:
    #     if "QueryTasks" in event.keys():
    #         UpdateTasks = event["QueryTasks"]
    #         for task in UpdateTasks:
    #             taskSQL = ("UPDATE es_db_test.Query_Task SET"
    #                        + "  ResponseRequiredBy = %(responserequiredby)s"
    #                        + ", TaskDescription = %(taskdescription)s"
    #                        + ", TaskResponsibility = %(taskresponsibility)s"
    #                        + ", TaskStatus = %(taskstatus)s"
    #                        + ", NextPlannedAction = %(nextplannedaction)s"
    #                        + ", WhenActionRequired = %(whenactionrequired)s"
    #                        + "  WHERE QueryReference = %(queryreference)s"
    #                        + "  AND   TaskSeqNo = %(taskseqno)s"
    #                        + ";")
    #             psql.execute(taskSQL, connection, params={"responserequiredby": task["responserequiredby"], "taskdescription": task["taskdescription"],
    #                                                       "taskresponsibility": task["taskresponsibility"], "taskstatus": task["taskstatus"],
    #                                                       "nextplannedaction": task["nextplannedaction"], "whenactionrequired": task["whenactionrequired"],
    #                                                       "queryreference": task["queryreference"], "taskseqno": task["taskseqno"]})

                try:
                    if "QueryTaskUpdates" in task.keys():
                        UpdateUpdate = task["QueryTaskUpdates"]
                        for update in UpdateUpdate:
                            updateSQL = ("INSERT INTO es_db_test.Query_Task_Update " +
                                         "VALUES (%(taskseqno)s, %(queryreference)s, %(lastupdated)s, %(taskupdatedescription)s, %(updatedby)s)" +
                                         "ON CONFLICT (TaskSeqNo,QueryReference,LastUpdated) " +
                                         "DO NOTHING;")

                            psql.execute(updateSQL, connection, params={"taskseqno": update["taskseqno"], "queryreference": update["queryreference"],
                                                                        "lastupdated": update["lastupdated"], "taskupdatedescription": update["taskupdatedescription"],
                                                                        "updatedby": update["updatedby"]})
                except:
                    return json.loads('{"UpdateData":"Failed To Update Query_Task_Updates."}')
    except:
        return json.loads('{"UpdateData":"Failed To Update Query_Tasks."}')
    try:
        connection.commit()
    except:
        return json.loads('{"UpdateData":"Failed To Commit Changes To The Database."}')

    try:
        connection.close()
    except:
        return json.loads('{"UpdateData":"Connection To Database Closed Badly."}')

    return json.loads('{"UpdateData":"Successfully Updated The Tables."}')
