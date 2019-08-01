from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import os
import logging

logger = logging.getLogger("UpdateQuery")


def lambda_handler(event, context):
    usr = os.environ['Username']
    pas = os.environ['Password']

    logger.warning("Input {}".format(event))
    try:
        result = ioValidation.Query(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    try:
        connection = psycopg2.connect(host="", database="es_results_db", user=usr, password=pas)
    except:
        return json.loads('{"UpdateData":"Failed To Connect To Database."}')

    try:
        querySQL = ("UPDATE es_db_test.Query SET"
                    + "  QueryType = %(querytype)s"
                    + ", GeneralSpecificFlag = %(generalspecificflag)s"
                    + ", IndustryGroup = %(industrygroup)s"
                    + ", LastQueryUpdate = %(lastqueryupdate)s"
                    + ", QueryActive = %(queryactive)s"
                    + ", QueryDescription = %(querydescription)s"
                    + ", QueryStatus = %(querystatus)s"
                    + ", ResultsState = %(resultsstate)s"
                    + ", TargetResolutionDate = %(targetresolutiondate)s"
                    + ", CurrentPeriod = %(currentperiod)s"
                    + "  WHERE QueryReference = %(queryreference)s"
                    + ";")
        psql.execute(querySQL, connection, params={"querytype": event["querytype"], "generalspecificflag": event["generalspecificflag"],
                                                   "industrygroup": event["industrygroup"], "lastqueryupdate": event["lastqueryupdate"],
                                                   "queryactive": event["queryactive"], "querydescription": event["querydescription"],
                                                   "querystatus": event["querystatus"], "resultsstate": event["resultsstate"],
                                                   "targetresolutiondate": event["targetresolutiondate"], "queryreference": event["queryreference"],
                                                   "currentperiod": event["currentperiod"]
                                                   })
    except Exception as exc:
        logger.warning("Failed to update query {}", str(exc))
        return json.loads('{"UpdateData":"Failed To Update Query."}')

    try:
        if "Exceptions" in event.keys():
            Exception = event["Exceptions"]
            for exception in Exception:
                exceptionSQL = ("INSERT INTO es_db_test.Step_Exception "
                                + "(queryreference,"
                                + "surveyperiod,"
                                + "runreference,"
                                + "rureference,"
                                + "step,"
                                + "surveyoutputcode,"
                                + "errorcode,"
                                + "errordescription)"
                                + "VALUES ( %(queryreference)s"
                                + ", %(surveyperiod)s"
                                + ", %(runreference)s"
                                + ", %(rureference)s"
                                + ", %(step)s"
                                + ", %(surveyoutputcode)s"
                                + ", %(errorcode)s"
                                + ", %(errordescription)s ) "
                                + "ON CONFLICT (surveyperiod, runreference, rureference, step, surveyoutputcode) "
                                + "DO NOTHING;")

                psql.execute(exceptionSQL, connection, params={"queryreference": exception["queryreference"], "surveyperiod": exception["surveyperiod"],
                                                               "runreference": exception["runreference"],
                                                               "rureference": exception["rureference"],
                                                               "step": exception["step"], "surveyoutputcode": exception["surveyoutputcode"],
                                                               "errorcode": exception["errorcode"], "errordescription": exception["errordescription"]})

                try:
                    if "Anomalies" in exception.keys():
                        Anomaly = exception["Anomalies"]
                        for anomaly in Anomaly:
                            anomalySQL = ("INSERT INTO es_db_test.Question_Anomaly "
                                          + "(surveyperiod,"
                                          + "questionno,"
                                          + "runreference,"
                                          + "rureference,"
                                          + "step,"
                                          + "surveyoutputcode,"
                                          + "description)"
                                          + " VALUES ("
                                          + "  %(surveyperiod)s"
                                          + ", %(questionno)s"
                                          + ", %(runreference)s"
                                          + ", %(rureference)s"
                                          + ", %(step)s"
                                          + ", %(surveyoutputcode)s"
                                          + ", %(description)s ) "
                                          + "ON CONFLICT (surveyperiod, questionno, runreference, rureference, step, surveyoutputcode) "
                                          + "DO NOTHING;")

                            psql.execute(anomalySQL, connection, params={"surveyperiod": anomaly["surveyperiod"], "questionno": anomaly["questionno"],
                                                                         "runreference": anomaly["runreference"], "rureference": anomaly["rureference"],
                                                                         "step": anomaly["step"], "surveyoutputcode": anomaly["surveyoutputcode"],
                                                                         "description": anomaly["description"]})

                            try:
                                if "FailedVETs" in anomaly.keys():
                                    Vets = anomaly["FailedVETs"]
                                    for vets in Vets:
                                        vetsSQL = ("INSERT INTO es_db_test.Failed_VET "
                                                   + "(failedvet,"
                                                   + "surveyperiod,"
                                                   + "questionno,"
                                                   + "runreference,"
                                                   + "rureference,"
                                                   + "step,"
                                                   + "surveyoutputcode)"
                                                   + "VALUES (%(failedvet)s"
                                                   + ", %(surveyperiod)s"
                                                   + ", %(questionno)s"
                                                   + ", %(runreference)s"
                                                   + ", %(rureference)s"
                                                   + ", %(step)s"
                                                   + ", %(surveyoutputcode)s ) "
                                                   + "ON CONFLICT (failedvet, surveyperiod, questionno, runreference, rureference, step, surveyoutputcode) "
                                                   + "DO NOTHING;")

                                        psql.execute(vetsSQL, connection, params={"surveyperiod": vets["surveyperiod"], "questionno": vets["questionno"],
                                                                                  "runreference": vets["runreference"], "failedvet": vets["failedvet"],
                                                                                  "rureference": vets["rureference"], "step": vets["step"],
                                                                                  "surveyoutputcode": vets["surveyoutputcode"]})

                            except:
                                return json.loads('{"UpdateData":"Failed To Update Query in Failed_VET Table."}')
                except:
                    return json.loads('{"UpdateData":"Failed To Update Query in Question_Anomaly Table."}')
    except:
        return json.loads('{"UpdateData":"Failed To Update Query in Step_Exception Table."}')
    # In future QueryTask May Need An Insert On Conflict Update.
    try:
        if "QueryTasks" in event.keys():
            UpdateTasks = event["QueryTasks"]
            for task in UpdateTasks:
                taskSQL = ("UPDATE es_db_test.Query_Task SET"
                           + "  ResponseRequiredBy = %(responserequiredby)s"
                           + ", TaskDescription = %(taskdescription)s"
                           + ", TaskResponsibility = %(taskresponsibility)s"
                           + ", TaskStatus = %(taskstatus)s"
                           + ", NextPlannedAction = %(nextplannedaction)s"
                           + ", WhenActionRequired = %(whenactionrequired)s"
                           + "  WHERE QueryReference = %(queryreference)s"
                           + "  AND   TaskSeqNo = %(taskseqno)s"
                           + ";")
                psql.execute(taskSQL, connection, params={"responserequiredby": task["responserequiredby"], "taskdescription": task["taskdescription"],
                                                          "taskresponsibility": task["taskresponsibility"], "taskstatus": task["taskstatus"],
                                                          "nextplannedaction": task["nextplannedaction"], "whenactionrequired": task["whenactionrequired"],
                                                          "queryreference": task["queryreference"], "taskseqno": task["taskseqno"]})

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
