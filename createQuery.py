from marshmallow import ValidationError
import ioValidation
import json
import logging
import psycopg2
import pandas.io.sql as psql
import os

logger = logging.getLogger("createQuery")


def lambda_handler(event, context):
    usr = os.environ['Username']
    pas = os.environ['Password']

    logger.warning("INPUT DATA: {}".format(event))

    try:
        result = ioValidation.Query(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    try:
        connection = psycopg2.connect(host="", database="es_results_db", user=usr, password=pas)
    except:
        return json.loads('{"querytype":"Failed To Connect To Database."}')

    try:
        querySQL = ("INSERT INTO es_db_test.Query "
                    + "(querytype,"
                    + "rureference,"
                    + "surveyoutputcode,"
                    + "periodqueryrelates,"
                    + "currentperiod,"
                    + "datetimeraised,"
                    + "generalspecificflag,"
                    + "industrygroup,"
                    + "lastqueryupdate,"
                    + "queryactive,"
                    + "querydescription,"
                    + "querystatus,"
                    + "raisedby,"
                    + "resultsstate,"
                    + "targetresolutiondate) "
                    + "VALUES (%(querytype)s"
                    + ", %(rureference)s"
                    + ", %(surveyoutputcode)s"
                    + ", %(periodqueryrelates)s"
                    + ", %(currentperiod)s"
                    + ", %(datetimeraised)s"
                    + ", %(generalspecificflag)s"
                    + ", %(industrygroup)s"
                    + ", %(lastqueryupdate)s"
                    + ", %(queryactive)s"
                    + ", %(querydescription)s"
                    + ", %(querystatus)s"
                    + ", %(raisedby)s"
                    + ", %(resultsstate)s"
                    + ", %(targetresolutiondate)s ) "
                    + "ON CONFLICT (queryreference) "
                    + "DO NOTHING "
                    + "RETURNING queryreference;"
                    )

        newQuery = psql.execute(querySQL, connection, params={"querytype": event["querytype"], "rureference": event["rureference"],
                                                              "surveyoutputcode": event["surveyoutputcode"], "periodqueryrelates": event["periodqueryrelates"],
                                                              "currentperiod": event["currentperiod"], "datetimeraised": event["datetimeraised"],
                                                              "generalspecificflag": event["generalspecificflag"], "industrygroup": event["industrygroup"],
                                                              "lastqueryupdate": event["lastqueryupdate"], "queryactive": event["queryactive"],
                                                              "querydescription": event["querydescription"], "querystatus": event["querystatus"],
                                                              "raisedby": event["raisedby"], "resultsstate": event["resultsstate"],
                                                              "targetresolutiondate": event["targetresolutiondate"]})

    except Exception as exc:
        logging.warning("Error inserting into table: {}".format(exc))
        return json.loads('{"querytype":"Failed To Create Query in Query Table."}')

    newQuery = newQuery.fetchone()[0]
    try:
        if "Exceptions" in event.keys():
            exceptions = event["Exceptions"]
            for count, exception in enumerate(exceptions):
                logger.warning("Inserting excepton {}".format(count))
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

                psql.execute(exceptionSQL, connection, params={"queryreference": newQuery, "surveyperiod": exception["surveyperiod"],
                                                               "runreference": exception["runreference"],
                                                               "rureference": exception["rureference"],
                                                               "step": exception["step"], "surveyoutputcode": exception["surveyoutputcode"],
                                                               "errorcode": exception["errorcode"], "errordescription": exception["errordescription"]})

                try:
                    if "Anomalies" in exception.keys():
                        Anomaly = exception["Anomalies"]
                        for count, anomaly in enumerate(Anomaly):
                            logging.warning("inserting into anomaly {}".format(count))
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
                                    for count, vets in enumerate(Vets):
                                        logging.warning("inserting into failedvet {}".format(count))
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
                                return json.loads('{"querytype":"Failed To Create Query in Failed_VET Table."}')
                except:
                    return json.loads('{"querytype":"Failed To Create Query in Question_Anomaly Table."}')
    except:
        return json.loads('{"querytype":"Failed To Update Query in Step_Exception Table."}')

    try:
        if "QueryTasks" in event.keys():
            Tasks = event["QueryTasks"]
            for count, task in enumerate(Tasks):
                logging.warning("inserting into tasks {}".format(count))
                taskSQL = ("INSERT INTO es_db_test.Query_Task "
                           + "(taskseqno,"
                           + "queryreference,"
                           + "responserequiredby,"
                           + "taskdescription,"
                           + "taskresponsibility,"
                           + "taskstatus,"
                           + "nextplannedaction,"
                           + "whenactionrequired)"
                           + "VALUES (%(taskseqno)s "
                           + ", %(queryreference)s"
                           + ", %(responserequiredby)s"
                           + ", %(taskdescription)s"
                           + ", %(taskresponsibility)s"
                           + ", %(taskstatus)s"
                           + ", %(nextplannedaction)s"
                           + ", %(whenactionrequired)s ) "
                           + "ON CONFLICT (taskseqno, queryreference) "
                           + "DO NOTHING;")

                psql.execute(taskSQL, connection, params={"queryreference": newQuery, "taskseqno": task["taskseqno"],
                                                          "responserequiredby": task.get("responserequiredby", None), "taskdescription": task.get("taskdescription", None),
                                                          "taskresponsibility": task.get("taskresponsibility", None), "taskstatus": task.get("taskstatus", None),
                                                          "nextplannedaction": task.get("nextplannedaction", None), "whenactionrequired": task.get("whenactionrequired", None)})

                try:
                    if "QueryTaskUpdates" in task.keys():
                        updateTask = task["QueryTaskUpdates"]
                        for count, update in enumerate(updateTask):
                            logging.warning("inserting into query task updates {}".format(count))
                            updateSQL = ("INSERT INTO es_db_test.Query_Task_Update "
                                         + "(taskseqno,"
                                         + "queryreference,"
                                         + "lastupdated,"
                                         + "taskupdatedescription,"
                                         + "updatedby)"
                                         + "VALUES (%(taskseqno)s "
                                         + ", %(queryreference)s"
                                         + ", %(lastupdated)s"
                                         + ", %(taskupdatedescription)s"
                                         + ", %(updatedby)s ) "
                                         + "ON CONFLICT (taskseqno,queryreference,lastupdated) "
                                         + "DO NOTHING;")

                            psql.execute(updateSQL, connection, params={"queryreference": newQuery, "taskseqno": task["taskseqno"],
                                                                        "lastupdated": update["lastupdated"], "taskupdatedescription": update["taskupdatedescription"],
                                                                        "updatedby": update["updatedby"]})

                except:
                    return json.loads('{"querytype":"Failed To Create Query in Query_Task_Update Table."}')
    except:
        return json.loads('{"querytype":"Failed To Create Query in Query_Task Table."}')
    try:
        connection.commit()
    except:
        return json.loads('{"querytype":"Failed To Commit Changes To The Database."}')

    try:
        connection.close()
    except:
        return json.loads('{"querytype":"Connection To Database Closed Badly."}')

    return json.loads('{"querytype":"Query created successfully", "queryreference": ' + str(newQuery) + '}')
