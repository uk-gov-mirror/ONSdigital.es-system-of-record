import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("get_query_dashboard")


# Same as findQuery but with one extra return key/value pair.
def lambda_handler(event, context):
    """Collects data on a passed in References from eight tables and combines
    them into a single Json.
    Parameters:
      event (Dict):A series of key value pairs used in the search.
    Returns:
      out_json (Json):Nested Json responce of the eight tables data.
    """

    logger.info("get_query_dashboard Has Started Running.")

    try:
        database, error = io_validation.Database(strict=True).load(os.environ)
    except ValidationError as e:
        logger.error(
            "Database_Location Environment Variable" +
            "Has Not Been Set Correctly: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": "Configuration Error: {}"
                .format(e)}}

    try:
        io_validation.QuerySearch(strict=True).load(event)
    except ValidationError as e:
        logger.error("Input: {}".format(event))
        logger.error("Failed To Validate The Input: {}".format(e.messages))
        return {"statusCode": 400, "body": {"Error": e.messages}}

    search_list = ['query_reference',
                   'survey_period',
                   'query_type',
                   'ru_reference',
                   'survey_code',
                   'query_status']

    try:
        logger.info("Connecting To The Database.")
        engine = db.create_engine(database['Database_Location'])
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as e:
        logger.error("Driver Error, Failed To Connect: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Driver Error, Failed To Connect."}}
    except db.exc.OperationalError as e:
        logger.error("Operational Error, Encountered: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, Failed To Connect."}}
    except Exception as e:
        logger.error("General Error, Failed To Connect To The Database: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, Failed To Connect To The Database."}}

    try:
        logger.info("Fetching Table Model: {}".format("query"))
        table_model = alchemy_functions.table_model(engine, metadata, "query")
        logger.info("Fetching Table Model: {}".format("contributor"))
        other_model = alchemy_functions.table_model(engine, metadata,
                                                    "contributor")

        logger.info("Building SQL Statement: {}".format("Combined Tables"))
        all_query_sql = session.query(table_model, other_model)\
            .join(other_model, table_model.columns.ru_reference ==
                  other_model.columns.ru_reference)

        added_query_sql = 0

        for criteria in search_list:
            if criteria not in event.keys():
                logger.info("No parameters have been passed for {}."
                            .format(criteria))
                continue

            added_query_sql += 1
            all_query_sql = all_query_sql.filter(getattr(
                table_model.columns, criteria) == event[criteria])

        if added_query_sql == 0:
            all_query_sql = all_query_sql.filter(
                table_model.columns.query_status == 'Open')

        logger.info("Converting Data: {}".format("query"))
        query = alchemy_functions.to_df(all_query_sql)

    except db.exc.OperationalError as e:
        logger.error(
            "Operational Error, When Retrieving Data: {} {}"
            .format("query", e))
        return {"statusCode": 500,
                "body": {"Error":
                         "Operational Error, Failed To Retrieve Data: {}"
                         .format("query")}}
    except Exception as e:
        logger.error("General Error, Problem Retrieving Data From The Table: {} {}"
                     .format("query", e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, Failed To Retrieve Data: {}"
                         .format("query")}}

    table_list = {'step_exception': None,
                  'question_anomaly': None,
                  'failed_vet': None,
                  'query_task': None,
                  'query_task_update': None}

    logger.info("Initialising JSON.")
    out_json = '{"Queries":[ '
    for index, query_row in query.iterrows():
        curr_query = query[query['query_reference'] ==
                           query_row['query_reference']]

        if curr_query.empty:
            logger.info("No Queries Found.")
            continue

        ref = int(curr_query['query_reference'].iloc[0])
        ru = curr_query['ru_reference'].iloc[0]
        period = str(curr_query['survey_period'].iloc[0])
        survey = curr_query['survey_code'].iloc[0]

        try:
            for current_table in table_list:
                logger.info("Fetching Table Model: {}".format(current_table))
                table_model = alchemy_functions.table_model(engine, metadata,
                                                            current_table)

                logger.info("Fetching Table Data: {}".format(current_table))
                if current_table in ['step_exception', 'query_task',
                                     'query_task_update']:
                    statement = session.query(table_model).filter(
                        table_model.columns.query_reference == ref)
                elif current_table == "failed_vet":
                    other_model = alchemy_functions.table_model(
                        engine, metadata, "vet")
                    statement = session.query(table_model, other_model)\
                        .filter(
                        db.and_(table_model.columns.survey_period == period,
                                table_model.columns.survey_code == survey,
                                table_model.columns.ru_reference == ru))
                else:
                    statement = session.query(table_model).filter(
                        db.and_(table_model.columns.survey_period == period,
                                table_model.columns.survey_code == survey,
                                table_model.columns.ru_reference == ru))

                logger.info("Converting Data: {}".format(current_table))
                table_list[current_table] = alchemy_functions.to_df(statement)
        except db.exc.OperationalError as e:
            logger.error(
                "Operational Error, When Retrieving Data: {} {}"
                .format(current_table, e))
            return {"statusCode": 500,
                    "body": {
                        "Error": "Operational Error, Failed To Retrieve Data: {}"
                        .format(current_table)}}
        except Exception as e:
            logger.error("General Error, Problem Retrieving Data From The Table: {} {}"
                         .format(current_table, e))
            return {"statusCode": 500,
                    "body": {"Error": "General Error, Failed To Retrieve Data: {}"
                             .format(current_table)}}

        logger.info("Creating JSON.")
        curr_query = json.dumps(curr_query.to_dict(orient='records'),
                                sort_keys=True, default=str)
        curr_query = curr_query[1:-2]
        out_json += curr_query + ',"Exceptions":[ '
        for index, step_row in table_list['step_exception'].iterrows():
            row_step = step_row['step']
            curr_step = table_list['step_exception'][
                (table_list['step_exception']['step'] == row_step) &
                (table_list['step_exception']['run_id'] == step_row['run_id'])]

            if curr_step.empty:
                logger.info("No Exceptions Found.")
                continue

            curr_step = json.dumps(curr_step.to_dict(orient='records'),
                                   sort_keys=True, default=str)
            curr_step = curr_step[1:-2]
            out_json += (curr_step + ',"Anomalies":[ ')
            for index, ano_row in table_list['question_anomaly'].iterrows():
                curr_ano = table_list['question_anomaly'][
                    (table_list['question_anomaly']['step'] == ano_row['step'])
                    & (table_list['question_anomaly']['question_number'] ==
                       ano_row['question_number']) &
                    (table_list['question_anomaly']['step'] == row_step)]

                if curr_ano.empty:
                    logger.info("No Anomalies Found.")
                    continue

                curr_ano = json.dumps(curr_ano.to_dict(orient='records'),
                                      sort_keys=True, default=str)
                curr_ano = curr_ano[1:-2]

                out_json += curr_ano + ',"FailedVETs":'
                curr_per = table_list['failed_vet'][
                    (table_list['failed_vet']['step'] == row_step) &
                    (table_list['failed_vet']['question_number'] ==
                     ano_row['question_number'])]

                curr_per = json.dumps(curr_per.to_dict(orient='records'),
                                      sort_keys=True, default=str)
                out_json += (curr_per + '},')

            out_json = out_json[:-1]
            out_json += ']},'
        out_json = out_json[:-1]

        out_json += '],"QueryTasks":[ '
        for index, tas_row in table_list['query_task'].iterrows():
            curr_tas = table_list['query_task'][
                (table_list['query_task']['query_reference'] ==
                 tas_row['query_reference']) &
                (table_list['query_task']['task_sequence_number'] ==
                 tas_row['task_sequence_number'])]

            if curr_tas.empty:
                logger.info("No Tasks Found.")
                continue

            curr_tas = json.dumps(curr_tas.to_dict(orient='records'),
                                  sort_keys=True, default=str)
            curr_tas = curr_tas[1:-2]
            out_json += curr_tas
            out_json = out_json + ',"QueryTaskUpdates":'
            if not table_list['query_task_update'].empty:
                curr_up = table_list['query_task_update'][
                    (table_list['query_task_update']['query_reference'] ==
                     tas_row['query_reference']) &
                    (table_list['query_task_update']['task_sequence_number'] ==
                     tas_row['task_sequence_number'])]

                curr_up = json.dumps(curr_up.to_dict(orient='records'),
                                     sort_keys=True, default=str)
            else:
                curr_up = "[]"
            out_json += curr_up + '},'

        out_json = out_json[:-1]
        out_json += ']},'

    out_json = out_json[:-1]
    out_json += ']}'
    out_json = out_json.replace("NaN", "null")

    try:
        logger.info("Closing Session.")
        session.close()
    except db.exc.OperationalError as e:
        logger.error(
            "Operational Error, Failed To Close The Session: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "Operational Error, Database Session Closed Badly."}}
    except Exception as e:
        logger.error("General Error, Failed To Close The Session: {}".format(e))
        return {"statusCode": 500,
                "body": {"Error": "General Error, Database Session Closed Badly."}}

    try:
        io_validation.Queries(strict=True).loads(out_json)
    except ValidationError as e:
        logger.error("Output: {}".format(out_json))
        logger.error("Failed To Validate The Output: {}".format(e.messages))
        return {"statusCode": 500, "body": {"Error": e.messages}}

    logger.info("get_query_dashboard Has Successfully Run.")
    return {"statusCode": 200, "body": json.loads(out_json)}
