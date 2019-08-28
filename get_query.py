import json
import logging
import os

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation

logger = logging.getLogger("get_query")


def lambda_handler(event, context):
    """Collects data on a passed in Reference from seven tables and combines them into a single Json.
    Parameters:
      event (Dict):A series of key value pairs used in the search.
    Returns:
      out_json (Json):Nested Json responce of the seven tables data.
    """

    database = os.environ.get('Database_Location', None)
    if database is None:
        logger.error("Database_Location env not set")
        return {"statusCode": 500, "body": {"Error": "Configuration Error."}}

    try:
        io_validation.QuerySearch(strict=True).load(event)
    except ValidationError as err:
        logger.error("Failed to validate input: {}".format(err.messages))
        return {"statusCode": 500, "body": err.messages}

    search_list = ['query_reference',
                   'survey_period',
                   'query_type',
                   'ru_reference',
                   'survey_code',
                   'query_status']

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.NoSuchModuleError as exc:
        logger.error("Error: Failed to connect to the database(driver error): {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Connect To Database." + str(type(exc))}}
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Connect To Database."}}
    except Exception as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Connect To Database."}}

    try:
        table_model = alchemy_functions.table_model(engine, metadata, "query")
        all_query_sql = db.select([table_model])

        added_query_sql = 0

        for criteria in search_list:
            if criteria not in event.keys():
                continue

            added_query_sql += 1
            all_query_sql = all_query_sql.where(getattr(table_model.columns, criteria) == event[criteria])

        if added_query_sql == 0:
            all_query_sql = all_query_sql.where(table_model.columns.query_status == 'Open')

        query = alchemy_functions.select(all_query_sql, session)
    except db.exc.OperationalError as exc:
        logger.error("Error selecting data from table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Retrieve Data."}}
    except Exception as exc:
        logger.error("Error selecting data from table: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Failed To Retrieve Data."}}

    table_list = {'step_exception': None,
                  'question_anomaly': None,
                  'failed_vet': None,
                  'query_task': None,
                  'query_task_update': None}

    out_json = '{"Queries":[ '
    for index, query_row in query.iterrows():
        curr_query = query[query['query_reference'] == query_row['query_reference']]
        if curr_query.empty:
            continue
        ref = int(curr_query['query_reference'].iloc[0])
        ru = curr_query['ru_reference'].iloc[0]
        period = str(curr_query['survey_period'].iloc[0])
        survey = curr_query['survey_code'].iloc[0]

        try:
            for current_table in table_list:
                logger.info("Selecting query data from table: {}".format(current_table))
                table_model = alchemy_functions.table_model(engine, metadata, current_table)

                # Can't use a single select for the 5 tables as two use different criteria. Will meed to change.
                if (current_table == 'step_exception') or (current_table == 'query_task')\
                        or (current_table == 'query_task_update'):
                    statement = db.select([table_model]).where(table_model.columns.query_reference == ref)
                elif current_table == "failed_vet":
                    other_model = alchemy_functions.table_model(engine, metadata, "vet")
                    statement = db.select([table_model, other_model.columns.vet_description]).\
                        where(db.and_(table_model.columns.survey_period == period,
                                      table_model.columns.survey_code == survey, table_model.columns.ru_reference == ru,
                                      table_model.columns.failed_vet == other_model.columns.vet_code))
                else:
                    statement = db.select([table_model]).where(db.and_(table_model.columns.survey_period ==
                                                                       period, table_model.columns.survey_code ==
                                                                       survey, table_model.columns.ru_reference == ru))

                table_data = alchemy_functions.select(statement, session)
                table_list[current_table] = table_data

        except db.exc.OperationalError as exc:
            logger.error("Error updating the database." + str(type(exc)))
            return {"statusCode": 500, "body": {"Error": "Failed To Find Query."}}
        except Exception as exc:
            logger.error("Error finding query: {}".format(exc))
            return {"statusCode": 500, "body": {"Error": "Failed To Find Query."}}

        curr_query = json.dumps(curr_query.to_dict(orient='records'), sort_keys=True, default=str)
        curr_query = curr_query[1:-2]
        out_json += curr_query + ',"Exceptions":[ '
        for index, step_row in table_list['step_exception'].iterrows():
            row_step = step_row['step']
            curr_step = table_list['step_exception'][(table_list['step_exception']['step'] == row_step)
                                                     & (table_list['step_exception']['run_id'] == step_row['run_id'])]
            if curr_step.empty:
                logger.info("No query information in step_exception table")
                continue

            curr_step = json.dumps(curr_step.to_dict(orient='records'), sort_keys=True, default=str)
            curr_step = curr_step[1:-2]
            out_json += (curr_step + ',"Anomalies":[ ')
            for index, ano_row in table_list['question_anomaly'].iterrows():
                curr_ano = table_list['question_anomaly'][(table_list['question_anomaly']['step'] == ano_row['step'])
                                                          & (table_list['question_anomaly']['question_number'] ==
                                                             ano_row['question_number'])
                                                          & (table_list['question_anomaly']['step'] == row_step)]
                if curr_ano.empty:
                    logger.info("No query information in question_anomaly table")
                    continue

                curr_ano = json.dumps(curr_ano.to_dict(orient='records'), sort_keys=True, default=str)
                curr_ano = curr_ano[1:-2]

                out_json += curr_ano + ',"FailedVETs":'
                curr_per = table_list['failed_vet'][(table_list['failed_vet']['step'] == row_step)
                                                    & (table_list['failed_vet']['question_number']
                                                        == ano_row['question_number'])]
                curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
                out_json += (curr_per + '},')

            out_json = out_json[:-1]
            out_json += ']},'
        out_json = out_json[:-1]

        out_json += '],"QueryTasks":[ '
        for index, tas_row in table_list['query_task'].iterrows():
            curr_tas = table_list['query_task'][(table_list['query_task']['query_reference'] ==
                                                 tas_row['query_reference']) & (
                    table_list['query_task']['task_sequence_number'] == tas_row['task_sequence_number'])]
            if curr_tas.empty:
                logger.info("No query information in query_task table")
                continue
            curr_tas = json.dumps(curr_tas.to_dict(orient='records'), sort_keys=True, default=str)
            curr_tas = curr_tas[1:-2]
            out_json += curr_tas
            out_json = out_json + ',"QueryTaskUpdates":'
            if not table_list['query_task_update'].empty:
                curr_up = table_list['query_task_update'][(table_list['query_task_update']['query_reference'] ==
                                                           tas_row['query_reference'])
                                                          & (table_list['query_task_update']['task_sequence_number'] ==
                                                             tas_row['task_sequence_number'])]

                curr_up = json.dumps(curr_up.to_dict(orient='records'), sort_keys=True, default=str)
            else:
                curr_up = "[]"
            out_json += curr_up + '},'

        out_json = out_json[:-1]
        out_json += ']},'

    try:
        session.close()
    except db.exc.OperationalError as exc:
        logger.error("Error: Failed to close connection to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Database Session Closed Badly."}}
    except Exception as exc:
        logger.error("Error: Failed to close connection to the database: {}".format(exc))
        return {"statusCode": 500, "body": {"Error": "Database Session Closed Badly."}}

    out_json = out_json[:-1]
    out_json += ']}'
    out_json = out_json.replace("NaN", "null")

    try:
        io_validation.Queries(strict=True).loads(out_json)
    except ValidationError as err:
        logger.error("Failed to validate output: {}".format(err.messages))
        return {"statusCode": 500, "body": err.messages}

    logger.info("Successfully completed find query")
    return {"statusCode": 200, "body": {"Error": "Successfully ran find_query."}}


# x = lambda_handler({'query_reference': 1}, '')
# print(x)
