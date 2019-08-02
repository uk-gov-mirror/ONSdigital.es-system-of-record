from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import os
import sqlalchemy as db
from sqlalchemy.orm import Session
import alchemy_functions


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    try:
        result = ioValidation.QuerySearch(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    search_list = ['query_reference',
                   'survey_period',
                   'query_type',
                   'ru_reference',
                   'survey_code',
                   'query_status']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"contributor_name":"Failed To Connect To Database."}')


#    all_query_sql = "SELECT * FROM es_db_test.Query WHERE"
    table_model = alchemy_functions.table_model(engine, metadata, "query")
    all_query_sql = db.select([table_model])
    added_query_sql = 0

    for criteria in search_list:
        if event[criteria] is None:
            continue
        if event[criteria] == "":
            continue
        added_query_sql += 1
        all_query_sql = all_query_sql.where(table_model.columns.criteria == event[criteria])

    if added_query_sql == 0:
        all_query_sql = all_query_sql.where(table_model.columns.query_status == 'Open')

    query = alchemy_functions.select(all_query_sql, session)
    #query = psql.read_sql(all_query_sql, connection, params={'QueryReference': event['QueryReference'], 'PeriodQueryRelates': event['PeriodQueryRelates'], 'QueryType': event['QueryType'], 'RUReference': event['RUReference'], 'SurveyOutputCode': event['SurveyOutputCode'], 'QueryStatus': event['QueryStatus']})

    table_list = {'step_exception': None,
                  'question_anomaly': None,
                  'failed_vet': None,
                  'query_task': None,
                  'query_task_update': None}

    outJSON = '{"Queries":[ '
    for index, query_row in query.iterrows():
        curr_query = query[query['query_reference'] == query_row['query_reference']]
        if curr_query.empty:
            continue
        Ref = int(curr_query['query_reference'].iloc[0])
        RU = curr_query['ru_reference'].iloc[0]
        Period = str(curr_query['survey_period'].iloc[0])
        Survey = curr_query['survey_code'].iloc[0]

#        try:

        for current_table in table_list:
            table_model = alchemy_functions.table_model(engine, metadata, current_table)

            statement = db.select([table_model]).where(table_model.columns.ru_reference == Ref)

            if current_table == "failed_vet":
                other_model = alchemy_functions.table_model(engine, metadata, "vet")
                joined = table_model.join(other_model)
                statement = statement.select_from(joined)

            table_data = alchemy_functions.select(statement, session)
            table_list[current_table] = table_data

#            step_exceptions = psql.read_sql("SELECT * FROM es_db_test.Step_Exception WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
#            question_anomaly = psql.read_sql("SELECT * FROM es_db_test.Question_Anomaly WHERE SurveyPeriod = %(Period)s AND RUReference = %(RU)s AND SurveyOutputCode = %(Survey)s;", connection, params={'RU': RU, 'Period': Period, 'Survey': Survey})
#            VETs = psql.read_sql("SELECT a.*,b.Description FROM es_db_test.Failed_VET a, es_db_test.VET b WHERE a.SurveyPeriod = %(Period)s AND a.RUReference = %(RU)s AND a.SurveyOutputCode = %(Survey)s AND a.FailedVET = b.VET;", connection, params={'RU': RU, 'Period': Period, 'Survey': Survey})
#            query_tasks = psql.read_sql("SELECT * FROM es_db_test.Query_Task WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
#            query_task_updates = psql.read_sql("SELECT * FROM es_db_test.Query_Task_Update WHERE QueryReference = %(Ref)s;", connection, params={'Ref': Ref})
#        except:
#            return json.loads('{"queryreference":"Error","querytype":"Error selecting query from database"}')

        curr_query = json.dumps(curr_query.to_dict(orient='records'), sort_keys=True, default=str)
        curr_query = curr_query[1:-2]
        outJSON += curr_query + ',"Exceptions":[ '
        for index, step_row in table_list['step_exceptions'].iterrows():
            row_step = step_row['step']
            curr_step = table_list['step_exceptions']['step'] == \
                        (row_step & (table_list['step_exceptions']['run_id'] ==
                                     step_row['run_id']))
            if curr_step.empty:
                continue

            curr_step = json.dumps(curr_step.to_dict(orient='records'), sort_keys=True, default=str)
            curr_step = curr_step[1:-2]
            outJSON += (curr_step + ',"Anomalies":[ ')
            for index, ano_row in table_list['question_anomaly'].iterrows():
                curr_ano = table_list['question_anomaly'](table_list['question_anomaly']['step'] == ano_row['step']) \
                           & (table_list['question_anomaly']['question_number'] == ano_row['question_number']) & \
                           (table_list['question_anomaly']['step'] == row_step)
                if curr_ano.empty:
                    continue

                curr_ano = json.dumps(curr_ano.to_dict(orient='records'), sort_keys=True, default=str)
                curr_ano = curr_ano[1:-2]

                outJSON += curr_ano + ',"FailedVETs":'
                curr_per = table_list['failed_vet'][(table_list['failed_vet']['step'] == row_step)
                                              & (table_list['failed_vet']['question_number'] == ano_row['question_number'])]
                curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
                outJSON += (curr_per + '},')

            outJSON = outJSON[:-1]
            outJSON += ']},'
        outJSON = outJSON[:-1]

        outJSON += '],"QueryTasks":[ '
        for index, tas_row in table_list['query_task'].iterrows():
            curr_tas = table_list['query_task'][(table_list['query_task']['query_reference'] ==
                                                  tas_row['query_reference']) & (
                    table_list['query_task']['task_sequence_number'] == tas_row['task_sequence_number'])]
            if curr_tas.empty:
                continue
            curr_tas = json.dumps(curr_tas.to_dict(orient='records'), sort_keys=True, default=str)
            curr_tas = curr_tas[1:-2]
            outJSON += curr_tas
            outJSON = outJSON + ',"QueryTaskUpdates":'
            curr_up = table_list['query_task_update'][(table_list['query_task_update']['query_reference']
                                                        == tas_row['query_reference']) &
                                                       (table_list['query_task_update']['task_sequence_number'] ==
                                                        tas_row['task_sequence_number'])]

            curr_up = json.dumps(curr_up.to_dict(orient='records'), sort_keys=True, default=str)
            outJSON += curr_up + '},'

        outJSON = outJSON[:-1]
        outJSON += ']},'

    try:
        session.close()
    except:
        return json.loads('{"query_reference":"Connection To Database Closed Badly."}')

    outJSON = outJSON[:-1]
    outJSON += ']}'
    outJSON = outJSON.replace("NaN", "null")

    try:
        result = ioValidation.Query(strict=True).loads(outJSON)
    except ValidationError as err:
        return err.messages

    return json.loads(outJSON)

x = lambda_handler({'query_reference': 1,
                   'survey_period': '',
                   'query_type': '',
                   'ru_reference': '',
                   'survey_code': '',
                   'query_status': ''}, '')
print(x)
