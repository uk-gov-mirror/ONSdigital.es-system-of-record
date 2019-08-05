from marshmallow import ValidationError
import ioValidation
import json
import psycopg2
import pandas.io.sql as psql
import os
import sqlalchemy as db
from sqlalchemy.orm import Session
import alchemy_functions


# Same as findQuery but with one extra return key/value pair.
def lambda_handler(event, context):
    database = os.environ['Database_Location']

    try:
        result = ioValidation.QuerySearch(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    search_list = ['QueryReference',
                   'PeriodQueryRelates',
                   'QueryType',
                   'RUReference',
                   'SurveyOutputCode',
                   'QueryStatus']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"contributor_name":"Failed To Connect To Database."}')



    table_model = alchemy_functions.table_model(engine, metadata, "query")
    table_mod2 = alchemy_functions.table_model(engine, metadata, "contributor")
    all_query_sql = db.select([table_model, table_mod2.contributor_name])

    print(all_query_sql)

    added_query_sql = 0

    for criteria in search_list:
        if event[criteria] is None:
            continue
        if event[criteria] == "":
            continue
        added_query_sql += 1
        all_query_sql = all_query_sql.where(getattr(table_model.columns, criteria) == event[criteria])

    if added_query_sql == 0:
        all_query_sql = all_query_sql.where(table_model.columns.query_status == 'Open')

    query = alchemy_functions.select(all_query_sql, session)


#    all_query_sql = "SELECT a.*, b.contributorname FROM es_db_test.Query a, es_db_test.Contributor b WHERE a.rureference = b.rureference"
#
#    added_query_sql = ""
#
#    for criteria in search_list:
#        if event[criteria] is None:
#            continue
#        if event[criteria] == "":
#            continue
#
#        added_query_sql += " AND a." + criteria + " = %(" + criteria + ")s"
#
#    all_query_sql += added_query_sql + ";"


    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"query_reference":"Failed To Connect To Database."}')

    query = psql.read_sql(all_query_sql, session, params={'QueryReference': event['QueryReference'], 'PeriodQueryRelates': event['PeriodQueryRelates'], 'QueryType': event['QueryType'], 'RUReference': event['RUReference'], 'SurveyOutputCode': event['SurveyOutputCode'], 'QueryStatus': event['QueryStatus']})

    table_list = {'step_exception': None,
                  'question_anomaly': None,
                  'failed_vet': None,
                  'query_task': None,
                  'query_task_update': None}


    outJSON = '{"Queries":[ '
    for index, query_row in query.iterrows():
        curr_query = query[query['queryreference'] == query_row['queryreference']]
        if curr_query.empty:
            continue
        Ref = int(curr_query['query_reference'].iloc[0])
        RU = curr_query['ru_reference'].iloc[0]
        Period = str(curr_query['period_query_relates'].iloc[0])
        Survey = curr_query['survey_output_code'].iloc[0]

        try:
            for current_table in table_list:
                table_model = alchemy_functions.table_model(engine, metadata, current_table)

                # Can't use a single select for the 5 tables as two use different criteria. Will meed to change.
                if (current_table == 'step_exception') or (current_table == 'query_task') \
                        or (current_table == 'query_task_update'):
                    statement = db.select([table_model]).where(table_model.columns.query_reference == Ref)
                else:
                    statement = db.select([table_model]).where(
                        db.and_(table_model.columns.survey_period == Period, table_model.columns.survey_code == Survey,
                                table_model.columns.ru_reference == RU))

                if current_table == "failed_vet":
#                    other_model = alchemy_functions.table_model(engine, metadata, "vet")
#                    joined = table_model.join(other_model)
#                    statement = statement.select_from(joined)
                    other_model = alchemy_functions.table_model(engine, metadata, "vet")
                    statement = db.select([table_model, other_model.columns.description])

                table_data = alchemy_functions.select(statement, session)
                table_list[current_table] = table_data

#            step_exceptions = psql.read_sql("SELECT * FROM es_db_test.Step_Exception WHERE QueryReference = %(Ref)s;", session, params={'Ref': Ref})
#            question_anomaly = psql.read_sql("SELECT * FROM es_db_test.Question_Anomaly WHERE SurveyPeriod = %(Period)s AND RUReference = %(RU)s AND SurveyOutputCode = %(Survey)s;", session, params={'RU': RU, 'Period': Period, 'Survey': Survey})
#            VETs = psql.read_sql("SELECT a.*,b.Description FROM es_db_test.Failed_VET a, es_db_test.VET b WHERE a.SurveyPeriod = %(Period)s AND a.RUReference = %(RU)s AND a.SurveyOutputCode = %(Survey)s AND a.FailedVET = b.VET;", session, params={'RU': RU, 'Period': Period, 'Survey': Survey})
#            query_tasks = psql.read_sql("SELECT * FROM es_db_test.Query_Task WHERE QueryReference = %(Ref)s;", session, params={'Ref': Ref})
#            query_task_updates = psql.read_sql("SELECT * FROM es_db_test.Query_Task_Update WHERE QueryReference = %(Ref)s;", session, params={'Ref': Ref})
        except:
            return json.loads('{"queryreference":"Error","querytype":"Error selecting query from database"}')

#        curr_query = json.dumps(curr_query.to_dict(orient='records'), sort_keys=True, default=str)
#        curr_query = curr_query[1:-2]
#        outJSON += curr_query + ',"Exceptions":[ '
#        for index, step_row in step_exceptions.iterrows():
#            row_step = step_row['step']
#            curr_step = step_exceptions[(step_exceptions['step'] == row_step) & (step_exceptions['runreference'] == step_row['runreference'])]
#            if curr_step.empty:
#                continue

        for index, step_row in table_list['step_exception'].iterrows():
            row_step = step_row['step']
            curr_step = table_list['step_exception'][(table_list['step_exception']['step'] == row_step) & (
                        table_list['step_exception']['run_id'] == step_row['run_id'])]
            if curr_step.empty:
                continue



#            curr_step = json.dumps(curr_step.to_dict(orient='records'), sort_keys=True, default=str)
#            curr_step = curr_step[1:-2]
#            outJSON += (curr_step + ',"Anomalies":[ ')
#            for index, ano_row in question_anomaly.iterrows():
#                curr_ano = question_anomaly[(question_anomaly['step'] == ano_row['step']) & (question_anomaly['questionno'] == ano_row['questionno']) & (question_anomaly['step'] == row_step)]
#                if curr_ano.empty:
#                    continue

            curr_step = json.dumps(curr_step.to_dict(orient='records'), sort_keys=True, default=str)
            curr_step = curr_step[1:-2]
            outJSON += (curr_step + ',"Anomalies":[ ')
            for index, ano_row in table_list['question_anomaly'].iterrows():
                curr_ano = table_list['question_anomaly'][
                    (table_list['question_anomaly']['step'] == ano_row['step']) \
                    & (table_list['question_anomaly']['question_number'] == ano_row['question_number']) & \
                    (table_list['question_anomaly']['step'] == row_step)]
                if curr_ano.empty:
                    continue

                curr_ano = json.dumps(curr_ano.to_dict(orient='records'), sort_keys=True, default=str)
                curr_ano = curr_ano[1:-2]

                outJSON += curr_ano + ',"FailedVETs":'
                curr_per = table_list['failed_vet'][(table_list['failed_vet']['step'] == row_step)
                                                    & (table_list['failed_vet']['question_number'] == ano_row[
                    'question_number'])]
                curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
                outJSON += (curr_per + '},')

            outJSON = outJSON[:-1]
            outJSON += ']},'
        outJSON = outJSON[:-1]

#        curr_ano = json.dumps(curr_ano.to_dict(orient='records'), sort_keys=True, default=str)
#                curr_ano = curr_ano[1:-2]
#
#                outJSON += curr_ano + ',"FailedVETs":'
#                curr_per = VETs[(VETs['step'] == row_step) & (VETs['questionno'] == ano_row['questionno'])]
#                curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
#                outJSON += (curr_per + '},')
#
#            outJSON = outJSON[:-1]
#            outJSON += ']},'
#        outJSON = outJSON[:-1]

        outJSON += '],"QueryTasks":[ '
        for index, tas_row in table_list['query_task'].iterrows():
            curr_tas = table_list['query_task'][(table_list['query_task']['query_reference'] ==
                                                 tas_row['query_reference']) & (
                                                        table_list['query_task']['task_sequence_number'] == tas_row[
                                                    'task_sequence_number'])]
            if curr_tas.empty:
                continue
            curr_tas = json.dumps(curr_tas.to_dict(orient='records'), sort_keys=True, default=str)
            curr_tas = curr_tas[1:-2]
            outJSON += curr_tas
            outJSON = outJSON + ',"QueryTaskUpdates":'
            if not table_list['query_task_update'].empty:
                curr_up = table_list['query_task_update'][(table_list['query_task_update']['query_reference']
                                                           == tas_row['query_reference']) &
                                                          (table_list['query_task_update']['task_sequence_number'] ==
                                                           tas_row['task_sequence_number'])]

                curr_up = json.dumps(curr_up.to_dict(orient='records'), sort_keys=True, default=str)
            else:
                curr_up = "[]"
            outJSON += curr_up + '},'

        outJSON = outJSON[:-1]
        outJSON += ']},'

#        outJSON += '],"QueryTasks":[ '
#        for index, tas_row in query_tasks.iterrows():
#             curr_tas = query_tasks[(query_tasks['queryreference'] == tas_row['queryreference']) & (
#                     query_tasks['taskseqno'] == tas_row['taskseqno'])]
#             if curr_tas.empty:
#                 continue
#             curr_tas = json.dumps(curr_tas.to_dict(orient='records'), sort_keys=True, default=str)
#             curr_tas = curr_tas[1:-2]
#             outJSON += curr_tas
#             outJSON = outJSON + ',"QueryTaskUpdates":'
#             curr_up = query_task_updates[(query_task_updates['queryreference'] == tas_row['queryreference']) & (
#                     query_task_updates['taskseqno'] == tas_row['taskseqno'])]
#             curr_up = json.dumps(curr_up.to_dict(orient='records'), sort_keys=True, default=str)
#             outJSON += curr_up + '},'
#         outJSON = outJSON[:-1]
#         outJSON += ']},'

    try:
        session.close()
    except:
        return json.loads('{"queryreference":"session To Database Closed Badly."}')

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
