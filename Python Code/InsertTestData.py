import json
import sqlalchemy as db
from sqlalchemy.orm import Session
import os


# If this fails to complete, you need to drop and recreate the database to fix the query tables serialisation.
def main():
    database = os.environ['Database_Location']
    input_file = open('Data.json').read()
    input_json = json.loads(input_file)

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except:
        return json.loads('{"QueryTypes":"Failed To Connect To Database."}')

    table_list = ['ssr_old_regions',
                  'gor_regions',
                  'vet',
                  'query_type',
                  'survey',
                  'contributor',
                  'survey_enrolment',
                  'survey_period',
                  'contributor_survey_period',
                  'contact',
                  'survey_contact',
                  'query',
                  'query_task',
                  'query_task_update',
                  'step_exception',
                  'question_anomaly',
                  'failed_vet'
                  ]

    for current in table_list:
        print(current)
        insert(engine, session, metadata, current, input_json[current])
    session.commit()
    session.close()


def insert(engine, session, metadata, table_name, table_data):
    if table_data:
        table_model = db.Table(table_name, metadata, schema='es_db_test', autoload=True, autoload_with=engine)
        insert_sql = db.insert(table_model)
        session.execute(insert_sql, table_data)


main()
