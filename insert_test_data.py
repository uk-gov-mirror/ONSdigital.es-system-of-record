import json
import os
import logging

import sqlalchemy as db
from sqlalchemy.orm import Session

import alchemy_functions as af

logger = logging.getLogger("insert_test_data")


# If this fails to complete, you need to drop and recreate
# the database to fix the query tables serialisation.
def main():
    """Load data from a file and attempt to insert data into the database.
    Data is loaded in from a json file (data.json).
    For every table in the list call the insert function
    and pass in data to attemp insert into specified database.
    """

    database = os.environ['Database_Location']

    try:
        input_file = open('data.json').read()
        input_json = json.loads(input_file)
    except IOError as exc:
        logger.error("Failed to load from file: {}".format(exc))
        return
    except Exception as exc:
        logger.error("Failed to load from file: {}".format(exc))
        return

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to connect to the database: {}"
                     .format(exc))
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
        logger.info("inserting data for table: " + str(current))
        insert(engine, session, metadata, current, input_json[current])

    try:
        session.commit()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to commit changes to the database: {}"
                     .format(exc))

        return json.loads(
            '{"QueryTypes":"Failed To Commit Changes To The Database."}')

    try:
        session.close()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to close connection to the database: {}"
                     .format(exc))

        return json.loads(
            '{"QueryTypes":"Connection To Database Closed Badly."}')
    logger.info("Succesfully completed insert")


def insert(engine, session, metadata, table_name, table_data):
    """If data is not empty, retrieve table model and insert into it.
    Parameters:
      engine (Engine):The prepared bind to allow a database session.
      session (Session):A database session created from an engine.
      metadata (MetaData):Basic Metadata object.
      table_name (String):The name of the table.
      table_data (List[Dict]):Data to be inserted.
    """

    if table_data:
        table_model = af.table_model(engine, metadata, table_name)
        insert_sql = db.insert(table_model)
        session.execute(insert_sql, table_data)


main()
