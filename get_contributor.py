import json
import os
import logging

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import io_validation
from sqlalchemy.exc import DatabaseError

logger = logging.getLogger("get_contributor")


def lambda_handler(event, context):
    """Collects data on a passed in Reference from six tables and combines them into a single Json.
    Parameters:
      event (Dict):A single key value pair of ru_reference and a string number.
    Returns:
      out_json (Json):Nested Json responce of the six tables data.
    """
    database = os.environ['Database_Location']

    try:
        io_validation.ContributorSearch(strict=True).load(event)
    except ValidationError as err:
        return err.messages

    ref = event['ru_reference']

    try:
        logger.info("Connecting to the database")
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to connect to the database: {}".format(exc))
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Failed To Connect To Database."}')

    try:
        table_list = {'contributor': None,
                      'survey_enrolment': None,
                      'survey_contact': None,
                      'contributor_survey_period': None}

        for current_table in table_list:
            logger.info("Fetching data from table: {}".format(current_table))
            table_model = alchemy_functions.table_model(engine, metadata, current_table)

            statement = db.select([table_model]).where(table_model.columns.ru_reference == ref)

            if current_table == "survey_contact":
                other_model = alchemy_functions.table_model(engine, metadata, "contact")
                statement = db.select([table_model.columns.ru_reference, table_model.columns.survey_code,
                                       table_model.columns.effective_start_date,
                                       table_model.columns.effective_end_date, other_model])\
                    .where(db.and_(table_model.columns.contact_reference == other_model.columns.contact_reference,
                                   table_model.columns.ru_reference == ref))
            elif current_table == "contributor_survey_period":
                other_model = alchemy_functions.table_model(engine, metadata, "survey_period")
                statement = db.select([table_model, other_model.columns.active_period, other_model.columns.sample_size,
                                       other_model.columns.number_cleared, other_model.columns.number_cleared_first_time,
                                       other_model.columns.number_of_responses])\
                    .where(db.and_(table_model.columns.survey_code == other_model.columns.survey_code,
                                   table_model.columns.survey_period == other_model.columns.survey_period,
                                   table_model.columns.ru_reference == ref))

            table_data = alchemy_functions.select(statement, session)
            table_list[current_table] = table_data

    except Exception as exc:
        logger.error("Error selecting data from table: {}".format(exc))
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Failed To Retrieve Data."}')

    try:
        session.close()
    except db.exc.DatabaseError as exc:
        logger.error("Error: Failed to close the database session: {}".format(exc))
        return json.loads('{"ru_reference":"' + ref + '","contributor_name":"Database Session Closed Badly."}')

    out_json = json.dumps(table_list["contributor"].to_dict(orient='records'), sort_keys=True, default=str)
    out_json = out_json[1:-2]
    out_json += ',"Surveys":[ '

    for index, row in table_list['survey_enrolment'].iterrows():
        curr_row = table_list['survey_enrolment'][(table_list['survey_enrolment']['survey_code']
                                                   == row['survey_code'])]
        curr_row = json.dumps(curr_row.to_dict(orient='records'), sort_keys=True, default=str)
        curr_row = curr_row[2:-2]

        out_json = out_json + "{" + curr_row + ',"Contacts":'
        curr_con = table_list['survey_contact'][(table_list['survey_contact']['survey_code']
                                                 == row['survey_code'])]
        curr_con = json.dumps(curr_con.to_dict(orient='records'), sort_keys=True, default=str)
        out_json += curr_con

        out_json = out_json + ',"Periods":'
        curr_per = table_list['contributor_survey_period'][(table_list['contributor_survey_period']['survey_code']
                                                            == row['survey_code'])]
        curr_per = json.dumps(curr_per.to_dict(orient='records'), sort_keys=True, default=str)
        out_json += curr_per + '},'

    out_json = out_json[:-1]
    out_json += ']}'

    try:
        io_validation.Contributor(strict=True).loads(out_json)
    except ValidationError as err:
        return err.messages

    return json.loads(out_json)


x = lambda_handler({"ru_reference": "77700000006"}, '')
print(x)
