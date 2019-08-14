import json
import os
import sqlalchemy as db
import alchemy_functions
from sqlalchemy.orm import Session


def lambda_handler(event, context):
    database = os.environ['Database_Location']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()

    except:
        return json.loads('{"QueryTypes":"Failed To Connect To Database."}')

    try:
        table_model = alchemy_functions.table_model(engine, metadata, 'survey_period')

        statement = db.update(table_model).\
            values(active_period=event['active_period'],
                   number_of_responses=event['number_of_responses'],
                   number_cleared=event['number_cleared'],
                   number_cleared_first_time=event['number_cleared_first_time'],
                   sample_size=event['sample_size']).\
            where(db.and_(table_model.columns.survey_period ==
                  event['survey_period'],
                  table_model.columns.survey_code ==
                  event['survey_code']))

        alchemy_functions.update(statement, session)

    except:
        return json.loads('{"SurveyPeriod":"Failed To Update Survey_Period."}')

    try:
        session.commit()
    except:
        return json.loads('{"SurveyPeriod":"Failed To Commit Changes To The Database."}')

    try:
        session.close()
    except:
        return json.loads('{"SurveyPeriod":"Connection To Database Closed Badly."}')

    return json.loads('{"SurveyPeriod":"Successfully Updated The Table."}')


x = lambda_handler({"active_period": True, "number_of_responses": 2, "number_cleared": 2,
                    "number_cleared_first_time": 1, "sample_size": 2, "survey_period": "201712",
                    "survey_code": "066"}, '')
print(x)
