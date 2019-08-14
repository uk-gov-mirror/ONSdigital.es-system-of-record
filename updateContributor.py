import json
import os
from datetime import datetime

import sqlalchemy as db
from marshmallow import ValidationError
from sqlalchemy.orm import Session

import alchemy_functions
import ioValidation

def lambda_handler(event, context):

    # try:
    #     ioValidation.ContributorUpdate(strict=True).load("test_data.txt")
    # except ValidationError as err:
    #     return err.messages

    database = os.environ['Database_Location']

    try:
        engine = db.create_engine(database)
        session = Session(engine)
        metadata = db.MetaData()

    except:
        return json.loads('{"ContributorData":"Failed To Connect To Database."}')

    current_time = str(datetime.now())

    try:
        table_model = alchemy_functions.table_model(engine, metadata, 'contributor_survey_period')
        statement = db.update(table_model).\
            values(additional_comments=event['additional_comments'],
                   contributor_comments=event['contributor_comments'],
                   last_updated=current_time).\
            where(db.and_(table_model.columns.survey_period == event['survey_period'],
                          table_model.columns.survey_code == event['survey_code'],
                          table_model.columns.ru_reference == event['ru_reference']))

        alchemy_functions.update(statement, session)

    except:
        return json.loads('{"ContributorData":"Failed To Update The Database."}')

    try:
        session.commit()
    except:
        return json.loads('{"ContributorData":"Failed To Commit Changes To The Database."}')

    try:
        session.close()
    except:
        return json.loads('{"ContributorData":"Connection To Database Closed Badly."}')

    return json.loads('{"ContributorData":"Successfully Updated The Table."}')


x = lambda_handler({"additional_comments": "Hello",
                    "contributor_comments": "Contributor says hello!",
                    "survey_period": "201712",
                    "survey_code": "066",
                    "ru_reference": "77700000001"}, "")
print(x)
