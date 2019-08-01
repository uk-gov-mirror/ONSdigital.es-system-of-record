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

#    try:
    table_list = {'query_type': None,
                  'vet': None,
                  'survey': None,
                  'gor_regions': None,
                  'ssr_old_regions': None}

    for current_table in table_list:
        table_model = alchemy_functions.table_model(engine, metadata, current_table)

        statement = db.select([table_model])

        table_data = alchemy_functions.select(statement, session)
        table_list[current_table] = table_data


#        query_type_table = db.Table('query_type', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
#        vet_table = db.Table('vet', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
#        survey_table = db.Table('survey', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
#        gor_region_table = db.Table('gor_regions', metadata, schema='es_db_test', autoload=True, autoload_with=engine)
#        ssr_region_table = db.Table('ssr_old_regions', metadata, schema='es_db_test', autoload=True, autoload_with=engine)

#        query_type_statement = db.select([query_type_table])
#        vet_statement = db.select([vet_table])
#        survey_statement = db.select([survey_table])
#        gor_region_statement = db.select([gor_region_table])
#        ssr_region_statement = db.select([ssr_region_table])
#
#        query_type_return = connection.execute(query_type_statement).fetchall()
#        vet_return = connection.execute(vet_statement).fetchall()
#        survey_return = connection.execute(survey_statement).fetchall()
#        gor_region_return = connection.execute(gor_region_statement).fetchall()
#        ssr_region_return = connection.execute(ssr_region_statement).fetchall()

#    except:
#        return json.loads('{"QueryTypes":"Failed To Retrieve Data."}')

    try:
        session.close()
    except:
        return json.loads('{"QueryTypes":"Connection To Database Closed Badly."}')

    outJSON = '{"QueryTypes":'
    outJSON += json.dumps(table_list["query_type"].to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"VETsCodes":'
    outJSON += json.dumps(table_list["vet"].to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"Surveys":'
    outJSON += json.dumps(table_list["survey"].to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += ',"GorRegions":'
    outJSON += json.dumps(table_list["gor_regions"].to_dict(orient='records'), sort_keys=True, default=str)
    outJSON += '}'


    return json.loads(outJSON)


x = lambda_handler('', '')
print(x)
