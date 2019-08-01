import sqlalchemy as db
import pandas as pd


def table_model(engine, metadata, table_name):
    table = db.Table(table_name, metadata, schema='es_db_test', autoload=True, autoload_with=engine)
    return table


def select(select_sql, session):
    table_data = session.execute(select_sql).fetchall()
    table_dataframe = pd.DataFrame(table_data)
    if not table_dataframe.empty:
        table_dataframe.columns = table_data[0].keys()
    return table_dataframe
