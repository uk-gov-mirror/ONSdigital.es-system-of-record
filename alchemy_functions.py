import pandas as pd

import db_model as db


def table_model(engine, metadata, table_name):
    """Retrieve Table model from a file.
    Parameters:
      engine (Engine):The prepared bind to allow a database session.
      metadata (MetaData):Basic Metadata object.
      table_name (String):The name of the table.
    Returns:
      table (Table):Object describing the table, its columns, relationships, datatypes etc.
    """

    table = db.Table(table_name, metadata, schema='es_db_test', autoload=True, autoload_with=engine)
    return table


def select(select_sql, session):
    """Fetches all rows from a table based on a prebuilt select statement and returns them in a Dataframe.
    Data comes back as a ResultProxy, it is then turned into a database with default column headers.
    Assuming the dataframe isn't empty the correct headers are then added into the dataframe.
    Parameters:
      select_sql (Expression):SQL expression to be used.
      session (Session):An open session of the database you are searching.
    Returns:
      table_dataframe (Dataframe):Results dataframe with table column headers.
    """
    table_data = session.execute(select_sql).fetchall()
    table_dataframe = pd.DataFrame(table_data)
    if not table_dataframe.empty:
        table_dataframe.columns = table_data[0].keys()
    return table_dataframe


def update(update_sql, session):
    """Updates or Inserts all rows into a table based on a prebuilt statement.
    Parameters:
      update_sql (Expression):SQL expression to be used.
      session (Session):An open session of the database you are searching.
    Returns:
      table_data (): ... Do we need this?
    """

    table_data = session.execute(update_sql)
    return table_data
