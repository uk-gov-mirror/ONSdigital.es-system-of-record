import alchemy_mock
import unittest
import json
import unittest.mock as mock
import sqlalchemy.exc as exc
import sys
import os
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import create_query as create_query
import sqlalchemy as db
from alchemy_mock.mocking import AlchemyMagicMock
class test_create_query(unittest.TestCase):

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.insert")
    @mock.patch("create_query.alchemy_functions")
    def test_lambda_handler_happy_path(self,mock_create_engine,mock_insert,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                mock_insert.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                mock_alchemy_funks.table_model.return_value = "moo"
                mock_alchemy_funks.update.return_value = "baa"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==201)
                assert ("successfully" in x['body']['query_type'])

    def test_environment_variable_exception(self):
        x = create_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 500)
        assert ("Configuration error" in x['body'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    create_query.os.environ, {"Database_Location": "sweden"}
            ):
                test_data.pop('query_type')
                x = create_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 500)
        assert ("Invalid" in str(x['body']))

    @mock.patch("create_query.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    create_query.os.environ, {"Database_Location": "sweden"}
            ):

                mock_create_engine.side_effect = db.exc.OperationalError("Misser Mike, ee say no","","")

                x = create_query.lambda_handler(test_data, '')
        assert (x["statusCode"] == 500)
        print(x['body'])
        assert ("Failed To Connect To Database" in str(x['body']['contributor_name']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.insert")
    @mock.patch("create_query.alchemy_functions")
    def test_query_table_insert_fail(self,mock_create_engine,mock_insert,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                mock_insert.side_effect = db.exc.OperationalError("Misser Mike, ee say no","","")
                mock_alchemy_funks.table_model.return_value = "moo"
                mock_alchemy_funks.update.return_value = "baa"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into query" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.alchemy_functions")
    def test_step_exception_insert_fail(self,mock_create_engine,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                with mock.patch("create_query.insert") as mock_insert :
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                    mock_insert.side_effect=[mock_therest,db.exc.OperationalError("Misser Mike, ee say no","","")]
                    mock_alchemy_funks.table_model.return_value = "moo"
                    mock_alchemy_funks.update.return_value = "baa"
                    x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into Step_Exception" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.alchemy_functions")
    def test_question_anomaly_insert_fail(self,mock_create_engine,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                with mock.patch("create_query.insert") as mock_insert :
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                    mock_insert.side_effect=[mock_therest,mock_therest,db.exc.OperationalError("Misser Mike, ee say no","","")]
                    mock_alchemy_funks.table_model.return_value = "moo"
                    mock_alchemy_funks.update.return_value = "baa"
                    x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into Question_Anomaly" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.alchemy_functions")
    def test_failed_vet_insert_fail(self,mock_create_engine,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                with mock.patch("create_query.insert") as mock_insert :
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                    mock_insert.side_effect=[mock_therest,
                                             mock_therest,
                                             mock_therest, db.exc.OperationalError("Misser Mike, ee say no","","")]
                    mock_alchemy_funks.table_model.return_value = "moo"
                    mock_alchemy_funks.update.return_value = "baa"
                    x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into failed_VET" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.alchemy_functions")
    def test_query_task_insert_fail(self,mock_create_engine,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                with mock.patch("create_query.insert") as mock_insert :
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"

                    #not sure why i need so many mock_therest, but it wont get there without it
                    mock_insert.side_effect=[mock_therest,mock_therest,
                                             mock_therest, mock_therest,
                                             mock_therest, mock_therest,
                                             mock_therest,
                                             db.exc.OperationalError("Misser Mike, ee say no","","")]
                    mock_alchemy_funks.table_model.return_value = "moo"
                    mock_alchemy_funks.update.return_value = "baa"
                    x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into Query_Task" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.alchemy_functions")
    def test_query_task_update_insert_fail(self,mock_create_engine,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "potatoes"
                mock_session.close.return_value = "cheese"
                with mock.patch("create_query.insert") as mock_insert :
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"

                    #not sure why i need so many mock_therest, but it wont get there without it
                    mock_insert.side_effect=[mock_therest,mock_therest,
                                             mock_therest, mock_therest,
                                             mock_therest, mock_therest,
                                             mock_therest, mock_therest,
                                             db.exc.OperationalError("Misser Mike, ee say no","","")]
                    mock_alchemy_funks.table_model.return_value = "moo"
                    mock_alchemy_funks.update.return_value = "baa"
                    x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed to insert into Query_Task" in x['body']['query_type'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.insert")
    @mock.patch("create_query.alchemy_functions")
    def test_commit_fail(self,mock_create_engine,mock_insert,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect = db.exc.OperationalError("Misser Mike, ee say no","","")
                mock_session.close.return_value = "cheese"
                mock_insert.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                mock_alchemy_funks.table_model.return_value = "moo"
                mock_alchemy_funks.update.return_value = "baa"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Failed To Commit Changes" in x['body']['UpdateData'])

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.insert")
    @mock.patch("create_query.alchemy_functions")
    def test_close_fail(self,mock_create_engine,mock_insert,mock_alchemy_funks):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location":"sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("create_query.db.create_engine") as bob:
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "Sherpoopie"
                mock_session.close.side_effect = db.exc.OperationalError("Misser Mike, ee say no","","")
                mock_insert.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                mock_alchemy_funks.table_model.return_value = "moo"
                mock_alchemy_funks.update.return_value = "baa"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"]==500)
                assert ("Database Session Closed Badly" in x['body']['contributor_name'])