import json
import sys
import os

import unittest
import unittest.mock as mock
import sqlalchemy as db
import pandas as pd
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import create_query as create_query  # noqa: 402


class TestCreateQuery(unittest.TestCase):

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    @mock.patch("create_query.Session.refresh")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_insert,
                                       mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.query") as mock_return:
                mock_return.return_value.all.return_value = pd.DataFrame(
                    {0: [1]})

                x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 201)
            assert ("Successfully" in x['body']['Success'])

    def test_environment_variable_exception(self):
        x = create_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    create_query.os.environ, {"Database_Location": "sweden"}
            ):
                test_data.pop('query_type')
                x = create_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 400)
        assert ("Invalid" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    create_query.os.environ, {"Database_Location": "sweden"}
            ):

                mock_create_engine.side_effect =\
                    db.exc.OperationalError("", "", "")

                x = create_query.lambda_handler(test_data, '')
        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    create_query.os.environ, {"Database_Location": "sweden"}
            ):

                mock_create_engine.side_effect = Exception("Bad Me")

                x = create_query.lambda_handler(test_data, '')
        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    @mock.patch("create_query.Session.refresh")
    def test_query_table_insert_fail(self, mock_create_engine, mock_insert,
                                     mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            mock_insert.side_effect =\
                db.exc.OperationalError("", "", "")

            x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: query" in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    @mock.patch("create_query.Session.refresh")
    def test_query_table_insert_fail_general(self, mock_create_engine,
                                             mock_insert, mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            mock_insert.side_effect = Exception("Bad Me")

            x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: query" in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_step_exception_insert_fail(self, mock_create_engine,
                                        mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest,
                        db.exc.OperationalError("", "", "")]

                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: step_exception"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_step_exception_insert_fail_general(self, mock_create_engine,
                                                mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest,
                        Exception("Bad Me")]

                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: step_exception"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_question_anomaly_insert_fail(self, mock_create_engine,
                                          mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        db.exc.OperationalError("", "", "")]
                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: question_anomaly"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_question_anomaly_insert_fail_general(self, mock_create_engine,
                                                  mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        Exception("Bad Me")]
                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: question_anomaly"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_failed_vet_insert_fail(self, mock_create_engine, mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest,
                        mock_therest,
                        mock_therest,
                        db.exc.OperationalError("", "", "")]
                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: failed_vet" in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_failed_vet_insert_fail_general(self, mock_create_engine,
                                            mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()
                    mock_insert.side_effect = [
                        mock_therest,
                        mock_therest,
                        mock_therest,
                        Exception("Bad Me")]
                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: failed_vet" in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_query_task_insert_fail_general(self, mock_create_engine,
                                            mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})

                    mock_therest = mock.Mock()

                    #  So many 'mock_therest's are needed
                    #  due to the nature of the test data used.
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest,
                        Exception("Bad Me")]

                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: query_task"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_query_task_update_insert_fail(self, mock_create_engine,
                                           mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()

                    #  So many 'mock_therest's are needed
                    #  due to the nature of the test data used.
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        db.exc.OperationalError("", "", "")]

                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: query_task_update"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.refresh")
    def test_query_task_update_insert_fail_general(self, mock_create_engine,
                                                   mock_refresh):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("create_query.Session.add") as mock_insert:
                with mock.patch("create_query.Session.query") as mock_return:
                    mock_return.return_value.all.return_value = pd.DataFrame(
                        {0: [1]})
                    mock_therest = mock.Mock()

                    #  So many 'mock_therest's are needed
                    #  due to the nature of the test data used.
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        Exception("Bad Me")]

                    x = create_query.lambda_handler(test_data, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Insert Data: query_task_update"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    def test_commit_fail(self, mock_create_engine, mock_insert):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_insert.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"
                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"] == 500)
                assert ("Failed To Commit" in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    def test_commit_fail_general(self, mock_create_engine, mock_insert):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect = Exception("Bad Me")
                mock_insert.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"
                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"] == 500)
                assert ("Failed To Commit" in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    def test_close_fail(self, mock_create_engine, mock_insert):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_insert.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("create_query.db.create_engine")
    @mock.patch("create_query.Session.add")
    def test_close_fail_general(self, mock_create_engine, mock_insert):
        with mock.patch.dict(
            create_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("create_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = Exception("Bad Me")
                mock_insert.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = create_query.lambda_handler(test_data, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))
