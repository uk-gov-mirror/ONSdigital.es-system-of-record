import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_query as get_query


class test_get_query(unittest.TestCase):

    @mock.patch("get_query.db.create_engine")
    @mock.patch("get_query.db.select")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_query.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_query.alchemy_functions") as mock_alchemy_functions:
                mock_alchemy_functions.return_value.table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.select.side_effect = [pd.DataFrame(),
                                                             pd.DataFrame(),
                                                             pd.DataFrame(),
                                                             pd.DataFrame(),
                                                             pd.DataFrame()]

                x = get_query.lambda_handler({"query_reference": 0}, '')

                assert(x["statusCode"] == 200)
                assert ("query_reference" in x['body'])

    @mock.patch("get_query.db.create_engine")
    @mock.patch("get_query.db.select")
    @mock.patch("get_query.alchemy_functions")
    def test_lambda_handler_select_fail(self, mock_create_engine, mock_select, mock_alchemy_functions):

        with mock.patch.dict(
            get_query.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect = sqlalchemy.exc.OperationalError
            x = get_query.lambda_handler({"query_reference": 0}, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data." in x['body']['Error'])

    @mock.patch("get_query.db.create_engine")
    @mock.patch("get_query.db.select")
    def test_lambda_handler_output_error(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_query.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_query.alchemy_functions") as mock_alchemy_functions:
                mock_alchemy_functions.select.side_effect = [pd.DataFrame({"query_reference": [0], "ru_reference": ["1"], "survey_period": ["2"], "survey_code": ["3"]}),
                                                             pd.DataFrame({}),
                                                             pd.DataFrame({}),
                                                             pd.DataFrame({}),
                                                             pd.DataFrame({}),
                                                             pd.DataFrame({})]

                x = get_query.lambda_handler({"query_reference": 0}, '')
                print(x)
                assert(x["statusCode"] == 500)
                assert ("Missing" in str(x['body']))

    def test_environment_variable_exception(self):
        x = get_query.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error." in x['body']['Error'])

    @mock.patch("get_query.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_query.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = sqlalchemy.exc.OperationalError
            x = get_query.lambda_handler({"query_reference": 0}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect To Database." in str(x['body']['Error']))

    @mock.patch("get_query.db.create_engine")
    @mock.patch("get_query.db.select")
    @mock.patch("get_query.alchemy_functions")
    def test_lambda_handler_connection_close(self, mock_create_engine, mock_select, mock_alchemy_functions):

        with mock.patch.dict(
            get_query.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = sqlalchemy.exc.OperationalError
                x = get_query.lambda_handler({"query_reference": 0}, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
