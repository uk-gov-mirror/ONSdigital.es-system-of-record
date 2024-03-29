import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_survey_periods as get_survey_periods  # noqa: 402


class TestGetSurveyPeriods(unittest.TestCase):

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_survey_periods.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"survey_period": ["Year"],
                                  "survey_code": ["Sand"],
                                  "active_period": [True],
                                  "number_of_responses": [0],
                                  "number_cleared": [0],
                                  "number_cleared_first_time": [0],
                                  "sample_size": [0]})]

                x = get_survey_periods.lambda_handler({"survey_period": "",
                                                       "survey_code": ""}, '')

                assert(x["statusCode"] == 200)
                assert ("survey_period" in x['body'][0])

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    @mock.patch("get_survey_periods.alchemy_functions")
    def test_lambda_handler_select_fail(self, mock_create_engine, mock_select,
                                        mock_alchemy_functions):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_survey_periods.lambda_handler({"survey_period": "",
                                                   "survey_code": ""}, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data:" in x['body']['Error'])
            assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    @mock.patch("get_survey_periods.alchemy_functions")
    def test_lambda_handler_select_fail_general(self, mock_create_engine,
                                                mock_select,
                                                mock_alchemy_functions):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect = Exception("Bad Me")
            x = get_survey_periods.lambda_handler({"survey_period": "",
                                                   "survey_code": ""}, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data:" in x['body']['Error'])
            assert ("General Error" in x['body']['Error'])

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    def test_lambda_handler_output_error(self, mock_create_engine,
                                         mock_select):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_survey_periods.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"Me": ["My"]})]

                x = get_survey_periods.lambda_handler({"survey_period": "",
                                                       "survey_code": ""}, '')
                assert(x["statusCode"] == 500)
                assert ("Missing" in str(x['body']))

    def test_environment_variable_exception(self):
        x = get_survey_periods.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    def test_input_exception(self):
        with mock.patch.dict(
                get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            x = get_survey_periods.lambda_handler('', '')

        assert (x["statusCode"] == 400)
        assert ("Invalid" in str(x['body']['Error']))

    @mock.patch("get_survey_periods.db.create_engine")
    def test_db_connection_exception_driver(self, mock_create_engine):

        with mock.patch.dict(
                get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.NoSuchModuleError('', '', '')
            x = get_survey_periods.lambda_handler({"survey_period": "",
                                                   "survey_code": ""}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Driver Error" in str(x['body']['Error']))

    @mock.patch("get_survey_periods.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_survey_periods.lambda_handler({"survey_period": "",
                                                   "survey_code": ""}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_survey_periods.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine):

        with mock.patch.dict(
                get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = Exception("Bad Me")
            x = get_survey_periods.lambda_handler({"survey_period": "",
                                                   "survey_code": ""}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("General Error" in x['body']['Error'])

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    @mock.patch("get_survey_periods.alchemy_functions")
    def test_lambda_handler_connection_close(self, mock_create_engine,
                                             mock_select,
                                             mock_alchemy_functions):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_survey_periods.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    sqlalchemy.exc.OperationalError('', '', '')
                x = get_survey_periods.lambda_handler({"survey_period": "",
                                                       "survey_code": ""}, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_survey_periods.db.create_engine")
    @mock.patch("get_survey_periods.Session.query")
    @mock.patch("get_survey_periods.alchemy_functions")
    def test_lambda_handler_connection_close_general(self, mock_create_engine,
                                                     mock_select,
                                                     mock_alchemy_functions):

        with mock.patch.dict(
            get_survey_periods.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_survey_periods.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = Exception("Bad Me")
                x = get_survey_periods.lambda_handler({"survey_period": "",
                                                       "survey_code": ""}, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("General Error" in x['body']['Error'])
