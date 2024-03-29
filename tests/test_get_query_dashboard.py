import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_query_dashboard as get_query_dashboard  # noqa: 402


class TestGetQueryDashboard(unittest.TestCase):

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_query_dashboard.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_reference": [123456],
                                  "ru_reference": ["xxxxx"],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxxx"],
                                  "current_period": ["xxxxx"],
                                  "date_raised": ["2007-05-04"],
                                  "industry_group": ["xxxx"],
                                  "last_query_update": ["1999-12-16"],
                                  "query_active": [True],
                                  "query_description": ["xxxxx"],
                                  "query_status": ["xxxxxx"],
                                  "raised_by": ["xxxxxx"],
                                  "results_state": ["xxxx"],
                                  "query_type": ["Data Cleaning"],
                                  "query_type_description": ["xxxxxxxxxxx"],
                                  "general_specific_flag": [False],
                                  "target_resolution_date": ["2000-09-11"]}),
                    pd.DataFrame({"run_id": [1],
                                  "step": ["VETs"],
                                  "error_code": ["xxxxx"],
                                  "error_description": ["xxxxxxx"],
                                  "query_reference": [123456],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxx"],
                                  "ru_reference": ["xxxx"]}),
                    pd.DataFrame({"question_number": ["1"],
                                  "anomaly_description": ["xxxxx"],
                                  "step": ["VETs"],
                                  "run_id": [1],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxx"],
                                  "ru_reference": ["xxxx"]}),
                    pd.DataFrame({"failed_vet": [1],
                                  "vet_description": ["xxxx"],
                                  "step": ["VETs"],
                                  "question_number": ["1"],
                                  "run_id": [1],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxx"],
                                  "ru_reference": ["xxxx"]}),
                    pd.DataFrame({"query_reference": [123456],
                                  "task_sequence_number": [1],
                                  "next_planned_action": ["xxxxx"],
                                  "response_required_by": ["2007-11-28"],
                                  "task_description": ["xxxx"],
                                  "task_responsibility": ["xxxx"],
                                  "task_status": ["xxxxx"],
                                  "when_action_required": ["2006-01-18"]}),
                    pd.DataFrame({"query_reference": [123456],
                                  "task_sequence_number": [1],
                                  "last_updated": ["2002-06-11"],
                                  "task_update_description": ["xxxx"],
                                  "updated_by": ["xxxx"]})]

                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')

                assert(x["statusCode"] == 200)
                assert ("Queries" in x['body'])

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    def test_lambda_handler_select_fail(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_query_dashboard.os.environ, {"Database_Location":
                                             "MyPostgresDatase"}
        ):
            with mock.patch("get_query_dashboard.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_reference": [123456],
                                  "ru_reference": ["xxxxx"],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxxx"],
                                  "current_period": ["xxxxx"],
                                  "date_raised": ["2007-05-04"],
                                  "industry_group": ["xxxx"],
                                  "last_query_update": ["1999-12-16"],
                                  "query_active": [True],
                                  "query_description": ["xxxxx"],
                                  "query_status": ["xxxxxx"],
                                  "raised_by": ["xxxxxx"],
                                  "results_state": ["xxxx"],
                                  "query_type": ["Data Cleaning"],
                                  "query_type_description": ["xxxxxxxxxxx"],
                                  "general_specific_flag": [False],
                                  "target_resolution_date": ["2000-09-11"]}),
                    sqlalchemy.exc.OperationalError('', '', '')]
                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data: step" in x['body']['Error'])
            assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    def test_lambda_handler_select_fail_general(self, mock_create_engine,
                                                mock_select):

        with mock.patch.dict(
            get_query_dashboard.os.environ, {"Database_Location":
                                             "MyPostgresDatase"}
        ):
            with mock.patch("get_query_dashboard.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_reference": [123456],
                                  "ru_reference": ["xxxxx"],
                                  "survey_code": ["xxxx"],
                                  "survey_period": ["xxxxx"],
                                  "current_period": ["xxxxx"],
                                  "date_raised": ["2007-05-04"],
                                  "industry_group": ["xxxx"],
                                  "last_query_update": ["1999-12-16"],
                                  "query_active": [True],
                                  "query_description": ["xxxxx"],
                                  "query_status": ["xxxxxx"],
                                  "raised_by": ["xxxxxx"],
                                  "results_state": ["xxxx"],
                                  "query_type": ["Data Cleaning"],
                                  "query_type_description": ["xxxxxxxxxxx"],
                                  "general_specific_flag": [False],
                                  "target_resolution_date": ["2000-09-11"]}),
                    Exception("Bad Me")]
                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data: step" in x['body']['Error'])
            assert ("General Error" in x['body']['Error'])

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    @mock.patch("get_query_dashboard.alchemy_functions")
    def test_lambda_handler_query_fail(self, mock_create_engine, mock_select,
                                       mock_alchemy_functions):

        with mock.patch.dict(
            get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_query_dashboard.lambda_handler({"query_reference": 0}, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data:" in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    @mock.patch("get_query_dashboard.alchemy_functions")
    def test_lambda_handler_query_fail_general(self, mock_create_engine,
                                               mock_select,
                                               mock_alchemy_functions):
        with mock.patch.dict(
                get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect = Exception("Bad Me")
            x = get_query_dashboard.lambda_handler({"query_reference": 0}, '')

            assert (x["statusCode"] == 500)
            assert ("Failed To Retrieve Data:" in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    def test_lambda_handler_output_error(self, mock_create_engine,
                                         mock_select):

        with mock.patch.dict(
            get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_query_dashboard.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_reference": [0],
                                  "ru_reference": ["1"],
                                  "survey_period": ["2"],
                                  "survey_code": ["3"]}),
                    pd.DataFrame({}),
                    pd.DataFrame({}),
                    pd.DataFrame({}),
                    pd.DataFrame({}),
                    pd.DataFrame({})]

                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')
                assert(x["statusCode"] == 500)
                assert ("Missing" in str(x['body']))

    def test_environment_variable_exception(self):
        x = get_query_dashboard.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    def test_input_exception(self):
        with mock.patch.dict(
                get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            x = get_query_dashboard.lambda_handler('', '')

        assert (x["statusCode"] == 400)
        assert ("Invalid" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    def test_db_connection_exception_driver(self, mock_create_engine):

        with mock.patch.dict(
                get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.NoSuchModuleError('', '', '')
            x = get_query_dashboard.lambda_handler({"query_reference": 0}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Driver Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_query_dashboard.lambda_handler({"query_reference": 0}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine):

        with mock.patch.dict(
                get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = Exception("Bad Me")
            x = get_query_dashboard.lambda_handler({"query_reference": 0}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    @mock.patch("get_query_dashboard.alchemy_functions")
    def test_lambda_handler_connection_close(self, mock_create_engine,
                                             mock_select,
                                             mock_alchemy_functions):

        with mock.patch.dict(
            get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_query_dashboard.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    sqlalchemy.exc.OperationalError('', '', '')
                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("get_query_dashboard.db.create_engine")
    @mock.patch("get_query_dashboard.Session.query")
    @mock.patch("get_query_dashboard.alchemy_functions")
    def test_lambda_handler_connection_close_general(self, mock_create_engine,
                                                     mock_select,
                                                     mock_alchemy_functions):

        with mock.patch.dict(
            get_query_dashboard.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_query_dashboard.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = Exception("Bad Me")
                x = get_query_dashboard.lambda_handler({"query_reference": 0},
                                                       '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))
