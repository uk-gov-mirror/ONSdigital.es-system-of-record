import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_all_reference_data as get_all_reference_data  # noqa: 402


class TestGetAllReferenceData(unittest.TestCase):

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_all_reference_data.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_type": ["Register"],
                                  "query_type_description": [
                        "Queries raised to manage register change requests"]}),
                    pd.DataFrame({"vet_code": [1],
                                  "vet_description": ["Value Present"]}),
                    pd.DataFrame({"survey_code": ["066"],
                                  "survey_name": [
                                      "Sand & Gravel {Land Won}"]}),
                    pd.DataFrame({"gor_reference": [1], "idbr_region": ["AA"],
                                  "region_name": ["North East"]}),
                    {}]

                x = get_all_reference_data.lambda_handler('', '')

                assert(x["statusCode"] == 200)
                assert ("QueryTypes" in x['body'])

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_select_fail(self, mock_create_engine, mock_select,
                                        mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_all_reference_data.lambda_handler('', '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve" in x['body']['Error'])
            assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_select_fail_general(self, mock_create_engine,
                                                mock_select,
                                                mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect = Exception("Bad Me")
            x = get_all_reference_data.lambda_handler('', '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve" in x['body']['Error'])
            assert ("General Error" in x['body']['Error'])

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    def test_lambda_handler_output_error(self, mock_create_engine,
                                         mock_select):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_all_reference_data.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"query_type": [1]}),
                    pd.DataFrame({"vet_code": ["W"]}),
                    pd.DataFrame({"survey_code": [66]}),
                    pd.DataFrame({"gor_reference": ["W"]}),
                    {}]

                x = get_all_reference_data.lambda_handler('', '')
                assert(x["statusCode"] == 500)
                assert ("Missing" in str(x['body']))

    def test_environment_variable_exception(self):
        x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    @mock.patch("get_all_reference_data.db.create_engine")
    def test_db_connection_exception_driver(self, mock_create_engine):

        with mock.patch.dict(
                get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.NoSuchModuleError('', '', '')
            x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Driver Error" in str(x['body']['Error']))

    @mock.patch("get_all_reference_data.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                sqlalchemy.exc.OperationalError('', '', '')
            x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("get_all_reference_data.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine):
        with mock.patch.dict(
                get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = Exception("Bad Me")
            x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_connection_close(self, mock_create_engine,
                                             mock_select,
                                             mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_all_reference_data.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    sqlalchemy.exc.OperationalError('', '', '')
                x = get_all_reference_data.lambda_handler('', '')

                assert(x["statusCode"] == 500)
                assert ("Closed Badly" in x['body']['Error'])
                assert ("Operational Error" in x['body']['Error'])

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.Session.query")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_connection_close_general(self, mock_create_engine,
                                                     mock_select,
                                                     mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_all_reference_data.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = Exception("Bad Me")
                x = get_all_reference_data.lambda_handler('', '')

                assert(x["statusCode"] == 500)
                assert ("Closed Badly" in x['body']['Error'])
                assert ("General Error" in x['body']['Error'])
