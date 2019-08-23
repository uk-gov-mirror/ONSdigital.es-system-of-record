import json
import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
import alchemy_mock
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_all_reference_data as get_all_reference_data
import alchemy_functions




def try_me(*args):
    print(args)
    if "query_type" in args[0]:
        return pd.DataFrame({'query_type': ['Register'], 'query_type_description': ['Queries raised to manage register change requests']})
    else:
        return "A"


class test_get_all_reference_data(unittest.TestCase):

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.db.select")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select, mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_all_reference_data.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "Committed"
                mock_session.close.return_value = "Closed"
                mock_select.return_value.where.side_effect = "Give Me Stuff"
                mock_alchemy_functions.table_model.return_value = "I Am A Table"
                mock_alchemy_functions.select.return_value = [pd.DataFrame({"query_type": ["Register"], "query_type_description": ["Queries raised to manage register change requests"]}),
                                                      pd.DataFrame({"vet_code": [1], "vet_description": ["Value Present"]}),
                                                      pd.DataFrame({"survey_code": ["066"], "survey_name": ["Sand & Gravel {Land Won}"]}),
                                                      pd.DataFrame({"gor_reference": [1], "idbr_region": ["AA"], "region_name": ["North East"]}),
                                                      {}]

                x = get_all_reference_data.lambda_handler('', '')

                assert(x["statusCode"] == 201)
                assert ("QueryTypes" in x['body'][''])

    @mock.patch("get_all_reference_data.db.create_engine")
    @mock.patch("get_all_reference_data.db.select")
    @mock.patch("get_all_reference_data.alchemy_functions")
    def test_lambda_handler_sad_path(self, mock_create_engine, mock_select, mock_alchemy_functions):

        with mock.patch.dict(
            get_all_reference_data.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_all_reference_data.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_select.side_effect = sqlalchemy.exc.OperationalError
                x = get_all_reference_data.lambda_handler('', '')

                assert(x["statusCode"] == 500)
                assert ("Failed To Retrieve Data." in x['body']['Error'])

    def test_environment_variable_exception(self):
        x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error." in x['body']['Error'])

    @mock.patch("get_all_reference_data.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_all_reference_data.os.environ, {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = sqlalchemy.exc.OperationalError
            x = get_all_reference_data.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect To Database." in str(x['body']['Error']))
