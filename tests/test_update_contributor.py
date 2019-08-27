import alchemy_mock
import unittest
import json
import unittest.mock as mock
import sys
import os
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))

import update_contributor as update_contributor
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock


class TestUpdateContributor(unittest.TestCase):
    @mock.patch("update_contributor.db.create_engine")
    @mock.patch("update_contributor.db.update")
    @mock.patch("update_contributor.alchemy_functions")
    @mock.patch("update_contributor.io_validation.ContributorUpdate")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_insert, mock_alchemy_funks, mock_marsh):
        with mock.patch.dict(
            update_contributor.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            ##mock connecting to db
            #with mock.patch("update_contributor.db.create_engine") as bob:
            with mock.patch("update_contributor.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.return_value = "Happy!"
                mock_session.close.return_value = "Path!"
                mock_insert.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value = "bob"
                mock_alchemy_funks.table_model.return_value = "Happier!"
                mock_alchemy_funks.update.return_value = "Pathier!"

                x = update_contributor.lambda_handler(mock_marsh, "")
                mock_marsh.return_value = True

                assert(x["statusCode"] == 200)
                assert ("Successfully" in x['body']['ContributorData'])

    def test_environment_variable_exception(self):
        x = update_contributor.lambda_handler("JAMIE - FORMER SOFTWARE ENGINEER", '')

        assert (x["statusCode"] == 500)
        assert ("Should say something else" in x['body'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_contributor.os.environ, {"Database_Location": "sweden"}
            ):
                test_data.pop('query_type')
                x = update_contributor.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 500)
        assert ("Invalid" in str(x['body']))

    @mock.patch("update_contributor.db.create_engine")
    @mock.patch("update_contributor.io_validation.ContributorUpdate")
    def test_db_connection_exception(self, mock_create_engine, mock_marsh):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_contributor.os.environ, {"Database_Location": "sweden"}
            ):
                mock_marsh.return_value = True
                mock_create_engine.side_effect = sqlalchemy.exc.OperationalError("","","")
                x = update_contributor.lambda_handler({}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed to connect to the database" in str(x['body']['contributor_name']))
