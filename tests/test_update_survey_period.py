import unittest
import json
import unittest.mock as mock
import sys
import os
import sqlalchemy as db
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))  # noqa
import update_survey_period as update_survey_period


class TestUpdateSurveyPeriod(unittest.TestCase):
    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.db.update")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_update, mock_alchemy_funks):
        with mock.patch.dict(
            update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            mock_update.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value\
                = "bob"

            x = update_survey_period.lambda_handler({'active_period': True, 'number_of_responses': 2,
                                                     'number_cleared': 2, 'number_cleared_first_time': 1,
                                                     'sample_size': 2, 'survey_period': '201712', 'survey_code':
                                                         '066'}, '')

        assert(x["statusCode"] == 200)
        assert ("Successfully" in x['body']['Success'])

    def test_environment_variable_exception(self):
        x = update_survey_period.lambda_handler("JAMIE WAS HERE!! FORMER SOFTWARE ENGINEER", '')

        assert (x["statusCode"] == 500)
        assert ("Should say something else" in x['body'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_survey_period.os.environ, {"Database_Location": "Djibouti"}
            ):
                test_data.pop('query_type')
                x = update_survey_period.lambda_handler("MIKE LOVES BOUNTY BARS!!", '')

        assert (x["statusCode"] == 500)
        assert ("Configuration error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):
        with mock.patch.dict(
                update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            mock_create_engine.side_effect = db.exc.OperationalError("Side effect in full effect", "", "")

            x = update_survey_period.lambda_handler(
                {'active_period': True, 'number_of_responses': 2, 'number_cleared': 2,
                 'number_cleared_first_time': 1, 'sample_size': 2, 'survey_period': '201712',
                 'survey_code': '066'}, '')

            print(x)

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect To Database." in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_update_survey_period_fail(self, mock_create_engine, mock_alchemy_funcs):
        with mock.patch.dict(
            update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.db.update") as mock_update:
                mock_update.side_effect = db.exc.OperationalError("Side effect in full effect", "", "")

                x = update_survey_period.lambda_handler({'active_period': True, 'number_of_responses': 2,
                                                         'number_cleared': 2, 'number_cleared_first_time': 1,
                                                         'sample_size': 2, 'survey_period': '201712', 'survey_code':
                                                             '066'}, '')

        assert(x["statusCode"] == 500)
        assert ("Failed To Update Survey_Period." in x['body']['Error'])

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.db.update")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_commit_fail(self, mock_create_engine, mock_update, mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect = db.exc.OperationalError("Side effect in full effect", "", "")
                mock_update.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.return_value\
                    = "bob"

                x = update_survey_period.lambda_handler({'active_period': True, 'number_of_responses': 2,
                                                         'number_cleared': 2, 'number_cleared_first_time': 1,
                                                         'sample_size': 2, 'survey_period': '201712', 'survey_code':
                                                             '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Commit Changes" in x['body']['Error'])

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.db.update")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_close_fail(self, mock_create_engine, mock_update, mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = db.exc.OperationalError("Side effect in full effect", "", "")
                mock_update.return_value.values.return_value.returning.return_value.on_conflict_do_nothing.\
                    return_value = "bob"

                x = update_survey_period.lambda_handler({'active_period': True, 'number_of_responses': 2,
                                                         'number_cleared': 2, 'number_cleared_first_time': 1,
                                                         'sample_size': 2, 'survey_period': '201712', 'survey_code':
                                                             '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Connection To Database Closed Badly." in x['body']['Error'])
