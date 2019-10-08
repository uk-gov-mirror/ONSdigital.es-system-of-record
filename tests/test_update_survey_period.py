import unittest
import json
import unittest.mock as mock
import sys
import os
import sqlalchemy as db
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import update_survey_period as update_survey_period  # noqa: 402


class TestUpdateSurveyPeriod(unittest.TestCase):
    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.Session.query")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_update,
                                       mock_alchemy_funks):
        with mock.patch.dict(
            update_survey_period.os.environ, {"Database_Location": "Djibouti"}
        ):
            mock_update.return_value.values.return_value\
                .returning.return_value\
                .on_conflict_do_nothing.return_value = "bob"

            x = update_survey_period.lambda_handler(
                {'active_period': True, 'number_of_responses': 2,
                 'number_cleared': 2, 'number_cleared_first_time': 1,
                 'sample_size': 2, 'survey_period': '201712', 'survey_code':
                     '066'}, '')

        assert(x["statusCode"] == 200)
        assert ("Successfully" in x['body']['Success'])

    def test_environment_variable_exception(self):
        x = update_survey_period.lambda_handler(
            "JAMIE WAS HERE!! FORMER SOFTWARE ENGINEER", '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_survey_period.os.environ,
                    {"Database_Location": "Djibouti"}
            ):
                test_data.pop('query_type')
                x = update_survey_period.lambda_handler(
                    "MIKE LOVES BOUNTY BARS!!", '')

        assert (x["statusCode"] == 400)
        assert ("Invalid" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    def test_db_connection_exception_driver(self, mock_create_engine):

        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect =\
                db.exc.NoSuchModuleError('', '', '')
            x = update_survey_period.lambda_handler(
                {'active_period': True, 'number_of_responses': 2,
                 'number_cleared': 2, 'number_cleared_first_time': 1,
                 'sample_size': 2, 'survey_period': '201712',
                 'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Driver Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            mock_create_engine.side_effect =\
                db.exc.OperationalError("", "", "")

            x = update_survey_period.lambda_handler(
                {'active_period': True, 'number_of_responses': 2,
                 'number_cleared': 2, 'number_cleared_first_time': 1,
                 'sample_size': 2, 'survey_period': '201712',
                 'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            mock_create_engine.side_effect = Exception("Bad Me")

            x = update_survey_period.lambda_handler(
                {'active_period': True, 'number_of_responses': 2,
                 'number_cleared': 2, 'number_cleared_first_time': 1,
                 'sample_size': 2, 'survey_period': '201712',
                 'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_update_survey_period_fail(self, mock_create_engine,
                                       mock_alchemy_funcs):
        with mock.patch.dict(
            update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session.query")\
                    as mock_update:
                mock_update.side_effect =\
                    db.exc.OperationalError("", "", "")

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert(x["statusCode"] == 500)
        assert ("Failed To Update Data: survey_period" in x['body']['Error'])
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_update_survey_period_fail_general(self, mock_create_engine,
                                               mock_alchemy_funcs):
        with mock.patch.dict(
            update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session.query")\
                    as mock_update:
                mock_update.side_effect = Exception('', '', '')

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert(x["statusCode"] == 500)
        assert ("Failed To Update Data: survey_period" in x['body']['Error'])
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.Session.query")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_commit_fail(self, mock_create_engine, mock_update,
                         mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Commit Changes" in x['body']['Error'])
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.Session.query")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_commit_fail_general(self, mock_create_engine, mock_update,
                                 mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect = Exception("Bad Me")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Commit Changes" in x['body']['Error'])
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.Session.query")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_close_fail(self, mock_create_engine, mock_update,
                        mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Database Session Closed Badly." in x['body']['Error'])
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_survey_period.db.create_engine")
    @mock.patch("update_survey_period.Session.query")
    @mock.patch("update_survey_period.alchemy_functions")
    def test_close_fail_general(self, mock_create_engine, mock_update,
                                mock_alchemy_funks):
        with mock.patch.dict(
                update_survey_period.os.environ,
                {"Database_Location": "Djibouti"}
        ):
            with mock.patch("update_survey_period.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect = Exception("Bad Me")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_survey_period.lambda_handler(
                    {'active_period': True, 'number_of_responses': 2,
                     'number_cleared': 2, 'number_cleared_first_time': 1,
                     'sample_size': 2, 'survey_period': '201712',
                     'survey_code': '066'}, '')

        assert (x["statusCode"] == 500)
        assert ("Database Session Closed Badly." in x['body']['Error'])
        assert ("General Error" in str(x['body']['Error']))
