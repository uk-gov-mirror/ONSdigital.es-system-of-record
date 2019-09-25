import sys
import os

import pandas as pd
import unittest
import unittest.mock as mock
import sqlalchemy
from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import get_contributor as get_contributor  # noqa: 402


class TestGetContributor(unittest.TestCase):

    @mock.patch("get_contributor.db.create_engine")
    @mock.patch("get_contributor.Session.query")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_select):

        with mock.patch.dict(
            get_contributor.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_contributor.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.return_value\
                    .table_model.return_value = 'I Am A Table'
                mock_alchemy_functions.to_df.side_effect = [
                    pd.DataFrame({"ru_reference": ["100"],
                                  "parent_ru_reference": ["200"],
                                  "house_name_number": ["1"],
                                  "street": ["My Place"],
                                  "additional_address_line": [None],
                                  "town_city": ["Somewhere"],
                                  "county": ["Here"],
                                  "country": ["UKnow"],
                                  "postcode": ["UT1 TT5"],
                                  "birth_date": ["2019-08-28"],
                                  "business_profiling_team_case": [False],
                                  "contact": ["Pain"],
                                  "death_date": [None],
                                  "enforcement_flag": [False],
                                  "enforcement_status": ["No"],
                                  "fax": [None],
                                  "contributor_name": ["A Company"],
                                  "profile_information": [None],
                                  "sic2003": [None],
                                  "sic2007": [None],
                                  "telephone": ["Test"]}),
                    pd.DataFrame({"ru_reference": ["100"],
                                  "survey_code": ["066"],
                                  "number_of_consecutive_non_response": [0],
                                  "number_of_periods_without_queries": [0],
                                  "period_of_enrolment": ["201908"]}),
                    pd.DataFrame({"contact_reference": ["099"],
                                  "house_name_number": ["South"],
                                  "street": ["North"],
                                  "additional_address_line": [None],
                                  "town_city": ["East"],
                                  "county": ["West"],
                                  "country": [None],
                                  "postcode": ["AAA AAA"],
                                  "contact_constraints": [None],
                                  "contact_fax": [None],
                                  "contact_email": [None],
                                  "contact_name": ["Person"],
                                  "contact_organisation": ["A Name"],
                                  "contact_preferences": [None],
                                  "contact_telephone": [None],
                                  "ru_reference": ["100"],
                                  "survey_code": ["066"],
                                  "effective_end_date": [None],
                                  "effective_start_date": ["2019-08-28"]}),
                    pd.DataFrame({"ru_reference": ["100"],
                                  "survey_code": ["066"],
                                  "survey_period": ["201908"],
                                  "additional_comments": [None],
                                  "contributor_comments": [None],
                                  "last_updated": ["2019-08-28"],
                                  "active_queries": [0],
                                  "contributor_interactions": [0],
                                  "priority_response_list": [None],
                                  "response_status": ["Test"],
                                  "short_description": ["Test"],
                                  "status_changed": ["2019-08-28"],
                                  "active_period": [False],
                                  "number_of_responses": [0],
                                  "number_cleared": [0],
                                  "sample_size": [0],
                                  "number_cleared_first_time": [0]}),
                    pd.DataFrame({})]

                x = get_contributor.lambda_handler({"ru_reference": ""}, '')

                assert(x["statusCode"] == 200)
                assert ("ru_reference" in x['body'])

    @mock.patch("get_contributor.db.create_engine")
    @mock.patch("get_contributor.Session.query")
    @mock.patch("get_contributor.alchemy_functions")
    def test_lambda_handler_select_fail(self, mock_create_engine, mock_select,
                                        mock_alchemy_functions):

        with mock.patch.dict(
            get_contributor.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_select.side_effect = sqlalchemy.exc.OperationalError
            x = get_contributor.lambda_handler({"ru_reference": ""}, '')

            assert(x["statusCode"] == 500)
            assert ("Failed To Retrieve Data:" in x['body']['Error'])

    @mock.patch("get_contributor.db.create_engine")
    @mock.patch("get_contributor.Session.query")
    def test_lambda_handler_output_error(self, mock_create_engine,
                                         mock_select):

        with mock.patch.dict(
            get_contributor.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            with mock.patch("get_contributor.alchemy_functions")\
                    as mock_alchemy_functions:
                mock_alchemy_functions.to_df.side_effect = [pd.DataFrame({}),
                                                            pd.DataFrame({}),
                                                            pd.DataFrame({}),
                                                            pd.DataFrame({}),
                                                            pd.DataFrame({})]

                x = get_contributor.lambda_handler({"ru_reference": ""}, '')
                assert(x["statusCode"] == 500)
                assert ("Invalid" in str(x['body']))

    def test_environment_variable_exception(self):
        x = get_contributor.lambda_handler('', '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error." in x['body']['Error'])

    @mock.patch("get_contributor.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine):

        with mock.patch.dict(
                get_contributor.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):
            mock_create_engine.side_effect = sqlalchemy.exc.OperationalError
            x = get_contributor.lambda_handler({"ru_reference": ""}, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))

    @mock.patch("get_contributor.db.create_engine")
    @mock.patch("get_contributor.Session.query")
    @mock.patch("get_contributor.alchemy_functions")
    def test_lambda_handler_connection_close(self, mock_create_engine,
                                             mock_select,
                                             mock_alchemy_functions):

        with mock.patch.dict(
            get_contributor.os.environ,
                {"Database_Location": "MyPostgresDatase"}
        ):

            with mock.patch("get_contributor.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.close.side_effect =\
                    sqlalchemy.exc.OperationalError
                x = get_contributor.lambda_handler({"ru_reference": ""}, '')

                assert(x["statusCode"] == 500)
                assert ("Database Session Closed Badly" in x['body']['Error'])
