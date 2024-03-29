import unittest
import json
import unittest.mock as mock
import sys
import os
import sqlalchemy as db

from alchemy_mock.mocking import AlchemyMagicMock

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))  # noqa: 402
import update_query as update_query


class TestUpdateQuery(unittest.TestCase):
    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.Session.query")
    @mock.patch("update_query.alchemy_functions")
    def test_lambda_handler_happy_path(self, mock_create_engine, mock_update,
                                       mock_alchemy_funks):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "Djibouti"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            mock_update.return_value.values.return_value\
                .returning.return_value\
                .on_conflict_do_nothing.return_value = "bob"
            x = update_query.lambda_handler(test_data, "")
            print(x)
            assert(x["statusCode"] == 200)
            assert ("Successfully" in str(x['body']))

    def test_environment_variable_exception(self):
        x = update_query.lambda_handler("LORD MIIIIIKE!", '')

        assert (x["statusCode"] == 500)
        assert ("Configuration Error:" in x['body']['Error'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ, {"Database_Location": "Djibouti"}
            ):
                test_data.pop('query_type')
                x = update_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 400)
        assert ("Invalid" in str(x['body']))

    @mock.patch("update_query.db.create_engine")
    def test_db_connection_exception_driver(self, mock_create_engine):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ,
                    {"Database_Location": "MyPostgresDatase"}
            ):
                mock_create_engine.side_effect =\
                    db.exc.NoSuchModuleError('', '', '')
                x = update_query.lambda_handler(test_data, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in x['body']['Error'])
        assert ("Driver Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine,):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ, {"Database_Location": "Djibouti"}
            ):
                mock_create_engine.side_effect =\
                    db.exc.OperationalError("", "", "")
                x = update_query.lambda_handler(test_data, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    def test_db_connection_exception_general(self, mock_create_engine,):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ, {"Database_Location": "Djibouti"}
            ):
                mock_create_engine.side_effect = Exception("Bad Me")
                x = update_query.lambda_handler(test_data, '')

        assert (x["statusCode"] == 500)
        assert ("Failed To Connect" in str(x['body']['Error']))
        assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_update_query_fail(self, mock_create_engine, mock_alchemy_funks,
                               mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                mock_update.return_value.filter.return_value\
                    .update.side_effect =\
                    db.exc.OperationalError("", "", "")

                x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query" in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_update_query_fail_general(self, mock_create_engine,
                                       mock_alchemy_funks,
                                       mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                mock_update.return_value.filter.return_value\
                    .update.side_effect = Exception("Bad Me")

                x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query" in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_failed_vet_fail(self, mock_create_engine,
                                         mock_alchemy_funks, mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()

                    mock_update.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value \
                        .scalar.return_value = None
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        db.exc.OperationalError("", "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: failed_vet"
                        in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_failed_vet_fail_general(self, mock_create_engine,
                                                 mock_alchemy_funks,
                                                 mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)

            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()

                    mock_update.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value \
                        .scalar.return_value = None
                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        Exception("Bad Me")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: failed_vet"
                        in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_question_anomaly_fail(self, mock_create_engine,
                                               mock_alchemy_funks, mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.filter.return_value\
                        .update.return_value = "MikesiQuatilo"
                    mock_update.return_value.filter.return_value \
                        .scalar.return_value = None

                    mock_insert.side_effect = [
                        mock_therest,
                        db.exc.OperationalError("", "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")

            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: question_anomaly"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_question_anomaly_fail_general(self,
                                                       mock_create_engine,
                                                       mock_alchemy_funks,
                                                       mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.filter.return_value\
                        .update.return_value = "MikesiQuatilo"
                    mock_update.return_value.filter.return_value \
                        .scalar.return_value = None

                    mock_insert.side_effect = [
                        mock_therest,
                        Exception("Bad Me")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")

            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: question_anomaly"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_step_exception_fail(self, mock_create_engine,
                                             mock_alchemy_funks, mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None

                    mock_insert.side_effect = db.exc\
                        .OperationalError("", "", "")
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"

                    x = update_query.lambda_handler(test_data, "")
            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: step_exception"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_step_exception_fail_general(self, mock_create_engine,
                                                     mock_alchemy_funks,
                                                     mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None

                    mock_insert.side_effect = Exception("Bad Me")
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"

                    x = update_query.lambda_handler(test_data, "")
            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: step_exception"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_query_tasks_update_fail(self, mock_create_engine,
                                                 mock_alchemy_funks,
                                                 mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None
                    mock_insert.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .update.side_effect = [
                            mock_therest,
                            db.exc.OperationalError("", "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")
            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: query_task"
                    in x['body']['Error'])
            assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_query_tasks_update_fail_general(self,
                                                         mock_create_engine,
                                                         mock_alchemy_funks,
                                                         mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None
                    mock_insert.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .update.side_effect = [
                            mock_therest,
                            Exception("Bad Me")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")
            assert(x["statusCode"] == 500)
            assert ("Failed To Update Data: query_task"
                    in x['body']['Error'])
            assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_into_query_task_updates_update_fail(self,
                                                        mock_create_engine,
                                                        mock_alchemy_funks,
                                                        mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None

                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        db.exc.OperationalError("", "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"

                    x = update_query.lambda_handler(test_data, "")
                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query_task_update"
                        in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.alchemy_functions")
    @mock.patch("update_query.io_validation.QueryReference")
    def test_insert_query_task_updates_update_fail_general(self,
                                                           mock_create_engine,
                                                           mock_alchemy_funks,
                                                           mock_marsh):
        with mock.patch.dict(
            update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session.query") as mock_update:
                with mock.patch("update_query.Session.add") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_update.return_value.filter.return_value\
                        .update.return_value = mock_therest
                    mock_update.return_value.filter.return_value\
                        .scalar.return_value = None

                    mock_insert.side_effect = [
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        mock_therest, mock_therest,
                        Exception("Bad Me")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"

                    x = update_query.lambda_handler(test_data, "")
                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query_task_update"
                        in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.Session.query")
    @mock.patch("update_query.alchemy_functions")
    def test_commit_fail(self, mock_create_engine, mock_update,
                         mock_alchemy_funks):
        with mock.patch.dict(
                update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"
                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Failed To Commit Changes" in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.Session.query")
    @mock.patch("update_query.alchemy_functions")
    def test_commit_fail_general(self, mock_create_engine, mock_update,
                                 mock_alchemy_funks):
        with mock.patch.dict(
                update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                mock_session.commit.side_effect = Exception("Bad Me")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"
                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Failed To Commit Changes" in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.Session.query")
    @mock.patch("update_query.alchemy_functions")
    def test_close_fail(self, mock_create_engine, mock_update,
                        mock_alchemy_funks):
        with mock.patch.dict(
                update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session

                mock_session.close.side_effect =\
                    db.exc.OperationalError("", "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Database Session Closed Badly." in x['body']['Error'])
                assert ("Operational Error" in str(x['body']['Error']))

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.Session.query")
    @mock.patch("update_query.alchemy_functions")
    def test_close_fail_general(self, mock_create_engine, mock_update,
                                mock_alchemy_funks):
        with mock.patch.dict(
                update_query.os.environ, {"Database_Location": "sweden"}
        ):
            with open('tests/fixtures/test_data.txt') as infile:
                test_data = json.load(infile)
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session

                mock_session.close.side_effect = Exception("Bad Me")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Database Session Closed Badly." in x['body']['Error'])
                assert ("General Error" in str(x['body']['Error']))
