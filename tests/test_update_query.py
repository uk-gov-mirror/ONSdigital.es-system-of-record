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
    @mock.patch("update_query.db.update")
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
        assert ("Configuration Error." in x['body']['Error'])

    def test_input_data_exception(self):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ, {"Database_Location": "Djibouti"}
            ):
                test_data.pop('query_type')
                x = update_query.lambda_handler("MIKE", '')

        assert (x["statusCode"] == 500)
        assert ("Invalid" in str(x['body']))

    @mock.patch("update_query.db.create_engine")
    def test_db_connection_exception(self, mock_create_engine,):
        with open('tests/fixtures/test_data.txt') as infile:
            test_data = json.load(infile)
            with mock.patch.dict(
                    update_query.os.environ, {"Database_Location": "Djibouti"}
            ):
                mock_create_engine.side_effect =\
                    db.exc.OperationalError("Side effect in full effect",
                                            "", "")
                x = update_query.lambda_handler(test_data, '')

        assert (x["statusCode"] == 500)
        assert ("Operational Error, Failed To Connect."
                in str(x['body']['Error']))

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
            with mock.patch("update_query.db.update") as mock_update:
                mock_therest = mock.Mock()
                mock_therest.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "Bob"
                mock_update.side_effect =\
                    db.exc.OperationalError("Side effect in full effect",
                                            "", "")

                x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query" in x['body']['Error'])

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

            with mock.patch("update_query.db.update") as mock_update:
                with mock.patch("update_query.insert") as mock_insert:
                    mock_therestofinsert = mock.Mock()
                    mock_therestofinsert.values.return_value\
                        .returning.return_value\
                        .on_conflict_do_nothing.return_value = "Bob"
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value\
                        .where.return_value = "MikesiQuatilo"
                    mock_update.return_value = mock_therest
                    mock_insert.side_effect = [
                        mock_therestofinsert, mock_therestofinsert,
                        db.exc.OperationalError("Side effect in full effect",
                                                "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"
                    x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: failed_vet"
                        in x['body']['Error'])

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
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                with mock.patch("update_query.db.update") as mock_update:
                    with mock.patch("update_query.insert") as mock_insert:
                        mock_therestofinsert = mock.Mock()
                        mock_therestofinsert.values.return_value\
                            .returning.return_value\
                            .on_conflict_do_nothing.return_value = "Bob"
                        mock_therest = mock.Mock()
                        mock_therest.values.return_value\
                            .where.return_value = "MikesiQuatilo"
                        mock_update.return_value = mock_therest

                        mock_insert.side_effect = [
                            mock_therestofinsert,
                            db.exc.OperationalError("Side effect in effect",
                                                    "", "")]
                        mock_alchemy_funks.table_model.return_value\
                            .columns.return_value\
                            .query_reference.return_value = "Reeves"
                        x = update_query.lambda_handler(test_data, "")

                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: question_anomaly"
                        in x['body']['Error'])

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
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                with mock.patch("update_query.db.update") as mock_update:
                    with mock.patch("update_query.insert") as mock_insert:
                        mock_therest = mock.Mock()
                        mock_therest.values.return_value\
                            .where.return_value = "MikesiQuatilo"
                        mock_update.return_value = mock_therest

                        mock_insert.side_effect =\
                            db.exc.OperationalError("Side effect in effect",
                                                    "", "")
                        mock_alchemy_funks.table_model.return_value\
                            .columns.return_value\
                            .query_reference.return_value = "Reeves"

                        x = update_query.lambda_handler(test_data, "")
                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: step_exception"
                        in x['body']['Error'])

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
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                with mock.patch("update_query.db.update") as mock_update:
                    with mock.patch("update_query.insert") as mock_insert:
                        mock_therest = mock.Mock()
                        mock_therest.values.return_value\
                            .where.return_value = "MikesiQuatilo"
                        mock_insert.return_value.values.return_value\
                            .returning.return_value\
                            .on_conflict_do_nothing.return_value = "Bob"
                        mock_update.side_effect = [
                            mock_therest,
                            db.exc.OperationalError("Side effect in effect",
                                                    "", "")]
                        mock_alchemy_funks.table_model.return_value\
                            .columns.return_value\
                            .query_reference.return_value = "Reeves"
                        x = update_query.lambda_handler(test_data, "")
                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query_task"
                        in x['body']['Error'])

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
            with mock.patch("update_query.Session") as mock_sesh:
                mock_session = AlchemyMagicMock()
                mock_sesh.return_value = mock_session
                with mock.patch("update_query.insert") as mock_insert:
                    mock_therest = mock.Mock()
                    mock_therest.values.return_value\
                        .where.return_value = "MikesiQuatilo"
                    mock_therestofinsert = mock.Mock()
                    mock_therestofinsert.values.return_value\
                        .on_conflict_do_nothing\
                        .return_value = "moo says the moo cow"

                    mock_insert.side_effect = [
                        mock_therestofinsert, mock_therestofinsert,
                        mock_therestofinsert, mock_therestofinsert,
                        mock_therestofinsert, mock_therestofinsert,
                        db.exc.OperationalError("Side effect in full effect",
                                                "", "")]
                    mock_alchemy_funks.table_model.return_value\
                        .columns.return_value\
                        .query_reference.return_value = "Reeves"

                    x = update_query.lambda_handler(test_data, "")
                assert(x["statusCode"] == 500)
                assert ("Failed To Update Data: query_task_update"
                        in x['body']['Error'])

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.db.update")
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
                    db.exc.OperationalError("Side effect in full effect",
                                            "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"
                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Failed To Commit Changes" in x['body']['Error'])

    @mock.patch("update_query.db.create_engine")
    @mock.patch("update_query.db.update")
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
                    db.exc.OperationalError("Side effect in full effect",
                                            "", "")
                mock_update.return_value.values.return_value\
                    .returning.return_value\
                    .on_conflict_do_nothing.return_value = "bob"

                x = update_query.lambda_handler(test_data, "")

                assert (x["statusCode"] == 500)
                assert ("Database Session Closed Badly." in x['body']['Error'])
