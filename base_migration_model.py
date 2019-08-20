"""Base Revision

Revision ID: 7e4c6b9e5a5d
Revises:
Create Date: 2019-07-23 12:00:27.755804

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'base_migration_model'
down_revision = 'empty_database'
branch_labels = None
depends_on = None
schema_name = 'es_db_test'


def upgrade():
    """All tables to be created during the alembic upgrade process."""

    op.create_table('ssr_old_regions',
                    sa.Column('idbr_region', sa.CHAR(length=2), autoincrement=False, nullable=False),
                    sa.Column('ssr_reference', sa.SMALLINT(), autoincrement=False, nullable=False),
                    sa.Column('region_name', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.PrimaryKeyConstraint('idbr_region', name='ssr_old_regions_pkey'),
                    schema=schema_name
                    )
    op.create_table('gor_regions',
                    sa.Column('idbr_region', sa.CHAR(length=2), autoincrement=False, nullable=False),
                    sa.Column('gor_reference', sa.SMALLINT(), autoincrement=False, nullable=False),
                    sa.Column('region_name', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.PrimaryKeyConstraint('idbr_region', name='gor_regions_pkey'),
                    schema=schema_name
                    )
    op.create_table('vet',
                    sa.Column('vet_code', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('vet_description', sa.VARCHAR(length=60), autoincrement=False, nullable=False),
                    sa.PrimaryKeyConstraint('vet_code', name='vet_pkey'),
                    schema=schema_name
                    )
    op.create_table('query_type',
                    sa.Column('query_type', sa.VARCHAR(length=25), autoincrement=False, nullable=False),
                    sa.Column('query_type_description', sa.TEXT(), autoincrement=False, nullable=False),
                    sa.PrimaryKeyConstraint('query_type', name='query_type_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('survey',
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('survey_name', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.PrimaryKeyConstraint('survey_code', name='survey_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('contributor',
                    sa.Column('parent_ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=True),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('house_name_number', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.Column('street', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.Column('additional_address_line', sa.VARCHAR(length=100), autoincrement=False, nullable=True),
                    sa.Column('town_city', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.Column('county', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.Column('country', sa.VARCHAR(length=30), autoincrement=False, nullable=True),
                    sa.Column('post_code', sa.VARCHAR(length=9), autoincrement=False, nullable=False),
                    sa.Column('birth_date', sa.DATE(), autoincrement=False, nullable=False),
                    sa.Column('business_profiling_team_case', sa.BOOLEAN(), autoincrement=False, nullable=True),
                    sa.Column('contact', sa.VARCHAR(length=60), autoincrement=False, nullable=True),
                    sa.Column('death_date', sa.DATE(), autoincrement=False, nullable=True),
                    sa.Column('enforcement_flag', sa.BOOLEAN(), autoincrement=False, nullable=True),
                    sa.Column('enforcement_status', sa.VARCHAR(length=15), autoincrement=False, nullable=True),
                    sa.Column('fax', sa.VARCHAR(length=15), autoincrement=False, nullable=True),
                    sa.Column('contributor_name', sa.VARCHAR(length=60), autoincrement=False, nullable=False),
                    sa.Column('profile_information', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('sic2003', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('sic2007', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('telephone', sa.VARCHAR(length=15), autoincrement=False, nullable=True),
                    sa.PrimaryKeyConstraint('ru_reference', name='contributor_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('survey_enrolment',
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('number_of_consecutive_non_response', sa.SMALLINT(), autoincrement=False, nullable=True),
                    sa.Column('number_of_periods_without_queries', sa.SMALLINT(), autoincrement=False, nullable=True),
                    sa.Column('period_of_enrolment', sa.VARCHAR(length=10), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['ru_reference'], ['es_db_test.contributor.ru_reference'], name='survey_enrolment_ru_reference_fkey'),
                    sa.ForeignKeyConstraint(['survey_code'], ['es_db_test.survey.survey_code'], name='survey_enrolment_survey_code_fkey'),
                    sa.PrimaryKeyConstraint('ru_reference', 'survey_code', name='survey_enrolment_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('survey_period',
                    sa.Column('survey_period', sa.CHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('active_period', sa.BOOLEAN(), autoincrement=False, nullable=True),
                    sa.Column('number_of_responses', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('number_cleared', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('sample_size', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('number_cleared_first_time', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.ForeignKeyConstraint(['survey_code'], ['es_db_test.survey.survey_code'], name='survey_period_survey_code_fkey'),
                    sa.PrimaryKeyConstraint('survey_period', 'survey_code', name='survey_period_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('contributor_survey_period',
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('survey_period', sa.VARCHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('run_id', sa.INTEGER(), autoincrement=False, nullable=True),
                    sa.Column('additional_comments', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('contributor_comments', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('last_updated', sa.DATE(), autoincrement=False, nullable=True),
                    sa.Column('active_queries', sa.SMALLINT(), autoincrement=False, nullable=True),
                    sa.Column('contributor_interactions', sa.SMALLINT(), autoincrement=False, nullable=False),
                    sa.Column('priority_response_list', sa.VARCHAR(length=100), autoincrement=False, nullable=True),
                    sa.Column('response_status', sa.VARCHAR(length=20), autoincrement=False, nullable=True),
                    sa.Column('short_description', sa.VARCHAR(length=50), autoincrement=False, nullable=True),
                    sa.Column('status_changed', sa.DATE(), autoincrement=False, nullable=True),
                    sa.ForeignKeyConstraint(['ru_reference', 'survey_code'], ['es_db_test.survey_enrolment.ru_reference', 'es_db_test.survey_enrolment.survey_code'], name='contributor_survey_period_ru_reference_fkey'),
                    sa.ForeignKeyConstraint(['survey_code', 'survey_period'], ['es_db_test.survey_period.survey_code', 'es_db_test.survey_period.survey_period'], name='contributor_survey_period_survey_code_fkey'),
                    sa.PrimaryKeyConstraint('ru_reference', 'survey_code', 'survey_period', name='contributor_survey_period_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('contact',
                    sa.Column('contact_reference', sa.VARCHAR(length=25), autoincrement=False, nullable=False),
                    sa.Column('house_name_number', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.Column('street', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.Column('additional_address_line', sa.VARCHAR(length=50), autoincrement=False, nullable=True),
                    sa.Column('town_city', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.Column('county', sa.VARCHAR(length=15), autoincrement=False, nullable=False),
                    sa.Column('country', sa.VARCHAR(length=20), autoincrement=False, nullable=True),
                    sa.Column('postcode', sa.VARCHAR(length=9), autoincrement=False, nullable=False),
                    sa.Column('contact_constraints', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('contact_email', sa.VARCHAR(length=50), autoincrement=False, nullable=True),
                    sa.Column('contact_fax', sa.VARCHAR(length=12), autoincrement=False, nullable=True),
                    sa.Column('contact_name', sa.VARCHAR(length=50), autoincrement=False, nullable=False),
                    sa.Column('contact_organisation', sa.VARCHAR(length=50), autoincrement=False, nullable=False),
                    sa.Column('contact_preferences', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('contact_telephone', sa.VARCHAR(length=12), autoincrement=False, nullable=True),
                    sa.PrimaryKeyConstraint('contact_reference', name='contact_pkey'),
                    schema=schema_name
                    )
    op.create_table('survey_contact',
                    sa.Column('contact_reference', sa.VARCHAR(length=15), autoincrement=False, nullable=False),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('effective_end_date', sa.DATE(), autoincrement=False, nullable=True),
                    sa.Column('effective_start_date', sa.DATE(), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['contact_reference'], ['es_db_test.contact.contact_reference'], name='survey_contact_contact_reference_fkey'),
                    sa.ForeignKeyConstraint(['ru_reference', 'survey_code'], ['es_db_test.survey_enrolment.ru_reference', 'es_db_test.survey_enrolment.survey_code'], name='survey_contact_ru_reference_fkey'),
                    sa.PrimaryKeyConstraint('contact_reference', 'ru_reference', 'survey_code', name='survey_contact_pkey'),
                    schema=schema_name
                    )
    op.create_table('query',
                    sa.Column('query_reference', sa.INTEGER(), server_default=sa.text("nextval('es_db_test.query_queryreference_seq'::regclass)"), autoincrement=True, nullable=False),
                    sa.Column('query_type', sa.VARCHAR(length=25), autoincrement=False, nullable=False),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('survey_period', sa.CHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('current_period', sa.VARCHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('date_raised', sa.DATE(), autoincrement=False, nullable=False),
                    sa.Column('general_specific_flag', sa.BOOLEAN(), autoincrement=False, nullable=True),
                    sa.Column('industry_group', sa.VARCHAR(length=20), autoincrement=False, nullable=True),
                    sa.Column('last_query_update', sa.DATE(), autoincrement=False, nullable=True),
                    sa.Column('query_active', sa.BOOLEAN(), autoincrement=False, nullable=True),
                    sa.Column('query_description', sa.TEXT(), autoincrement=False, nullable=False),
                    sa.Column('query_status', sa.VARCHAR(length=25), autoincrement=False, nullable=False),
                    sa.Column('raised_by', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
                    sa.Column('results_state', sa.VARCHAR(length=15), autoincrement=False, nullable=True),
                    sa.Column('target_resolution_date', sa.DATE(), autoincrement=False, nullable=True),
                    sa.ForeignKeyConstraint(['survey_period', 'ru_reference', 'survey_code'], ['es_db_test.contributor_survey_period.survey_period', 'es_db_test.contributor_survey_period.ru_reference', 'es_db_test.contributor_survey_period.survey_code'], name='query_survey_period_fkey'),
                    sa.ForeignKeyConstraint(['query_type'], ['es_db_test.query_type.query_type'], name='query_query_type_fkey'),
                    sa.PrimaryKeyConstraint('query_reference', name='query_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('query_task',
                    sa.Column('task_sequence_number', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('query_reference', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('response_required_by', sa.DATE(), autoincrement=False, nullable=True),
                    sa.Column('task_description', sa.TEXT(), autoincrement=False, nullable=False),
                    sa.Column('task_responsibility', sa.VARCHAR(length=50), autoincrement=False, nullable=True),
                    sa.Column('task_status', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.Column('next_planned_action', sa.TEXT(), autoincrement=False, nullable=True),
                    sa.Column('when_action_required', sa.DATE(), autoincrement=False, nullable=True),
                    sa.ForeignKeyConstraint(['query_reference'], ['es_db_test.query.query_reference'], name='query_task_query_reference_fkey'),
                    sa.PrimaryKeyConstraint('task_sequence_number', 'query_reference', name='query_task_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('query_task_update',
                    sa.Column('task_sequence_number', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('query_reference', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('last_updated', sa.DATE(), autoincrement=False, nullable=False),
                    sa.Column('task_update_description', sa.VARCHAR(length=50), autoincrement=False, nullable=False),
                    sa.Column('updated_by', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['task_sequence_number', 'query_reference'], ['es_db_test.query_task.task_sequence_number', 'es_db_test.query_task.query_reference'], name='query_task_update_task_sequence_number_fkey'),
                    sa.PrimaryKeyConstraint('task_sequence_number', 'query_reference', 'last_updated', name='query_task_update_pkey'),
                    schema=schema_name
                    )
    op.create_table('step_exception',
                    sa.Column('query_reference', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('survey_period', sa.VARCHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('run_id', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('step', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('error_code', sa.VARCHAR(length=10), autoincrement=False, nullable=False),
                    sa.Column('error_description', sa.VARCHAR(length=60), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['query_reference'], ['es_db_test.query.query_reference'], name='step_exception_query_reference_fkey'),
                    sa.ForeignKeyConstraint(['survey_period', 'ru_reference', 'survey_code'], ['es_db_test.contributor_survey_period.survey_period', 'es_db_test.contributor_survey_period.ru_reference', 'es_db_test.contributor_survey_period.survey_code'], name='step_exception_survey_period_fkey'),
                    sa.PrimaryKeyConstraint('survey_period', 'run_id', 'ru_reference', 'step', 'survey_code', name='step_exception_pkey'),
                    schema=schema_name,
                    postgresql_ignore_search_path=False
                    )
    op.create_table('question_anomaly',
                    sa.Column('survey_period', sa.VARCHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('question_number', sa.VARCHAR(length=8), autoincrement=False, nullable=False),
                    sa.Column('run_id', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('step', sa.VARCHAR(length=10), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.CHAR(length=3), autoincrement=False, nullable=False),
                    sa.Column('anomaly_description', sa.VARCHAR(length=20), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['survey_period', 'run_id', 'ru_reference', 'step', 'survey_code'], ['es_db_test.step_exception.survey_period', 'es_db_test.step_exception.run_id', 'es_db_test.step_exception.ru_reference', 'es_db_test.step_exception.step', 'es_db_test.step_exception.survey_code'], name='question_anomaly_survey_period_fkey'),
                    sa.PrimaryKeyConstraint('survey_period', 'question_number', 'run_id', 'ru_reference', 'step', 'survey_code', name='question_anomaly_pkey'),
                    schema=schema_name
                    )
    op.create_table('failed_vet',
                    sa.Column('failed_vet', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('survey_period', sa.VARCHAR(length=6), autoincrement=False, nullable=False),
                    sa.Column('question_number', sa.VARCHAR(length=4), autoincrement=False, nullable=False),
                    sa.Column('run_id', sa.INTEGER(), autoincrement=False, nullable=False),
                    sa.Column('ru_reference', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('step', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
                    sa.Column('survey_code', sa.VARCHAR(length=25), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['failed_vet'], ['es_db_test.vet.vet_code'], name='failed_vet_failed_vet_fkey'),
                    sa.ForeignKeyConstraint(['survey_period', 'question_number', 'run_id', 'ru_reference', 'step', 'survey_code'], ['es_db_test.question_anomaly.survey_period', 'es_db_test.question_anomaly.question_number', 'es_db_test.question_anomaly.run_id', 'es_db_test.question_anomaly.ru_reference', 'es_db_test.question_anomaly.step', 'es_db_test.question_anomaly.survey_code'], name='failed_vet_survey_period_fkey'),
                    sa.PrimaryKeyConstraint('failed_vet', 'survey_period', 'question_number', 'run_id', 'ru_reference', 'step', 'survey_code', name='failed_vet_pkey'),
                    schema=schema_name
                    )


def downgrade():
    """All tables to be dropped during the alembic downgrade process."""
    op.drop_table('failed_vet', schema=schema_name)
    op.drop_table('question_anomaly', schema=schema_name)
    op.drop_table('step_exception', schema=schema_name)
    op.drop_table('query_task_update', schema=schema_name)
    op.drop_table('query_task', schema=schema_name)
    op.drop_table('query', schema=schema_name)
    op.drop_table('survey_contact', schema=schema_name)
    op.drop_table('contact', schema=schema_name)
    op.drop_table('contributor_survey_period', schema=schema_name)
    op.drop_table('survey_period', schema=schema_name)
    op.drop_table('survey_enrolment', schema=schema_name)
    op.drop_table('contributor', schema=schema_name)
    op.drop_table('survey', schema=schema_name)
    op.drop_table('query_type', schema=schema_name)
    op.drop_table('vet', schema=schema_name)
    op.drop_table('gor_regions', schema=schema_name)
    op.drop_table('ssr_old_regions', schema=schema_name)
