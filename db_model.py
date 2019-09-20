# coding: utf-8
from sqlalchemy import Boolean, CHAR, Column, Date, ForeignKey,\
    ForeignKeyConstraint, Integer, SmallInteger, String, Table, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class Contact(Base):
    __tablename__ = 'contact'
    __table_args__ = {'schema': 'es_db_test'}

    contact_reference = Column(String(25), primary_key=True)
    house_name_number = Column(String(20), nullable=False)
    street = Column(String(30), nullable=False)
    additional_address_line = Column(String(50))
    town_city = Column(String(20), nullable=False)
    county = Column(String(15), nullable=False)
    country = Column(String(20))
    postcode = Column(String(9), nullable=False)
    contact_constraints = Column(Text)
    contact_email = Column(String(50))
    contact_fax = Column(String(12))
    contact_name = Column(String(50), nullable=False)
    contact_organisation = Column(String(50), nullable=False)
    contact_preferences = Column(Text)
    contact_telephone = Column(String(12))


class Contributor(Base):
    __tablename__ = 'contributor'
    __table_args__ = {'schema': 'es_db_test'}

    parent_ru_reference = Column(String(11))
    ru_reference = Column(String(11), primary_key=True)
    house_name_number = Column(String(20), nullable=False)
    street = Column(String(30), nullable=False)
    additional_address_line = Column(String(100))
    town_city = Column(String(30), nullable=False)
    county = Column(String(30), nullable=False)
    country = Column(String(30))
    post_code = Column(String(9), nullable=False)
    birth_date = Column(Date, nullable=False)
    business_profiling_team_case = Column(Boolean)
    contact = Column(String(60))
    death_date = Column(Date)
    enforcement_flag = Column(Boolean)
    enforcement_status = Column(String(15))
    fax = Column(String(15))
    contributor_name = Column(String(60), nullable=False)
    profile_information = Column(Text)
    sic2003 = Column(Integer)
    sic2007 = Column(Integer)
    telephone = Column(String(15))


class GorRegion(Base):
    __tablename__ = 'gor_regions'
    __table_args__ = {'schema': 'es_db_test'}

    idbr_region = Column(CHAR(2), primary_key=True)
    gor_reference = Column(SmallInteger, nullable=False)
    region_name = Column(String(20), nullable=False)


class QueryType(Base):
    __tablename__ = 'query_type'
    __table_args__ = {'schema': 'es_db_test'}

    query_type = Column(String(25), primary_key=True)
    query_type_description = Column(Text, nullable=False)


class SsrOldRegion(Base):
    __tablename__ = 'ssr_old_regions'
    __table_args__ = {'schema': 'es_db_test'}

    idbr_region = Column(CHAR(2), primary_key=True)
    ssr_reference = Column(SmallInteger, nullable=False)
    region_name = Column(String(20), nullable=False)


class Survey(Base):
    __tablename__ = 'survey'
    __table_args__ = {'schema': 'es_db_test'}

    survey_code = Column(CHAR(3), primary_key=True)
    survey_name = Column(String(30), nullable=False)


class Vet(Base):
    __tablename__ = 'vet'
    __table_args__ = {'schema': 'es_db_test'}

    vet_code = Column(Integer, primary_key=True)
    vet_description = Column(String(60), nullable=False)

    question_anomaly = relationship('QuestionAnomaly',
                                    secondary='es_db_test.failed_vet')


class SurveyEnrolment(Base):
    __tablename__ = 'survey_enrolment'
    __table_args__ = {'schema': 'es_db_test'}

    ru_reference = Column(ForeignKey('es_db_test.contributor.ru_reference'),
                          primary_key=True, nullable=False)
    survey_code = Column(ForeignKey('es_db_test.survey.survey_code'),
                         primary_key=True, nullable=False)
    number_of_consecutive_non_response = Column(SmallInteger)
    number_of_periods_without_queries = Column(SmallInteger)
    period_of_enrolment = Column(String(10), nullable=False)

    contributor = relationship('Contributor')
    survey = relationship('Survey')


class SurveyPeriod(Base):
    __tablename__ = 'survey_period'
    __table_args__ = {'schema': 'es_db_test'}

    survey_period = Column(CHAR(6), primary_key=True, nullable=False)
    survey_code = Column(ForeignKey('es_db_test.survey.survey_code'),
                         primary_key=True, nullable=False)
    active_period = Column(Boolean)
    number_of_responses = Column(Integer)
    number_cleared = Column(Integer)
    sample_size = Column(Integer)
    number_cleared_first_time = Column(Integer)

    survey = relationship('Survey')


class ContributorSurveyPeriod(Base):
    __tablename__ = 'contributor_survey_period'
    __table_args__ = (
        ForeignKeyConstraint(['ru_reference', 'survey_code'],
                             ['es_db_test.survey_enrolment.ru_reference',
                              'es_db_test.survey_enrolment.survey_code']),
        ForeignKeyConstraint(['survey_code', 'survey_period'],
                             ['es_db_test.survey_period.survey_code',
                              'es_db_test.survey_period.survey_period']),
        {'schema': 'es_db_test'}
    )

    ru_reference = Column(String(11), primary_key=True, nullable=False)
    survey_code = Column(CHAR(3), primary_key=True, nullable=False)
    survey_period = Column(String(6), primary_key=True, nullable=False)
    run_id = Column(Integer)
    additional_comments = Column(Text)
    contributor_comments = Column(Text)
    last_updated = Column(Date)
    active_queries = Column(SmallInteger)
    contributor_interactions = Column(SmallInteger, nullable=False)
    priority_response_list = Column(String(100))
    response_status = Column(String(20))
    short_description = Column(String(50))
    status_changed = Column(Date)

    survey_enrolment = relationship('SurveyEnrolment')
    survey_period1 = relationship('SurveyPeriod')


class SurveyContact(Base):
    __tablename__ = 'survey_contact'
    __table_args__ = (
        ForeignKeyConstraint(['ru_reference', 'survey_code'],
                             ['es_db_test.survey_enrolment.ru_reference',
                              'es_db_test.survey_enrolment.survey_code']),
        {'schema': 'es_db_test'}
    )

    contact_reference = Column(
        ForeignKey('es_db_test.contact.contact_reference'),
        primary_key=True, nullable=False)
    ru_reference = Column(String(11), primary_key=True, nullable=False)
    survey_code = Column(CHAR(3), primary_key=True, nullable=False)
    effective_end_date = Column(Date)
    effective_start_date = Column(Date, nullable=False)

    contact = relationship('Contact')
    survey_enrolment = relationship('SurveyEnrolment')


class Query(Base):
    __tablename__ = 'query'
    __table_args__ = (
        ForeignKeyConstraint(
            ['survey_period', 'ru_reference', 'survey_code'],
            ['es_db_test.contributor_survey_period.survey_period',
             'es_db_test.contributor_survey_period.ru_reference',
             'es_db_test.contributor_survey_period.survey_code']),
        {'schema': 'es_db_test'}
    )

    query_reference = Column(
        Integer, primary_key=True, server_default=text(
            "nextval('es_db_test.query_query_reference_seq'::regclass)"))
    query_type = Column(ForeignKey('es_db_test.query_type.query_type'),
                        nullable=False)
    ru_reference = Column(String(11), nullable=False)
    survey_code = Column(CHAR(3), nullable=False)
    survey_period = Column(CHAR(6), nullable=False)
    current_period = Column(String(6), nullable=False)
    date_raised = Column(Date, nullable=False)
    general_specific_flag = Column(Boolean)
    industry_group = Column(String(20))
    last_query_update = Column(Date)
    query_active = Column(Boolean)
    query_description = Column(Text, nullable=False)
    query_status = Column(String(25), nullable=False)
    raised_by = Column(String(30), nullable=False)
    results_state = Column(String(15))
    target_resolution_date = Column(Date)

    query_type1 = relationship('QueryType')
    contributor_survey_period = relationship('ContributorSurveyPeriod')


class QueryTask(Base):
    __tablename__ = 'query_task'
    __table_args__ = {'schema': 'es_db_test'}

    task_sequence_number = Column(Integer, primary_key=True, nullable=False)
    query_reference = Column(ForeignKey('es_db_test.query.query_reference'),
                             primary_key=True, nullable=False)
    response_required_by = Column(Date)
    task_description = Column(Text, nullable=False)
    task_responsibility = Column(String(50))
    task_status = Column(String(20), nullable=False)
    next_planned_action = Column(Text)
    when_action_required = Column(Date)

    query = relationship('Query')


class StepException(Base):
    __tablename__ = 'step_exception'
    __table_args__ = (
        ForeignKeyConstraint(
            ['survey_period', 'ru_reference', 'survey_code'],
            ['es_db_test.contributor_survey_period.survey_period',
             'es_db_test.contributor_survey_period.ru_reference',
             'es_db_test.contributor_survey_period.survey_code']),
        {'schema': 'es_db_test'}
    )

    query_reference = Column(ForeignKey('es_db_test.query.query_reference'),
                             nullable=False)
    survey_period = Column(String(6), primary_key=True, nullable=False)
    run_id = Column(Integer, primary_key=True, nullable=False)
    ru_reference = Column(String(11), primary_key=True, nullable=False)
    step = Column(String(11), primary_key=True, nullable=False)
    survey_code = Column(CHAR(3), primary_key=True, nullable=False)
    error_code = Column(String(10), nullable=False)
    error_description = Column(String(60), nullable=False)

    query = relationship('Query')
    contributor_survey_period = relationship('ContributorSurveyPeriod')


class QueryTaskUpdate(Base):
    __tablename__ = 'query_task_update'
    __table_args__ = (
        ForeignKeyConstraint(['task_sequence_number', 'query_reference'],
                             ['es_db_test.query_task.task_sequence_number',
                              'es_db_test.query_task.query_reference']),
        {'schema': 'es_db_test'}
    )

    task_sequence_number = Column(Integer, primary_key=True, nullable=False)
    query_reference = Column(Integer, primary_key=True, nullable=False)
    last_updated = Column(Date, primary_key=True, nullable=False)
    task_update_description = Column(String(50), nullable=False)
    updated_by = Column(String(20), nullable=False)

    query_task = relationship('QueryTask')


class QuestionAnomaly(Base):
    __tablename__ = 'question_anomaly'
    __table_args__ = (
        ForeignKeyConstraint(
            ['survey_period', 'run_id', 'ru_reference', 'step', 'survey_code'],
            ['es_db_test.step_exception.survey_period',
             'es_db_test.step_exception.run_id',
             'es_db_test.step_exception.ru_reference',
             'es_db_test.step_exception.step',
             'es_db_test.step_exception.survey_code']),
        {'schema': 'es_db_test'}
    )

    survey_period = Column(String(6), primary_key=True, nullable=False)
    question_number = Column(String(8), primary_key=True, nullable=False)
    run_id = Column(Integer, primary_key=True, nullable=False)
    ru_reference = Column(String(11), primary_key=True, nullable=False)
    step = Column(String(10), primary_key=True, nullable=False)
    survey_code = Column(CHAR(3), primary_key=True, nullable=False)
    anomaly_description = Column(String(20), nullable=False)

    step_exception = relationship('StepException')


class FailedVET(Base):
    __tablename__ = 'failed_vet'
    __table_args__ = (
        ForeignKeyConstraint(
            ['survey_period', 'question_number', 'run_id', 'ru_reference',
             'step',
             'survey_code'],
            ['es_db_test.question_anomaly.survey_period',
             'es_db_test.question_anomaly.question_number',
             'es_db_test.question_anomaly.run_id',
             'es_db_test.question_anomaly.ru_reference',
             'es_db_test.question_anomaly.step',
             'es_db_test.question_anomaly.survey_code']),
        {'schema': 'es_db_test'}
    )
    failed_vet = Column(ForeignKey('es_db_test.vet.vet_code'),
                        primary_key=True, nullable=False)
    survey_period = Column(String(6), primary_key=True, nullable=False)
    question_number = Column(String(4), primary_key=True, nullable=False)
    run_id = Column(Integer, primary_key=True, nullable=False)
    ru_reference = Column(String(11), primary_key=True, nullable=False)
    step = Column(String(11), primary_key=True, nullable=False)
    survey_code = Column(String(25), primary_key=True, nullable=False)
