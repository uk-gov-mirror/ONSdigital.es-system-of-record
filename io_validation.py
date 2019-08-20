from marshmallow import Schema, fields


class Contributorsurvey_period_survey_period(Schema):
    ru_reference = fields.Str()
    survey_code = fields.Str()
    survey_period = fields.Str()
    additional_comments = fields.Str()
    contributor_comments = fields.Str()
    last_updated = fields.Date()
    active_queries = fields.Int()
    contributor_interactions = fields.Int()
    priority_response_list = fields.Str()
    response_status = fields.Str()
    short_description = fields.Str()
    status_changed = fields.Date()
    active_period = fields.Bool()
    number_of_responses = fields.Int()
    number_cleared = fields.Int()
    sample_size = fields.Int()
    number_cleared_first_time = fields.Int()


class SurveyContact_Contact(Schema):
    contact_reference = fields.Str()
    house_name_number = fields.Str()
    street = fields.Str()
    additional_address_line = fields.Str()
    town_city = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    contact_constraints = fields.Str()
    contact_fax = fields.Str()
    contact_name = fields.Str()
    contact_organisation = fields.Str()
    contact_preferences = fields.Str()
    contact_telephone = fields.Str()
    ru_reference = fields.Str()
    survey_code = fields.Str()
    effective_end_date = fields.Date()
    effective_start_date = fields.Date()


class SurveyEnrolment(Schema):
    ru_reference = fields.Str()
    survey_code = fields.Str()
    number_of_consecutive_non_response = fields.Int()
    number_of_periods_without_queries = fields.Int()
    period_of_enrolment = fields.Str()
    Period = fields.Nested(Contributorsurvey_period_survey_period(many=True))
    Contacts = fields.Nested(SurveyContact_Contact(many=True))


class Contributor(Schema):
    ru_reference = fields.Str()
    parent_ru_reference = fields.Str()
    house_name_number = fields.Str()
    street = fields.Str()
    additional_address_line = fields.Str()
    town_city = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    birth_date = fields.Date()
    business_profiling_teamcase = fields.Bool()
    contact = fields.Str()
    death_date = fields.Date()
    enforcement_flag = fields.Bool()
    enforcement_status = fields.Str()
    fax = fields.Str()
    contributor_name = fields.Str()
    profile_information = fields.Str()
    sic2003 = fields.Int()
    sic2007 = fields.Int()
    telephone = fields.Str()
    Surveys = fields.Nested(SurveyEnrolment(many=True))


class QueryTaskUpdates(Schema):
    task_sequence_number = fields.Int()
    query_reference = fields.Int()
    last_updated = fields.Date()
    task_update_description = fields.Str()
    updated_by = fields.Str()


class QueryTasks(Schema):
    task_sequence_number = fields.Int()
    query_reference = fields.Int()
    response_required_by = fields.Date()
    task_description = fields.Str()
    task_responsibility = fields.Str()
    task_status = fields.Str()
    next_planned_action = fields.Str()
    when_action_required = fields.Date()
    QueryTaskUpdates = fields.Nested(QueryTaskUpdates(many=True))


class FailedVETs_VETs(Schema):
    failed_vet = fields.Int()
    survey_period = fields.Str()
    question_number = fields.Str()
    run_id = fields.Int()
    ru_reference = fields.Str()
    step = fields.Str()
    survey_code = fields.Str()
    vet_description = fields.Str()


class Anomalies(Schema):
    survey_period = fields.Str()
    question_number = fields.Str()
    run_id = fields.Int()
    ru_reference = fields.Str()
    step = fields.Str()
    survey_code = fields.Str()
    anomaly_description = fields.Str()
    FailedVETs = fields.Nested(FailedVETs_VETs(many=True))


class Exceptions(Schema):
    query_reference = fields.Int()
    survey_period = fields.Str()
    run_id = fields.Int()
    ru_reference = fields.Str()
    step = fields.Str()
    survey_code = fields.Str()
    error_code = fields.Str()
    error_description = fields.Str()
    Anomalies = fields.Nested(Anomalies(many=True))


class Query(Schema):
    query_reference = fields.Int()
    query = fields.Str()
    ru_reference = fields.Str()
    survey_code = fields.Str()
    survey_period = fields.Str()
    current_period = fields.Str()
    date_raised = fields.Date()
    general_specific_flag = fields.Bool()
    industry_group = fields.Str()
    last_query_update = fields.Date()
    query_active = fields.Bool()
    query_description = fields.Str()
    query_status = fields.Str()
    raised_by = fields.Str()
    results_state = fields.Str()
    target_resolution_date = fields.Date()
    Exceptions = fields.Nested(Exceptions(many=True))
    QueryTasks = fields.Nested(QueryTasks(many=True))


class ContributorSearch(Schema):
    RURef = fields.Str(required=True, allow_none=False)


class QuerySearch(Schema):
    query_reference = fields.Int(required=True, allow_none=True)
    survey_period = fields.Str(required=True, allow_none=True)
    query_type = fields.Str(required=True, allow_none=True)
    ru_reference = fields.Str(required=True, allow_none=True)
    survey_code = fields.Str(required=True, allow_none=True)
    query_status = fields.Str(required=True, allow_none=True)


class ContributorUpdate(Schema):
    ru_reference = fields.Str()
    survey_period = fields.Str()
    survey_code = fields.Str()
    additional_comments = fields.Str()
    contributor_comments = fields.Str()


class FindSurvey(Schema):
    survey_period = fields.Str()
    survey_code = fields.Str()
