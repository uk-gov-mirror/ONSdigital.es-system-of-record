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
    task_sequence_number = fields.Int(required=True)
    query_reference = fields.Int(required=True)
    last_updated = fields.Date(required=True)
    task_update_description = fields.Str(required=True)
    updated_by = fields.Str(required=True)


class QueryTasks(Schema):
    task_sequence_number = fields.Int(required=True)
    query_reference = fields.Int(required=True)
    response_required_by = fields.Date(required=True, allow_none=True)
    task_description = fields.Str(required=True)
    task_responsibility = fields.Str(required=True)
    task_status = fields.Str(required=True)
    next_planned_action = fields.Str(required=True, allow_none=True)
    when_action_required = fields.Date(required=True, allow_none=True)
    QueryTaskUpdates = fields.Nested(QueryTaskUpdates(required=True, many=True, allow_none=True))


class FailedVETs_VETs(Schema):
    failed_vet = fields.Int(required=True)
    survey_period = fields.Str(required=True)
    question_number = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    vet_description = fields.Str(required=True)


class Anomalies(Schema):
    survey_period = fields.Str(required=True)
    question_number = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    anomaly_description = fields.Str(required=True)
    FailedVETs = fields.Nested(FailedVETs_VETs(required=True, many=True, allow_none=True))


class Exceptions(Schema):
    query_reference = fields.Int(required=True)
    survey_period = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    error_code = fields.Str(required=True)
    error_description = fields.Str(required=True)
    Anomalies = fields.Nested(Anomalies(required=True, many=True))


class Query(Schema):
    query_reference = fields.Int()
    query_type = fields.Str(required=True)
    query_type_description = fields.Str()
    ru_reference = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    survey_period = fields.Str(required=True)
    current_period = fields.Str(required=True, allow_none=True)
    date_raised = fields.Date(required=True)
    general_specific_flag = fields.Bool(required=True, allow_none=True)
    industry_group = fields.Str(required=True, allow_none=True)
    last_query_update = fields.Date(required=True, allow_none=True)
    query_active = fields.Bool(required=True, allow_none=True)
    query_description = fields.Str(required=True)
    query_status = fields.Str(required=True)
    raised_by = fields.Str(required=True)
    results_state = fields.Str(required=True, allow_none=True)
    target_resolution_date = fields.Date(required=True, allow_none=True)
    Exceptions = fields.Nested(Exceptions(required=True, many=True, allow_none=True))
    QueryTasks = fields.Nested(QueryTasks(required=True, many=True))


class QueryReference(Schema):
    query_reference = fields.Int(required=True)


class ContributorSearch(Schema):
    ru_reference = fields.Str(required=True)


class QuerySearch(Schema):
    query_reference = fields.Int(required=False)
    survey_period = fields.Str(required=False)
    query_type = fields.Str(required=False)
    ru_reference = fields.Str(required=False)
    survey_code = fields.Str(required=False)
    query_status = fields.Str(required=False)


class ContributorUpdate(Schema):
    ru_reference = fields.Str()
    survey_period = fields.Str()
    survey_code = fields.Str()
    additional_comments = fields.Str()
    contributor_comments = fields.Str()


class FindSurvey(Schema):
    survey_period = fields.Str()
    survey_code = fields.Str()
