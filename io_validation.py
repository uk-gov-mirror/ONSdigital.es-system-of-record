from marshmallow import Schema, fields


class CombinedContributorSurveyPeriod(Schema):
    ru_reference = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    survey_period = fields.Str(required=True)
    additional_comments = fields.Str(required=True, allow_none=True)
    contributor_comments = fields.Str(required=True, allow_none=True)
    last_updated = fields.Date(required=True)
    active_queries = fields.Int(required=True)
    contributor_interactions = fields.Int(required=True)
    priority_response_list = fields.Str(required=True, allow_none=True)
    response_status = fields.Str(required=True)
    short_description = fields.Str(required=True, allow_none=True)
    status_changed = fields.Date(required=True, allow_none=True)
    active_period = fields.Bool(required=True, allow_none=True)
    number_of_responses = fields.Int(required=True)
    number_cleared = fields.Int(required=True)
    sample_size = fields.Int(required=True)
    number_cleared_first_time = fields.Int(required=True)


class CombinedContact(Schema):
    contact_reference = fields.Str(required=True)
    house_name_number = fields.Str(required=True)
    street = fields.Str(required=True)
    additional_address_line = fields.Str(required=True, allow_none=True)
    town_city = fields.Str(required=True)
    county = fields.Str(required=True)
    country = fields.Str(required=True, allow_none=True)
    postcode = fields.Str(required=True)
    contact_constraints = fields.Str(required=True, allow_none=True)
    contact_fax = fields.Str(required=True, allow_none=True)
    contact_email = fields.Str(required=True, allow_none=True)
    contact_name = fields.Str(required=True)
    contact_organisation = fields.Str(required=True)
    contact_preferences = fields.Str(required=True, allow_none=True)
    contact_telephone = fields.Str(required=True, allow_none=True)
    ru_reference = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    effective_end_date = fields.Date(required=True, allow_none=True)
    effective_start_date = fields.Date(required=True)


class SurveyEnrolment(Schema):
    ru_reference = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    number_of_consecutive_non_response = fields.Int(required=True)
    number_of_periods_without_queries = fields.Int(required=True)
    period_of_enrolment = fields.Str(required=True)
    Periods = fields.Nested(CombinedContributorSurveyPeriod(many=True),
                            required=True)
    Contacts = fields.Nested(CombinedContact(many=True), required=True)


class Contributor(Schema):
    ru_reference = fields.Str(required=True)
    parent_ru_reference = fields.Str(required=True, allow_none=True)
    house_name_number = fields.Str(required=True)
    street = fields.Str(required=True)
    additional_address_line = fields.Str(required=True, allow_none=True)
    town_city = fields.Str(required=True)
    county = fields.Str(required=True)
    country = fields.Str(required=True, allow_none=True)
    postcode = fields.Str(required=True)
    birth_date = fields.Date(required=True)
    business_profiling_team_case = fields.Bool(required=True, allow_none=True)
    contact = fields.Str(required=True, allow_none=True)
    death_date = fields.Date(required=True, allow_none=True)
    enforcement_flag = fields.Bool(required=True, allow_none=True)
    enforcement_status = fields.Str(required=True, allow_none=True)
    fax = fields.Str(required=True, allow_none=True)
    contributor_name = fields.Str(required=True)
    profile_information = fields.Str(required=True, allow_none=True)
    sic2003 = fields.Int(required=True, allow_none=True)
    sic2007 = fields.Int(required=True, allow_none=True)
    telephone = fields.Str(required=True, allow_none=True)
    Surveys = fields.Nested(SurveyEnrolment(many=True), required=True,
                            allow_none=True)


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
    QueryTaskUpdates = fields.Nested(QueryTaskUpdates(many=True),
                                     required=True, allow_none=True)


class CombinedVETs(Schema):
    failed_vet = fields.Int(required=True)
    survey_period = fields.Str(required=True)
    question_number = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    vet_description = fields.Str()


class Anomalies(Schema):
    survey_period = fields.Str(required=True)
    question_number = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    anomaly_description = fields.Str(required=True)
    FailedVETs = fields.Nested(CombinedVETs(many=True), required=True,
                               allow_none=True)


class Exceptions(Schema):
    query_reference = fields.Int(required=True)
    survey_period = fields.Str(required=True)
    run_id = fields.Int(required=True)
    ru_reference = fields.Str(required=True)
    step = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    error_code = fields.Str(required=True)
    error_description = fields.Str(required=True)
    Anomalies = fields.Nested(Anomalies(many=True), required=True)


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
    Exceptions = fields.Nested(Exceptions(many=True), required=True,
                               allow_none=True)
    QueryTasks = fields.Nested(QueryTasks(many=True), required=True)


class Queries(Schema):
    Queries = fields.Nested(Query(many=True), allow_none=True)


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
    ru_reference = fields.Str(required=True)
    survey_period = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    additional_comments = fields.Str(required=True)
    contributor_comments = fields.Str(required=True)


class SurveySearch(Schema):
    survey_period = fields.Str(required=False)
    survey_code = fields.Str(required=False)


class SurveyPeriod(Schema):
    survey_period = fields.Str(required=True)
    survey_code = fields.Str(required=True)
    active_period = fields.Bool(required=True)
    number_of_responses = fields.Int(required=True)
    number_cleared = fields.Int(required=True)
    number_cleared_first_time = fields.Int(required=True)
    sample_size = fields.Int(required=True)


class QueryTypes(Schema):
    query_type = fields.Str(required=True)
    query_type_description = fields.Str(required=True)


class VETsCodes(Schema):
    vet_code = fields.Int(required=True)
    vet_description = fields.Str(required=True)


class Surveys(Schema):
    survey_code = fields.Str(required=True)
    survey_name = fields.Str(required=True)


class GorRegions(Schema):
    idbr_region = fields.Str(required=True)
    gor_reference = fields.Int(required=True)
    region_name = fields.Str(required=True)


class AllReferenceData(Schema):
    QueryTypes = fields.Nested(QueryTypes(many=True), required=True)
    VETsCodes = fields.Nested(VETsCodes(many=True), required=True)
    Surveys = fields.Nested(Surveys(many=True), required=True)
    GorRegions = fields.Nested(GorRegions(many=True), required=True)
