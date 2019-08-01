from marshmallow import Schema, fields


class ContributorSurveyPeriod_SurveyPeriod(Schema):
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    surveyperiod = fields.Str()
    additionalcomments = fields.Str()
    contributorcomments = fields.Str()
    lastupdated = fields.Date()
    noofactivequeriesinperiod = fields.Int()
    noofcontributorinteractionsinperiod = fields.Int()
    priorityresponselist = fields.Str()
    responsestatus = fields.Str()
    shortdescription = fields.Str()
    whenstatuslastchanged = fields.Date()
    activeperiod = fields.Bool()
    noofresponses = fields.Int()
    numbercleared = fields.Int()
    samplesize = fields.Int()
    numberclearedfirsttime = fields.Int()


class SurveyContact_Contact(Schema):
    contactreference = fields.Str()
    housenameno = fields.Str()
    street = fields.Str()
    additionaladdressline = fields.Str()
    towncity = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    contactconstraints = fields.Str()
    contactfax = fields.Str()
    contactname = fields.Str()
    contactorganisation = fields.Str()
    contactpreferences = fields.Str()
    contacttelephone = fields.Str()
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    effectiveenddate = fields.Date()
    effectivestartdate = fields.Date()


class SurveyEnrolment(Schema):
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    noofcurrentconsecutiveperiodsofnonresponse = fields.Int()
    noofperiodswithoutstandingqueries = fields.Int()
    periodofenrolment = fields.Str()
    Period = fields.Nested(ContributorSurveyPeriod_SurveyPeriod(many=True))
    Contacts = fields.Nested(SurveyContact_Contact(many=True))


class Contributor(Schema):
    rureference = fields.Str()
    parentrureference = fields.Str()
    housenameno = fields.Str()
    street = fields.Str()
    additionaladdressline = fields.Str()
    towncity = fields.Str()
    county = fields.Str()
    country = fields.Str()
    postcode = fields.Str()
    birthdate = fields.Date()
    businessprofilingteamcase = fields.Bool()
    contact = fields.Str()
    deathdate = fields.Date()
    enforcementflag = fields.Bool()
    enforcementstatus = fields.Str()
    fax = fields.Str()
    contributorname = fields.Str()
    profileinformation = fields.Str()
    sic2003 = fields.Int()
    sic2007 = fields.Int()
    telephone = fields.Str()
    Surveys = fields.Nested(SurveyEnrolment(many=True))


class QueryTaskUpdates(Schema):
    taskseqno = fields.Int()
    queryreference = fields.Int()
    lastupdated = fields.Date()
    taskupdateddescription = fields.Str()
    updatedby = fields.Str()


class QueryTasks(Schema):
    taskseqno = fields.Int()
    queryreference = fields.Int()
    responserequiredby = fields.Date()
    taskdescription = fields.Str()
    taskresponsibility = fields.Str()
    taskstatus = fields.Str()
    nextplannedaction = fields.Str()
    whenactionrequired = fields.Date()
    QueryTaskUpdates = fields.Nested(QueryTaskUpdates(many=True))


class FailedVETs_VETs(Schema):
    failedvet = fields.Int()
    surveyperiod = fields.Str()
    questionno = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    description = fields.Str()


class Anomalies(Schema):
    surveyperiod = fields.Str()
    questionno = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    description = fields.Str()
    FailedVETs = fields.Nested(FailedVETs_VETs(many=True))


class Exceptions(Schema):
    queryreference = fields.Int()
    surveyperiod = fields.Str()
    runreference = fields.Int()
    rureference = fields.Str()
    step = fields.Str()
    surveyoutputcode = fields.Str()
    errorcode = fields.Str()
    errordescription = fields.Str()
    Anomalies = fields.Nested(Anomalies(many=True))


class Query(Schema):
    queryreference = fields.Int()
    query = fields.Str()
    rureference = fields.Str()
    surveyoutputcode = fields.Str()
    periodqueryrelates = fields.Str()
    currentperiod = fields.Str()
    datetimeraised = fields.Date()
    generalspecificflag = fields.Bool()
    industrygroup = fields.Str()
    lastqueryupdate = fields.Date()
    queryactive = fields.Bool()
    querydescription = fields.Str()
    querystatus = fields.Str()
    raisedby = fields.Str()
    resultsstate = fields.Str()
    targetresolutiondate = fields.Date()
    Exceptions = fields.Nested(Exceptions(many=True))
    QueryTasks = fields.Nested(QueryTasks(many=True))


class ContributorSearch(Schema):
    RURef = fields.Str(required=True, allow_none=False)


class QuerySearch(Schema):
    QueryReference = fields.Int(required=True, allow_none=True)
    PeriodQueryRelates = fields.Str(required=True, allow_none=True)
    QueryType = fields.Str(required=True, allow_none=True)
    RUReference = fields.Str(required=True, allow_none=True)
    SurveyOutputCode = fields.Str(required=True, allow_none=True)
    QueryStatus = fields.Str(required=True, allow_none=True)


class ContributorUpdate(Schema):
    rureference = fields.Str()
    surveyperiod = fields.Str()
    surveyoutputcode = fields.Str()
    additionalcomments = fields.Str()
    contributorcomments = fields.Str()


class FindSurvey(Schema):
    SurveyPeriod = fields.Str()
    SurveyOutputCode = fields.Str()
