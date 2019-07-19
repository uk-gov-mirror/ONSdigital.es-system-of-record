import psycopg2
import pandas.io.sql as psql


def main():
    usr = ''
    pas = ''

    search_list = ['Failed_VET',
                   'Question_Anomaly',
                   'Step_Exception',
                   'Query_Task_Update',
                   'Query_Task',
                   'Query',
                   'Survey_Contact',
                   'Contact',
                   'Contributor_Survey_Period',
                   'Survey_Period',
                   'Survey_Enrolment',
                   'Contributor',
                   'Survey',
                   'Query_Type',
                   'VET',
                   'GOR_Regions',
                   'SSR_Old_Regions']

    connection = psycopg2.connect(host="", database="es_results_db", user=usr, password=pas)

    for table in search_list:
        print(table)
        psql.execute("DELETE FROM es_db_test." + table + ";", connection)
        psql.execute("DROP TABLE es_db_test." + table + ";", connection)
    connection.commit()

    make_list = ['create table es_db_test.SSR_Old_Regions (IDBR varchar(2) not null, SSRReference smallint, RegionName varchar(255), primary key (IDBR));',
                 'create table es_db_test.GOR_Regions (IDBR varchar(2) not null, GORReference smallint, RegionName varchar(255), primary key (IDBR));',
                 'create table es_db_test.VET (VET integer not null, Description varchar(255), primary key (VET));',
                 'create table es_db_test.Query_Type (QueryType varchar(50) primary key , Description varchar(255));',
                 'create table es_db_test.Survey (SurveyOutputCode varchar(25) not null unique, SurveyName varchar(30) not null, primary key (SurveyOutputCode));',
                 'create table es_db_test.Contributor (ParentRUReference varchar(11), RUReference varchar(11) not null, HouseNameNo varchar(50) not null, Street varchar(100) not null, AdditionalAddressLine varchar(100), TownCity varchar(60) not null, County varchar(50) not null, Country varchar(55), Postcode varchar(9) not null, BirthDate date, BusinessProfilingTeamCase boolean, Contact varchar(255), DeathDate date, EnforcementFlag boolean, EnforcementStatus varchar(255), Fax varchar(25), ContributorName varchar(255), ProfileInformation text, SIC2003 int, SIC2007 int, Telephone varchar(25), primary key (RUReference));',
                 'create table es_db_test.Survey_Enrolment (RUReference varchar(11) not null, SurveyOutputCode varchar(25) not null, NoOfCurrentConsecutivePeriodsOfNonResponse smallint, NoOfPeriodsWithOutstandingQueries smallint, PeriodOfEnrolment varchar(10) not null, primary key (RUReference, SurveyOutputCode), foreign key (RUReference) references es_db_test.Contributor(RUReference), foreign key (SurveyOutputCode) references es_db_test.Survey(SurveyOutputCode));',
                 'create table es_db_test.Survey_Period (SurveyPeriod varchar(6) not null, SurveyOutputCode varchar(25) not null, ActivePeriod boolean, NoOfResponses smallint, NumberCleared smallint, SampleSize smallint, NumberClearedFirstTime smallint, primary key (SurveyPeriod, SurveyOutputCode), foreign key (SurveyOutputCode) references es_db_test.Survey(SurveyOutputCode));',
                 'create table es_db_test.Contributor_Survey_Period (RUReference varchar(11) not null, SurveyOutputCode varchar(25) not null, SurveyPeriod varchar(6) not null, AdditionalComments text, ContributorComments text, LastUpdated date, NoOfActiveQueriesInPeriod smallint, NoOfContributorinteractionsInPeriod smallint, PriorityResponseList varchar(255), ResponseStatus varchar(255), ShortDescription varchar(255), WhenStatusLastChanged date, primary key (RUReference, SurveyOutputCode, SurveyPeriod), foreign key (RUReference, SurveyOutputCode) references es_db_test.Survey_Enrolment(RUReference, SurveyOutputCode), foreign key (SurveyOutputCode, SurveyPeriod) references es_db_test.Survey_Period(SurveyOutputCode, SurveyPeriod));',
                 'create table es_db_test.Contact (ContactReference varchar(25) primary key, HouseNameNo varchar(50) not null, Street varchar(100), AdditionalAddressLine varchar(100), TownCity varchar(60) not null, County varchar(50) not null, Country varchar(55), Postcode varchar(9) not null, ContactConstraints text, ContactEmail varchar(255), ContactFax varchar(25), ContactName varchar(255), ContactOrganisation varchar(255), ContactPreferences  text, ContactTelephone varchar(25));',
                 'create table es_db_test.Survey_Contact (ContactReference varchar(25) not null, RUReference varchar(11) not null, SurveyOutputCode varchar(25) not null, EffectiveEndDate date, EffectiveStartDate date, primary key (ContactReference, RUReference, SurveyOutputCode), foreign key (ContactReference) references es_db_test.Contact(ContactReference), foreign key (RUReference, SurveyOutputCode) references es_db_test.Survey_Enrolment(RUReference, SurveyOutputCode));',
                 'create table es_db_test.Query (QueryReference SERIAL, QueryType varchar(50) not null, RUReference varchar(11) null, SurveyOutputCode varchar(25) null, PeriodQueryRelates varchar(25) null, CurrentPeriod varchar(6), RunReference int null, DateTimeRaised date, GeneralSpecificFlag boolean, IndustryGroup varchar(255), LastQueryUpdate date, QueryActive boolean, QueryDescription text, QueryStatus varchar(255), RaisedBy varchar(255), ResultsState varchar(255), TargetResolutionDate date, primary key (QueryReference), foreign key (QueryType) references es_db_test.Query_Type(QueryType), foreign key (PeriodQueryRelates, RUReference, SurveyOutputCode) references es_db_test.Contributor_Survey_Period(SurveyPeriod, RUReference, SurveyOutputCode));',
                 'create table es_db_test.Query_Task (TaskSeqNo int not null, QueryReference integer, ResponseRequiredBy date, TaskDescription text, TaskResponsibility varchar(255), TaskStatus varchar(255), NextPlannedAction varchar(255), WhenActionRequired date, primary key (TaskSeqNo, QueryReference), foreign key (QueryReference) references es_db_test.Query(QueryReference));',
                 'create table es_db_test.Query_Task_Update (TaskSeqNo int, QueryReference integer, LastUpdated date, TaskUpdateDescription varchar(255), UpdatedBy varchar(50), primary key (TaskSeqNo, QueryReference, LastUpdated), foreign key (TaskSeqNo, QueryReference) references es_db_test.Query_Task(TaskSeqNo, QueryReference));',
                 'create table es_db_test.Step_Exception (QueryReference integer not null, SurveyPeriod varchar(6) not null, RunReference int not null, RUReference varchar(11) not null, Step varchar(11) not null, SurveyOutputCode varchar(25) not null, ErrorCode varchar(25) not null, ErrorDescription varchar(255) not null, primary key (SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode), foreign key (QueryReference) references es_db_test.Query(QueryReference), foreign key (SurveyPeriod, RUReference, SurveyOutputCode) references es_db_test.Contributor_Survey_Period( SurveyPeriod, RUReference, SurveyOutputCode));',
                 'create table es_db_test.Question_Anomaly (SurveyPeriod varchar(6) not null, QuestionNo varchar(8) not null, RunReference int not null, RUReference varchar(11) not null, Step varchar(11) not null, SurveyOutputCode varchar(25) not null, Description varchar(255) not null, primary key (SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode), foreign key (SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode) references es_db_test.Step_Exception(SurveyPeriod, RunReference, RUReference, Step, SurveyOutputCode));',
                 'create table es_db_test.Failed_VET (FailedVET integer not null, SurveyPeriod varchar(6) not null, QuestionNo varchar(8) not null, RunReference int not null, RUReference varchar(11) not null, Step varchar(11) not null, SurveyOutputCode varchar(25) not null, primary key (FailedVET, SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode), foreign key (FailedVET) references es_db_test.VET(VET), foreign key (SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode) references es_db_test.Question_Anomaly(SurveyPeriod, QuestionNo, RunReference, RUReference, Step, SurveyOutputCode));'
                 ]

    for run_sql in make_list:
        print(run_sql)
        psql.execute(run_sql, connection)
    connection.commit()
    connection.close()
    return "Done"


y = main()
print(y)
