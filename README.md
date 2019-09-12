# es-system-of-record 

<a id='top'>

Contains code used to create the system of records, insert some test data and interact 
with said data through a series of updates, inserts and selects.<br><br>
Designed to be used as lambda on aws. The use of alembic and sqlalchemy means that this
 will(should) be database agnostic.
<hr>

##### Contents
[Testing Instructions](#test)<br>
[File listing](#files)<br>
[Example input params](#input)<br>
[Technical Debt](#td)<br>
[Useful Links](#links)<br>


<hr>

**Test Instructions** <br>
<a id='test'>
To run the basic tests, you just need to:
1 - Build:

```
./do.sh build
```
2 - Test:

```
./do.sh test
```
<hr>
In order to create a postgres database locally and fill it with test data in order to 
run the query lambda and recieve test output.:


1 - Build docker images and bring up postgres
```
./do.sh build
./do.sh start postgres
```
2 - Start a postgres container running psql and create the DB
```
./do.sh run postgres psql -h postgres --user postgres
```
Then run following commands in psql
```
CREATE DATABASE es_results_db;
\c es_results_db;
CREATE SCHEMA es_db_test;
```
3 - Run alembic to migrate to latest version of DB:
```
./do.sh run python alembic upgrade --verbose head
```
4 - Now you can run the insert_test_data script:
```buildoutcfg
./do.sh run python bash
```
Open the alembic ini and copy the sqlalchemy.url.
Then within the python container:
```
Database_Location=<thesqlalchemy.url> python insert_test_data.py
```
Database should now be stocked with test data, you can run the various queries in the 
same way, eg:
```
Database_Location=<thesqlalchemy.url> python get_query.py
```
[(Back to top)](#top)
<hr>
<a id='files'>

**Query Lambda** <br>
create_query -> For inserting new queries into database<br>
get_all_reference_data -> Returns all data<br>
get_contributor -> Return data for a given contributor<br>
get_query -> Return data for a given query<br>
get_query_dashboard -> As getQuery, but also includes contributor name.<br>
get_survey_periods -> Return data for a given survey/period<br>
update_query -> Update a query<br>
update_survey_period -> Update a survey/period<br>
update_contributor -> Update a contributor<br>
<br>
[(Back to top)](#top)

**Non-sor Functions**<br>
insert_test_data -> Inserts test data into database to enable each of the lambda to be 
tested.<br>
run_alembic -> May be an old file. Runs the alembic upgrade/downgrade without needing 
to goto command line.<br>
io_validation -> Marshmallow schema to allow input/output validation.<br>
empty_database -> Used during alembic downgrade.<br>
db_model -> A model of the database tables, used by sqlAlchemy.<br>
base_migration_model -> Used during alembic upgrade to create all db tables.<br>
alchemy_functions.py -> Used by all of the system of records query lambda. Provides 
standard functions for using sqlalchemy.<br>
README -> This file.....

[(Back to top)](#top)

## Example input parameters to methods <a id='input'>
Example data to use in calling the methods.:

```
getAllRefData:
  {}
```  

```
getContributor:
  {
  "RURef": "77700000001"
  }
 ``` 
 
 ```
updateContributor:
  {
  "rureference": "77700000001",
  "surveyperiod": "201803",
  "surveyoutputcode": "066",
  "additionalcomments": "Recently Had Machinery Break Down.",
  "contributorcomments": "Lower Than Usual values Are Correct."
  }
 ```
 
 ```
  createQuery:
  {
    "currentperiod": 201806,
    "datetimeraised": "2019-06-27",
    "generalspecificflag": false,
    "industrygroup": "Construction",
    "lastqueryupdate": "2019-06-27",
    "periodqueryrelates": "201803",
    "queryactive": true,
    "querydescription": "Company Data Suspicous.",
    "queryreference": 1,
    "querystatus": "open",
    "querytype": "Register",
    "raisedby": "Anna McCaffery",
    "resultsstate": "NA",
    "runreference": 1,
    "rureference": "77700000001",
    "surveyoutputcode": "066",
    "targetresolutiondate": "2019-06-30",
    "Exceptions": [
      {
        "errorcode": "ERR0867",
        "errordescription": "QuestionNo Failed",
        "queryreference": 2,
        "runreference": 1,
        "rureference": "77700000001",
        "step": "VETs",
        "surveyoutputcode": "066",
        "surveyperiod": "201803",
        "Anomalies": [
          {
            "description": "Validation Failure On Concrete Agg.",
            "questionno": "Q605",
            "runreference": 1,
            "rureference": "77700000001",
            "step": "VETs",
            "surveyoutputcode": "066",
            "surveyperiod": "201803",
            "FailedVETs": [
              {
                "description": "First Validation",
                "failedvet": 123,
                "questionno": "Q605",
                "runreference": 1,
                "rureference": "77700000001",
                "step": "VETs",
                "surveyoutputcode": "066",
                "surveyperiod": "201803"
              }
            ]
          }
        ]
      }
    ],
    "QueryTasks": [
      {
        "nextplannedaction": "Update Data",
        "queryreference": 1,
        "responserequiredby": "2019-06-28",
        "taskdescription": "Call Company To See If Figures Incorrect.",
        "taskresponsibility": "Bill Bob",
        "taskseqno": 1,
        "taskstatus": "In Progress",
        "whenactionrequired": "2018-07-12",
        "QueryTaskUpdates": [
          {
            "lastupdated": "2019-06-27",
            "queryreference": 1,
            "taskseqno": 1,
            "taskupdatedescription": "Contact Not In Office Until Tomorrow",
            "updatedby": "Bill Bob"
          }
        ]
      }
    ]
  }
  
  ```
  
  ```
  getQuery/getQueryDashboard:
  {
  "QueryReference": 1,
  "PeriodQueryRelates": "",
  "QueryType": "",
  "RUReference": "77700000001",
  "SurveyOutputCode": "",
  "QueryStatus": "",
  "RunReference": ""
  }
  OR
  {
  "QueryReference": 2,
  "PeriodQueryRelates": "",
  "QueryType": "",
  "RUReference": "",
  "SurveyOutputCode": "066",
  "QueryStatus": "",
  "RunReference": ""
  }
  OR
  {
    "QueryReference": 1,
    "PeriodQueryRelates": "",
    "QueryType": "",
    "RUReference": "",
    "SurveyOutputCode": "",
    "QueryStatus": "",
    "RunReference": ""
  }
  ```
  
  ```
Update Query:
  {
    "currentperiod": 201806,
    "datetimeraised": "2019-06-27",
    "generalspecificflag": false,
    "industrygroup": "Construction",
    "lastqueryupdate": "2019-06-27",
    "periodqueryrelates": "201803",
    "queryactive": true,
    "querydescription": "Company Data Suspisously.",
    "queryreference": 1,
    "querystatus": "open",
    "querytype": "Register",
    "raisedby": "Anna McCaffery",
    "resultsstate": "NA",
    "runreference": 1,
    "rureference": "77700000001",
    "surveyoutputcode": "066",
    "targetresolutiondate": "2019-06-30",
    "Exceptions": [],
    "QueryTasks": [
      {
        "nextplannedaction": "Update Data",
        "queryreference": 1,
        "responserequiredby": "2019-06-28",
        "taskdescription": "Call Company To See If Figures Incorrect.",
        "taskresponsibility": "Bill Bob",
        "taskseqno": 1,
        "taskstatus": "In Progress",
        "whenactionrequired": "2018-07-13",
        "QueryTaskUpdates": [
          {
            "lastupdated": "2019-06-27",
            "queryreference": 1,
            "taskseqno": 1,
            "taskupdatedescription": "Contact Not In Office Until Tomorrow",
            "updatedby": "Bill Bob"
          },
          {
            "lastupdated": "2019-06-30",
            "queryreference": 1,
            "taskseqno": 1,
            "taskupdatedescription": "Contact In Office Now",
            "updatedby": "Bob Bob"
          }
        ]
      }
    ]
  }
```
[(Back to top)](#top)

<hr>

**Technical Debt** <a id='td'>
1) Lots of the INSERTS have On Conflict Do Nothing or Do Update. These may need to be removed or have more conditions on them.
2) Code sometimes allows to work and sometimes doesn't the functions if no rows are returned from the gets. This will need some refinement.
3) Current SQL based on the IBM BPM Alpha and do not represent an accurate picture of what data should be allowed to be updated.

[(Back to top)](#top)

<hr>

**Useful Links** <a id='links'>
<br>

[Alembic](https://collaborate2.ons.gov.uk/confluence/pages/viewpage.action?spaceKey=ESD&title=Alembic+Migrations) 

[API Gateway](https://collaborate2.ons.gov.uk/confluence/display/ESD/AWS+API+Gateway)

[Marshmallow](https://collaborate2.ons.gov.uk/confluence/display/ESD/Marshmallow)

[(Back to top)](#top)
