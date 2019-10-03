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
create_query -> For inserting new queries into the database.<br>
get_all_reference_data -> Returns all reference data.<br>
get_contributor -> Return data for a given contributor.<br>
get_query -> Return data for matching queries.<br>
get_query_dashboard -> As getQuery, but also includes contributor name.<br>
get_survey_periods -> Return data for matching survey/periods.<br>
update_query -> Update a query.<br>
update_survey_period -> Update a survey/period.<br>
update_contributor -> Update a contributor.<br>
<br>
[(Back to top)](#top)

**Non-SOR Functions**<br>
insert_test_data -> Inserts test data into database to enable each of the lambda to be 
tested.<br>
run_alembic -> May be an old file. Runs the Alembic upgrade/downgrade without needing 
to goto command line.<br>
io_validation -> Marshmallow schema to allow input/output validation.<br>
empty_database -> Used during Alembic downgrade.<br>
db_model -> A model of the database tables, used by SQLAlchemy.<br>
base_migration_model -> Used during Alembic upgrade to create all database tables.<br>
alchemy_functions.py -> Used by all of the system of records query lambda. Provides 
standard functions for using SQLAlchemy.<br>

[(Back to top)](#top)

<hr>

**Technical Debt** <a id='td'>
1) More conditions needed on the replacements for the On Conflict keywords.
2) Update Query needs an if statement for Query Task because this should be an update and an insert.
3) How the Get functions work when returning nothing needs to be better refined.
4) Current SQL based on the IBM BPM Alpha and they do not represent an accurate picture of what the data should be allowed to be updated.

[(Back to top)](#top)

<hr>

**Useful Links** <a id='links'>
<br>
[Example Input Data](./ExampleInput.md)

[Alembic](https://collaborate2.ons.gov.uk/confluence/pages/viewpage.action?spaceKey=ESD&title=Alembic+Migrations) 

[API Gateway](https://collaborate2.ons.gov.uk/confluence/display/ESD/AWS+API+Gateway)

[Marshmallow](https://collaborate2.ons.gov.uk/confluence/display/ESD/Marshmallow)

[(Back to top)](#top)
