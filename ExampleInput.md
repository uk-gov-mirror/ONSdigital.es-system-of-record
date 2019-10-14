## Example input parameters to methods <a id='input'>
Example data to use in calling the methods:

```
get_all_reference_data:
  {}
```  

```
get_contributor:
  {
  "ru_reference": "77700000001"
  }
 ``` 
 
 ```
update_contributor:
  {
  "ru_reference": "77700000001",
  "survey_period": "201803",
  "survey_code": "066",
  "additional_comments": "Recently Had Machinery Break Down.",
  "contributor_comments": "Lower Than Usual Values Are Correct."
  }
 ```
 
 ```
  create_query:
  {
    "current_period": 201806,
    "date_raised": "2019-06-27",
    "general_specific_flag": false,
    "industry_group": "Construction",
    "last_query_update": "2019-06-27",
    "survey_period": "201803",
    "query_active": true,
    "query_description": "Company Data Suspicous.",
    "query_reference": 1,
    "query_status": "open",
    "query_type": "Register",
    "raised_by": "Anna McCaffery",
    "results_state": "NA",
    "run_id": 1,
    "ru_reference": "77700000001",
    "survey_code": "066",
    "target_resolution_date": "2019-06-30",
    "Exceptions": [
      {
        "error_code": "ERR0867",
        "error_description": "QuestionNo Failed",
        "query_reference": 2,
        "run_id": 1,
        "ru_reference": "77700000001",
        "step": "VETs",
        "survey_code": "066",
        "survey_period": "201803",
        "Anomalies": [
          {
            "anomaly_description": "Validation Failure On Concrete Agg.",
            "question_number": "Q605",
            "run_id": 1,
            "ru_reference": "77700000001",
            "step": "VETs",
            "survey_code": "066",
            "survey_period": "201803",
            "FailedVETs": [
              {
                "failed_vet": 123,
                "question_number": "Q605",
                "run_id": 1,
                "ru_reference": "77700000001",
                "step": "VETs",
                "survey_code": "066",
                "survey_period": "201803"
              }
            ]
          }
        ]
      }
    ],
    "QueryTasks": [
      {
        "next_planned_action": "Update Data",
        "query_reference": 1,
        "response_required_by": "2019-06-28",
        "task_description": "Call Company To See If Figures Incorrect.",
        "task_responsibility": "Bill Bob",
        "task_sequence_number": 1,
        "task_status": "In Progress",
        "when_action_required": "2018-07-12",
        "QueryTaskUpdates": [
          {
            "last_updated": "2019-06-27",
            "query_reference": 1,
            "task_sequence_number": 1,
            "task_update_description": "Contact Not In Office Until Tomorrow",
            "updated_by": "Bill Bob"
          }
        ]
      }
    ]
  }
  
  ```
  
  ```
  get_query/get_query_dashboard:
  {
  "query_reference": 1,
  "survey_period": "",
  "query_type": "",
  "ru_reference": "77700000001",
  "survey_code": "",
  "query_status": "",
  "run_id": ""
  }
  OR
  {
  "query_reference": 2,
  "survey_period": "",
  "query_type": "",
  "ru_reference": "",
  "survey_code": "066",
  "query_status": "",
  "run_id": ""
  }
  OR
  {
  "query_reference": 1,
  "survey_period": "",
  "query_type": "",
  "ru_reference": "",
  "survey_code": "",
  "query_status": "",
  "run_id": ""
  }
  ```
  
  ```
update_query:
  {
    "current_period": 201806,
    "date_raised": "2019-06-27",
    "general_specific_flag": false,
    "industry_group": "Construction",
    "last_query_update": "2019-06-27",
    "survey_period": "201803",
    "query_active": true,
    "query_description": "Company Data Suspisously.",
    "query_reference": 1,
    "query_status": "open",
    "query_type": "Register",
    "raised_by": "Anna McCaffery",
    "results_state": "NA",
    "run_id": 1,
    "ru_reference": "77700000001",
    "survey_code": "066",
    "target_resolution_date": "2019-06-30",
    "Exceptions": [],
    "QueryTasks": [
      {
        "next_planned_action": "Update Data",
        "query_reference": 1,
        "response_required_by": "2019-06-28",
        "task_description": "Call Company To See If Figures Incorrect.",
        "task_responsibility": "Bill Bob",
        "task_sequence_number": 1,
        "task_status": "In Progress",
        "when_action_required": "2018-07-13",
        "QueryTaskUpdates": [
          {
            "last_updated": "2019-06-27",
            "query_reference": 1,
            "task_sequence_number": 1,
            "task_update_description": "Contact Not In Office Until Tomorrow",
            "updated_by": "Bill Bob"
          },
          {
            "last_updated": "2019-06-30",
            "query_reference": 1,
            "task_sequence_number": 1,
            "task_update_description": "Contact In Office Now",
            "updated_by": "Bob Bob"
          }
        ]
      }
    ]
  }
```
[(Back to top)](#top)