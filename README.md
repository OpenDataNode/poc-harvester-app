# POC_HarvesterApp
Proof of concept application for harvesting of data from catalog via API

# How it works
In CKAN create public dataset called GENERATED_DATA.

In Unifiedviews create pipeline consisting of `T-GeneratedToRelational` DPU and `L-RelationalDiffToCkan` DPU conected from `output` (`T-GeneratedToRelational`) to `tablesInput` (`L-RelationalDiffToCkan`) and schedule the pipeline to execute every 15 minutes.

To test CKAN REST API run script `read_rest.py` from test_app directory.
It has 3 parameters.
- mandatory parameter `-i` followed by CKAN datastore resource ID to process
- parameter `-u` followed by URL of CKAN installation API, where the dataset was created (Default value is `http://localhost/api/action/`)
- parameter `-a` followed by user API KEY, it is required to access resources of private datasets
- mandatory parameter '-f' followed by path to CSV file, where the records will be saved

First run of the script will download all rows of CKAN datastore resource and saved them to CSV file specified in one of the options. Subsequent runs will download and save only changed records from last run.

Run command:
python read_rest.py -u {host}/api/action/ -i {resource_id} -file /tmp/records.csv
