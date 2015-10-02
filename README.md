# POC_HarvesterApp
Proof of concept application for harvesting of data from catalog via API

# How it works
In CKAN create public dataset called GENERATED_DATA.

In Unifiedviews create pipeline consisting of `T-GeneratedToRelational` DPU and `L-RelationalDiffToCkan` DPU conected from `output` (`T-GeneratedToRelational`) to `tablesInput` (`L-RelationalDiffToCkan`) and schedule the pipeline to execute every 15 minutes.

To test CKAN REST API run script `read_rest.py` from test_app directory.
It has 3 parameters.
- mandatory parameter `-d` followed by name of the dataset to browse. (Text after the last slash in the dataset URL)
- parameter `-u` followed by URL of CKAN installation API, where the dataset was created (Default value is `http://localhost/api/action/`)
- parameter `-i` followed by ID of the record to show

The script with only mandatory parameter `-d` set, will list IDs of all records from the table.
