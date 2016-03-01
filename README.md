# Proof-of-concept Harvesting Application

This is a proof-of-concept application for harvesting of data from [Open Data Node](http://opendatanode.org/) via API showing how to work with relational datasets published using [uv-t-relationalDiffToCkan DPU](https://github.com/OpenDataNode/UVPlugin-relationalDiffToCKAN).

# How it works

This is proof-of-concept which:

1. simulates some dataset which is being updated gradually over time
2. provides example application which can download a copy that dataset and maintain this copy up-to-date

## Simulated dataset

'[t-generatedToRelational DPU](./t-generatedToRelational/)' is used in pipeline in ODN/UnifiedViews (schedulted to run every 15 minutes) to generate some dummy data:

| id  | data                |
| --- | ------------------- |
| 1   | 2016-02-29 23:03:43 |
| 2   | 2016-03-01 12:03:44 |

In short: Each day new item is added. EAch 15 minutes it gets updated (putting current date and time into 'data' column). The next day item gets deleted. That represents some quite typical datasets hich do change in time but do not track changes being done to them.

'uv-t-relationalDiffToCkan DPU' is then used to detect the differences/updates in that data and load it into ODN catalog, adding columns 'modified_timestamp' and 'deleted_timestamp' in the process to mark new, modified and deleted items:

| id  | data                | modified_timestamp  | deleted_timestamp   |
| --- | ------------------- | ------------------- | ------------------- |
| 1   | 2016-02-29 23:03:43 | 2016-03-01T00:03:47 | 2016-03-01T00:03:47 |
| 2   | 2016-03-01 12:03:44 | 2016-03-01T12:03:44 |                     |

So the purpose of 'uv-t-relationalDiffToCkan' is to add change tracking missing in original dataset:

- changed items gets their 'modified_timestamp' column update to data and time when change was detected
- deleted items are peserved, and both 'modified_timestamp' and 'deleted_timestamp' are updated (first one, because deletion is change too, second to indicate that the update was deletion)

You can see a result of that as dataset 'gen-data' at http://data.comsode.eu/dataset/gen-data .

## Harvesting application

The purpose of the harvesting application is to:

1. on first run: to get whole copy of the dataset to your PC
2. on subsequent runs: to download only changed items

Such harvesting is intended mainly for bigger datasets to avoid repeated download of items which have not changed since last harvesting.

Note: This application is written mainly for easy code reading, effective operation is only second goal. So by tweaking it, you might get better performance.

# How to use it

Simply:

1. checkout the app from GitHub
2. go into 'test_app' subdirectory
3. run the harvester: `python read_rest.py -u http://data.comsode.eu/api/action/ -i 210948d1-087f-4bb4-bc54-6f3205c4eefa -f /tmp/records.csv`

## More information

How to run harvesting application:

`python read_rest.py -u {host}/api/action/ -i {resource_id} -f {local file}`

Harvesting application has 4 parameters, 2 are mandatory.

- mandatory parameter `-i` followed by CKAN datastore resource ID to process
- parameter `-u` followed by URL of CKAN installation API, where the dataset was created (default value is `http://localhost/api/action/`)
- parameter `-a` followed by user API KEY, it is required to access resources of private datasets
- mandatory parameter '-f' followed by path to CSV file, where the records will be saved

First run of the script will download all rows of CKAN datastore resource and save them to CSV file specified in -f option. Subsequent runs will download and save only records which changed since last run.

Note: Internal CKAN IDs (column '_id') can be seen on catalogue but are skipped in local copy of data.

# Licensing

This proof-of-concept code is licensed as follows:

- t-generatedToRelational DPU is licensed under LGPLv3 (see (./t-generatedToRelational/LICENSE) file)
- harvesting application is licensed under Simplified BSD license (see (./test_app/LICENSE) file)
