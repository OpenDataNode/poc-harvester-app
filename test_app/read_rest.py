#!/usr/bin/env python
'''
Copyright (c) 2015, Martin Virag.
Copyright (c) 2015, Peter Golian.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Simple CKAN datastore consumer example
On first run, it downloads all rows from CKAN datastore resource and saves them to
CSV file. On subsequent runs it downloads only records changed from last run.

Example of CKAN datastore resource used in this simple program:
id | data                | modified_timestamp  | deleted_timestamp
--------------------------------------------------------------------
0  | 2015-10-05 23:00:08 | 2015-10-06T00:00:10 | 2015-10-06T00:00:10
1  | 2015-10-06 13:00:28 | 2015-10-06T13:00:29 |

This example should work the same at least up to CKAN version 2.3.2

There can be little differences in API between different CKAN versions, latest:
CKAN API documentation: http://docs.ckan.org/en/latest/api/index.html
Datastore CKAN API docs: http://docs.ckan.org/en/latest/maintaining/datastore.html#the-datastore-api

You can check CKAN version with request to API: <host>/api/action/status_show

Run command:
python read_rest.py -u {host}/api/action/ -i {resource_id} -f /tmp/records.csv
'''
import csv
import optparse
import os
import json
import urllib, urllib2


def parse_args():
    usage = "python read_test.py [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-i', '--id', dest='resource_id', default=None, help='resource id (required)')
    parser.add_option('-u', '--url', dest='url', default='http://localhost/api/action/', help='URL of REST API (default: http://localhost/api/action/)')
    parser.add_option('-a', '--api_key', dest='api_key', default=None, help='API KEY is needed to access resources of private datasets')
    parser.add_option('-f', '--file', dest='file', default=None, help='Path to csv file, where the records will be saved (required)')

    (options, args) = parser.parse_args()
    # simple check for mandatory arguments
    if not options.resource_id or not options.file:
        parser.print_help()
        raise SystemExit
    
    return options, args

def send_request_to_CKAN(parameters_dict, url, api_key=None):
    request = urllib2.Request(url)
    # convert dictionary to encoded JSON string
    if parameters_dict:
        data_string = urllib.quote(json.dumps(parameters_dict))
    # accessing private dataset requires user's API KEY
    # for accessing public dataset it's not necessary
    if api_key:
        request.add_header('Authorization', api_key)
    try:
        response = None
        # Make the HTTP request.
        response = urllib2.urlopen(request, data_string)
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.read())
        return response_dict['result']
    except urllib2.HTTPError, e:
        print e.read() # some http errors return also JSON (see CKAN API doc)
        raise SystemExit
    finally:
        if response:
            response.close()

def print_records(records):
    for record in records:
        print u"id={0}; data={1}; modified_date={2}; deleted_date={3}"\
            .format(record['id'], record['data'], record['modified_timestamp'],
                    record['deleted_timestamp'])

class ResourceRecords:
    '''
    Class responsible for reading records from CSV file
    and updating the records in this file
    '''
    
    records = []
    fields = ['id', 'data', 'modified_timestamp', 'deleted_timestamp']
    last_update = None
    
    def __init__(self, csv_file_path):
        self.file_path = csv_file_path
    
    def update_records(self, update_records):
        for record in update_records:
            old_record = None
            for r in self.records:
                # warning: CKAN supports compound primary keys
                if r['id'] == record['id']: # found match
                    old_record = r
                    break
            
            if old_record: # updating records
                old_record.update(record)
            else: # adding new record
                self.records.append(record)
    
    def load_from_csv(self):
        if not os.path.exists(self.file_path):
            # creating empty csv
            file(self.file_path, "w").close()
            return
        
        # loading records from csv file
        with open(self.file_path, 'rb') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                self.records.append(row)
                # getting last modification date
                if not self.last_update or self.last_update < row['modified_timestamp']:
                    self.last_update = row['modified_timestamp']

    def write_to_csv(self):
        # writing records to csv file
        with open(self.file_path, 'wb') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=self.fields)
            writer.writeheader()
            for record in self.records:
                writer.writerow(record)

def get_ids_for_sql(sql_query, options):
    '''
    Calls datastore_search_sql API call with query given and returns only ids
    '''
    print 'sql query: {}'.format(sql_query)
    parameters_dict = {}
    parameters_dict['sql'] = sql_query
    # datastore_search_sql: paging can be done through sql
    records = send_request_to_CKAN(parameters_dict,
                                   options.url + 'datastore_search_sql',
                                   options.api_key)
    # retrieve only ids from JSON response
    return [r['id'] for r in records['records']]

def update_saved_records(saved_records, record_ids, options):
    '''
    Calls datastore_search for given record_ids and updates saved_records with
    retrieved records (the updated records aren't saved to CSV file yet, 
    saved_records.write_to_csv() must be called after this method)
    '''
    if not record_ids:
        print "No records were updated"
        raise SystemExit
    
    # datastore_search_sql returns all records matching the sql query
    # datastore_search needs to be done through paging
    LIMIT = 1000 # limit records per page/request, CKAN default value: 100
    offset = 0
    total = len(record_ids)
        
    # paginate through returned ids
    while offset < total:
        print "records: {0} - {1}".format(offset, offset + LIMIT - 1)
        parameters_dict = {}
        # identify CKAN datastore resource (mandatory)
        parameters_dict['resource_id'] = options.resource_id
        # filter only selected IDs
        parameters_dict['filters'] = {'id': record_ids}
        # we want only fields (columns) saved in csv
        parameters_dict['fields'] = saved_records.fields
        # paging parameters
        parameters_dict['limit'] = LIMIT
        parameters_dict['offset'] = offset
        # calling datastore_search API call
        response = send_request_to_CKAN(parameters_dict,
                                        options.url + 'datastore_search',
                                        options.api_key)
        
        print_records(response['records'])
        # update already saved records
        saved_records.update_records(response['records'])
        offset += LIMIT

if __name__ == '__main__':
    (options, args) = parse_args()

    saved_records = ResourceRecords(options.file)
    # load previously saved records
    saved_records.load_from_csv()
    # it would be better save last_update time to DB / file, determine it
    # from periodicity or use other way
    # for simplicity, it's read from CSV as latest modified_timestamp

    if not saved_records.last_update: # first run, there are no records saved yet
        print u"requesting all records for resource: {}".format(options.resource_id)
        sql_query = u'SELECT id FROM "{0}"'.format(options.resource_id)
        record_ids = get_ids_for_sql(sql_query, options)
        update_saved_records(saved_records, record_ids, options)
        
    else: # subsequent runs, request only changes
        print u"requesting records updated from {}".format(saved_records.last_update)
        sql_query = u'SELECT id FROM "{0}" WHERE modified_timestamp > \'{1}\''.format(options.resource_id, saved_records.last_update)
        record_ids = get_ids_for_sql(sql_query, options)
        print 'changed records from last request:'
        update_saved_records(saved_records, record_ids, options)
    
    print 'saving records to CSV'
    saved_records.write_to_csv()
