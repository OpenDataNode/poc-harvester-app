#!/usr/bin/env python
import optparse
import json
from restkit import Resource, BasicAuth, request

def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('-i', '--id', dest='id', default='', help='ID of record to select')
    parser.add_option('-d', '--dataset_name', dest='dataset', default='gen-data', help='Name of dataset to browse')
    parser.add_option('-u', '--url', dest='url', default='http://localhost/api/action/', help='URL of REST API (default: http://localhost/api/action/)')

    return parser.parse_args()

if __name__ == '__main__':
    (options, args) = parse_args()
    command_search = 'datastore_search_sql'
    command_package_show = 'package_show?id='

	#Creating request to retrieve resource_id from entered dataset
    if options.dataset:
        request01 = "{arg1}{arg2}{arg3}".format(arg1=options.url,arg2=command_package_show,arg3=options.dataset)
        print "Package_list request: {}".format(request01)
    else:
        print "Package name required!"
        raise SystemExit
	
    resource01 = Resource(request01)
    response01 = None
    try:
        response01 = resource01.get(headers = {'Content-Type' : 'application/json'})
        if response01.status_int == 200:
		    #parsing resource_id from JSON response
            json_result = json.loads(response01.body_string())
            res = json_result['result']
            for resource1 in res['resources']:
                resource_id=resource1['id']
                break
    except Exception:
        print 'Dataset {} doesn\'t exist!'.format(options.dataset)
        raise SystemExit
    finally:
        if response01:
            response01.close()

    #Creating request to retrieve data from CKAN resource (table)
    request02 = "{arg1}{arg2}".format(arg1=options.url,arg2=command_search)
    print "Resource search request: {}".format(request02)
    if not options.id:
        data='select \"id\" from \"{}\"'.format(resource_id)
    else:
        data='select * from \"{resource}\" where \"id\"={id}'.format(resource=resource_id,id=options.id)
    print "Using select: %s" % data
    json_data = {}
    json_data['sql'] = data
    resource02 = Resource(request02)
    response02 = None
    try:
        response02 = resource02.post(headers = {'Content-Type' : 'application/json'}, payload=json.dumps(json_data))
        if response02.status_int == 200:
            json_result = json.loads(response02.body_string())
			#parsing data from JSON response from CKAN
            res = json_result['result']
            for record in res['records']:
                id=record['id']
                if options.id != '':
                    data=record['data']
                    mod_date=record['modified_timestamp']
                    del2=record['deleted_timestamp']
                    print "id=%s; data=%s; modified_date=%s; deleted_date=%s" %(id, data, mod_date, del2)
                else:
                    print "id=%s" % id
    except Exception:
        print 'Error!'
    finally:
        if response02:
            response02.close()
