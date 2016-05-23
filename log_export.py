#!/usr/bin/env python2

from __future__ import print_function
import csv
import itertools
import collections
from elasticsearch import Elasticsearch
from datetime import datetime

not_analyzed = { 'type': 'string', 'index': 'not_analyzed' }

mapping = {
    'properties': {
        'user_id': { 'type': 'integer' },
        'actor_id': { 'type': 'integer' },
        'results': { 'type': 'integer' },
        'search_type': not_analyzed,
        'name': not_analyzed,
        'gender': not_analyzed,
        'age_simple': not_analyzed,
        'sample_type': not_analyzed,
        'interpretation': not_analyzed,
        'language': not_analyzed,
        'age': not_analyzed,
        'singing_voice': not_analyzed,
        'timbre': not_analyzed,
        'double': not_analyzed
    }
}

columns = ['user_id', 'actor_id', 'results', 'search_type', 'name', 'gender', 'age_simple',
           'sample_type', 'interpretation', 'language', 'age', 'singing_voice', 'timbre',
           'double', 'timestamp']

def skip(iterable, at_start=0, at_end=0):
    it = iter(iterable)
    for x in itertools.islice(it, at_start):
        pass
    queue = collections.deque(itertools.islice(it, at_end))
    for x in it:
        queue.append(x)
        yield queue.popleft()

def export_csv(fname, es):

    indices = set()
    
    with open(fname, mode='r') as f:
        index = 1
        for row in skip(csv.reader(f, delimiter=';'), 1):
            d = dict(zip(columns, row))

            # convert timestamp
            timestamp = datetime.strptime(d['timestamp'], '%d/%m/%Y %H:%M:%S')
            d['timestamp'] = timestamp

            # convert some keys to int
            intkeys = [k for k in mapping['properties'].keys()
                       if mapping['properties'][k]['type'] == 'integer']
            for k in intkeys:
                if d[k]:
                    d[k] = int(d[k])
            
            # print(d)
            iname = 'log-csv-' + timestamp.strftime('%Y-%m-%d')

            if not iname in indices:
                es.indices.create(index=iname)
                print('creating mapping for %s' % iname)
                es.indices.put_mapping(doc_type='log_csv', index=iname, body=mapping)
                indices.add(iname)

            es.index(index=iname,
                     doc_type='log-csv', id=index, body=d)
            index = index + 1

if __name__ == '__main__':
    es = Elasticsearch()
    es.indices.delete('log-csv-*')
    export_csv('../log_recherche.csv', es)
