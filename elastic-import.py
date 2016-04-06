#!/usr/bin/env python2

# MYSQL-python does not support python3

from __future__ import print_function

import MySQLdb as mdb
import sys
import json
from keyring import get_keyring

from algoliasearch import algoliasearch

def truthy(v):
    # 0 is True
    if type(v) == int:
        return True

    # but otherwise use python's concept of truthiness
    return bool(v)

def emit(name, _type=None):
    def fn(con, column, value):
        if _type is None:
            return name, value
        return name, _type(value)
    return fn

def emit_urls(con, columns, values):
    rv = None
    names = ['personal', 'myspace', 'facebook']
    for n, v in zip(names, values):
        if v:
            if rv is None:
                rv = {}
            rv[n] = v

    return 'urls', rv

cm_comediens = {
    'table': 'cm_comediens',
    'typename': 'actor',
    'columns': {
        'id_comedien': emit('id'),
        'comedien_ft': emit('name'),
        'comedien_perso_adresse': emit('address'),
        'comedien_perso_email': emit('email'),
        'infos_cv': emit('cv'),
        'infos_news': emit('news'),
        ('site_perso', 'lien_myspace', 'lien_facebook'): emit_urls,
    }
}

User = 'pbenso'

def check_str_list(l):
    for i in l:
        if type(i) != str:
            raise ValueError('expected list of strings')

def flatten_str_2l(l):
    '''Flatten a list of either strings or lists of strings'''

    result = []

    for i in l:
        t = type(i)
        if t == str:
            result.append(i)
        elif t in [list, tuple]:
            check_str_list(i)
            result.extend(i)

    return result

def generate_tables(con, desc):
    '''Generate a list of dictionaries from the table description
    Column names may be tuples'''
    result = []
    columns = flatten_str_2l(desc['columns'].keys())
    sql = 'SELECT %s FROM %s' % (', '.join(columns), desc['table'])
    cur.execute(sql)

    for row in cur:
        rv = { 'type': desc['typename'] }
        i = 0
        for cnames, emit in desc['columns'].iteritems():
            if type(cnames) == str:
                k, v = emit(con, cnames, row[i])
                i = i + 1
            else:
                l = len(cnames)
                k, v = emit(con, cnames, row[i:i+l])
                i = i + l

            if truthy(v):
                rv[k] = v

        result.append(rv)

    return result

con = mdb.connect(
    '139.162.157.175', User,
    get_keyring().get_password('studiotime', User),
    'cm');

cur = con.cursor()

actors = generate_tables(con, cm_comediens)

print(actors[0])

indexname = 'cm_import'

print('uploading to algolia as ', indexname)

pw = get_keyring().get_password('studiotime', 'algolia')
# print(pw)

algo = algoliasearch.Client('OYKQJYHGCO', pw)

algo.delete_index(indexname)

index = algo.init_index(indexname)
index.add_objects(actors)
