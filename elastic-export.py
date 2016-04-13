#!/usr/bin/env python2

# MYSQL-python does not support python3

from __future__ import print_function

import MySQLdb as mdb
import sys
import json
from keyring import get_keyring
from argparse import ArgumentParser

from algoliasearch import algoliasearch

# Cached cm_value_list, in the form id -> value
cm_values = {}

def emit(name, _type=None):
    def fn(con, column, value):
        if _type is None:
            return name, value
        return name, _type(value)

    return fn

def emit_vl(name):
    def fn(con, column, value):
        if value > 0:
            return name, cm_values[value]
        return name, None

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

def emit_interpretation(con, colums, values):
    return 'interpretation', [cm_values[v] for v in values if v]

def emit_id_media(con, column, value):
    return 'attributes', read_table(con, cm_sons, 'id_media = %d' % value)

def emit_id_comedien(con, column, value):
    return 'recordings', read_table(con, cm_medias, 'id_comedien = %d' % value)

cm_value_list = {
    'table': 'cm_value_list',
    'columns': {
        'id_value': emit('id_value'),
        'list_name': emit('list_name'),
        'tri': emit('tri'),
        'value': emit('value')
    }
}

cm_sons = {
    'table': 'cm_sons',
    'columns': {
        'qf_diffusion': emit('diffusion'),
        'qf_accent': emit('accent'),
        'qf_age': emit_vl('age'),
        'qf_cartoon': emit_vl('cartoon'),
        'qf_doublage': emit('dubbing'),
        ('qf_interpretation1', 'qf_interpretation2', 'qf_interpretation3'): emit_interpretation,
        'qf_imitation': emit_vl('imitation'),
        'qf_langue': emit_vl('language'),
        'qf_personnage': emit_vl('personality'),
        'qf_timbre': emit_vl('timbre'),
        'qf_chante': emit_vl('genre'),
        'qf_genre': emit('gender'),
        'timestamp_creation': emit('created'),
        'timestamp_modification': emit('modified')
    }
}

cm_medias = {
    'table': 'cm_medias',
    'columns': {
        'id_media': emit_id_media,
        'intitule': emit('title'),
        'filename': emit('filename'),
        'original_filename': emit('original')
    }
}

cm_comediens = {
    'table': 'cm_comediens',
    'typename': 'actor',
    'columns': {
        'id_comedien': emit_id_comedien,
        'comedien_ft': emit('name'),
        'comedien_perso_adresse': emit('address'),
        'comedien_perso_email': emit('email'),
        'infos_cv': emit('cv'),
        'infos_news': emit('news'),
        ('site_perso', 'lien_myspace', 'lien_facebook'): emit_urls,
    }
}

def truthy(v):
    # 0 is True
    if type(v) == int:
        return True

    # but otherwise use python's concept of truthiness
    return bool(v)

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

def read_table(con, meta, where=None):
    '''Generate a list of dictionaries from the meta table.
    Column names may be tuples'''
    result = []
    columns = flatten_str_2l(meta['columns'].keys())
    sql = 'SELECT %s FROM %s' % (', '.join(columns), meta['table'])

    if where:
        sql = sql + ' WHERE ' + where

    cur = con.cursor()

    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    print('%s: %d rows' % (sql, len(rows)))

    for row in rows:
        rv = {  }
        tn = meta.get('typename', None)
        if tn:
            rv['type'] = tn

        i = 0
        for cnames, emit in meta['columns'].iteritems():
            if type(cnames) == str:
                kv = emit(con, cnames, row[i])
                i = i + 1
            else:
                l = len(cnames)
                kv = emit(con, cnames, row[i:i+l])
                i = i + l

            try:
                k, v = kv

                if truthy(v):
                    rv[k] = v

            except ValueError:
                print(kv)
                raise

        result.append(rv)

    return result

def read_value_list(con):
    values = {}

    value_list = read_table(con, cm_value_list)
    # print(value_list)

    for v in value_list:
        _id = v['id_value']
        category = v['list_name']
        key = v['tri']
        value = v['value']

        # if not values.has_key(category):
        #     values[category] = {}
        #
        # values[category][key] = value

        values[_id] = value

    return values

if __name__ == '__main__':
    parser = ArgumentParser(description='export castingmachine database')
    parser.add_argument('-e', '--export', default=False, action='store_true')
    parser.add_argument('-u', '--user', default='pbenso')
    parser.add_argument('-s', '--server', default='139.162.157.175')
    parser.add_argument('-i', '--index',
                        default='cm_import',
                        help="index name (default is 'cm_import')")

    args = parser.parse_args()

    con = mdb.connect(
        args.server, args.user,
        get_keyring().get_password('studiotime', args.user),
        'cm');

    cm_values = read_value_list(con)
    # print(cm_values)

    # media = read_table(con, cm_medias, 'id_comedien = 19')
    # print(media)
    # sys.exit(0)

    actors = read_table(con, cm_comediens)

    print(actors[0])

    if args.export:
        indexname = args.index

        print('uploading to algolia as ', indexname)

        pw = get_keyring().get_password('studiotime', 'algolia')
        # print(pw)

        algo = algoliasearch.Client('OYKQJYHGCO', pw)

        algo.delete_index(indexname)

        index = algo.init_index(indexname)
        index.add_objects(actors)
