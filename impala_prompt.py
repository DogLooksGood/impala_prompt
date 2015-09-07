# !/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from prompt_toolkit.shortcuts import get_input
from prompt_toolkit.filters import Always
from prompt_toolkit.history import History
from impala.dbapi import connect
from pprint import pprint
from prompt_toolkit.contrib.completers import WordCompleter
import time
import sys
import re
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, help='host of impala')
args = parser.parse_args()

format_mode = False
multi_mode = False


def _check_one(info=None):
    if not info:
        return None
    elif len(info) > 1:
        raise Exception("Multiple rows returned for Database.get() query")
    else:
        return info[0]

class ImpalaWapper(object):

    def __init__(self, **kwargs):
        self.set_host(kwargs.get("host", "0.0.0.0"))
        self.set_port(kwargs.get("port", 21050))
        self.kwargs = kwargs
        self._db = None
        self.cursor = None
        self.connect()

    def __del__(self):
        self.close()

    def set_host(self, host):
        self.host = host

    def get_host(self):
        return self.host

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    def reconnect(self):
        self.close()
        self.connect()

    def connect(self):
        _base_ = {"host": self.get_host(),
                  "port": self.get_port()
                  }
        _base_.update(**self.kwargs)
        self._db = connect(**_base_)
        self.cursor = self._cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self._db:
            self._db.close()
            self._db = None

    def _cursor(self):
        if self._db:
            return self._db.cursor()
        else:
            raise Exception("Impala not connect")

    def raw_query(self, query, **kwargs):
        self.cursor.execute(query, parameters=kwargs)
        try:
            table_keys = [keys[0] for keys in self.cursor.description]
            table_vales = [value for value in self.cursor]
            return table_keys, table_vales
        except:
            return None, None

    def get(self, query, **kwargs):
        rows = self.query(query, **kwargs)
        return _check_one(rows)

    def one(self, query, **kwargs):
        rows = self.onelist(query, **kwargs)
        return _check_one(rows)

    def query(self, query, **kwargs):
        keys, values = self.raw_query(query, **kwargs)
        #return [dict(zip(keys, value)) for value in values]
        return keys, values

    def onelist(self, query, **kwargs):
        _, values = self.raw_query(query, **kwargs)
        return values

    def oneset(self, query, **kwargs):
        return set(self.onelist(query, **kwargs))

    def execute(self, query, **kwargs):
        self.cursor.execute(query, parameters=kwargs)
        return None


def print_table(data):
    width_list = [0 for x in xrange(0, len(data[0]))]
    for line in data:
        for i, t in enumerate(line):
            if len(t) > width_list[i]:
                width_list[i] = len(t)
    hr = "-" * (sum(width_list) + 2*len(width_list) + 1) + "\n"
    output = hr
    width_list = map(lambda x: x + 2, width_list)
    for idx, line in enumerate(data):
        for i, t in enumerate(line):
            t = t.decode('u8')
            output += "|"
            output += " "*(width_list[i] - len(t) - 1 - len(re.findall(ur'[\u4300-\u9fa5]', t)))
            output += "%s" % t
        output += "|\n"
        if idx == 0:
            output += hr
    output += hr
    print output
    print "%d rows." % (len(data) - 1)

def rescure_replace_none(i):
    if isinstance(i, list) or isinstance(i, set) or isinstance(i, tuple):
        return map(rescure_replace_none, i)
    elif isinstance(i, dict):
        return i.__repr__()
    else:
        return str(i)

def execute(command):
    global format_mode, multi_mode
    if not command:
        return
    if command == r"\f":
        format_mode = not format_mode
        if format_mode:
            print "Format-mode is ON."
        else:
            print "Format-mode is OFF."
    elif command == r"\m":
        multi_mode = not multi_mode
        if multi_mode:
            print "Multiline-mode is ON."
            print "Press [Meta-Enter] or [Esc] followed by [Enter] to accept input."
        else:
            print "Multiline-mode is OFF."
            print "Press [Enter] to accept input."
    else:
        time_begin = time.time()
        try:
            keys, values = c.query(command)
            if keys is not None and values is not None:
                if format_mode:
                    data = [tuple(keys)] + map(tuple, rescure_replace_none(values))
                    print_table(data)
                else:
                    pprint(keys, indent=2, width=48)
                    rows, columns = os.popen('stty size', 'r').read().split()
                    print "*" * int(columns)
                    pprint(values, indent=2, width=48)
        except Exception as ex:
            print ex.args[0]
        print "Spawn time: %s ms" % ((time.time() - time_begin) * 1000)


completer = WordCompleter([
    'SELECT',
    'INSERT',
    'TABLE',
    'DROP',
    'FROM',
    'WHERE',
    'ORDER BY',
    'GROUP BY',
    'AS',
    'IN',
    'AND',
    'OR',
    'IS',
    'NOT',
    'NULL',
    'VALUES',
    'USE',
    'SHOW DATABASES',
    'SHOW TABLES',
    'JOIN',
    'ON',
    'USING',
    'LIMIT'
])

def print_manual():
    print "Impala Prompt Version 0.01."
    print r"Use \f to toggle Format-mode (Json / Table)."
    print r"Use \m to toggle Multiline-mode."
    print "Press [Ctrl+C] to quit."

if __name__ == "__main__":
    print_manual()
    history = History()
    if args.host is None:
        print "Must specify host"
        sys.exit(1)

    config = {"host": args.host,
              }
    try:
        c = ImpalaWapper(**config)
        print "connect to impala server succeed!"
    except:
        print "connect to impala server failed!"
        import sys
        sys.exit(1)

    while True:
        try:
            command = get_input('impala>> ',
                                history=history,
                                enable_history_search=Always(),
                                multiline=multi_mode
                                #completer=completer
                                )
            execute(command)
        except KeyboardInterrupt:
            print "Bye!"
            break

