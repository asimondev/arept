from __future__ import print_function
from __future__ import absolute_import

import sys


def desc_stmt(table, owner=None, size_before=80, size_after=256):
    tab_owner = owner + "." if owner else ""
    ret = """
set linesize 80

describe %s%s

set linesize 256 

""" % (tab_owner, table)
    return ret


def file_created(file_name, level=1):
    start = " " * (level if level == 1 else level * 2)
    print("%s- File %s created." % (start, file_name))


def select_object(obj_owner, obj_name, obj_type, tab_prefix):
    ret = """
select to_char(created, 'dd-mon-yyyy hh24:mi:ss') obj_created, 
  to_char(last_ddl_time, 'dd-mon-yyyy hh24:mi:ss') obj_last_ddl
from %s_objects where owner = '%s' and object_name = '%s' and 
  object_type = '%s'
/ 
""" % (tab_prefix, obj_owner, obj_name, obj_type)

    return ret


def str_to_int(s, msg, do_exit=True):
    if s is None:
        return None

    try:
        return int(s)
    except ValueError:
        print(msg % s)
        if do_exit:
            sys.exit(1)
        else:
            return None
