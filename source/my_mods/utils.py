from __future__ import print_function
from __future__ import absolute_import

import os.path
import sys

AREPT_HEADER=""

def set_arept_header(ver):
    global AREPT_HEADER
    AREPT_HEADER="AREPT Ver. %s  (https://github.com/asimondev/arept)" % ver


def get_arept_header():
    return AREPT_HEADER

def select_arept_header():
    return """set feedback off heading off
select '%s' from dual;
set feedback on heading on
prompt
""" % AREPT_HEADER


def desc_stmt(table, owner=None, size_before=80, size_after=256):
    tab_owner = owner + "." if owner else ""
    obj = "%s%s" % (tab_owner, table)
    ret = """
set linesize 80

prompt DESCRIBE %s
describe %s

set linesize 256 

""" % (obj, obj)
    return ret


def file_created(file_name, level=1):
    start = " " * (level if level == 1 else level * 2)
    if os.path.exists(file_name):
        print("%s- File %s created." % (start, file_name))
    else:
        print("%s- Error: could not create file %s." % (start, file_name))


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


def date_to_str(s):
    return s.replace(" ", "_").replace(":", "-")


def print_session_params(params):
    ret = ""
    if params['sid'] is not None:
        ret += "- session SID: %s\n" % params['sid']
    if params['serial'] is not None:
        ret += "- session serial number: %s\n" % params['serial']
    if params['instance_number'] is not None:
        ret += "- instance number: %s\n" % params['instance_number']

    return ret


def get_instance_predicate(inst_column, my_inst):
    ret = "decode(%s, NULL, %s, %s)" % (inst_column, inst_column, my_inst)
    return ret


def print_output(lines):
    print("\nSQL*Plus output:")
    print("==============================================================================")
    for x in lines:
        print(x)
    print("==============================================================================\n")
