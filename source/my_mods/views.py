from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import desc_stmt, file_created, select_object


class ViewNotFound(Exception): pass


def print_view_ddl(owner=None, view=None,
                     params={}, verbose=False):
    vw = View(owner=owner, name=view,
                params=params, verbose=verbose)
    if verbose:
        print(vw)

    user_vw = []
    all_vw = vw.check_all_views()
    if len(all_vw) == 1:
        vw.set_view_owner(all_vw[0][0])
    else:
        user_vw = vw.check_user_views()
        if len(user_vw) == 1:
            vw.set_view_owner(params['ses_user'])

    if len(user_vw) != 1 and len(all_vw) != 1:
        raise ViewNotFound("!!! DDL not found for view: %s" % view)

    vw.view_owner = vw.view_owner.upper()
    vw.name = vw.name.upper()

    vw.get_view_metadata(vw.view_owner, vw.name)
    vw.get_view(vw.view_owner, vw.name)


class View:
    def __init__(self,
                owner=None,
                name=None,
                params={},
                verbose=False):
        self.owner = owner
        self.name = name
        self.view_owner = None # Either current_user or selected owner.
        self.params = params
        self.verbose = verbose
        self.view_prefix = "dba" if params['is_dba'] else "all"

    def __str__(self):
        ret = "Class Index:\n"
        if self.owner:
            ret += "- owner: %s\n" % self.owner
        if self.name:
            ret += "- view: %s\n" % self.name
        if self.params['schema']:
            ret += "- schema: %s\n" % self.params['schema']
        if self.params['db_con']:
            ret += "- SQL*Plus connection string: %s\n" % self.params['db_con']
        if self.params['pdb']:
            ret += "- PDB: %s\n" % self.params['pdb']
        if self.params['is_dba'] is not None:
            ret += "- DBA? %s\n" % self.params['is_dba']
        if self.params['out_dir']:
            ret += "- output directory: %s\n" % self.params['out_dir']
        if self.params['out_format']:
            ret += "- output format: %s\n" % self.params['out_format']
        if self.params['out_level']:
            ret += "- output level: %s\n" % self.params['out_level']
        if self.params['version']:
            ret += "- database version: %s\n" % self.params['version']

        return ret

    def set_view_owner(self, owner):
        self.view_owner = owner

    def check_all_views(self):
        tab_name = "dba_views" if self.params['is_dba'] else "all_views"
        stmt = """SELECT 'OWNER:' || owner || ',VIEW:' || view_name 
from %s where view_name = upper('%s')""" % (tab_name, self.name)
        if self.owner is not None:
            stmt += " and owner = upper('%s')" % self.owner
        if self.owner is None and self.params['schema'] is not None:
            stmt += " and owner = upper('%s')" % self.schema
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\AOWNER:([\w]+),VIEW:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2)))

        return ret

    def check_user_views(self):
        stmt = """SELECT 'VIEW:' || view_name 
from user_views where view_name = upper('%s')""" % self.name
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\VIEW:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1)))
                break

        return ret

    def get_view_metadata(self, owner, view):
        file_name = "view_%s_%s_metadata.txt" % (owner, view)
        stmts = """%s
spool %s/%s

set pagesi 100 linesi 256 trimsp on long 100000 longch 10000
set serveroutput on 

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'VIEW',name=>'%s',schema=>'%s') from dual;

spool off
""" % (self.params['sqlp_set_metadata'], self.params['out_dir'], file_name,
        view, owner)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False, do_exit=False)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)

    def get_view(self, owner, view):
        for fmt in self.params['out_format']:
            name = "view_%s_%s" % (owner, view)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 100000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

%s

select * from %s_views where owner = '%s' and view_name = '%s'
/

set long 100000 longch 10000

select text from %s_views where owner = '%s' and view_name = '%s'
/

spool off
""" % (self.params['out_dir'], file_name,
       select_object(owner, view, "VIEW", self.view_prefix),
       desc_stmt("%s_views" % self.view_prefix),
       self.view_prefix, owner, view,
       self.view_prefix, owner, view)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name)
            if self.verbose:
                for line in out:
                    print(line)
