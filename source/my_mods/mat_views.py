from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import desc_stmt, file_created, select_object


class MatViewNotFound(Exception): pass


def print_mat_view_ddls(owner=None, mview=None,
                     params={}, verbose=False):
    mv = MatView(owner=owner, mview=mview,
                params=params, verbose=verbose)
    if verbose:
        print(mv)

    user_mviews = []
    all_mviews = mv.check_all_mviews()
    if len(all_mviews) == 1:
        mv.set_mview_owner(all_mviews[0][0])
        # mv.set_table(all_idx[0][2], all_idx[0][3])
    else:
        user_mviews = mv.check_user_mviews()
        if len(user_mviews) == 1:
            mv.set_mview_owner(params['ses_user'])
            # mv.set_table(user_[0][1], user_idx[0[2]])

    if len(user_mviews) != 1 and len(all_mviews) != 1:
        raise MatViewNotFound("!!! DDLs not found for mat. view: %s" % mview)

    mv.mview_owner = mv.mview_owner.upper()
    mv.mview = mv.mview.upper()

    mv.get_mview_metadata(mv.mview_owner, mv.mview)
    mv.get_mview(mv.mview_owner, mv.mview)

    mv.set_table()
    return [mv.tab_owner, mv.tab_name]

class MatView:
    def __init__(self,
                owner=None,
                mview=None,
                params={},
                verbose=False):
        self.owner = owner
        self.mview = mview
        self.mview_owner = None # Either current_user or selected owner.
        self.params = params
        self.verbose = verbose
        self.mview_prefix = "dba" if params['is_dba'] else "all"
        self.tab_owner = None
        self.tab_name = None

    def __str__(self):
        ret = "Class MatView:\n"
        if self.owner:
            ret += "- owner: %s\n" % self.owner
        if self.index:
            ret += "- mview: %s\n" % self.mview
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

    def set_mview_owner(self, owner):
        self.mview_owner = owner

    def set_table(self):
        self.tab_owner = self.mview_owner
        self.tab_name = self.mview

    def check_all_mviews(self):
        tab_name = "dba_mviews" if self.params['is_dba'] else "all_mviews"
        stmt = """SELECT 'OWNER:' || owner || ',MVIEW:' || mview_name
from %s where mview_name = upper('%s')""" % (tab_name, self.mview)
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
            matchobj = re.match(r'\AOWNER:([\w]+),MVIEW:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2)))

        return ret

        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def check_user_mviews(self):
        stmt = """SELECT 'MVIEW:' || mview_name
from user_mviews where mview_name = upper('%s')""" % self.mview
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\AMVIEW:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1)))
                break

        return ret
        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def get_mview_metadata(self, owner, mview):
        file_name = "mview_%s_%s_metadata.txt" % (owner, mview)
        stmts = """%s
spool %s/%s

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'MATERIALIZED_VIEW',name=>'%s',schema=>'%s') from dual;

spool off
""" % (self.params['sqlp_set_metadata'], self.params['out_dir'], file_name,
        mview, owner)

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
        # ret = []
        # for line in out:
        #     print(line)
            # matchobj = re.match(r'\ATABLE:([\w]+)\s*\Z', line)
            # if matchobj:
            #     ret.append(matchobj.group(1))
            #     break

        # return ret

    def get_mview(self, owner, mview):
        for fmt in self.params['out_format']:
            name = "mview_%s_%s" % (owner, mview)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
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

select * from %s_mviews where owner = '%s' and mview_name = '%s'
/

set long 100000 longch 10000

select query from %s_mviews where owner = '%s' and mview_name = '%s'
/

spool off
""" % (self.params['out_dir'], file_name,
       select_object(owner, mview, "MATERIALIZED VIEW", self.mview_prefix),
       desc_stmt("%s_mviews" % self.mview_prefix),
       self.mview_prefix, owner, mview,
       self.mview_prefix, owner, mview)

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
        # ret = []
        # for line in out:
        #     print(line)
            # matchobj = re.match(r'\ATABLE:([\w]+)\s*\Z', line)
            # if matchobj:
            #     ret.append(matchobj.group(1))
            #     break

        # return ret
