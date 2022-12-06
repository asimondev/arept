from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import desc_stmt, file_created, select_object


class SourceNotFound(Exception): pass


def print_source_code(owner=None, source=None,
                     params={}, verbose=False):
    src = Source(owner=owner, source=source,
                params=params, verbose=verbose)
    if verbose:
        print(src)

    # Format: owner, name, type
    all_sources = src.check_all_sources()
    if check_valid_source(all_sources):
        src.set_source_owner(all_sources[0][0])
        src.source_type = all_sources[0][2]
    else:
        user_sources = src.check_user_sources()
        if check_valid_source(user_sources):
            src.set_source_owner(params['ses_user'])
            print("Debug: ", user_sources)
            src.source_type = user_sources[0][2]
        else:
            raise SourceNotFound("!!! Source code not found  for: %s" % source)

    src.source_owner = src.source_owner.upper()
    src.source = src.source.upper()

    src.get_source_metadata(src.source_owner, src.source, src.source_type)
    src.get_source(src.source_owner, src.source, src.source_type)

# Returns true if Ok.
def check_valid_source(lst):
    if len(lst) == 1:
        return True

    if len(lst) == 0 or len(lst) > 2:
        return False

    if lst[0][2] == 'PACKAGE' and lst[1][2] == 'PACKAGE BODY':
        return lst[0][0] == lst[1][0] and lst[0][1] == lst[1][1]

    return False

class Source:
    def __init__(self,
                owner=None,
                source=None,
                params={},
                verbose=False):
        self.owner = owner
        self.source = source
        self.source_owner = None # Either current_user or selected owner.
        self.source_type = None
        self.params = params
        self.verbose = verbose
        self.source_prefix = "dba" if params['is_dba'] else "all"

    def __str__(self):
        ret = "Class Soruce:\n"
        if self.owner:
            ret += "- owner: %s\n" % self.owner
        if self.source:
            ret += "- source: %s\n" % self.source
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

    def set_source_owner(self, owner):
        self.source_owner = owner

    def check_all_sources(self):
        tab_name = "dba_source" if self.params['is_dba'] else "all_source"
        stmt = """SELECT distinct 'OWNER:' || owner || ',SOURCE:' || name ||
',TYPE:' || type from %s where name = upper('%s')""" % (tab_name, self.source)
        if self.owner is not None:
            stmt += " and owner = upper('%s')" % self.owner
        if self.owner is None and self.params['schema'] is not None:
            stmt += " and owner = upper('%s')" % self.params['schema']
        stmt += " ORDER BY 1;"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\AOWNER:([\w]+),SOURCE:([\w]+),TYPE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2), matchobj.group(3)))

        return ret

        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def check_user_sources(self):
        stmt = """SELECT distinct 'SOURCE:' || name || ',TYPE:' || type 
from user_source where name = upper('%s')""" % self.source
        stmt += " ORDER BY 1;"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\ASOURCE:([\w]+),TYPE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2)))
                break

        return ret
        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def get_source_metadata(self, owner, source, source_type):
        file_name = "%s/%s_%s_%s_metadata.txt" % (
            self.params['out_dir'], source_type, owner, source)
        stmts = """%s
spool %s

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'%s',name=>'%s',schema=>'%s') from dual;

spool off
""" % (self.params['sqlp_set_metadata'], file_name,
        source_type, source, owner)

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

    def get_source(self, owner, source, source_type):
        for fmt in self.params['out_format']:
            name = "%s/%s_%s_%s" % (self.params['out_dir'],
                                       source_type, owner, source)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 1000 linesi 512 trimsp on long 50000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 1000 linesi 512 trimsp on echo on
set markup html on spool on 
"""

            objects = select_object(owner, source, source_type,
                                    self.source_prefix)
            if source_type == 'PACKAGE BODY':
                objects += "\n" + select_object(owner, source,
                            'PACKAGE BODY', self.source_prefix)

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

%s

select text from %s_source where owner = '%s' and name = '%s'
order by owner, name, type, line
/

spool off
""" % (file_name, objects,
       desc_stmt("%s_source" % self.source_prefix),
       self.source_prefix, owner, source)

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
