from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import desc_stmt, file_created, select_object


class IndexNotFound(Exception): pass


def print_index_ddls(owner=None, index=None,
                     params={}, verbose=False, index_table=False):
    idx = Index(owner=owner, index=index,
                params=params, verbose=verbose)
    if verbose:
        print(idx)

    user_idx = []
    all_idx = idx.check_all_indexes()
    if len(all_idx) == 1:
        idx.set_idx_owner(all_idx[0][0])
        idx.set_table(all_idx[0][2], all_idx[0][3])
    else:
        user_idx = idx.check_user_indexes()
        if len(user_idx) == 1:
            idx.set_idx_owner(params['ses_user'])
            idx.set_table(user_idx[0][1], user_idx[0[2]])

    if len(user_idx) != 1 and len(all_idx) != 1:
        raise IndexNotFound("!!! DDLs not found for index: %s" % index)

    idx.idx_owner = idx.idx_owner.upper()
    idx.index = idx.index.upper()

    idx.get_index_metadata(idx.idx_owner, idx.index)
    idx.get_index(idx.idx_owner, idx.index)
    if params['out_level'] == "all":
        idx.get_index_stats(idx.idx_owner, idx.index)
        idx.get_index_part_stats(idx.idx_owner, idx.index)

    if index_table:
        return [idx.tab_owner, idx.tab_name]
    else:
        return []

class Index:
    def __init__(self,
                owner=None,
                index=None,
                params={},
                verbose=False):
        self.owner = owner
        self.index = index
        self.idx_owner = None # Either current_user or selected owner.
        self.params = params
        self.verbose = verbose
        self.idx_prefix = "dba" if params['is_dba'] else "all"
        self.tab_owner = None
        self.tab_name = None

    def __str__(self):
        ret = "Class Index:\n"
        if self.owner:
            ret += "- owner: %s\n" % self.owner
        if self.index:
            ret += "- index: %s\n" % self.index
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

    def set_idx_owner(self, owner):
        self.idx_owner = owner

    def set_table(self, owner, name):
        self.tab_owner = owner
        self.tab_name = name

    def check_all_indexes(self):
        tab_name = "dba_indexes" if self.params['is_dba'] else "all_indexes"
        stmt = """SELECT 'OWNER:' || owner || ',INDEX:' || index_name ||
',TABLE_OWNER:' || table_owner || ',TABLE:' || table_name
from %s where index_name = upper('%s')""" % (tab_name, self.index)
        if self.owner is not None:
            stmt += " and owner = upper('%s')" % self.owner
        if self.owner is None and self.params['schema'] is not None:
            stmt += " and owner = upper('%s')" % self.params['schema']
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\AOWNER:([\w]+),INDEX:([\w]+),'
                                r'TABLE_OWNER:([\w]+),TABLE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2),
                            matchobj.group(3), matchobj.group(4)))

        return ret

        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def check_user_indexes(self):
        stmt = """SELECT 'INDEX:' || index_name ||
',TABLE_OWNER:' || table_owner || ',TABLE:' || table_name         
from user_indexes where index_name = upper('%s')""" % self.index
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\AINDEX:([\w]+)'
                                r'TABLE_OWNER:([\w]+),TABLE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1),
                           matchobj.group(2), matchobj.group(3)))
                break

        return ret
        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def get_index_metadata(self, owner, index):
        file_name = "%s/index_%s_%s_metadata.txt" % (
            self.params['out_dir'], owner, index)
        stmts = """%s
spool %s

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'INDEX',name=>'%s',schema=>'%s') from dual;

spool off
""" % (self.params['sqlp_set_metadata'], file_name,
        index, owner)

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

    def get_index(self, owner, index):
        for fmt in self.params['out_format']:
            name = "%s/index_%s_%s." % (
                self.params['out_dir'], owner, index)
            if fmt == "text":
                file_name = name + "txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + "html"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

%s

select * from %s_indexes where owner = '%s' and index_name = '%s'
/

%s

select * from %s_ind_columns where index_owner = '%s' and index_name = '%s'
order by column_position
/

spool off
""" % (file_name,
       select_object(owner, index, "INDEX", self.idx_prefix),
       desc_stmt("%s_indexes" % self.idx_prefix),
       self.idx_prefix, owner, index,
       desc_stmt("%s_ind_columns" % self.idx_prefix),
       self.idx_prefix, owner, index)

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

    def get_index_stats(self, owner, index):
        for fmt in self.params['out_format']:
            name = "%s/index_%s_%s_stats." % (self.params['out_dir'],
                                              owner, index)
            if fmt == "text":
                file_name = name + "txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + "html"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_ind_statistics where owner = '%s' and index_name = '%s'
order by index_name, partition_position, subpartition_position
/

%s

select * from %s_index_usage where owner = '%s' and name = '%s'
/

spool off
""" % (file_name,
       desc_stmt("%s_ind_statistics" % self.idx_prefix),
       self.idx_prefix, owner, index,
       desc_stmt("%s_index_usage" % self.idx_prefix),
       self.idx_prefix, owner, index)

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

    def get_index_part_stats(self, owner, index):
        for fmt in self.params['out_format']:
            name = "%s/index_%s_%s_part_stats." % (
                self.params['out_dir'], owner, index)
            if fmt == "text":
                file_name = name + "txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + "html"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_ind_partitions where index_owner = '%s' and index_name = '%s'
order by partition_position
/

%s

select * from %s_ind_subpartitions where index_owner = '%s' and index_name = '%s'
order by partition_position, subpartition_position
/

spool off
""" % (file_name,
       desc_stmt("%s_ind_partitions" % self.idx_prefix),
       self.idx_prefix, owner, index,
       desc_stmt("%s_ind_subpartitions" % self.idx_prefix),
       self.idx_prefix, owner, index)

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
