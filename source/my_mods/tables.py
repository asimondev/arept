from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import desc_stmt, file_created, select_object


class TableNotFound(Exception): pass


def print_table_ddls(owner=None, table=None,
                     params={}, level=1, verbose=False):
    tab = Table(owner=owner, table=table,
                params=params, level=level, verbose=verbose)
    if verbose:
        print(tab)

    user_tabs = []
    all_tabs = tab.check_all_table()
    if len(all_tabs) == 1:
        tab.set_tab_owner(all_tabs[0][0])
    else:
        user_tabs = tab.check_user_table()
        if len(user_tabs) == 1:
            tab.set_tab_owner(params['ses_user'])

    if len(user_tabs) != 1 and len(all_tabs) != 1:
        raise TableNotFound("!!! DDLs not found for table: %s" % table)

    tab.tab_owner = tab.tab_owner.upper()
    tab.table = tab.table.upper()

    tab.get_table_metadata(tab.tab_owner, tab.table)
    tab.get_table(tab.tab_owner, tab.table)
    tab.get_table_indexes(tab.tab_owner, tab.table)
    if params['out_level'] == "all":
        tab.get_table_stats(tab.tab_owner, tab.table)
        tab.get_table_columns_stats(tab.tab_owner, tab.table)
        tab.get_table_index_stats(tab.tab_owner, tab.table)
        tab.get_table_histograms(tab.tab_owner, tab.table)
        tab.get_table_part_stats(tab.tab_owner, tab.table)
        tab.get_table_subpart_stats(tab.tab_owner, tab.table)


class Table:
    def __init__(self,
                owner=None,
                table=None,
                params={},
                level=1,
                verbose=False):
        self.owner = owner
        self.table = table
        self.tab_owner = None # Either current_user or selected owner.
        self.params = params
        self.level = level
        self.verbose = verbose
        self.tab_prefix = "dba" if params['is_dba'] else "all"

    def __str__(self):
        ret = "Class Table:\n"
        if self.owner:
            ret += "- owner: %s\n" % self.owner
        if self.table:
            ret += "- table: %s\n" % self.table
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

    def set_tab_owner(self, owner):
        self.tab_owner = owner

    def check_all_table(self):
        tab_name = "dba_tables" if self.params['is_dba'] else "all_tables"
        stmt = """SELECT 'OWNER:' || owner || ',TABLE:' || table_name
from %s where table_name = upper('%s')""" % (tab_name, self.table)
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
            matchobj = re.match(r'\AOWNER:([\w]+),TABLE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append((matchobj.group(1), matchobj.group(2)))

        return ret

        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def check_user_table(self):
        stmt = """SELECT 'TABLE:' || table_name
from user_tables where table_name = upper('%s')""" % self.table
        stmt += ";"
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        ret = []
        for line in out:
            matchobj = re.match(r'\ATABLE:([\w]+)\s*\Z', line)
            if matchobj:
                ret.append(matchobj.group(1))
                break

        return ret
        # else:
        #     print("Error: can not find the table in SQL*Plus output>>>")
        #     print(out)
        #     return False

    def get_table_metadata(self, owner, table):
        file_name = "table_%s_%s_metadata.txt" % (owner, table)
        stmts = """set pagesi 0 trimsp on long 50000 echo on
set long 500000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s/%s
%s

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'TABLE',name=>'%s',schema=>'%s') from dual;
select dbms_metadata.get_dependent_ddl(object_type=>'INDEX',base_object_name=>'%s',base_object_schema=>'%s') from dual;
select dbms_metadata.get_dependent_ddl(object_type=>'CONSTRAINT',base_object_name=>'%s',base_object_schema=>'%s') from dual;

select dbms_stats.report_col_usage(ownname=>'%s',tabname=>'%s') from dual; 

spool off
""" % (self.params['out_dir'], file_name, desc_stmt(table, owner),
       table, owner, table, owner, table, owner,
       owner, table)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False, do_exit=False)
        file_created(file_name, self.level)
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

    def get_table(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

%s

select * from %s_tables where owner = '%s' and table_name = '%s'
/

%s

select * from %s_tab_cols where owner = '%s' and table_name = '%s'
order by column_id
/

%s

select owner, trigger_name, trigger_type, triggering_event, 
  when_clause, status, before_statement, before_row,
  after_row, after_statement, instead_of_row
from %s_triggers
where owner = '%s' and table_name = '%s'
/

spool off
""" % (self.params['out_dir'], file_name,
       select_object(owner, table, 'TABLE', self.tab_prefix),
       desc_stmt("%s_tables" % self.tab_prefix),
       self.tab_prefix, owner, table,
       desc_stmt("%s_tab_cols" % self.tab_prefix),
       self.tab_prefix, owner, table,
       desc_stmt("%s_triggers" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_indexes(self, owner, table):
        for fmt in self.params['out_format']:
            name = "table_%s_%s_indexes" % (owner, table)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 100 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_indexes where table_owner = '%s' and table_name = '%s'
order by index_name
/

%s

select * from %s_ind_columns where table_owner = '%s' and table_name = '%s'
order by index_name, column_position
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_indexes" % self.tab_prefix),
       self.tab_prefix, owner, table,
       desc_stmt("%s_ind_columns" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_stats(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_stats.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s_stats.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

select * from %s_tab_statistics where owner = '%s' and table_name = '%s'
/

%s

select * from %s_tab_stat_prefs where owner = '%s' and table_name = '%s'
order by preference_name
/

spool off
""" % (self.params['out_dir'], file_name,
            desc_stmt("%s_tab_statistics" % self.tab_prefix),
            self.tab_prefix, owner, table,
            desc_stmt("%s_tab_stat_prefs" % self.tab_prefix),
            self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_columns_stats(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_columns_stats.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 longchunk 1000 echo on
"""
            else:
                file_name = "table_%s_%s_columns_stats.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 longchunk 1000 echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_tab_col_statistics where owner = '%s' and table_name = '%s'
order by column_name
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_tab_col_statistics" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_index_stats(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_index_stats.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s_index_stats.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_ind_statistics where table_owner = '%s' and table_name = '%s'
order by index_name, partition_position, subpartition_position
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_ind_statistics" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_histograms(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_histograms.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s_histograms.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_tab_histograms where owner = '%s' and table_name = '%s'
order by column_name
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_tab_histograms" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_part_stats(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_part_stats.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s_part_stats.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s

select * from %s_part_col_statistics where owner = '%s' and table_name = '%s'
order by partition_name, column_name
/

%s

select * from %s_part_histograms where owner = '%s' and table_name = '%s'
order by partition_name, column_name
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_part_col_statistics" % self.tab_prefix),
       self.tab_prefix, owner, table,
       desc_stmt("%s_part_histograms" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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

    def get_table_subpart_stats(self, owner, table):
        for fmt in self.params['out_format']:
            if fmt == "text":
                file_name = "table_%s_%s_subpart_stats.txt" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = "table_%s_%s_subpart_stats.html" % (owner, table)
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s 

select * from %s_subpart_col_statistics where owner = '%s' and table_name = '%s'
order by subpartition_name, column_name
/

%s

select * from %s_subpart_histograms where owner = '%s' and table_name = '%s'
order by subpartition_name, column_name
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("%s_subpart_col_statistics" % self.tab_prefix),
       self.tab_prefix, owner, table,
       desc_stmt("%s_subpart_histograms" % self.tab_prefix),
       self.tab_prefix, owner, table)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name, self.level)
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
