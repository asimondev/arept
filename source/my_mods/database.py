from __future__ import print_function
from __future__ import absolute_import

import re
import sys

from .sqlplus import SqlPlus
from .tables import print_table_ddls, TableNotFound
from .indexes import print_index_ddls, IndexNotFound
from .views import print_view_ddl, ViewNotFound
from .mat_views import print_mat_view_ddls, MatViewNotFound

from .awr import print_awr_sql, AWRSQLNotFound
from .awr_report import generate_awr_reports, generate_global_awr_reports
from .addm_report import generate_addm_reports, generate_rt_addm_report
from .ash_report import generate_ash_report
from .perfhub import generate_perfhub_report
from .shared_lib import print_sql, SQLNotFound


class Database:
    def __init__(self,
                 out_dir=None,
                 out_level=None,
                 out_format=None,
                 db_user=None,
                 db_pwd=None,
                 db_name=None,
                 db_internal=None,
                 pdb=None,
                 obj_tables=None,
                 obj_indexes=None,
                 obj_index_tables=None,
                 obj_views=None,
                 obj_mat_views=None,
                 schema=None,
                 begin_time=None, end_time=None,
                 begin_snap_id=None, end_snap_id=None,
                 awr_sql_ids=[],
                 awr_sql_format=None,
                 awr_report=False,
                 awr_summary=False,
                 global_awr_report=False,
                 global_awr_summary=False,
                 addm_report=False,
                 rt_addm_report=False,
                 parallel=None,
                 ash_report=False,
                 global_ash_report=False,
                 rt_perfhub_report=False,
                 awr_perfhub_report=False,
                 sql_id = None,
                 sql_child = None,
                 sql_format = None,
                 arept_args = [],
                 verbose=False):
        self.begin_time = begin_time
        self.end_time = end_time
        self.begin_snap_id = begin_snap_id
        self.end_snap_id = end_snap_id
        self.awr_sql_ids = awr_sql_ids
        self.awr_sql_format = awr_sql_format

        self.parallel = parallel
        self.awr_report = awr_report
        self.awr_summary = awr_summary
        self.global_awr_report = global_awr_report
        self.global_awr_summary = global_awr_summary
        self.addm_report = addm_report
        self.rt_addm_report = rt_addm_report
        self.ash_report = ash_report
        self.global_ash_report = global_ash_report
        self.rt_perfhub_report = rt_perfhub_report
        self.awr_perfhub_report = awr_perfhub_report

        self.in_inst_ids = None

        self.sql_id = sql_id
        self.sql_child = sql_child
        self.sql_format = sql_format

        self.arept_args = arept_args

        self.verbose = verbose

        self.out_dir = out_dir
        self.out_level = out_level
        self.out_format = out_format
        self.version = None
        self.major_version = None
        self.dbid = None
        self.inst_id = None
        self.inst_name = None
        self.is_rac = None
        self.is_dba = None
        self.is_cdb = None
        self.con_name = None
        self.ses_user = None
        self.rac_inst_ids = []

        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_name = db_name
        self.db_internal = db_internal
        self.pdb = pdb
        self.db_con = ""
        if self.db_user:
            self.db_con += self.db_user + "/" + self.db_pwd
        if self.db_name:
            self.db_con += "@" + self.db_name
        if self.db_user and self.db_user.lower() == "sys":
            self.db_con += " as sysdba"

        self.obj_tables = obj_tables
        self.obj_indexes = obj_indexes
        self.obj_index_tables = obj_index_tables
        self.obj_views = obj_views
        self.obj_mat_views = obj_mat_views
        self.schema = schema
        self.sqlp_set_metadata = """set pagesi 0 linesi 256 trimsp on long 50000 echo on
set long 500000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

"""

    def __str__(self):
        ret = "Class Database:\n"

        if self.db_name:
            ret += "- database user: %s\n" % self.db_user
        if self.db_pwd:
            ret += "- database user password: %s\n" % self.db_pwd
        if self.db_name:
            ret += "- database name: %s\n" % self.db_name
        if self.pdb:
            ret += "- PDB: %s\n" % self.pdb

        if self.begin_time:
            ret += "- begin_time: %s\n" % self.begin_time
        if self.end_time:
            ret += "- end_time: %s\n" % self.end_time
        if self.begin_snap_id:
            ret += "- begin_snap_id: %s\n" % self.begin_snap_id
        if self.end_snap_id:
            ret += "- end_snap_id: %s\n" % self.end_snap_id

        if self.version:
            ret += "- version: %s\n" % self.version
        if self.major_version:
            ret += "- major_varsion: %s\n" % self.major_version
        if self.dbid:
            ret += "- DBID: %s\n" % self.dbid
        if self.inst_name:
            ret += "- Instance name: %s\n" % self.inst_name
        if self.inst_id:
            ret += "- Instance ID: %s\n" % self.inst_id
        if self.is_rac is not None:
            ret += "- RAC?: %s\n" % self.is_rac
        if len(self.rac_inst_ids):
            ret += "- RAC Instance IDs: %s\n" % self.rac_inst_ids
        if self.is_dba is not None:
            ret += "- DBA? %s\n" % self.is_dba
        if self.is_cdb is not None:
            ret += "- CDB? %s\n" % self.is_cdb
        if self.con_name is not None:
            ret += "- Container name: %s\n" % self.con_name
        if self.ses_user is not None:
            ret += "- Session user: %s\n" % self.ses_user

        ret += "- AWR report: %s\n" % self.awr_report
        ret += "- AWR summary reports: %s\n" % self.awr_summary
        ret += "- global AWR reports: %s\n" % self.global_awr_report
        ret += "- global AWR summary reports: %s\n" % self.global_awr_summary
        ret += "- ADDM report: %s\n" % self.addm_report
        ret += "- real-time ADDM report: %s\n" % self.rt_addm_report
        ret += "- ASH report: %s\n" % self.ash_report
        ret += "- global ASH report: %s\n" % self.global_ash_report
        ret += "- real-time performance hub report: %s\n" % self.rt_perfhub_report
        ret += "- AWR performance hub report: %s\n" % self.awr_perfhub_report

        if self.out_dir:
            ret += "- out_dir: %s\n" % self.out_dir
        if self.out_level:
            ret += "- out_level: %s\n" % self.out_level
        if self.out_format:
            ret += "- out_format: %s\n" % self.out_format

        if self.obj_tables:
            ret += "- obj_tables: %s\n" % self.obj_tables
        if self.obj_indexes:
            ret += "- obj_indexes: %s\n" % self.obj_indexes
        if self.obj_index_tables:
            ret += "- obj_index_tables: %s\n" % self.obj_index_tables
        if self.obj_views:
            ret += "- obj_views: %s\n" % self.obj_views
        if self.obj_mat_views:
            ret += "- obj_mat_views: %s\n" % self.obj_mat_views
        if self.schema:
            ret += "- schema: %s\n" % self.schema

        return ret

    def select_version(self):
        sql = SqlPlus(con=self.db_con,
                      pdb=self.pdb,
                      stmts="select 'VERSION: ' || version || ':' from v$instance;",
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            matchobj = re.match(r'\AVERSION:\s+([\d]+\.[\d]+).*:.*\Z', line)
            if matchobj:
                self.set_version(matchobj.group(1), out)
                return
        else:
            print("Error: can not find the Oracle release in SQL*Plus output>>>")
            print(out)
            sys.exit(1)

    def set_version(self, version, out):
        if version[:2] == "11":
            self.version = "11g"
            self.major_version = 11
        elif version[:4] == "12.1":
            self.version = "12_1"
            self.major_version = 12
        elif version[:4] == "12.2":
            self.version = "12_2"
            self.major_version = 12
        elif version[:2] == "18":
            self.version = "18c"
            self.major_version = 18
        elif version[:2] == "19":
            self.version = "19c"
            self.major_version = 19
        else:
            print("Error: unknown Oracle release '%s' "
                  "found in SQL*Plus output." % version)
            print(out)
            sys.exit(1)

    def get_version(self):
        return self.version


    def select_properties(self):
        stmts = """select 'IS_DBA: ' || sys_context('userenv', 'isdba') from dual;
select 'CON_NAME: ' || sys_context('userenv', 'con_name') from dual;
select 'IS_CDB: ' || nvl(sys_context('userenv', 'cdb_name'), 'FALSE') from dual;        
select 'SES_USER: ' || sys_context('userenv', 'session_user') from dual;
select 'DBID: ' || dbid from v$database;
select 'INSTANCE_ID: ' || sys_context('userenv', 'instance') from dual;
select 'INSTANCE_NAME: ' || sys_context('userenv', 'instance_name') from dual;
select 'IS_RAC: ' || value from v$system_parameter where name = 'cluster_database';
"""
        sql = SqlPlus(con=self.db_con,
                      pdb=self.pdb,
                      stmts=stmts,
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            if self.is_dba is None:
                matchobj = re.match(r'\AIS_DBA:\s+(\w+)\s*\Z', line)
                if matchobj:
                    self.is_dba = True if matchobj.group(1) == "TRUE" else False
            if self.is_cdb is None:
                matchobj = re.match(r'\AIS_CDB:\s+([\w$]+)\s*\Z', line)
                if matchobj:
                    self.is_cdb = False if matchobj.group(1) == "FALSE" else True
            if self.con_name is None:
                matchobj = re.match(r'\ACON_NAME:\s+([\w$]+)\s*\Z', line)
                if matchobj:
                    self.con_name = matchobj.group(1)
            if self.ses_user is None:
                matchobj = re.match(r'\ASES_USER:\s+([\w#$]+)\s*\Z', line)
                if matchobj:
                    self.ses_user = matchobj.group(1)
            if self.dbid is None:
                matchobj = re.match(r'\ADBID:\s+([\d]+)\s*\Z', line)
                if matchobj:
                    self.dbid = matchobj.group(1)
            if self.is_rac is None:
                matchobj = re.match(r'\AIS_RAC:\s+([\w]+)\s*\Z', line)
                if matchobj:
                    self.is_rac = True if matchobj.group(1) == "TRUE" else False
            if self.inst_id is None:
                matchobj = re.match(r'\AINSTANCE_ID:\s+([\d]+)\s*\Z', line)
                if matchobj:
                    self.inst_id = matchobj.group(1)
            if self.inst_name is None:
                matchobj = re.match(r'\AINSTANCE_NAME:\s+([\w]+)\s*\Z', line)
                if matchobj:
                    self.inst_name = matchobj.group(1)

            if (self.is_dba is not None and
                    self.is_cdb is not None and
                    self.con_name is not None and
                    self.ses_user is not None and
                    self.dbid is not None and
                    self.is_rac is not None and
                    self.inst_id is not None and self.inst_name is not None):
                return
        else:
            print("Error: can not find the database properties in SQL*Plus output>>>")
            print(out)
            sys.exit(1)

    def select_awr_rac_instances(self, begin_id, end_id):
        if not self.is_rac:
            return

        stmt = """select distinct 
'INSTANCE_ID: ' || instance_number
from dba_hist_snapshot where dbid = %s and snap_id between %s and %s;
""" % (self.dbid, begin_id, end_id)
        sql = SqlPlus(con=self.db_con,
                      stmts=stmt,
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            matchobj = re.match(r'\AINSTANCE_ID:\s+(\d+)\s*\Z', line)
            if matchobj:
                self.rac_inst_ids.append(matchobj.group(1))

        if len(self.rac_inst_ids) == 0:
            print("Error: can not find RAC instances in SQL*Plus output>>>")
            print(out)
            sys.exit(1)

    def table_ddls(self):
        params = {
            'schema': self.schema,
            'ses_user': self.ses_user,
            'db_con': self.db_con,
            'pdb': self.pdb,
            'is_dba': self.is_dba,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format
        }

        for a in self.obj_tables:
            pos = a.find(".")
            if pos == -1:
                owner = None
                table = a
            else:
                owner = a[:pos]
                table = a[pos+1:]
            try:
                print_table_ddls(owner=owner, table=table,
                                 params=params, verbose=self.verbose)

            except TableNotFound as ex:
                print(ex)

    def index_ddls(self):
        params = {
            'schema': self.schema,
            'ses_user': self.ses_user,
            'db_con': self.db_con,
            'pdb': self.pdb,
            'is_dba': self.is_dba,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'sqlp_set_metadata': self.sqlp_set_metadata
        }

        for a in self.obj_indexes:
            pos = a.find(".")
            if pos == -1:
                owner = None
                index = a
            else:
                owner = a[:pos]
                index = a[pos+1:]
            try:
                print_index_ddls(owner=owner, index=index,
                                 params=params, verbose=self.verbose)

            except IndexNotFound as ex:
                print(ex)

    def index_table_ddls(self):
        params = {
            'schema': self.schema,
            'ses_user': self.ses_user,
            'db_con': self.db_con,
            'pdb': self.pdb,
            'is_dba': self.is_dba,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'sqlp_set_metadata': self.sqlp_set_metadata
        }

        for a in self.obj_index_tables:
            pos = a.find(".")
            if pos == -1:
                owner = None
                index = a
            else:
                owner = a[:pos]
                index = a[pos+1:]
            try:
                tab = print_index_ddls(owner=owner, index=index,
                                       params=params,
                                       verbose=self.verbose,
                                       index_table=True)

                if tab:
                    print_table_ddls(owner=tab[0],
                                     table=tab[1],
                                     params=params,
                                     level=2,
                                     verbose=self.verbose)

            except TableNotFound as tx:
                print(tx)

            except IndexNotFound as ix:
                print(ix)

    def view_ddls(self):
        params = {
            'schema': self.schema,
            'ses_user': self.ses_user,
            'db_con': self.db_con,
            'pdb': self.pdb,
            'is_dba': self.is_dba,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'sqlp_set_metadata': self.sqlp_set_metadata
        }

        for a in self.obj_views:
            pos = a.find(".")
            if pos == -1:
                owner = None
                view = a
            else:
                owner = a[:pos]
                view = a[pos+1:]
            try:
                vw = print_view_ddl(owner=owner, view=view,
                                       params=params,
                                       verbose=self.verbose)

            except ViewNotFound as ix:
                print(ix)

    def mat_view_ddls(self):
        params = {
            'schema': self.schema,
            'ses_user': self.ses_user,
            'db_con': self.db_con,
            'pdb': self.pdb,
            'is_dba': self.is_dba,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'sqlp_set_metadata': self.sqlp_set_metadata
        }

        for a in self.obj_mat_views:
            pos = a.find(".")
            if pos == -1:
                owner = None
                mview = a
            else:
                owner = a[:pos]
                mview = a[pos+1:]
            try:
                mv = print_mat_view_ddls(owner=owner, mview=mview,
                                       params=params,
                                       verbose=self.verbose)

                if mv:
                    print_table_ddls(owner=mv[0],
                                     table=mv[1],
                                     params=params,
                                     level=2,
                                     verbose=self.verbose)

            except TableNotFound as tx:
                print(tx)

            except MatViewNotFound as ix:
                print(ix)

    def awr_sql_reports(self, begin_id, end_id):
        params = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'awr_sql_format': self.awr_sql_format
        }

        for a in self.awr_sql_ids:
            try:
                print_awr_sql(sql_id=a,
                              begin_id=begin_id,
                              end_id=end_id,
                              params=params,
                              verbose=self.verbose)

            except AWRSQLNotFound as ex:
                print(ex)

    def get_awr_interval_ids(self):
        if self.begin_snap_id:
            return self.check_interval_ids()

        stmts = """set serveroutput on pagesi 0 linesi 256 trimsp on
                
declare 
    min_snap_id number;
    max_snap_id number;

begin
    select max(snap_id) into min_snap_id from dba_hist_snapshot
    where dbid = %s and 
        begin_interval_time <= to_timestamp('%s', 'yyyy-mm-dd hh24:mi');

    select max(snap_id) into max_snap_id from dba_hist_snapshot
    where dbid = %s and 
        end_interval_time <= to_timestamp('%s', 'yyyy-mm-dd hh24:mi');

    if min_snap_id is null or max_snap_id is null or min_snap_id >= max_snap_id then
        dbms_output.put_line('min timestamp: ' || '%s' ||
                     ', min_snap_id: ' || nvl(to_char(min_snap_id), 'NULL'));
        dbms_output.put_line('max timestamp: ' || '%s' ||
                     ', max_snap_id: ' || nvl(to_char(max_snap_id), 'NULL'));
        raise_application_error(-20001,
                        'Error: the specified time interval is not available.');
    else
        dbms_output.put_line('found min snap_id' || ':' || min_snap_id || ':');
        dbms_output.put_line('found max snap_id' || ':' || max_snap_id || ':');
    end if;
end;
/
""" % (self.dbid, self.begin_time,
       self.dbid, self.end_time,
       self.begin_time, self.end_time)

        sql = SqlPlus(con=self.db_con,
                      stmts=stmts,
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        (begin_id, end_id) = (None, None)
        for line in out:
            if begin_id is None:
                matchobj = re.match(r'\A.*found min snap_id:(\d+):.*\Z', line)
                if matchobj:
                    begin_id = matchobj.group(1)

            if end_id is None:
                matchobj = re.match(r'\A.*found max snap_id:(\d+):.*\Z', line)
                if matchobj:
                    end_id = matchobj.group(1)

            if begin_id is not None and end_id is not None:
                return [begin_id, end_id]

        print("Error: can not find begin / end snapshot IDs in SQL*Plus output>>>")
        print(out)
        sys.exit(1)

    def check_interval_ids(self):
        stmts = """set serveroutput on pagesi 0 linesi 256 trimsp on
        
declare 
    min_snap_id number;
    max_snap_id number;
    begin_time timestamp(3);
    end_time   timestamp(3);
    begin_ivl_id number := %s;
    end_ivl_id number := %s;
begin
    select snap_id, min(end_interval_time)
    into min_snap_id, begin_time
    from dba_hist_snapshot
    where dbid = %s and snap_id = begin_ivl_id
    group by snap_id;

    select snap_id, min(end_interval_time)
    into max_snap_id, end_time
    from dba_hist_snapshot
    where dbid = %s and snap_id = end_ivl_id
    group by snap_id;

    if min_snap_id is null or max_snap_id is null or min_snap_id >= max_snap_id then
        dbms_output.put_line('begin interval ID: ' || begin_ivl_id ||
              ', min_snap_id: ' || nvl(to_char(min_snap_id), 'NULL'));
        dbms_output.put_line('end interval ID: ' || end_ivl_id ||
              ', max_snap_id: ' || nvl(to_char(max_snap_id), 'NULL'));
        raise_application_error(-20001,
              'Error: the specified snapshot interval is not available.');    
    else
        dbms_output.put_line('found min snap_id' || ':' || min_snap_id || ':');
        dbms_output.put_line('found max snap_id' || ':' || max_snap_id || ':');
        dbms_output.put_line('found begin time' || '#' ||
                              to_char(begin_time, 'yyyy-mm-dd hh24:mi') || '#');
        dbms_output.put_line('found end time' || '#' ||
                              to_char(end_time, 'yyyy-mm-dd hh24:mi') || '#');    
    end if;
end;
/
""" % (self.begin_snap_id, self.end_snap_id, self.dbid, self.dbid)

        sql = SqlPlus(con=self.db_con,
                      stmts=stmts,
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        (begin_id, end_id) = (None, None)
        for line in out:
            if begin_id is None:
                matchobj = re.match(r'\A.*found min snap_id:(\d+):.*\Z', line)
                if matchobj:
                    begin_id = matchobj.group(1)

            if end_id is None:
                matchobj = re.match(r'\A.*found max snap_id:(\d+):.*\Z', line)
                if matchobj:
                    end_id = matchobj.group(1)

            if begin_id is not None and end_id is not None:
                return [self.begin_snap_id, self.end_snap_id]

        print("Error: snapshot interval IDs check failed in SQL*Plus output>>>")
        print(out)
        sys.exit(1)

    def sql_report(self):
        params = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'sql_format': self.sql_format,
            'version': self.major_version
        }

        try:
            print_sql(sql_id=self.sql_id,
                      sql_child=self.sql_child,
                      params=params,
                      verbose=self.verbose)

        except SQLNotFound as ex:
            print(ex)

    def get_awr_report(self, begin_id, end_id, awr_summary, snap_ids):
        params = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'parallel': self.parallel
        }

        generate_awr_reports(begin_id, end_id, awr_summary,
                             snap_ids, params, self.verbose)

    def get_global_awr_reports(self, begin_id, end_id, global_awr_summary, snap_ids):
        params = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'parallel': self.parallel
        }

        generate_global_awr_reports(begin_id, end_id, global_awr_summary,
                             snap_ids, params, self.verbose)


    def get_addm_report(self, begin_id, end_id, snap_ids):
        params = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'parallel': self.parallel
        }

        generate_addm_reports(begin_id, end_id, snap_ids, params, self.verbose)

    @staticmethod
    def read_snap_ids(out):
        res = dict()
        pattern = re.compile(r'SNAP_ID:([\d]+:\d+:.*):\Z')
        for line in out:
            matchobj = pattern.search(line)
            if matchobj:
                (snap_id_str, snap, inst, end_date, others) = line.split(':')
                if inst in res:
                    res[inst].append((snap, end_date))
                else:
                    res[inst] = [(snap, end_date)]

        return res

    def select_awr_report_snap_ids(self, begin_snap_id, end_snap_id):
        stmts = """set pagesi 0 linesi 255 trimsp on heading on
        
select 'SNAP_ID' || ':' || snap_id || ':' || 
  instance_number || ':' || to_char(end_interval_time, 
  'yyyy-mm-dd_hh24-mi-ss') || ':'  
from dba_hist_snapshot 
where dbid = %s and snap_id between %s and %s and 
  instance_number in %s
order by instance_number, snap_id
/

""" % (self.dbid, begin_snap_id, end_snap_id, self.in_inst_ids)
        sql = SqlPlus(con=self.db_con,
                      stmts=stmts,
                      out_dir=self.out_dir,
                      verbose=self.verbose)
        out = sql.run(silent=False)
        return self.read_snap_ids(out)

    def set_in_instance_ids(self):
        if self.is_rac:
            in_inst_ids = "(" + ','.join(self.rac_inst_ids) + ")"
        else:
            in_inst_ids = "(" + self.inst_id + ")"

        self.in_inst_ids = in_inst_ids

    def awr_objects(self):
        if (self.awr_sql_ids or self.awr_report or self.global_awr_report or
                self.awr_summary or self.global_awr_summary or self.addm_report):
            (begin_id, end_id) = self.get_awr_interval_ids()

            self.select_awr_rac_instances(begin_id, end_id)
            self.set_in_instance_ids()

            if self.awr_sql_ids:
                self.awr_sql_reports(begin_id, end_id)

            if (self.awr_report or self.awr_summary or self.global_awr_report or
                    self.global_awr_summary or self.addm_report):
                snap_ids = self.select_awr_report_snap_ids(begin_id, end_id)
                if self.awr_report or self.awr_summary:
                    self.get_awr_report(begin_id, end_id,
                                    self.awr_summary, snap_ids)
                elif self.global_awr_report or self.global_awr_summary:
                    self.get_global_awr_reports(begin_id, end_id,
                                                self.global_awr_summary, snap_ids)
                elif self.addm_report:
                    self.get_addm_report(begin_id, end_id, snap_ids)

    def addm_reports(self):
        if self.rt_addm_report:
            self.get_rt_addm_report()

    def perfhub_reports(self):
        if self.rt_perfhub_report or self.awr_perfhub_report:
            self.get_perfhub_report()

    def ash_reports(self):
        if self.ash_report:
            self.get_ash_report(False)

        if self.global_ash_report:
            self.get_ash_report(True)

    def get_ash_report(self, global_ash_report):
        params = self.set_default_params()

        generate_ash_report(self.begin_time, self.end_time, params,
                             self.verbose, global_ash_report)

    def set_default_params(self):
        ret = {
            'db_con': self.db_con,
            'pdb': self.pdb,
            'dbid': self.dbid,
            'is_dba': self.is_dba,
            'is_rac': self.is_rac,
            'inst_id': self.inst_id,
            'inst_name': self.inst_name,
            'rac_inst_ids': self.rac_inst_ids,
            'version': self.version,
            'out_dir': self.out_dir,
            'out_level': self.out_level,
            'out_format': self.out_format,
            'parallel': self.parallel
        }

        return ret

    def get_rt_addm_report(self):
        params = self.set_default_params()
        generate_rt_addm_report(params, self.verbose)

    def get_perfhub_report(self):
        params = self.set_default_params()
        generate_perfhub_report(params, self.verbose,
                                begin_time=self.begin_time,
                                end_time=self.end_time,
                                rt_perfhub_report=self.rt_perfhub_report,
                                awr_perfhub_report=self.awr_perfhub_report)
