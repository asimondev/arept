# AREPT
AREPT Python scripts for working with Oracle databases.

This *README* describes the usage of AREPT Python scripts. AREPT provides the following
features to the users:
- Generate different reports from Oracle database. The results are written into text and HTML files. The tool uses SQL*Plus to select the data.
- Generate SQL snippets (AREPT templates) as script ouput and SQL files for SQL*Plus.

Depending on your Python version (2 or 3), you should start AREPT using `./arept.py` or 
`./arept3.py`. 

## AREPT parameters

```
./arept.py -h
Usage: arept.py [options]

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -c CONFIG, --config=CONFIG
                        JSON configuration file
  -u USER, --user=USER  database user
  -p PASSWORD, --password=PASSWORD
                        database user password
  -d DATABASE, --database=DATABASE
                        TNS alias or Easy Connect
  --pdb=PDB             PDB name
  -o OUTPUT_DIR, --output-dir=OUTPUT_DIR
                        output directory
  --output-level=OUTPUT_LEVEL
                        output level {typical | all}. Default: typical
  --output-format=OUTPUT_FORMAT
                        output format: text, html, active-html. Default:
                        text,html
  --obj=OBJ             object names: [table:][owner].name,..;index:[owner.]na
                        me..;index_only:[owner.]name..;view:[owner.]name...;mv
                        iew:[owner.]name...
  --obj-file=OBJ_FILE   file name with objects
  --schema=SCHEMA       default schema for objects
  -b BEGIN_TIME, --begin-time=BEGIN_TIME
                        begin time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd |
                        hh24:mi}
  -e END_TIME, --end-time=END_TIME
                        end time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd |
                        hh24:mi | now}
  --awr-sql-id=AWR_SQL_ID
                        SQL_IDs in AWR
  --awr-sql-format=AWR_SQL_FORMAT
                        Additional AWR SQL format options.
  --parallel=PARALLEL   Number of parallel AWR/ADDM reports
  --sql-id=SQL_ID       Cursor SQL_ID in shared library
  --sql-child-number=SQL_CHILD_NUMBER
                        Cursor child nuber in shared library
  --sql-format=SQL_FORMAT
                        Additional SQL format options
  --cleanup             "rm -rf *" for existing output directory
  -v, --verbose         verbose
  --begin-snap-id=BEGIN_SNAP_ID
                        min. snapshot ID
  --end-snap-id=END_SNAP_ID
                        max. snapshot ID
  -t TEMPLATE, --template=TEMPLATE
                        {osproc | my_sql_trace | ses_sql_trace | meta_table |
                        sql_details | awr_sql_monintor | awr_sql_monitor_list
                        | sql_monitor | sql_monitor_list |sql_profile |
                        awr_sql_profile | sql_baseline | awr_baseline |
                        hinted_baseline | get_awr_snap_ids | hidden_parameters
                        | get_sql_id }
  --template-help       Show description of AREPT templates.
```

Use *--template-help* option to get short description of available SQL snippets 
(AREPT templates).

## Database Connection.

If you don't provide any connection details, AREPT would try to use SQL*Plus with 
the internal connection: `/ as sysdba`.

If you provide *--pdb PDB_Name* only, then AREPT connects as internal and changes 
session to this PDB:
```
connect / as sysdba
alter session set container=PDB_Name;
```

## AREPT Output Directory.

Per default the created files will be placed into the `./arept_output` directory. You can use *--output-directory* to change this.

## AREPT Templates.

AREPT templates are generated in 2 ways:
- SQL snippets displayed on the screen.
- SQL file for SQL*Plus.

Sometimes you have to modify the SQL statement or SQL files before running them. 
For instance, you would have to insert your SID and SERIAL# etc. 

## AREPT by Examples

### SQL Plan Details.

You can use AREPT to get SQL exection plan and other details for the SQL statement
either in library cache or in AWR:
```
./arept.py --sql-id ayfpfz93vnvt1
Output directory: /home/oracle/arept/arept_output
 - File sql_id_ayfpfz93vnvt1_xplan.txt created.
 - File sql_id_ayfpfz93vnvt1_sql.txt created.
 - File sql_id_ayfpfz93vnvt1_sql.html created.
 - File sql_id_ayfpfz93vnvt1_sqlarea.txt created.
 - File sql_id_ayfpfz93vnvt1_sqlarea.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list.txt created.
 - File sql_id_ayfpfz93vnvt1_mon_report.txt created.
 - File sql_id_ayfpfz93vnvt1_detail_active.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list_active.html created.
 - File sql_id_ayfpfz93vnvt1_mon_report.html created.
 - File sql_id_ayfpfz93vnvt1_mon_report_active.html created.
```

```
./arept.py --awr-sql-id  0b65shgchks4g --begin-snap-id 75 --end-snap-id 76
Output directory: /home/oracle/arept/arept_output
 - File awr_sql_id_0b65shgchks4g_inst_1_report.txt created.
 - File awr_sql_id_0b65shgchks4g_inst_1_report.html created.
 - File awr_sql_id_0b65shgchks4g_hist_reports.txt created.
 - File awr_sql_id_0b65shgchks4g_hist_reports.html created.
```

### Database Object Details.

Get DDLs and some other basic details about the table TEST01 from the database:
```
./arept.py --obj test01
Output directory: /home/oracle/arept/arept_output
 - File table_ANDREJ_TEST01_metadata.txt created.
 - File table_ANDREJ_TEST01.txt created.
 - File table_ANDREJ_TEST01.html created.
 - File table_ANDREJ_TEST01_indexes.txt created.
 - File table_ANDREJ_TEST01_indexes.html created.
```
For SQL tuning you need more details about the tables. For instance, you would like
to get optimizer statistics for this table etc. You should use the option *--output-level all* to get more details:

```
./arept.py --obj test01 --output-level all
Output directory: /home/oracle/arept/arept_output
 - File table_ANDREJ_TEST01_metadata.txt created.
 - File table_ANDREJ_TEST01.txt created.
 - File table_ANDREJ_TEST01.html created.
 - File table_ANDREJ_TEST01_indexes.txt created.
 - File table_ANDREJ_TEST01_indexes.html created.
 - File table_ANDREJ_TEST01_stats.txt created.
 - File table_ANDREJ_TEST01_stats.html created.
 - File table_ANDREJ_TEST01_columns_stats.txt created.
 - File table_ANDREJ_TEST01_columns_stats.html created.
 - File table_ANDREJ_TEST01_index_stats.txt created.
 - File table_ANDREJ_TEST01_index_stats.html created.
 - File table_ANDREJ_TEST01_histograms.txt created.
 - File table_ANDREJ_TEST01_histograms.html created.
 - File table_ANDREJ_TEST01_part_stats.txt created.
 - File table_ANDREJ_TEST01_part_stats.html created.
 - File table_ANDREJ_TEST01_subpart_stats.txt created.
 - File table_ANDREJ_TEST01_subpart_stats.html created.
```

### SQL Tuning.

You can use AREPT templates to generate SQL snippets for SQL*Plus to work with 
SQL profiles and SQL plan baselines.

### SQL Plan Baselines.
```
./arept.py -t sql_baseline
 - File /home/oracle/arept/arept_output/create_sql_baseline.sql created.

./arept.py -t awr_baseline
 - File /home/oracle/arept/arept_output/create_awr_baseline.sql created.

```

Sometimes you want to create a baseline from existing SQL statement but to 
change the execution plan using hints or optimizer parameters. The template *hinted_baseline* provides such SQL snippets:

```
./arept.py -t hinted_baseline 
 - File /home/oracle/arept/arept_output/create_sql_baseline.sql created.
 - File /home/oracle/arept/arept_output/change_sql_baseline.sql created.
```

### SQL Profiles.

You can start SQL Tuning Advisor to get SQL profile for the SQL statement. 
The corresponding SQL statement could be either in AWR or in the current library 
cache. You have to start the advisor task and after completition check the report.
That's why two different scripts will be generated:
```
./arept.py -t sql_profile     
 - File /home/oracle/arept/arept_output/start_sql_profile.sql created.
 - File /home/oracle/arept/arept_output/get_sql_profile.sql created.
```

```
./arept.py -t awr_sql_profile
 - File /home/oracle/arept/arept_output/awr_start_sql_profile.sql created.
 - File /home/oracle/arept/arept_output/awr_get_sql_profile.sql created.
```

