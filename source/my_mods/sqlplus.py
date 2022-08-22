#
# Author: Andrej Simon, Oracle ACS Germany
#
# Interface class to SQL*Plus.

from __future__ import print_function
from __future__ import absolute_import

import os
import os.path
import sys
import tempfile


from .utils import file_created


def run_sqlplus(con=None, pdb=None, stmts=None, out_dir=None,
                verbose=False, silent=False, do_exit=False,
                file_name=None):
    sql = SqlPlus(con=con,
                  pdb=pdb,
                  stmts=stmts,
                  out_dir=out_dir,
                  verbose=verbose)
    out = sql.run(silent=silent, do_exit=do_exit)
    file_created(file_name)
    if verbose:
        for line in out:
            print(line)


class SqlPlus:
    def __init__(self, con=None, pdb=None, stmts=None, sql_file=None,
                 log_file=None, cmd=None, verbose=False,
                 out_dir=None):
        self.con = con
        self.pdb = pdb
        self.stmts = stmts
        self.sql_file = sql_file
        self.sql_file_prefix = "arept_sqlplus"
        self.log_file = log_file
        self.cmd = "export NLS_LANG=american_america.al32utf8"
        if cmd is not None:
            self.cmd += ";" + cmd
        self.verbose = verbose
        self.out_dir = out_dir

    def __str__(self):
        ret = "Class SqlPlus:\n"
        if self.con:
            ret += "- connect string: %s\n" % self.con
        if self.pdb:
            ret += "- PDB: %s\n" % self.pdb
        if self.stmts:
            ret += "- stmts: %s\n" % self.stmts
        if self.sql_file:
            ret += "- sql_file: %s\n" % self.sql_file
        if self.sql_file_prefix:
            ret += "- sql_file_prefix: %s\n" % self.sql_file_prefix
        if self.log_file:
            ret += "- log_file: %s\n" % self.log_file
        if self.cmd:
            ret += "- cmd: %s\n" % self.cmd

        return ret

    def create_sql_file(self, do_exit=True, verify=False, version=None):
        if version:
            self.sql_file_prefix += "_%s" % version
        else:
            self.sql_file_prefix += "_%s" % os.getpid()

        fd, self.sql_file = tempfile.mkstemp(suffix='.sql',
                                             prefix=self.sql_file_prefix,
                                             dir=self.out_dir)
        if do_exit:
            os.write(fd, b"whenever sqlerror exit failure\n")
            os.write(fd, b"whenever oserror exit failure\n")

        if self.con:
            os.write(fd, bytearray("connect " + self.con + "\n", "utf-8"))
        else:
            os.write(fd, b"connect / as sysdba\n")
        if self.pdb:
            os.write(fd, bytearray("alter session set container=" + self.pdb + ";\n", "utf-8"))

        if self.verbose:
            os.write(fd, b"set echo on\n")
        else:
            os.write(fd, b"set echo off\n")

        if verify:
            os.write(fd, b"set verify on\n")
        else:
            os.write(fd, b"set verify off\n")

        os.write(fd, b"set arraysize 100\n")
        os.write(fd, bytearray(self.stmts + "\n" + "exit\n", "utf-8"))
        os.close(fd)

    def exit_on_error(self, out):
        print("SQL script for SQL*Plus>>>")
        for line in open(self.sql_file):
            print(line, end='')

        print("\n\nSQL*Plus output>>>")
        for line in out:
            print(line, end='')

        sys.exit(1)

    def run(self, do_exit=True, silent=True, verify=False, version=None):
        if self.sql_file is None:
            self.create_sql_file(do_exit, verify=verify, version=version)

        if self.verbose:
            print("=== SQL script for SQL*Plus>>>")
            for line in open(self.sql_file):
                print(line, end='')
            print("=== End of SQL script === \n")

        cmd = self.cmd + ';' if self.cmd else ''
        cmd += "sqlplus"
        if silent:
            cmd += " -s"
        cmd += ' /nolog < ' + self.sql_file + ' 2>&1'
        if self.verbose:
            print("SQL*Plus run command: %s" % cmd)
        out = []
        try:
            fp = os.popen(cmd)
            out = fp.readlines()
            rc = fp.close()

            if rc and do_exit:
                print("Error: SQL*Plus returns with error code %d." % rc)
                self.exit_on_error(out)

        except Exception as e:
            if do_exit:
                print('Error: Unexpected error during running SQL*Plus: ', e)
                print(e)
                self.exit_on_error(out)

        finally:
            if not self.verbose:
                os.remove(self.sql_file)

        if self.verbose:
            print("=== SQL*Plus output>>>")
            for line in out:
                print(line, end='')
            print("=== End of SQL*Plus output ===\n")

        return [x.strip() for x in out]
