import os

import pandas as pd
import teradatasql
import pytest
from dagster import asset, job, materialize, op
from dagster_teradata import TeradataResource


def test_resource(tmp_path):
    df = ['a']

    @asset
    def drop_table(teradatadb: TeradataResource):
        try:
            with teradatadb.get_connection() as conn:
                conn.cursor().execute("DROP TABLE dbcinfo;")
        except teradatasql.DatabaseError as e:
            # Error code 3807 corresponds to "table does not exist" in Teradata
            if "3807" in str(e):
                print("Table dbcinfo does not exist, ignoring error 3807.")
            else:
                # Re-raise the exception if it's not error 3807
                raise


    @asset
    def create_table(teradatadb: TeradataResource, drop_table):
        with teradatadb.get_connection() as conn:
            conn.cursor().execute("CREATE TABLE dbcinfo (infokey varchar(50));")


    @asset
    def insert_rows(teradatadb: TeradataResource, create_table):
        with teradatadb.get_connection() as conn:
            conn.cursor().execute("insert into dbcinfo (infokey) values ('a');")

    @asset
    def read_table(teradatadb: TeradataResource, insert_rows):
        with teradatadb.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("select * from dbcinfo;")
            res = cursor.fetchall()
            result_list = [row[0] for row in res]
            assert result_list==df


    materialize(
        [drop_table, create_table, insert_rows, read_table],
        resources={"teradatadb": TeradataResource(host="sdt47039.labs.teradata.com",user="mt255026",password="mt255026",database="mt255026")},
    )