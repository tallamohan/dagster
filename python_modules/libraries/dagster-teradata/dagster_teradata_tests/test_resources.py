import os
import uuid
import pandas as pd
import teradatasql
import pytest
from unittest import mock
from contextlib import contextmanager
from typing import Iterator
from dagster import (
    DagsterInstance,
    DagsterResourceFunctionError,
    DataVersion,
    EnvVar,
    ObserveResult,
    build_resources,
    job,
    observable_source_asset,
    op,
    asset,
    materialize,
)
from dagster._time import get_current_timestamp
from dagster_aws.s3 import S3Resource
from dagster_teradata import TeradataResource, fetch_last_updated_timestamps, teradata_resource


# def test_resource(tmp_path):
#     df = ['a']
#
#     @asset
#     def drop_table(teradata: TeradataResource):
#         try:
#             with teradata.get_connection() as conn:
#                 conn.cursor().execute("DROP TABLE dbcinfo;")
#         except teradatasql.DatabaseError as e:
#             # Error code 3807 corresponds to "table does not exist" in Teradata
#             if "3807" in str(e):
#                 print("Table dbcinfo does not exist, ignoring error 3807.")
#             else:
#                 # Re-raise the exception if it's not error 3807
#                 raise
#
#
#     @asset
#     def create_table(teradata: TeradataResource, drop_table):
#         with teradata.get_connection() as conn:
#             conn.cursor().execute("CREATE TABLE dbcinfo (infokey varchar(50));")
#
#
#     @asset
#     def insert_rows(teradata: TeradataResource, create_table):
#         with teradata.get_connection() as conn:
#             conn.cursor().execute("insert into dbcinfo (infokey) values ('a');")
#
#     @asset
#     def read_table(teradata: TeradataResource, insert_rows):
#         with teradata.get_connection() as conn:
#             cursor = conn.cursor()
#             cursor.execute("select * from dbcinfo;")
#             res = cursor.fetchall()
#             result_list = [row[0] for row in res]
#             assert result_list==df
#
#
#     materialize(
#         [drop_table, create_table, insert_rows, read_table],
#         resources={"teradata": TeradataResource(host="sdt47039.labs.teradata.com",user="mt255026",password="mt255026",database="mt255026")},
#     )


# @contextmanager
# def temporary_teradata_table() -> Iterator[str]:
#     with build_resources(
#         {
#             "teradata": TeradataResource(
#                 # host=os.getenv("TERADATA_HOST"),
#                 # user=os.environ["TERADATA_USER"],
#                 # password=os.getenv("TERADATA_PASSWORD"),
#                 # database="TESTDB",
#                 host="sdt47039.labs.teradata.com",
#                 user="mt255026",
#                 password="mt255026",
#                 database="mt255026",
#             )
#         }
#     ) as resources:
#         table_name = f"TEST_TABLE_{str(uuid.uuid4()).replace('-', '_').upper()}"  # Teradata table names are expected to be capitalized.
#         teradata: TeradataResource = resources.teradata
#         with teradata.get_connection() as conn:
#             try:
#                 conn.cursor().execute(f"create table {table_name} (foo varchar(10))")
#                 # Insert one row
#                 conn.cursor().execute(f"insert into {table_name} values ('bar')")
#                 yield table_name
#             finally:
#                 conn.cursor().execute(f"drop table {table_name}")

def temporary_teradata_table() -> Iterator[str]:
    with build_resources(
        {
            "teradata": TeradataResource(
                host="dbt-teradata-t3gwet5u19m2zwm9.env.clearscape.teradata.com",
                user="demo_user",
                password="demo_user",
                database="demo_user",
            )
        }
    ) as resources:
        table_name = f"TEST_TABLE_{str(uuid.uuid4()).replace('-', '_').upper()}"  # Teradata table names are expected to be capitalized.
        teradata: TeradataResource = resources.teradata
        with teradata.get_connection() as conn:
            try:
                conn.cursor().execute(f"create table {table_name} (foo string)")
                # Insert one row
                conn.cursor().execute(f"insert into {table_name} values ('bar')")
                yield table_name
            finally:
                try:
                    conn.cursor().execute(f"drop table if exists {table_name}")
                except Exception as ex:
                    ignored = False
                    if f"[Error 3807]" in str(ex):
                        ignored = True



# @pytest.mark.integration
# def test_resources_teradata_connection():
#     with TeradataResource(
#         host="dbt-teradata-t3gwet5u19m2zwm9.env.clearscape.teradata.com",
#         user="demo_user",
#         password="demo_user",
#         database="demo_user",
#     ).get_connection() as conn:
#         # Teradata table names are expected to be capitalized.
#         table_name = f"test_table_{str(uuid.uuid4()).replace('-', '_')}".lower()
#         try:
#             start_time = get_current_timestamp()
#             conn.cursor().execute(f"create table {table_name} (foo varchar(10))")
#             # Insert one row
#             conn.cursor().execute(f"insert into {table_name} values ('bar')")
#
#             freshness_for_table = fetch_last_updated_timestamps(
#                 teradata_connection=conn,
#                 database="demo_user",
#                 tables=[
#                     table_name
#                 ],  # Teradata table names are expected uppercase. Test that lowercase also works.
#             )[table_name].timestamp()
#
#             end_time = get_current_timestamp()
#
#             assert end_time > freshness_for_table
#         finally:
#             try:
#                 conn.cursor().execute(f"drop table if exists {table_name}")
#             except Exception as ex:
#                 ignored = False
#                 if f"[Error 3807]" in str(ex):
#                     ignored = True


def test_s3_to_teradata(tmp_path):

    @asset
    def read_table(teradata: TeradataResource, insert_rows):
        with teradata.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("select * from dbcinfo;")
            res = cursor.fetchall()
            result_list = [row[0] for row in res]
            assert result_list==df


    materialize(
        [drop_table, create_table, insert_rows, read_table],
        resources={"teradata": TeradataResource(host="sdt47039.labs.teradata.com",user="mt255026",password="mt255026",database="mt255026"),
                   "s3": S3Resource()},
    )