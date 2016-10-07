# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime


class RedshiftToS3Transfer(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, 
            schema,
            table,
            s3_bucket,
            s3_key,
            postgres_conn_id='redshift_default',
            s3_conn_id='s3_default', 
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(RedshiftToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()

        logging.info('Retrieving headers: ' + str(self.schema) + '.' + str(self.table))

        columns_query = """SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = '{0}'
                            AND   table_name = '{1}'
                            ORDER BY ordinal_position
                        """.format(self.schema, self.table)

        cursor = self.hook.get_conn().cursor()
        cursor.execute(columns_query)
        rows = cursor.fetchall()
        columns = map(lambda row: row[0], rows)
        column_names = (', ').join(map(lambda c: "\\'{0}\\'".format(c), columns))
        column_castings = (', ').join(map(lambda c: "CAST({0} AS text) AS {0}".format(c), columns))

        unload_query = """
                            UNLOAD ('SELECT {0} 
                            UNION ALL
                            SELECT {1} FROM {2}.{3}') 
                            TO 's3://{4}/{5}/{3}_' 
                            with
                            credentials 'aws_access_key_id={6};aws_secret_access_key={7}'
                            DELIMITER AS ','
                            ADDQUOTES
                            NULL AS ''
                            ALLOWOVERWRITE
                            PARALLEL OFF;
                        """.format(column_names, column_castings, self.schema, self.table, 
                            self.s3_bucket, self.s3_key, a_key, s_key)

        logging.info('Executing query...')
        self.hook.run(unload_query, self.autocommit)
        logging.info("Query finished...")
