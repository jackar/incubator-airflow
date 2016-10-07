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

from airflow.hooks.druid_hook import DruidHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime


class S3ToDruidTransfer(BaseOperator):
    """
    Moves data from S3 to Druid.

    :param druid_datasource: the datasource you want to ingest into in druid
    :type druid_datasource: str
    :param ts_dim: the timestamp dimension
    :type ts_dim: str
    :param metric_spec: the metrics you want to define for your data
    :type metric_spec: list
    :param druid_ingest_conn_id: the druid ingest connection id
    :type druid_ingest_conn_id: str
    :param metastore_conn_id: the metastore connection id
    :type metastore_conn_id: str
    :param intervals: list of time intervals that defines segments, this
        is passed as is to the json object
    :type intervals: list
    """

    template_fields = ('intervals', 'execution_date', 'tomorrow', 'execution_hour')
    template_ext = ()
    #ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            static_path,
            druid_datasource,
            ts_dim,
            s3_bucket,
            s3_key,
            headers=False,
            partitioned=False,
            metric_spec=None,
            columns=None,
            s3_conn_id='s3_default',
            druid_ingest_conn_id='druid_ingest_default',
            intervals=None,
            query_granularity=None,
            segment_granularity=None,
            *args, **kwargs):
        super(S3ToDruidTransfer, self).__init__(*args, **kwargs)
        self.static_path = static_path
        self.druid_datasource = druid_datasource
        self.ts_dim = ts_dim
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.headers = headers
        self.partitioned = partitioned
        self.execution_date = '{{ ds }}'
        self.tomorrow = '{{ tomorrow_ds }}'
        self.execution_hour = '{{ execution_date.hour }}'
        self.intervals = intervals or ['{{ ds }}/{{ tomorrow_ds }}']
        self.query_granularity = query_granularity
        self.segment_granularity = segment_granularity
        self.metric_spec = metric_spec or [{
            "name": "count",
            "type": "count"}]
        self.columns = columns
        self.s3_conn_id=s3_conn_id
        self.druid_ingest_conn_id = druid_ingest_conn_id

    def execute(self, context):
        druid = DruidHook(druid_ingest_conn_id=self.druid_ingest_conn_id)
        s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        start_ymd, start_hour = self.execution_date, self.execution_hour

        if self.partitioned and self.segment_granularity == 'HOUR':
            prefix = "{0}/ymd={1}/hour={2}/".format(self.s3_key, ymd, hour)
            interval = "{0}T{1}:00:00.000Z/{0}T{2}:00:00.000Z".format(ymd, '%02d' % hour, '%02d' % hour + 1)
        elif self.partitioned and self.segment_granularity == 'DAY':
            prefix = "{0}/ymd={1}/".format(self.s3_key, ymd)
            interval = "{0}T00:00:00.000Z".format(ymd, '%02d' % hour)
        else:
            prefix = "{0}/".format(self.s3_key)
    
        keys = s3.list_keys(self.s3_bucket, prefix=prefix)
        filtered_keys = filter(lambda key: '_SUCCESS' not in key, keys)
        s3_paths = map(lambda key: "s3://{0}/{1}".format(self.s3_bucket, key), filtered_keys)

        logging.info(s3_paths)

        logging.info("Inserting rows into Druid")
        #logging.info("S3 path: " + self.static_path)

        # TODO get static path from s3 bucket + key

        # druid.load_from_s3(
        #     datasource=self.druid_datasource,
        #     intervals=self.intervals,
        #     static_path=self.static_path, ts_dim=self.ts_dim,
        #     columns=self.columns, query_granularity=self.query_granularity, 
        #     segment_granularity=self.segment_granularity, metric_spec=self.metric_spec)
        logging.info("Load seems to have succeeded!")
