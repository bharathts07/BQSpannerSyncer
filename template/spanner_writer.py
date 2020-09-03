import argparse
import logging
import apache_beam as beam

from google.cloud import spanner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import iobase, WriteToText


def runDataflow(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        '--runner=DirectRunner',
        # '--runner=DataflowRunner',
        '--project=kouzoh-p-bharath',
        '--staging_location=gs://kouzoh-p-bharath-beam/staging',
        '--temp_location=gs://kouzoh-p-bharath-beam/temp',
        '--job_name=syncer-bq-spanner',
        # '--requirements_file=requirements.txt',
        '--num_workers=1',
        # '--region=us-central1',
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    sql_stmt = 'select user_id,is_registeration_completed,is_profile_completed,' \
               'is_list_completed,is_sold_completed,is_liked_completed,is_ss_completed,' \
               'is_buy_completed,is_fta_completed from [kouzoh-p-bharath:achievements.achieve_v2] LIMIT 10;'

    def makeRow(element):
        (user_id, register, profile, listed, sold, liked, savedsearch, bought, fta) = element
        return {
            "user_id": int(user_id), "register": int(register), "profile": int(profile),
            "listed": int(listed), "sold": int(sold), "liked": int(liked), "savedsearch": int(savedsearch),
            "bought": int(bought), "fta": int(fta),
        }

    cols = ['user_id', 'is_buy_completed', 'is_fta_completed', 'is_liked_completed', 'is_list_completed',
            'is_profile_completed', 'is_registration_completed', 'is_sold_completed', 'is_ss_completed']

    with beam.Pipeline(options=pipeline_options) as p:
        source = [
            (1, 1, 1, 1, 1, 1, 1, 1, 1),
            (2, 0, 1, 1, 1, 1, 1, 1, 0),
        ]

        source | "Writing to spanner" >> beam.io.Write(SimpleKVSink(table_name="achieve",
                                                                    project="kouzoh-p-bharath",
                                                                    instance_id="test",
                                                                    database_id="dummy",
                                                                    columns=cols,
                                                                    ))


# Making a sink to write into big table
class SimpleKVSink(iobase.Sink):

    def __init__(self, table_name, project, instance_id, database_id, columns):
        print("initializing kvSink")
        self.project = project
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name
        self.columns = columns

    def initialize_write(self):
        client = spanner.Client(project=self.project)
        instance = client.instance(self.instance_id)

    def open_writer(self, table, uid):
        return SimpleKVWriter(self.project, self.instance_id, self.database_id, self.table_name, self.columns)

    def pre_finalize(self, table, writer_results):
        pass

    def finalize_write(self, table, table_name, pre_finalize_result):
        pass


class SimpleKVWriter(iobase.Writer):

    def __init__(self, project, instance_id, database_id, table_name, column_names):
        self.project = project
        self.column_names = column_names
        self.spanner_client = spanner.Client(project=self.project)
        self.instance = self.spanner_client.instance(instance_id)
        self.database = self.instance.database(database_id)
        self.table_id = table_name

    def write(self, record):
        with self.database.batch() as batch:
            batch.insert(
                table=self.table_id,
                columns=self.column_names,
                values=[
                    record,
                ])

    def close(self):
        pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    runDataflow()
