from __future__ import print_function

import apache_beam as beam

import argparse
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import WriteToText


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        '--runner=DirectRunner',
        '--project=kouzoh-p-bharath',
        '--staging_location=gs://kouzoh-p-bharath-beam/staging',
        '--temp_location=gs://kouzoh-p-bharath-beam/temp',
        '--job_name=syncer-bq-spanner',
        '--num_workers=200',
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    sql_stmt = 'select user_id,is_registeration_completed,is_profile_completed,' \
               'is_list_completed,is_sold_completed,is_liked_completed,is_ss_completed,' \
               'is_buy_completed,is_fta_completed from [kouzoh-p-bharath:achievements.achieve_v2] LIMIT 100;'

    def makeRow(element):
        (user_id, register, profile, listed, sold, liked, savedsearch, bought, fta) = element
        return {
            "user_id": user_id, "register": register, "profile": profile, "listed": listed, "sold": sold,
            "liked": liked, "savedsearch": savedsearch, "bought": bought, "fta": fta,
        }

    with beam.Pipeline(options=pipeline_options) as p:
        source = p | 'Collecting' >> beam.io.Read(beam.io.BigQuerySource(query=sql_stmt))

        source | "Writing to Text" >> WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
