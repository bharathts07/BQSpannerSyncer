from google.cloud import spanner
import logging


def create_instance(instance_id):
    """Creates an instance."""
    spanner_client = spanner.Client()

    config_name = "{}/instanceConfigs/regional-us-central1".format(
        spanner_client.project_name
    )

    instance = spanner_client.instance(
        instance_id,
        configuration_name=config_name,
        display_name="test",
        node_count=1,
    )

    operation = instance.create()

    print('Waiting for operation to complete...')
    operation.result(120)

    print('Created instance {}'.format(instance_id))


def create_database(instance_id, database_id):
    """Creates a database and tables for sample data."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id, ddl_statements=[
        """CREATE TABLE achieve (
            user_id                     INT64 NOT NULL,
            is_buy_completed            INT64 NOT NULL,
            is_fta_completed            INT64 NOT NULL,
            is_liked_completed          INT64 NOT NULL,
            is_list_completed           INT64 NOT NULL,
            is_profile_completed        INT64 NOT NULL,
            is_registration_completed   INT64 NOT NULL,
            is_sold_completed           INT64 NOT NULL,
            is_ss_completed             INT64 NOT NULL,
        ) PRIMARY KEY (user_id)"""
    ])

    operation = database.create()

    print('Waiting for operation to complete...')
    operation.result(120)

    print('Created database {} on instance {}'.format(
        database_id, instance_id))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    create_instance("test")
    create_database("test", "dummy")
