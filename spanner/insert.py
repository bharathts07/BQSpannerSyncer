import logging

from google.cloud import spanner


def insert_data(instance_id, database_id):
    """Inserts sample data into the given database.
    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='achieve',
            columns=('user_id', 'is_buy_completed', 'is_fta_completed','is_liked_completed', 'is_list_completed', 'is_profile_completed','is_registeration_completed', 'is_sold_completed', 'is_ss_completed',),
            values=[
                (1, 1, 1, 1, 1, 1, 1, 1, 1),
                (2, 1, 1, 1, 1, 0, 0, 0, 0),
                (3, 1, 0, 1, 0, 1, 0, 1, 0),
                (4, 0, 1, 0, 1, 0, 1, 0, 1),
                (5, 0, 0, 0, 0, 0, 0, 0, 0)])

    print('Inserted data.')


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    insert_data("test", "dummy")
