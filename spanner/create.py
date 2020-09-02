from google.cloud import spanner
import logging


def create_database(instance_id, database_id):
    """Creates a database and tables for sample data."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id, ddl_statements=[
        """CREATE TABLE Singers (
            SingerId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            SingerInfo   BYTES(MAX)
        ) PRIMARY KEY (SingerId)""",
        """CREATE TABLE Albums (
            SingerId     INT64 NOT NULL,
            AlbumId      INT64 NOT NULL,
            AlbumTitle   STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE"""
    ])

    operation = database.create()

    print('Waiting for operation to complete...')
    operation.result(120)

    print('Created database {} on instance {}'.format(
        database_id, instance_id))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    create_database("test", "dummy2")
