from connection import OracleConnection
from config import BASE_CONNECTION, ZSBD_CONNECTION
from queries import Queries
from timer import Timer


class CleanCache:
    def __init__(self, cursor):
        self.cursor = cursor

    def without_cache(self):
        cursor = self.cursor

        class NoCacheInternal:
            def __enter__(self):
                cursor.execute("alter system flush buffer_cache")
                cursor.execute("alter system flush shared_pool")

            def __exit__(self, exc_type, exc_value, traceback):
                pass

        return NoCacheInternal()


if __name__ == "__main__":
    with OracleConnection.create(BASE_CONNECTION) as (sys_connection, sys_cursor):
        with OracleConnection.create(ZSBD_CONNECTION) as (connection, cursor):
            queries = Queries(connection, cursor)

            # TODO: add more queries, calculate average, save averages to file
            query1 = Timer("query 1")
            for _ in range(10):
                with CleanCache(sys_cursor).without_cache():
                    with query1.measure(""):
                        queries.query_1(1970, 2020)
                connection.rollback()
