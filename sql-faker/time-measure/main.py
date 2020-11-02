from connection import OracleConnection
from config import BASE_CONNECTION, ZSBD_CONNECTION
from queries import Queries
from timer import Timer
import csv


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

            za1 = Timer("ZA1")
            za2 = Timer("ZA2")
            za3 = Timer("ZA3")
            for _ in range(10):
                with CleanCache(sys_cursor).without_cache():
                    with za1.measure("ZA1"):
                        queries.za1(1970, 2020)
                    # with za2.measure("ZA2"):
                    #     queries.za2()
                    with za3.measure("ZA3"):
                        queries.za3('1980-01-01', '2020-01-01', '2020-01-01', "Buck", 5, 2)

                connection.rollback()

            print("Averages:")
            print(za1.sum / 10)
            # print(za2.sum / 10)
            print(za3.sum / 10)

            zd1 = Timer("zd1")
            zd2 = Timer("zd1")
            zd3 = Timer("zd3")
            for _ in range(10):
                with CleanCache(sys_cursor).without_cache():
                    with zd1.measure("zd1"):
                        queries.zd1(3, '2020-01-01', 2)
                    with zd2.measure("zd2"):
                        queries.zd2(50, 0.25)
                    with zd3.measure("zd3"):
                        queries.zd3(25)

                connection.rollback()

            with open("time-results.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)

                writer.writerow(["Nazwa", "Sredni czas wykonania"])
                writer.writerow([za1.name, za1.sum / 10])
                # writer.writerow([za2.name, za1.sum / 10])
                writer.writerow([za3.name, za3.sum / 10])
                writer.writerow([zd1.name, zd1.sum / 10])
                writer.writerow([zd2.name, zd2.sum / 10])
                writer.writerow([zd3.name, zd3.sum / 10])
                # writer.writerow([zd4.name, zd4.sum / 10])
