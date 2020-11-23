from connection import OracleConnection
from config import BASE_CONNECTION, ZSBD_CONNECTION
from queries import Queries
from timer import Timer
import csv


class IndexCache:
    def __init__(self, cursor, index_name, table_name, index, bitmap=False):
        self.cursor = cursor
        self.index_name = index_name
        self.table_name = table_name
        self.index = index
        self.bitmap = bitmap

    def with_index(self):
        cursor = self.cursor
        index_name = self.index_name
        table_name = self.table_name
        index = self.index
        bitmap = self.bitmap

        class Index:
            def __enter__(self):
                if bitmap:
                    cursor.execute("create bitmap index " + index_name + " on " + table_name + " (" + index + ")")
                else:
                    cursor.execute("create index " + index_name + " on " + table_name + " (" + index + ")")

            def __exit__(self, exc_type, exc_val, exc_tb):
                cursor.execute("drop index " + index_name)

        return Index()


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
                    with za2.measure("ZA2"):
                        queries.za2('2020-01-01', '2000', 1)
                    with za3.measure("ZA3"):
                        queries.za3('1980-01-01', 50, '2020-01-01', "Buck", 5, 2)
                connection.rollback()

            zd1 = Timer("ZD1")
            zd2 = Timer("ZD2")
            zd3 = Timer("ZD3")
            zd4 = Timer("ZD4")
            for _ in range(3):
                with CleanCache(sys_cursor).without_cache():
                    with zd1.measure("ZD1"):
                        queries.zd1(3, '2020-01-01', 2)
                    with zd2.measure("ZD2"):
                        queries.zd2(50, 0.25)
                    with zd3.measure("ZD3"):
                        queries.zd3(25)
                    with zd4.measure("ZD4"):
                        queries.zd4()
                connection.rollback()

            with open("time-results.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL)

                writer.writerow(["Nazwa", "Sredni czas wykonania", "Max czas", "Min czas", "Ilosc wykonan"])
                writer.writerow([za1.name, za1.sum / 10, za1.max, za1.min, 10])
                writer.writerow([za2.name, za2.sum / 10, za2.max, za2.min, 10])
                writer.writerow([za3.name, za3.sum / 10, za3.max, za3.min, 10])
                writer.writerow([zd1.name, zd1.sum / 3, zd1.max, zd1.min, 3])
                writer.writerow([zd2.name, zd2.sum / 3, zd2.max, zd2.min, 3])
                writer.writerow([zd3.name, zd3.sum / 3, zd3.max, zd3.min, 3])
                writer.writerow([zd4.name, zd4.sum / 3, zd4.max, zd4.min, 3])

            # INDEKSY
            za1 = Timer("ZA1-genre_movie_moviemovieid_index")
            za2 = Timer("ZA2")
            za3 = Timer("ZA3-title_sub_trim_lower_index")
            for _ in range(10):
                with CleanCache(sys_cursor).without_cache():
                    with IndexCache(cursor, "genre_movie_moviemovieid_index", "genre_movie", "moviemovieid").with_index():
                        with za1.measure("ZA1"):
                            queries.za1(1970, 2020)
                    with za2.measure("ZA2"):
                        queries.za2('2020-01-01', '2000', 1)
                    with IndexCache(cursor, "title_sub_trim_lower_index", "Movie", "substr(trim(lower(title)), 1, 4)").with_index():
                        with za3.measure("ZA3"):
                            queries.za3('1980-01-01', 50, '2020-01-01', "Buck", 5, 2)
                connection.rollback()

            zd1 = Timer("ZD1")
            zd2 = Timer("ZD2")
            zd3length = Timer("ZD3-length-description-index")
            zd3bitmapindex = Timer("ZD3-bitmap-index")
            zd4 = Timer("ZD4")

            for _ in range(3):
                with CleanCache(sys_cursor).without_cache():
                    with IndexCache(cursor, "rent_customercustomerid_index", "rent", "customercustomerid").with_index():
                        with zd1.measure("ZD1"):
                            queries.zd1(3, '2020-01-01', 2)
                    with zd2.measure("ZD2"):
                        queries.zd2(50, 0.25)
                    with IndexCache(cursor, "len_desc_index", "Rate", "Length(description)").with_index():
                        with zd3length.measure("ZD3-length-description"):
                            queries.zd3(25)
                    with IndexCache(cursor, "emailverified_bitmap_index", "Customer", "emailverified", True).with_index():
                        with zd3bitmapindex.measure("ZD3-bitmap-index"):
                            queries.zd3(25)
                    with zd4.measure("ZD4"):
                        queries.zd4()
                connection.rollback()

            with open("time-results-with-indexes.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL)

                writer.writerow(["Nazwa", "Sredni czas wykonania", "Max czas", "Min czas", "Ilosc wykonan"])
                writer.writerow([za1.name, za1.sum / 10, za1.max, za1.min, 10])
                writer.writerow([za2.name, za2.sum / 10, za2.max, za2.min, 10])
                writer.writerow([za3.name, za3.sum / 10, za3.max, za3.min, 10])
                writer.writerow([zd1.name, zd1.sum / 3, zd1.max, zd1.min, 3])
                writer.writerow([zd2.name, zd2.sum / 3, zd2.max, zd2.min, 3])
                writer.writerow([zd3length.name, zd3length.sum / 3, zd3length.max, zd3length.min, 3])
                writer.writerow([zd3bitmapindex.name, zd3bitmapindex.sum / 3, zd3bitmapindex.max, zd3bitmapindex.min, 3])
                writer.writerow([zd4.name, zd4.sum / 3, zd4.max, zd4.min, 3])

            # EKSPERYMENT
            zd3index = Timer("ZD3-index")
            zd3bitmapindex = Timer("ZD3-bitmap-index")
            for _ in range(3):
                with CleanCache(sys_cursor).without_cache():
                    with IndexCache(cursor, "value_index", "Rate", "Value").with_index():
                        with zd3index.measure("ZD3-index"):
                            queries.zd3(25)

                with CleanCache(sys_cursor).without_cache():
                    with IndexCache(cursor, "value_bitmap_index", "Rate", "Value", True).with_index():
                        with zd3bitmapindex.measure("ZD3-bitmap-index"):
                            queries.zd3(25)
                connection.rollback()

            with open("time-results-experiment.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL)

                writer.writerow(["Nazwa", "Sredni czas wykonania", "Max czas", "Min czas", "Ilosc wykonan"])
                writer.writerow([zd3index.name, zd3index.sum / 3, zd3index.max, zd3index.min, 3])
                writer.writerow([zd3bitmapindex.name, zd3bitmapindex.sum / 3, zd3bitmapindex.max, zd3bitmapindex.min, 3])
