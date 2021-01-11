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


class Inmemory:
    def __init__(self, cursor, table_name: str, table_name2: str = None, col: str = None, col2: str = None):
        self.cursor = cursor
        self.table_name = table_name
        self.table_name2 = table_name2
        self.col = col
        self.col2 = col2

    def with_inmemory_column(self):
        cursor = self.cursor
        table_name = self.table_name
        col = self.col

        class InMemory:
            def __enter__(self):
                cursor.execute("alter table C##ZSBD." + table_name + " INMEMORY")
                cursor.execute("alter table C##ZSBD." + table_name + " INMEMORY (" + col + ")")

                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', table_name])

            def __exit__(self, exc_type, exc_val, exc_tb):
                cursor.execute("alter table C##ZSBD." + table_name + " NO INMEMORY")
                cursor.execute("alter table C##ZSBD." + table_name + " NO INMEMORY (" + col + ")")

        return InMemory()

    def with_inmemory_join_group(self):
        table_name = self.table_name
        table_name2 = self.table_name2
        col = self.col
        col2 = self.col2

        class InMemoryJoinGroup:
            def __enter__(self):
                cursor.execute("ALTER TABLE C##ZSBD." + table_name + " INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD." + table_name2 + " INMEMORY")
                cursor.execute("CREATE INMEMORY JOIN GROUP " + table_name + col + table_name2 + col2 + " (C##ZSBD." + table_name + "(" + col + "), C##ZSBD." + table_name2 + "(" + col2 + "))")

                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', table_name])
                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', table_name2])


            def __exit__(self, exc_type, exc_val, exc_tb):
                cursor.execute("DROP INMEMORY JOIN GROUP " + table_name + col + table_name2 + col2)
                cursor.execute("ALTER TABLE C##ZSBD." + table_name + " NO INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD." + table_name2 + " NO INMEMORY")

        return InMemoryJoinGroup()

    def with_inmemory_za2(self):
        class InMemoryZa2:
            def __enter__(self):
                cursor.execute("ALTER TABLE CREWMEMBER INMEMORY")
                cursor.execute("ALTER TABLE CREWMEMBER_JOB INMEMORY")
                cursor.execute("ALTER TABLE JOB INMEMORY")
                cursor.execute("ALTER TABLE CREWMEMBER_AWARD INMEMORY")
                cursor.execute("ALTER TABLE AWARD INMEMORY")

                try:
                    cursor.execute("CREATE INMEMORY JOIN GROUP joingroup1 (C##ZSBD.CREWMEMBER(crewmemberid), C##ZSBD.CREWMEMBER_JOB(crewmembercrewmemberid), C##ZSBD.CREWMEMBER_AWARD(crewmembercrewmemberid))")
                    cursor.execute("CREATE INMEMORY JOIN GROUP joingroup2 (C##ZSBD.JOB(jobid), C##ZSBD.CREWMEMBER_JOB(jobjobid))")
                    cursor.execute("CREATE INMEMORY JOIN GROUP joingroup3 (C##ZSBD.AWARD(awardid), C##ZSBD.CREWMEMBER_AWARD(awardawardid))")
                except:
                    print(1)

                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', 'CREWMEMBER'])
                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', 'CREWMEMBER_JOB'])
                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', 'JOB'])
                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', 'CREWMEMBER_AWARD'])
                cursor.callproc("DBMS_INMEMORY.POPULATE", ['C##ZSBD', 'AWARD'])


            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    cursor.execute("DROP INMEMORY JOIN GROUP joingroup1")
                    cursor.execute("DROP INMEMORY JOIN GROUP joingroup2")
                    cursor.execute("DROP INMEMORY JOIN GROUP joingroup3")
                except:
                    print(1)
                cursor.execute("ALTER TABLE C##ZSBD.CREWMEMBER NO INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD.CREWMEMBER_JOB NO INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD.JOB NO INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD.CREWMEMBER_AWARD NO INMEMORY")
                cursor.execute("ALTER TABLE C##ZSBD.AWARD NO INMEMORY")

        return InMemoryZa2()


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
                    with Inmemory(sys_cursor, "MOVIE", "GENRE_MOVIE", "movieid",
                                  "moviemovieid").with_inmemory_join_group():
                        with Inmemory(sys_cursor, "GENRE", "GENRE_MOVIE", "genreid",
                                      "genregenreid").with_inmemory_join_group():
                            with za1.measure("ZA1"):
                                queries.za1(1970, 2020)
                    with Inmemory(sys_cursor, "fake").with_inmemory_za2():
                        with za2.measure("ZA2"):
                            queries.za2('2020-01-01', '2000', 1)
                    with Inmemory(sys_cursor, "MOVIE", col="budget").with_inmemory_column():
                        with za3.measure("ZA3"):
                            queries.za3('1980-01-01', 50, '2020-01-01', "Buck", 5, 2)
                connection.rollback()

            zd1 = Timer("ZD1")
            zd2 = Timer("ZD2")
            zd3 = Timer("ZD3")
            zd4 = Timer("ZD4")
            for _ in range(3):
                with CleanCache(sys_cursor).without_cache():
                    # with zd1.measure("ZD1"):
                    #     queries.zd1(3, '2020-01-01', 2)
                    with Inmemory(sys_cursor, "MOVIE", col="Title").with_inmemory_column():
                        with zd2.measure("ZD2"):
                            queries.zd2(50, 0.25)
                    # with zd3.measure("ZD3"):
                    #     queries.zd3(25)
                    with Inmemory(sys_cursor, "MOVIE", col="Duration").with_inmemory_column():
                        with zd4.measure("ZD4"):
                            queries.zd4()
                connection.rollback()

            with open("time-results.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL)

                writer.writerow(["Nazwa", "Sredni czas wykonania", "Max czas", "Min czas", "Ilosc wykonan"])
                writer.writerow([za1.name, za1.sum / 10, za1.max, za1.min, 10])
                writer.writerow([za2.name, za2.sum / 10, za2.max, za2.min, 10])
                writer.writerow([za3.name, za3.sum / 10, za3.max, za3.min, 10])
                # writer.writerow([zd1.name, zd1.sum / 3, zd1.max, zd1.min, 3])
                writer.writerow([zd2.name, zd2.sum / 3, zd2.max, zd2.min, 3])
                # writer.writerow([zd3.name, zd3.sum / 3, zd3.max, zd3.min, 3])
                writer.writerow([zd4.name, zd4.sum / 3, zd4.max, zd4.min, 3])
