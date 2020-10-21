from database_types.idatabase import IDatabase
from database_types.mysql import MySQL
from table import Table

error_01 = "Parameter {} was {}, but can only be: {}"


class Database:
    """This is the main class of this package. It is used to instantiate database objects.

    A database object holds multiple tables which again can hold multiple
    columns. Every data structure, that is done with sql-faker starts
    with a database.

    The db_type parameter is set to mysql by default.
    
    :param db_type: The type of database to export SQL for
    :param db_name: The name of the database
    :type db_name: String
    :type db_type: String
    """

    def __init__(self, db_name: str, db_type: IDatabase = MySQL, lang="en_EN"):
        # Store parameters in object
        self._db_name = db_name
        self._type = db_type

        # Add room for all tables of this table
        self.tables = {}
        self.lang = lang

        # Select sql generation engine
        self._db_strategy = db_type()

    @property
    def db_strategy(self) -> IDatabase:
        return self._db_strategy

    @db_strategy.setter
    def db_strategy(self, strategy: IDatabase) -> None:
        self._db_strategy = strategy

    @property
    def db_name(self):
        return self._db_name

    def add_table(self, table_name: str, n_rows: int = 100) -> None:
        """This method can be used to add a table to the database.

        The table object will be stored in the tables dictionary of the
        database. The table name will be used as key.

        :param table_name: Name of the new table
        :type table_name: String
        :param n_rows: Number of rows that the table should have in DML
        :type n_rows: Integer
        :returns: None
        :raises ValueError: IF n_rows is 0 or less
        """

        if n_rows < 1:
            raise ValueError("n_rows must be at least 1 but was {}".format(
                str(n_rows)
            ))

        self.tables[table_name] = Table(
            table_name=table_name,
            db_object=self,
            n_rows=n_rows
        )

    def generate_data(self, recursive: bool = False) -> None:
        """This method runs all generator methods of all tables
        
        To do so, the method will iterate all stored table objects and
        will run the generate_data method of each table.

        :param recursive: Whether data generation is done for recursive data
        :type recursive: Boolean
        :default recursive: False
        :returns: None
        """
        for key in self.tables.keys():
            self.tables[key].generate_data(recursive=recursive, lang=self.lang)

    def return_ddl(self) -> str:
        """This method generates the database's DDL and returns it as string.
        
        :returns: DDL script as string
        """

        return self._db_strategy.return_ddl(db_name=self._db_name, tables=self.tables)

    def return_dml(self) -> str:
        """This method generates the database's DML and returns it as string.
        
        :returns: DDL script as string
        """

        return self._db_strategy.return_dml(db_name=self._db_name, tables=self.tables)

    def export_ddl(self, file_name: str) -> None:
        """This method exports the database's DDL script to disk.
        
        :param file_name: The file name (e.g. "C:/my_ddl.sql")
        :type file_name: String
        :returns: None
        """

        with open(file_name, "w", encoding="utf-8") as out_file:
            out_file.write(self.return_ddl())

    def export_dml(self, file_name: str) -> None:
        """This method exports the database's DML script to disk.
        
        :param file_name: The file name (e.g. "C:/my_ddl.sql")
        :type file_name: String
        :returns: None
        """

        with open(file_name, "w", encoding="utf-8") as out_file:
            out_file.write(self.return_dml())

    def export_sql(self, file_name: str) -> None:
        """This method exports the database's complete SQL script to disk.
        
        :param file_name: The file name (e.g. "C:/my_ddl.sql")
        :type file_name: String
        :returns: None
        """

        with open(file_name, "w", encoding="utf-8") as out_file:
            out_file.write(self.return_ddl())

        with open(file_name, "a", encoding="utf-8") as out_file:
            out_file.write(self.return_dml())
