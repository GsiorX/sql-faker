from column import Column
from random import sample

from database_types.idatabase import IDatabase


class ForeignKey(Column):
    """This column class represents foreign key objects of a table.
    
    This class is used to setup a new column that references another column
    in another table. It is a sub class of Column and inherits most of its
    attributes.

    :param column_name: Defines the column's name
    :param n_rows: Defines how many rows to generate for the column
    :param target_db: Defines the key's target database
    :param target_table: Defines the key's target table
    :param target_column: Defines the key's target column (primary key)
    :param table_object: Holds the parent table which the key belongs to
    :type column_name: String
    :type table_object: sql-faker Table object
    :type target_db: sql-faker Database object
    :type target_table: String
    :type target_column: String
    :type n_rows: Integer
    """

    def __init__(self, column_name: str, table_object, target_db, target_table: str, target_column: str, n_rows: int,
                 engine: IDatabase):

        self._target_db = target_db
        self._target_table = target_table
        self._target_column = target_column

        # Locate the target column
        tbl = target_db.tables[self._target_table]
        clmn = tbl.columns[self._target_column]

        # Retrieve the real results from target column/table
        self._target_table_n_rows = clmn._n_rows
        self._target_row_data_type = clmn._data_type

        # Instantiate the master class but fix some parameters
        super().__init__(
            column_name=column_name,
            n_rows=n_rows,
            data_type=self._target_row_data_type,
            ai=False,
            not_null=False,
            table_object=table_object,
            data_target=None,
            engine=engine,
            kwargs=None
        )

    def generate_data(self, recursive=False, lang=None):
        """This method generates foreign key data by sampling the respective primary key.
        
        :param lang:
        :param recursive: Whether data generation is done for recursive data
        :type recursive: Boolean
        :default recursive: False
        :returns: None
        """
        foreign_key_list = []

        if not recursive:
            # sample foreign key value from list of primary key values
            for _ in range(self._n_rows):
                foreign_key_list.append(sample(list(range(self._target_table_n_rows)), 1)[0] + 1)
        else:
            n_fk = self._n_rows
            n_pk = self._target_table_n_rows
            pk = list(range(self._target_table_n_rows))
            nn = int(n_fk / n_pk)

            for _ in range(1, nn + 1):
                foreign_key_list.append("NULL")
            for value in pk[:-1]:
                for _ in range(nn):
                    foreign_key_list.append(str(value))

        self.data = foreign_key_list

    def return_ddl(self) -> str:
        """This method returns the DDL line of the foreign key column.
        
        :returns: DDL line as String
        """

        return self._engine.create_foreign_key(self._table_object._db_object._db_name, self._column_name,
                                               str.upper(self._data_type), self._table_object._table_name,
                                               self._target_db.tables[self._target_table]._table_name,
                                               self._target_db.tables[self._target_table].columns[
                                                   self._target_column]._column_name)

    def return_foreign_column(self) -> str:
        return super().return_ddl()
