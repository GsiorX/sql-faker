from database_types.idatabase import IDatabase
from functions import get_fake_data


class Column:
    """The column class represents column objects of a table.

    This class is used to setup a new column, define its meta data and generate
    column data afterwards. Data are stored within the column object and are later
    used when exporting the DML script for the table.

    :param column_name: Defines the column's name
    :param n_rows: Defines how many rows to generate for the column
    :param data_type: Defines the columns data type
    :param ai: Defines the auto increment attribute of the column
    :param not_null: Defines the not null attribute of the column
    :param data_target: The type of fake data that faker should create

    :type column_name: String
    :type n_rows: Integer
    :type ai: Boolean
    :type not_null: Boolean
    :type data_type: String
    :type data_target: String

    :raises ValueError: If column_name not string
    :raises ValueError: If data_target not string
    :raises ValueError: If data_type not string
    :raises ValueError: If ai not boolean
    :raises ValueError: If not_null not boolean
    """

    def __init__(
            self,
            column_name: str,
            n_rows: int,
            table_object,
            kwargs,
            engine: IDatabase,
            data_target="name",
            data_type: str = "int",
            ai: bool = False,
            not_null: bool = False
    ):
        # store all parameters
        self._column_name = column_name
        self._data_type = data_type
        self._ai = ai
        self._not_null = not_null
        self._n_rows = n_rows
        self._data_target = data_target
        self._engine = engine
        self._kwargs = kwargs

        # store own table object
        self._table_object = table_object

        # store data
        self.data = []

    @property
    def column_name(self):
        return self._column_name

    @property
    def not_null(self):
        return self._not_null

    @property
    def ai(self):
        return self._ai

    @property
    def data_type(self):
        return self._data_type

    def generate_data(self, recursive, lang):
        """This method generates data for a column object.
        
        :param lang:
        :param recursive: Whether data generation is done for recursive data
        :type recursive: Boolean
        :returns: None
        """

        if self._ai:
            # generate incrementing values from 1 to n
            self.data = list(range(1, self._n_rows + 1))

        else:
            # generate data using faker
            self.data = get_fake_data(
                data_target=self._data_target,
                n_rows=self._n_rows,
                lang=lang,
                kwargs=self._kwargs
            )

    def return_ddl(self):
        """This method returns the DDL line of the respective column.
        
        :returns: DDL line as String
        """

        return self._engine.create_column(self._column_name, self._not_null, self._ai, str.upper(self._data_type))
