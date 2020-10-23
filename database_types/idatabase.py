from abc import abstractmethod


class IDatabase:
    @abstractmethod
    def return_ddl(self, db_name, tables) -> str:
        pass

    @abstractmethod
    def create_table(self, db_name: str, table_name: str) -> str:
        pass

    @abstractmethod
    def create_column(self, column_name: str, not_null: bool, ai: bool, data_type: str) -> str:
        pass

    @abstractmethod
    def create_primary_key(self, column_name) -> str:
        pass

    @abstractmethod
    def create_foreign_key(self, db_name: str, column_name: str, data_type: str, table_name: str,
                           target_table_name: str,
                           target_column_name: str) -> str:
        pass

    @abstractmethod
    def return_dml(self, db_name, tables) -> str:
        pass

    @abstractmethod
    def insert_data(self, db_name: str, table_name: str, rows: int, attributes, data, datatype) -> str:
        pass
