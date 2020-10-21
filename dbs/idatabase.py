from abc import abstractmethod


class IDatabase:
    @abstractmethod
    def return_ddl(self, db_name, tables) -> str:
        pass

    def return_dml(self, db_name, tables) -> str:
        pass
