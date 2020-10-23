from database_types.idatabase import IDatabase
from foreign_key import ForeignKey


class MySQL(IDatabase):
    def return_ddl(self, db_name: str, tables) -> str:
        ddl_output = "DROP DATABASE IF EXISTS {};\n".format(db_name)
        ddl_output += "CREATE DATABASE {};\n".format(db_name)
        ddl_output += "USE {};\n\n".format(db_name)

        # Create Tables without foreign keys
        for tkey in tables:
            ddl_output += tables[tkey].return_ddl()

        # Create foreign keys after all the tables are created
        for tkey in tables:
            for ckey in tables[tkey].columns:
                if type(tables[tkey].columns[ckey]) is ForeignKey:
                    ddl_output += tables[tkey].columns[ckey].return_ddl()

        return ddl_output

    def create_table(self, db_name: str, table_name: str) -> str:
        return "CREATE TABLE `{}`.`{}` (\n".format(
            db_name,
            table_name
        )

    def create_column(self, column_name: str, not_null: bool, ai: bool, data_type: str) -> str:
        return "\t`{}` {}{}{},\n".format(
            column_name,
            data_type,
            " AUTO_INCREMENT" if ai else "",
            " NOT NULL" if not_null else ""
        )

    def create_primary_key(self, column_name) -> str:
        return "\t`{}` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,\n".format(
            column_name
        )

    def create_foreign_key(self, db_name: str, column_name: str, data_type: str, table_name: str,
                           target_table_name: str,
                           target_column_name: str) -> str:
        return "ALTER TABLE `{}`.`{}` \n" \
               "\tADD FOREIGN KEY ({}) REFERENCES {}(`{}`) ON DELETE CASCADE;\n\n".format(db_name, table_name,
                                                                                          column_name,
                                                                                          target_table_name,
                                                                                          target_column_name)

    def return_dml(self, db_name: str, tables) -> str:
        dml_output = "USE {};\n\n".format(db_name)

        for tkey in tables:
            dml_output += tables[tkey].return_dml()

        return dml_output

    def insert_data(self, db_name: str, table_name: str, rows: int, attributes, data, datatype) -> str:
        dml_output = "INSERT INTO `{}`.`{}` (`{}`) VALUES\n".format(
            db_name,
            table_name,
            "`, `".join(list(attributes))
        )

        numtypes = ["int", "float", "single", "decimal", "numeric"]

        for row in range(rows):
            line = ""
            for col in range(len(attributes)):
                split = datatype[col].split("(")[0]

                # Escape single quote
                if "'" in str(data[row][col]):
                    data[row][col] = "''".join(data[row][col].split("'"))

                if col > 0:
                    line += ", "
                if split in numtypes:
                    line += str(data[row][col])
                elif split not in numtypes:
                    line += "'" + data[row][col] + "'"

            dml_output += "\t(" + line + "),\n"

        # add semi colon to end of statement
        dml_output = dml_output[:-2] + ";\n\n"

        return dml_output
