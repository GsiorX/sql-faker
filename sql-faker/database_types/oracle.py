from database_types.idatabase import IDatabase
from foreign_key import ForeignKey


def create_auto_increment_columns(db_name, columns) -> str:
    ddl_output = ""

    for ckey in columns:
        if columns[ckey]._ai:
            column_name = columns[ckey]._column_name
            ddl_output += "CREATE SEQUENCE {}.{}_seq START WITH 1 INCREMENT BY 1;\n\n".format(db_name, column_name)

            ddl_output += "CREATE OR REPLACE TRIGGER {}.{}_seq_tr\n" \
                          "\tBEFORE INSERT ON {}.{} FOR EACH ROW\n" \
                          "\tWHEN (NEW.{} IS NULL)\n" \
                          "BEGIN\n" \
                          "\tSELECT {}.{}_seq.NEXTVAL INTO :NEW.{} FROM DUAL;\n" \
                          "END;\n\n".format(db_name, column_name, db_name, column_name, column_name, db_name,
                                            column_name, column_name)

    return ddl_output


class Oracle(IDatabase):
    def return_ddl(self, db_name, tables) -> str:
        ddl_output = "SET DEFINE OFF;\n"
        ddl_output += "DROP USER {} CASCADE;\n".format(db_name)
        ddl_output += "CREATE USER {} IDENTIFIED BY {};\n".format(db_name, db_name)
        ddl_output += "ALTER USER {} QUOTA UNLIMITED on USERS;\n".format(db_name)
        ddl_output += "ALTER SESSION SET CURRENT_SCHEMA = {};\n\n".format(db_name)

        # Create Tables without foreign keys
        for tkey in tables:
            ddl_output += tables[tkey].return_ddl()
            ddl_output += create_auto_increment_columns(db_name, tables[tkey].columns)

        # Create foreign keys after all the tables are created
        for tkey in tables:
            for ckey in tables[tkey].columns:
                if type(tables[tkey].columns[ckey]) is ForeignKey:
                    ddl_output += tables[tkey].columns[ckey].return_ddl()

        return ddl_output

    def create_table(self, db_name: str, table_name: str) -> str:
        return "CREATE TABLE {}.{} (\n".format(
            db_name,
            table_name
        )

    def create_column(self, column_name: str, not_null: bool, ai: bool, data_type: str) -> str:
        return "\t{} {} {},\n".format(
            column_name,
            data_type,
            " NOT NULL" if not_null else ""
        )

    def create_primary_key(self, column_name) -> str:
        return "\t{} NUMBER(10) PRIMARY KEY NOT NULL,\n".format(column_name)

    def create_foreign_key(self, db_name: str, column_name: str, data_type: str, table_name: str,
                           target_table_name: str, target_column_name: str) -> str:
        return "ALTER TABLE {}.{} ADD CONSTRAINT fk_{}\n" \
               "\tFOREIGN KEY ({}) REFERENCES {}({});\n\n".format(db_name, table_name, column_name, column_name,
                                                                  target_table_name, target_column_name)

    def return_dml(self, db_name, tables) -> str:
        dml_output = ""

        for tkey in tables:
            dml_output += tables[tkey].return_dml()

        return dml_output

    def insert_data(self, db_name: str, table_name: str, rows: int, attributes, data, datatype) -> str:
        dml_output = "INSERT INTO {}.{} ({}) VALUES\n".format(
            db_name,
            table_name,
            ", ".join(list(attributes))
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

            dml_output += "\tSELECT {} FROM dual UNION ALL\n".format(line)

        # add semi colon to end of statement, cut UNION ALL
        dml_output = dml_output[:-11] + ";\n\n"

        return dml_output
