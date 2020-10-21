from database_types.idatabase import IDatabase


class Oracle(IDatabase):
    def return_ddl(self, db_name, tables) -> str:
        ddl_output = "SET DEFINE OFF;\n"
        ddl_output += "DROP USER {} CASCADE;\n".format(db_name)
        ddl_output += "CREATE USER {} IDENTIFIED BY {};\n".format(db_name, db_name)
        ddl_output += "ALTER USER {} quota unlimited on USERS;\n".format(db_name)
        ddl_output += "ALTER SESSION SET CURRENT_SCHEMA = {};\n\n".format(db_name)

        for key in tables:
            table_object = tables[key]

            ddl_output += table_object.return_ddl()

        return ddl_output

    def return_dml(self, db_name, tables) -> str:
        dml_output = ""

        for key in tables:
            table_object = tables[key]

            dml_output += table_object.return_dml()

        return dml_output
