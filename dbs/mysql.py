from dbs.idatabase import IDatabase


class MySQL(IDatabase):
    def return_ddl(self, db_name, tables) -> str:
        ddl_output = "DROP DATABASE IF EXISTS {};\n".format(db_name)
        ddl_output += "CREATE DATABASE {};\n".format(db_name)
        ddl_output += "USE {};\n\n".format(db_name)

        for key in tables:
            table_object = tables[key]

            ddl_output += table_object.return_ddl()

        return ddl_output

    def return_dml(self, db_name, tables) -> str:
        dml_output = "USE {};\n\n".format(db_name)

        for key in tables:
            table_object = tables[key]

            dml_output += table_object.return_dml()

        return dml_output
