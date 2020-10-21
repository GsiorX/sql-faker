from numpy import array

from database_types.idatabase import IDatabase


class MySQL(IDatabase):
    def return_ddl(self, db_name: str, tables) -> str:
        ddl_output = "DROP DATABASE IF EXISTS {};\n".format(db_name)
        ddl_output += "CREATE DATABASE {};\n".format(db_name)
        ddl_output += "USE {};\n\n".format(db_name)

        # Create Tables
        for tkey in tables:
            ddl_output += "CREATE TABLE `{}`.`{}` (\n".format(
                db_name,
                tables[tkey].table_name
            )

            # Create columns
            for ckey in tables[tkey].columns:
                name = tables[tkey].columns[ckey].column_name
                not_null = tables[tkey].columns[ckey].not_null
                ai = tables[tkey].columns[ckey].ai
                data_type = str.upper(tables[tkey].columns[ckey].data_type)

                ddl_output += "\t`{}` {}{}{},\n".format(
                    name,
                    data_type,
                    " AUTO_INCREMENT" if ai else "",
                    " NOT NULL" if not_null else ""
                )

            # remove the comma at the end of the last line
            ddl_output = ddl_output[:-2]

            # add closing bracket
            ddl_output += "\n);\n\n"

        return ddl_output

    def return_dml(self, db_name, tables) -> str:
        dml_output = "USE {};\n\n".format(db_name)

        for tkey in tables:
            data = []
            attributes = []
            datatype = []

            # get all data into one place
            for ckey in tables[tkey].columns:
                datatype.append(tables[tkey].columns[ckey].data_type)
                data.append(tables[tkey].columns[ckey].data)
                attributes.append(ckey)

            # transpose the data
            data = array(data).transpose()

            # get table meta
            rows = tables[tkey].n_rows
            cols = len(attributes)

            # TODO Adopt this for dbs support

            dml_output = "INSERT INTO `{}`.`{}` (`{}`) VALUES\n".format(
                db_name,
                tables[tkey].table_name,
                "`, `".join(list(attributes))
            )

            numtypes = ["int", "float", "single", "decimal", "numeric"]

            for row in range(rows):
                line = ""
                for col in range(cols):
                    if col > 0:
                        line += ", "
                    if datatype[col].split("(")[0] not in numtypes:
                        # try:
                        line += "'" + data[row][col] + "'"
                        # except:
                        #     print(data[row][col])
                    if datatype[col].split("(")[0] in numtypes:
                        line += str(data[row][col])
                dml_output += "\t(" + line + "),\n"

            # add semi colon to end of statement
            dml_output = dml_output[:-2] + ";\n\n"

        return dml_output
