import cx_Oracle


class OracleConnection:
    @staticmethod
    def create(connection_data):
        class ConnectionWrapper:
            def __init__(self):
                dsn_tns = cx_Oracle.makedsn(
                    connection_data.HOST_NAME,
                    str(connection_data.PORT),
                    sid=connection_data.SID,
                )
                self.connection = cx_Oracle.connect(
                    user=connection_data.USER,
                    password=connection_data.PASSWD,
                    dsn=dsn_tns,
                    mode=cx_Oracle.SYSDBA
                    if connection_data.IS_SYSDBA
                    else cx_Oracle.DEFAULT_AUTH,
                    encoding="utf8",
                )
                self.cursor = self.connection.cursor()

            def __enter__(self):
                return (self.connection, self.cursor)

            def __exit__(self, exc_type, exc_value, traceback):
                try:
                    self.connection.close()
                except Exception as e:
                    print(e)

        return ConnectionWrapper()
