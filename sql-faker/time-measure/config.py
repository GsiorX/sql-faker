class BASE_CONNECTION:
    HOST_NAME = "localhost"
    PORT = 1521
    SID = "XE"
    USER = "sys"
    PASSWD = "root"
    DB_NAME = "sysdba"
    IS_SYSDBA = True

class ZSBD_CONNECTION:
    HOST_NAME = BASE_CONNECTION.HOST_NAME
    PORT = BASE_CONNECTION.PORT
    SID = BASE_CONNECTION.SID
    USER = "C##ZSBD"
    PASSWD = "root"
    DB_NAME = "C##ZSBD"
    IS_SYSDBA = False
