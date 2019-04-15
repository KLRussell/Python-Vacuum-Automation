from Vacuum_Global import SQLConnect


def newuser(file):
    f = open(file, "r")

    try:
        data = f.read()

        if len(data) > 0:
            asql = SQLConnect('alch')
            asql.connect()

            try:
                asql.execute(data)
            finally:
                asql.close()
    finally:
        f.close()
