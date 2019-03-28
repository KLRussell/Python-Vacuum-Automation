from Vacuum_Global import SQLConnect


def newuser(file):
    f = open(file, "r")
    data = f.read()

    if len(data) > 0:
        asql = SQLConnect('alch')
        asql.connect()

        asql.execute(data)

        asql.close()

    f.close()
