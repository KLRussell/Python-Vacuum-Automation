from Vacuum_Global import SQLConnect


def newuser(file):
    data = open(file, "r").read()

    if len(data) > 0:
        asql = SQLConnect('alch')
        asql.connect()

        asql.execute(data)

        asql.close()
