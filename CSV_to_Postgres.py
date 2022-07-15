import os
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np
import DB_Table_Math


def initialize_DB(file_name):
    # connection credentials
    hostname = "localhost"
    username = "zzlotnick"
    pwd = "zach"
    port_id = 5432

    if file_name.find("fantrax") != -1:
        database = "Fantrax"
    elif file_name.find("rotowire") != -1:
        database = "RotoWire"
    else:
        print("File name unable to be associated with an existing database")

    return "host=%s dbname=%s user=%s password=%s port=%s" % (hostname, database, username, pwd, port_id)

# establish connection to database and execute all sub-methods
def read_csv_file(directory, file_name, file_type, database):
    path = directory + file_name + file_type
    print("copying file: \"%s\" to Postgres DB..." % file_name)

    # get the dataframe from the file path
    dataframe = pd.read_csv(path)

    # reformat rows and columns to fix postgres naming protocols
    file_name_formatted = reformat_table_name(file_name)
    col_name_formatted = reformat_col_name(dataframe, database)

    try:
        conn_string = initialize_DB(file_name)
        print("establishing connection...")

        # initialize connection and cursor
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("connected to database")

        # create a table with refactored titles & columns
        cur.execute("DROP table if exists %s" % file_name_formatted)
        cur.execute("CREATE table %s (%s)" % (file_name_formatted, col_name_formatted))
        print("table \"%s\" created successfully!" % file_name_formatted)

        dataframe.to_csv("temp.csv", header=dataframe.columns, index=False, encoding='utf-8')
        print("dataframe converted to CSV file")

        temp_file = open("temp.csv")
        print("CSV opened")

        # generate SQL statement
        SQL_STATEMENT = """
        COPY %s FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
        """ % file_name_formatted

        # COPY all data from CSV to Database Table via SQL_Statement
        cur.copy_expert(sql=SQL_STATEMENT, file=temp_file)
        cur.execute("grant select on table %s to public" % file_name_formatted)
        print("%s copied successfully!" % file_name_formatted)

        # write changes
        conn.commit()

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close
        if os.path.exists(path):
            os.remove("temp.csv")


# take name of file and standardize it somehow
def reformat_table_name(file_name):

    return file_name.lower().replace(" ", "_").replace("-", "_")


# take name of column and standardize it somehow
def reformat_col_name(dataframe, database):

    dataframe.columns = [col.lower().replace("+/-", "add_drop_delta").replace(" ", "_").replace("-", "_"). \
                         replace("/", "p").replace("%", "percent") \
                         for col in dataframe.columns]

    # additional formatting for database specific purposes

    # FANTRAX ONLY
    if database == "Fantrax":
        dataframe = DB_Table_Math.add_ghost_points(dataframe)

    # ROTOWIRE ONLY
    elif database == "RotoWire":
        dataframe.columns = [col.replace("on", "sub_on").replace("off", "sub_off") \
                             for col in dataframe.columns]

    # Data type conversions to SQL design types
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    # form the final column string with commas as the delimiter
    col_str = ", ".join(
        "{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str
