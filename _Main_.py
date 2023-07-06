import CSV_to_DB
import psycopg2
import psycopg2.extras
import time
import pathlib
import os

num_weeks = 38
season = list(range(1, num_weeks + 1))

fantrax_directory = '/Users/zacharyzlotnick/Documents/Programming/EPL Draft/Fantrax Weekly Data/'
rotowire_directory = '/Users/zacharyzlotnick/Documents/Programming/EPL Draft/Rotowire Data/'


# ==================================================
# CONVERT ALL CSVs into Postgres tables
# ==================================================

def populate_fantrax_data():
    for week in season:

        if os.path.exists(fantrax_directory + 'fantrax ' + str(week) + '.csv'):

            # insert any new data to a Fantrax-specific table
            CSV_to_DB.read_csv_file(fantrax_directory, 'fantrax ' + str(week), '.csv', 'Fantrax')
            time.sleep(0.025)

        else:
            print("No Fantrax data found for week %d " % week)


def populate_rotowire_data():
    for week in season:

        # check that the path exists before proceeding
        if os.path.exists(rotowire_directory + 'rotowire ' + str(week) + '.csv'):

            # create all RotoWire advanced player statistic tables by gameweek
            CSV_to_DB.read_csv_file(rotowire_directory, 'rotowire ' + str(week), '.csv', 'RotoWire')
            time.sleep(0.025)

        else:
            print("No RotoWire data found for week %d " % week)

# =================================================================
# Run "Main" functions here
# =================================================================

# populate the fantrax table in the DB
populate_fantrax_data()

# populate the rotowire table in the DB
populate_rotowire_data()
