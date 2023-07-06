import pandas as pd
import numpy as np

def add_ghost_points(df):
    positions = ['D', 'M', 'F']
    g = 9
    at = 6
    cs = {"D": 6, "M": 1, "F": 0}
    ga = {"D": -2, "M": 0, "F": 0}

    conditions = [df['position'] == positions[0],
                  df['position'] == positions[1],
                  df['position'] == positions[2]]

    choices = [df['fpts'] - (g*df['g'] + at*df['at'] + cs["D"]*df['cs'] + ga["D"]*df['gao']),
               df['fpts'] - (g*df['g'] + at*df['at'] + cs["M"]*df['cs'] + ga["M"]*df['gao']),
               df['fpts'] - (g*df['g'] + at*df['at'] + cs["F"]*df['cs'] + ga["F"]*df['gao'])]

    # subtract any goals, assists, or clean sheets
    df['gh_pts'] = np.select(conditions, choices, default=0)

    return df


def add_gameweek(df, gameweek):
    df['GW'] = gameweek

    return df

