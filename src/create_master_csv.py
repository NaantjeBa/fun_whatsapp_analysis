import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import logging

LOG_FORMAT = '%(asctime)s:%(funcName)s:%(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w'
                    )


def change_to_masterdf_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\3_master_csv_fact")


def get_master_df():
    filename = "master_fact.csv"
    df_master = pd.read_csv(filename, sep=';')

    df_master['datetime'] = pd.to_datetime(df_master['datetime'])

    logging.debug(f'Read in master_fact with {len(df_master)} records')

    return df_master


def get_temp_csvs_to_add():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_temp_csv_fact\\android"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    logging.debug(f'Going to add these files: {file_list}')

    return file_list


def change_to_android_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_temp_csv_fact\\android")


def _read_in_temp_fact(filename):
    "read in txt file to df"
    df = pd.read_csv(filename, sep=';', names=['datetime', 'user', 'message'])

    logging.debug(f'Adding: {filename}...')
    # log_topic = df
    # logging.debug(len(log_topic))
    # logging.debug(log_topic.head(3))

    return df


def _append_to_masterdf(master_df, df):
    "Append txt file to master_df"

    master_df = master_df.append(df)

    logging.debug(f'Facttable is now {len(master_df)} long')

    return master_df


def loop_append_to_masterdf(master_df, file_list):
    for file in file_list:
        df = _read_in_temp_fact(file)
        df = _create_datetime_type(df)
        master_df = _append_to_masterdf(master_df, df)

    return master_df


def correct_names(master_df):

    master_df.loc[master_df.user == 'Jesse', 'user'] = 'Jesse Niëns'

    return master_df


def filterout_fun_as_user(master_df):

    df_filtered_fun = master_df[master_df.user != 'Fun Fun Fun']  # Filter out Fun Fun Fun User

    return df_filtered_fun


def remove_duplicates(master_df):

    df_without_duplicates = master_df.drop_duplicates(subset=[
        'datetime',
        'message',
        'user'
    ])

    logging.debug(f'Removed {(len(master_df)-len(df_without_duplicates))} duplicate records')
    logging.debug(f'Which is {round( (len(master_df)-len(df_without_duplicates)) / len(master_df) * 100, 2)} percent')

    return df_without_duplicates


def sort_datetime(master_df):
    master_df.sort_values(by='datetime', inplace=True)

    return master_df


def _create_datetime_type(df):

    df['datetime'] = pd.to_datetime(df['datetime']
                                    , format='%d-%m-%y %H:%M'
                                    )

    return df


def change_to_master_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\3_master_csv_fact")


def write_new_master_csv(master_df):

    filename = 'new_master_facttable.csv'
    master_df.to_csv(filename, sep=';', encoding='utf-8', index=False)


change_to_masterdf_dir()
master_df = get_master_df()
change_to_android_dir()
file_list_android = get_temp_csvs_to_add()
raw_master_df = loop_append_to_masterdf(master_df, file_list_android)
df_correct_names = correct_names(raw_master_df)
df_filtered_fun = filterout_fun_as_user(df_correct_names)
df_clean = remove_duplicates(df_filtered_fun)
df_sorted = sort_datetime(df_clean)
change_to_master_dir()
write_new_master_csv(df_sorted)

