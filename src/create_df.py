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


# def create_empty_df():
#     master_df = pd.DataFrame(columns=['datetime', 'user', 'message'])
#
#     return master_df


def change_to_masterdf_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\3_master_csv_fact")


def get_master_df():
    filename = "master_facttable.csv"
    df_master = pd.read_csv(filename, sep=';')

    log_topic = df_master
    logging.debug(type(log_topic))
    logging.debug(len(log_topic))
    logging.debug(log_topic.columns)
    logging.debug(log_topic.user.unique())
    #logging.debug(log_topic.head())

    return df_master


def get_android_temp_csvs():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_temp_csv_fact\\android"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    log_topic = file_list
    logging.debug(type(log_topic))
    logging.debug(log_topic)
    return file_list


def change_to_android_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_temp_csv_fact\\android")


def _read_in_temp_fact(filename):
    "read in txt file to df"
    df = pd.read_csv(filename, sep=r';', names=['datetime', 'user', 'message'])

    log_topic = df
    logging.debug(len(log_topic))
    # logging.debug(log_topic.head(3))

    return df


def _append_to_masterdf(master_df, df):
    "Append txt file to master_df"

    master_df = master_df.append(df)

    log_topic = master_df
    logging.debug(len(log_topic))
    # logging.debug(log_topic.head(3))
    return master_df


def loop_append_to_masterdf(master_df, file_list):
    for file in file_list:
        df = _read_in_temp_fact(file)
        master_df = _append_to_masterdf(master_df, df)

    log_topic = master_df
    logging.debug(len(log_topic))
    #logging.debug(log_topic.head(3))
    return master_df


def correct_names(master_df):

    log_topic = master_df
    logging.debug(len(log_topic.user.unique()))
    logging.debug(log_topic.user.unique())

    master_df.loc[master_df.user == 'Jesse', 'user'] = 'Jesse Niëns'

    logging.debug(len(log_topic.user.unique()))
    logging.debug(log_topic.user.unique())

    return master_df


def filterout_fun_as_user(master_df):
    log_topic = master_df
    logging.debug(len(log_topic))
    logging.debug(len(log_topic.user.unique()))
    logging.debug(log_topic.user.unique())

    df_filtered_fun = master_df[master_df.user != 'Fun Fun Fun']  # Filter out Fun Fun Fun User

    log_topic = df_filtered_fun
    logging.debug(len(log_topic))
    logging.debug(len(log_topic.user.unique()))
    logging.debug(log_topic.user.unique())

    return df_filtered_fun


def remove_duplicates(master_df):
    logging.debug(len(master_df))

    df_without_duplicates = master_df.drop_duplicates(subset=[
        'datetime',
        'message',
        'user'
    ])

    logging.debug(len(df_without_duplicates))
    logging.debug(f'Removed {round( (len(master_df)-len(df_without_duplicates)) / len(master_df) * 100, 2)} percent')

    return df_without_duplicates


def new_master_df_to_csv(master_df):
    filename = 'new_master_facttable.csv'
    master_df.to_csv(filename, sep=';', encoding='utf-8', index=False)


def change_to_master_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\3_master_csv_fact")

change_to_masterdf_dir()
master_df = get_master_df()
change_to_android_dir()
file_list_android = get_android_temp_csvs()
raw_master_df = loop_append_to_masterdf(master_df, file_list_android)
df_correct_names = correct_names(raw_master_df)
df_filtered_fun = filterout_fun_as_user(df_correct_names)
df_clean = remove_duplicates(df_filtered_fun)
change_to_master_dir()
new_master_df_to_csv(df_clean)

