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


def create_empty_df():
    master_df = pd.DataFrame(columns=['datetime', 'user', 'message'])

    return master_df


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
    logging.debug(log_topic.head())

    return df_master


def get_android_tab_dels():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\android"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    log_topic = file_list
    logging.debug(type(log_topic))
    logging.debug(log_topic)
    return file_list


def get_iphonetab_dels():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\iphone"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list


def get_iphone_tab_dels():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\iphone"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list


def change_to_android_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\android")


def change_to_iphone_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\iphone")


def _read_in_tabdel(filename):
    "read in txt file to df"
    df = pd.read_csv(filename, sep=r'\t', engine='python', header=None,
                     names=['datetime', 'user', 'message'],
                     parse_dates=['datetime'], encoding='utf-8')

    return df


def _append_to_masterdf(master_df, df):
    "Append txt file to master_df"

    master_df = master_df.append(df)

    return master_df


def loop_append_to_masterdf(master_df, file_list):
    for file in file_list:
        df = _read_in_tabdel(file)
        master_df = _append_to_masterdf(master_df, df)

    return master_df


# master_df = create_empty_df()

change_to_masterdf_dir()
master_df = get_master_df()
file_list_android = get_android_tab_dels()
# change_to_android_dir()
# raw_master_df = loop_append_to_masterdf(master_df, file_list_android)
# file_list_iphone = get_iphone_tab_dels()
# change_to_iphone_dir()
# raw_master_df2 = loop_append_to_masterdf(raw_master_df, file_list_iphone)



"Remove duplicates based on Datetime and Message"
"Adjust user names to one user per person"




"Make name columns for first and last name"
"""Add Year, Month, Weekday and Hour column based on Datetime"""

"Make columns for nr words"
"Make columns to check if message contains certain topics"

"Save file to csv"
"Create pickle"
