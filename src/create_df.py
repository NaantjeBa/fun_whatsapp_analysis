import pandas as pd
import os
from os import listdir
from os.path import isfile, join

"Create empty master_df with columns Datetime, User and Message"
def create_empty_df():
    master_df = pd.DataFrame(columns=['datetime', 'user', 'message'])

    return master_df


def get_android_tab_dels():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_tab_dels\\android"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

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


def read_in_tabdel(filename):
    "read in txt file to df"
    df = pd.read_csv(filename, sep=r'\t', engine='python', header=None,
                     names=['datetime', 'user', 'message'],
                     parse_dates=['datetime'], encoding='utf-8')

    return df


def append_to_masterdf(master_df, df):
    "Append txt file to master_df"

    master_df = master_df.append(df)

    return master_df


def loop_create_masterdf(master_df, file_list):
    for file in file_list:
        df = read_in_tabdel(file)
        master_df = append_to_masterdf(master_df, df)

    return master_df


file_list_android = get_android_tab_dels()
master_df = create_empty_df()
change_to_android_dir()
raw_master_df = loop_create_masterdf(master_df, file_list_android)
change_to_iphone_dir()
file_list_iphone = get_iphone_tab_dels()
raw_master_df2 = loop_create_masterdf(raw_master_df, file_list_iphone)

print(len(raw_master_df2))


"Remove duplicates based on Datetime and Message"
"Adjust user names to one user per person"




"Make name columns for first and last name"
"""Add Year, Month, Weekday and Hour column based on Datetime"""

"Make columns for nr words"
"Make columns to check if message contains certain topics"

"Save file to csv"
"Create pickle"
