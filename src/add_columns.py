import pandas as pd
import os
import logging
import numpy as np

LOG_FORMAT = '%(asctime)s:%(funcName)s:%(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w'
                    )


def change_to_master_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\3_master_csv_fact")


def read_in_df():
    "Read in master csv file"

    filename = 'new_master_facttable.csv'
    df = pd.read_csv(filename, sep=';')

    log_topic = df
    logging.debug(type(log_topic))
    logging.debug(len(log_topic))
    logging.debug(log_topic.columns)
    logging.debug(log_topic.dtypes)
    #logging.debug(log_topic.head())

    return df


def add_date_columns(df):

    "Add date columns"

    df['year'] = pd.DatetimeIndex(df['datetime']).year
    df['month_name'] = pd.DatetimeIndex(df['datetime']).month_name()
    df['month_nr'] = pd.DatetimeIndex(df['datetime']).month
    df['weekday_name'] = pd.DatetimeIndex(df['datetime']).weekday_name
    df['weekday_nr'] = pd.DatetimeIndex(df['datetime']).weekday
    df['hour'] = pd.DatetimeIndex(df['datetime']).hour
    df['hour_adj'] = np.where(df['hour'] >= 6, df.hour - 6, df.hour + 18)

    log_topic = df
    logging.debug(type(log_topic))
    logging.debug(len(log_topic))
    logging.debug(log_topic.columns)
    logging.debug(log_topic.dtypes)

    return df


def add_name_cols(df):

    "Make name columns for first and last name"
    df['first_name'] = df.user.str.split().str[0]
    df['last_name'] = df.user.str.split().str[1:]

    log_topic = df
    logging.debug(type(log_topic))
    logging.debug(len(log_topic))
    logging.debug(log_topic.first_name.unique())
    logging.debug(log_topic.last_name)
    # logging.debug(log_topic.columns)
    logging.debug(log_topic.dtypes)

    return df


def add_fun_col(df):
    df['group'] = 'Fun Fun Fun'

    return df


def add_contains_cols(df):

    "Add contains columns"
    df['contains_beer'] = (df['message'].str.count('bier') +
                           df['message'].str.count('Bier') +
                           df['message'].str.count('pils') +
                           df['message'].str.count('Pils')
                           ) > 0
    df['contains_media'] = df['message'].str.count('weggelaten') > 0
    df['contains_url'] = df['message'].str.count('://www.') > 0
    df['contains_hashtag'] = df['message'].str.count('#') > 0

    return df


def add_cum_cols(df):
    "Make cumulative and YTD columns"
    df['ytd_count_all'] = df.groupby('year')['message'].cumcount()
    df['ytd_count_user'] = df.groupby(['year', 'user'])['message'].cumcount()
    df['cum_count_user'] = df.groupby(['user'])['message'].cumcount()
    df['cum_count_fun'] = df.groupby(['group'])['message'].cumcount()


    logging.debug(len(df))
    return df


def change_to_addedcols_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\4_added_cols_to_master")


def addedcols_df_to_csv(df):
    filename = 'added_cols_facttable.csv'
    df.to_csv(filename, sep=';', encoding='utf-8', index=False)


change_to_master_dir()
df = read_in_df()
df = add_date_columns(df)
df = add_name_cols(df)
df = add_fun_col(df)
df = add_contains_cols(df)
df = add_cum_cols(df)
change_to_addedcols_dir()
addedcols_df_to_csv(df)
