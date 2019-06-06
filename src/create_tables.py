import pandas as pd
import numpy as np
from collections import Counter
import re

def prep_txt_file_jesse(raw_text):
    """
    This function creates a .txt and .csv file from raw exported chat history.

    :param raw_text: a raw text file exported directly from android phone
    :type raw_text: .txt

    :return: tab delimited txt file and comma delimited csv file which has columns: 'datetime', 'user', 'message'
     on directory of module (+ confirmation print)
    :type: .txt & .csv

    """

    file = open(raw_text, encoding='utf-8')
    read_lines = file.readlines()
    count_input = len(read_lines)

    # Make new list with lines that have more than 1 ':' characters out of original file
    filter_list = []
    for line in read_lines:
        char_count = line.count(':')
        if char_count > 1:
            filter_list.append(line)
    count_first_filter = len(filter_list)

    # Make new list with first tab delimeter after date
    first_tab_list = []
    for line in filter_list:
        first_del = line.find(' - ')
        new_line = line[:first_del] + '\t' + line[first_del + 3:]
        if new_line[0] == '[':
            new_line = new_line[1:]
        first_tab_list.append(new_line)
    count_first_tab = len(first_tab_list)

    # Make new list with second tab delimeter username
    second_tab_list = []
    for line in first_tab_list:
        second_del = line.find(': ')
        new_line = line[:second_del] + '\t' + line[second_del + 2:]
        second_tab_list.append(new_line)
    count_second_tab = len(second_tab_list)

    # Filter out line with more than 2 delimeters
    filter_tab_list = []
    for line in second_tab_list:
        count_tab = line.count('\t')
        my_regex = re.compile(r'\d\d-\d\d-\d\d \d\d:\d\d')  # regex to check if line start with datetime
        mo = my_regex.search(line)
        if count_tab == 2 and mo != None: # Check if line has two tabs and start with datetime
            filter_tab_list.append(line)

    count_filter_tab = len(filter_tab_list)

    first_day = filter_tab_list[0][:2]
    first_month = filter_tab_list[0][3:5]
    first_year = filter_tab_list[0][6:8]

    last_day = filter_tab_list[-1][:2]
    last_month = filter_tab_list[-1][3:5]
    last_year = filter_tab_list[-1][6:8]

    txt = '.txt'
    csv = '.csv'

    filename = first_day + first_month + first_year + '-' + last_day + last_month + last_year + 'r' + str(count_filter_tab)
    file_txt = filename + txt
    file_csv = filename + csv

    # Write list to new txt file
    with open(file_txt, 'w', encoding='utf-8') as f:
        for item in filter_tab_list:
            f.write(item)

    df = pd.read_csv(file_txt, sep=r'\t', engine='python', header=None, names=['datetime', 'user', 'message'],
                      parse_dates=['datetime'], encoding='utf-8')

    df['datetime'] = df['datetime'].dt.strftime('%d/%m/%Y %H:%M')
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime')  # Set Datetime as index column

    df.to_csv(file_csv, ',', encoding='utf-8')

    # Print out tracking of line removal
    print('\nFILE CREATED!\n'
          '# Input file lines: {}\n'
          '# First filter lines = {}\n'
          '# First tab lines = {}\n'
          '# Second lines = {}\n'
          '# Tab filter lines = {}\n'.format(count_input, count_first_filter, count_first_tab, count_second_tab, count_filter_tab))

    # Return the file that is outputted as string
    return filename


def create_fact_messages(main_csv, new_csv):
    """
    This function appends to csv files containing the columns 'datetime', 'user' and 'message'

    :param main_csv: the main csv file
    :type main_csv: .csv
    :param new_csv: the new, to append, csv file which has columns: 'datetime', 'user', 'message'
    :type new_csv: .csv

    :return: Combined csv file and picklefile for csv
    :type .csv & .pickle

    """

    "Create variables for read_csv function"
    csv1 = main_csv + '.csv'
    csv2 = new_csv + '.csv'


    "Read in text files to dataframe"
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)

    df1 = df1[['datetime', 'user', 'message']]

    "Make list for concat function"
    frames = [df1, df2]

    "Union and remove duplicates"
    combined_df = pd.concat(frames) # Union the tables
    # combined_df['datetime'] = combined_df['datetime'].dt.strftime('%d/%m/%Y %H:%M')  # Format dates in same way as string
    combined_df = combined_df.drop_duplicates(subset=['datetime', 'message'])  # Remove duplicates where Columns Datetime and Message are the same

    "Change the different user names"
    combined_df.loc[combined_df.user == 'Maurits', 'user'] = 'Maurits Edens'
    combined_df.loc[combined_df.user == 'Adam Van Der Maat', 'user'] = 'Adam van der Maat'
    combined_df.loc[combined_df.user == 'Hendrik Strabbinb', 'user'] = 'Hendrik Strabbing'
    combined_df.loc[combined_df.user == 'Ramses', 'user'] = 'Ramses Walon'
    combined_df.loc[combined_df.user == 'Jesse', 'user'] = 'Jesse Niëns'
    combined_df.loc[combined_df.user == 'Nils De Koning', 'user'] = 'Nils Koning'
    combined_df.loc[combined_df.user == 'Django', 'user'] = 'Django Walon'
    combined_df.loc[combined_df.user == '\u202a+31\xa06\xa081031891\u202c', 'user'] = 'Lenny von Liechtenstein'
    combined_df.loc[combined_df.user == 'Koen De Boer', 'user'] = 'Koen de Boer'
    combined_df['userid'] = combined_df['user'].str.replace(' ', '')

    "Filter out Fun Fun Fun as user"
    combined_df = combined_df[combined_df.user != "Fun Fun Fun"]  # Filter out Fun Fun Fun User

    """Add Year, Month, Weekday and Hour column based on Datetime"""
    combined_df['year'] = pd.DatetimeIndex(combined_df['datetime']).year
    combined_df['month_name'] = pd.DatetimeIndex(combined_df['datetime']).month_name()
    combined_df['month_nr'] = pd.DatetimeIndex(combined_df['datetime']).month
    combined_df['weekday_name'] = pd.DatetimeIndex(combined_df['datetime']).weekday_name
    combined_df['weekday_nr'] = pd.DatetimeIndex(combined_df['datetime']).weekday
    combined_df['hour'] = pd.DatetimeIndex(combined_df['datetime']).hour
    combined_df['hour_adj'] = np.where(combined_df['hour'] >= 6, combined_df.hour - 6, combined_df.hour + 18)

    "Make name columns for first and last name"
    combined_df['first_name'] = combined_df.user.str.split().str[0]
    combined_df['last_name'] = combined_df.user.str.split().str[1:]

    "Make one column for the Fun Fun Fun"
    combined_df['group'] = 'Fun Fun Fun'

    "Make columns for nr words"
    combined_df['nr_words'] = combined_df['message'].str.count(' ') + 1

    "Make columns to check if message contains certain topics"
    combined_df['contains_beer'] = (combined_df['message'].str.count('bier') +
                                    combined_df['message'].str.count('Bier') +
                                    combined_df['message'].str.count('pils') +
                                    combined_df['message'].str.count('Pils')
                                    ) > 0
    combined_df['contains_media'] = combined_df['message'].str.count('weggelaten') > 0
    combined_df['contains_url'] = combined_df['message'].str.count('://www.') > 0
    combined_df['contains_hashtag'] = combined_df['message'].str.count('#') > 0

    "Create values for filename. Before datetime is converted to datetime format"
    first_day = combined_df["datetime"].iloc[0][8:10]
    first_month = combined_df["datetime"].iloc[0][5:7]
    first_year = combined_df["datetime"].iloc[0][2:4]

    last_day = combined_df["datetime"].iloc[-1][8:10]
    last_month = combined_df["datetime"].iloc[-1][5:7]
    last_year = combined_df["datetime"].iloc[-1][2:4]

    "Set datetime as index column and sort on index"
    combined_df['datetime'] = pd.to_datetime(combined_df['datetime'])
    combined_df = combined_df.sort_values('datetime')
    combined_df = combined_df.set_index('datetime')  # Set Datetime as index column

    "Make cumulative and YTD columns"
    combined_df['ytd_count_all'] = combined_df.groupby('year')['message'].cumcount()
    combined_df['ytd_count_user'] = combined_df.groupby(['year', 'user'])['message'].cumcount()
    combined_df['cum_count_user'] = combined_df.groupby(['user'])['message'].cumcount()
    combined_df['cum_count_fun'] = combined_df.groupby(['group'])['message'].cumcount()

    row_count = str(len(combined_df))
    ext = '.csv'

    filename = 'facttable_' + first_day + first_month + first_year + '-' + last_day + last_month + last_year + 'r' + row_count + ext

    "Save file to csv"
    combined_df.to_csv(filename, sep=',', encoding='utf-8')

    'Create pickle'
    df = pd.read_csv(filename, sep=',', engine='python', encoding='utf-8', parse_dates=['datetime'], index_col=['datetime'])
    df.to_pickle('fact_messages.pickle')

    "Print end message"
    print('\nDB_FILE CREATED SUCCESSFULLY!')


def create_dim_words(df):
    """
    This creates a wordcount table from the main facttable

    :param df: dataframe with all datetime, users and messages
    :type df: Dataframe
    :return: new .csv file with wordcount for the whole group and indidual users. Also a .pickle is created for the table
    :type: .csv & .pickle

    """

    "Name for output file"
    name_file = 'dim_words'

    """Make Dataframe for the whole fun"""
    df= df.query('contains_media==False')

    "Convert the message column to string type"
    mes_to_str_fun = df['message'].to_string(index=False).split()

    "Create list with only lowercase words"
    lower_list_fun = []
    for word in mes_to_str_fun:
        low_word = word.lower()
        lower_list_fun.append(low_word)

    "Create list without punctuations"
    no_punct_list = []
    for word in lower_list_fun:
        word = word.replace('.', '')
        word = word.replace(',', '')
        word = word.replace('!', '')
        word = word.replace('?', '')
        word = word.replace(':', '')
        no_punct_list.append(word)

    "Make dictionary-like variable with word count"
    words_fun = Counter(no_punct_list)

    "Convert dictionary into dataframe"
    df_wordcount = pd.DataFrame.from_dict(words_fun, orient='index').reset_index()

    "Rename the columns"
    df_wordcount = df_wordcount.rename(columns={'index': 'word', 0: 'count'})

    "Create column that is True if word only contains letters"
    df_wordcount['only_letters'] = df_wordcount['word'].str.isalpha()

    """From here query through every user and merge to pandas df """

    "Create list with all users"
    user_list = df.user.unique()

    "Create loop for every user"
    for user in user_list:
        user = str(user)
        df_user = df[df['user'] == user]

        "Convert the message column to string type"
        mes_to_str = df_user['message'].to_string(index=False).split()

        "Create lowercase list with all words"
        lower_list = []
        for word in mes_to_str:
            low_word = word.lower()
            lower_list.append(low_word)

        "Create list without punctuations"
        no_punct_list_user = []
        for word in lower_list:
            word = word.replace('.', '')
            word = word.replace(',', '')
            word = word.replace('!', '')
            word = word.replace('?', '')
            word = word.replace(':', '')
            no_punct_list_user.append(word)

        "Make dictionary-like variable with word count"
        words_user = Counter(no_punct_list_user)

        "Convert dictionary into dataframe"
        df_usercount = pd.DataFrame.from_dict(words_user, orient='index').reset_index()

        "Rename the column with first 3 letters from name"
        colname = user[:3].lower()
        df_usercount = df_usercount.rename(columns={'index': 'word', 0: colname})

        "Join the user count column to the main table"
        df_wordcount = pd.merge(df_wordcount,
                                df_usercount,
                          left_on='word',
                          right_on='word',
                          how='left')

        " Create new column with relative word count contribution"
        colname_share = colname + '_share'
        df_wordcount[colname_share] = df_wordcount[colname] / df_wordcount['count']

    "sort values by count"
    df_wordcount = df_wordcount.sort_values(by=['count'], ascending=False)

    "Create csv"
    df_wordcount.to_csv(name_file + '.csv', index=False)

    "Create pickle"
    df = pd.read_csv(name_file + '.csv', sep=',', engine='python', encoding='utf-8', index_col=['word'])
    df.to_pickle(name_file + '.pickle')

    print('CSV AND PICKLE CREATED!\nNAME=' + name_file)


def create_dim_users(df_fact, df_word):
    """

    :param df1:
    :return:
    """

    name_file = 'dim_users'
    user_list = df_fact.user.unique()

    df_only_letters = df_word.query('only_letters==True')

    "Make empty df"
    df_dim_users = pd.DataFrame(columns=['user', 'first_name', 'nr_messages', 'nr_words',
                                         'voc', 'unique_words', 'percent_unique', 'nr_beer',
                                         'nr_media', 'nr_url',  'nr_hashtag', 'nr_beer_100',  'nr_media_100',
                                         'nr_url_100', 'nr_hashtag_100'])

    for user in user_list:
        user_list = []

        col_count = user[:3].lower()
        col_share = col_count + '_share'

        first_name = user.split()[0]

        nr_messages = len(df_fact.query('user=="' + user + '"'))
        nr_words = df_only_letters[col_count].sum()
        nr_words_int = int(nr_words)
        voc = df_only_letters[col_count].count()

        df_one_words = df_only_letters.query(col_count + '==count')
        unique_words = df_one_words[col_count].count()

        percent_unique = (unique_words / voc) * 100

        df_user = df_fact.query('user=="' + user + '"')

        "Count topics"
        nr_beer = len(df_user.query('contains_beer==True'))
        nr_beer_100 = (nr_beer / nr_messages) * 100

        nr_media = len(df_user.query('contains_media==True'))
        nr_media_100 = (nr_media / nr_messages) * 100

        nr_url = len(df_user.query('contains_url==True'))
        nr_url_100 = (nr_url / nr_messages) * 100

        nr_hashtag = len(df_user.query('contains_hashtag==True'))
        nr_hashtag_100 = (nr_hashtag / nr_messages) * 100

        df_dim_users = df_dim_users.append({'user': user, 'first_name': first_name, 'nr_messages': nr_messages, 'nr_words': nr_words_int,
                                            'voc': voc, 'unique_words': unique_words, 'percent_unique': percent_unique,
                                            'nr_beer': nr_beer, 'nr_media' : nr_media, 'nr_url': nr_url,
                                            'nr_hashtag': nr_hashtag, 'nr_beer_100': nr_beer_100, 'nr_media_100': nr_media_100,
                                            'nr_url_100': nr_url_100, 'nr_hashtag_100': nr_hashtag_100}, ignore_index=True)

    "Create Rank columns"
    df_dim_users['nr_messages_rank'] = df_dim_users['nr_messages'].rank(ascending=False)
    df_dim_users['nr_words_rank'] = df_dim_users['nr_words'].rank(ascending=False)
    df_dim_users['voc_rank'] = df_dim_users['voc'].rank(ascending=False)
    df_dim_users['unique_words_rank'] = df_dim_users['unique_words'].rank(ascending=False)
    df_dim_users['percent_unique_rank'] = df_dim_users['percent_unique'].rank(ascending=False)
    df_dim_users['nr_beer_rank'] = df_dim_users['nr_beer'].rank(ascending=False)
    df_dim_users['nr_media_rank'] = df_dim_users['nr_media'].rank(ascending=False)
    df_dim_users['nr_url_rank'] = df_dim_users['nr_url'].rank(ascending=False)
    df_dim_users['nr_hashtag_rank'] = df_dim_users['nr_hashtag'].rank(ascending=False)
    df_dim_users['nr_beer_100_rank'] = df_dim_users['nr_beer_100'].rank(ascending=False)
    df_dim_users['nr_media_100_rank'] = df_dim_users['nr_media_100'].rank(ascending=False)
    df_dim_users['nr_url_100_rank'] = df_dim_users['nr_url_100'].rank(ascending=False)
    df_dim_users['nr_hashtag_100_rank'] = df_dim_users['nr_hashtag_100'].rank(ascending=False)

    "Create csv"
    df_dim_users.to_csv(name_file + '.csv', index=False)

    "Create pickle"
    df = pd.read_csv(name_file + '.csv', sep=',', engine='python', encoding='utf-8', index_col=['first_name'])
    df.to_pickle(name_file + '.pickle')

    print('CSV AND PICKLE CREATED \n SAMPLE: ' + str(df_dim_users.head(3)))


# prep_txt_file_jesse('WhatsApp-chat met Fun Fun Fun (1).txt')

# create_fact_messages('facttable_230213-260119r89754', '300418-030219r11884')

df_fact = pd.read_pickle('fact_messages.pickle')
#
# create_dim_words(df_fact)
#
df_word = pd.read_pickle('dim_words.pickle')
#
create_dim_users(df_fact, df_word)
