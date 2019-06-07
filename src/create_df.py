import os
from os import listdir
from os.path import isfile, join
import logging

LOG_FORMAT = '%(asctime)s:%(message)s'
logging.basicConfig(filename='C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\log\\debug.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w'
                    )

def get_raw_txtfiles():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list


def read_in_txt(file_name):
    "Read in txt file"
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports")
    file = open(file_name, encoding='utf-8')
    read_lines = file.readlines()  # This makes it a list with an item per record

    return read_lines


def filter_out(read_lines):
    "Filter out rows with less than two ':' characters (eg: Koen heeft groep aangemaakt)"
    filtered_list = [line for line in read_lines if line.count(':') > 1]

    return filtered_list


def first_tab(filtered_list):
    first_tab_list = []
    for line in filtered_list:
        first_tab_index = line.find(' - ')  # Find first tab index
        new_line = line[:first_tab_index] + '\t' + line[first_tab_index + 3:]  # create line with first tab
        if new_line[0] == '[':
            new_line = new_line[1:]
        first_tab_list.append(new_line)

    # count_first_tab = len(first_tab_list)
    return first_tab_list


def second_tab(first_tab_list):
    second_tab_list = []
    for line in first_tab_list:
        second_tab_index = line.find(': ')  # Find second tab index
        new_line = line[:second_tab_index] + '\t' + line[second_tab_index + 2:]  # create line with second tab
        second_tab_list.append(new_line)

    # count_second_tab = len(second_tab_list)
    return second_tab_list


def filter_in_twotabs(second_tab_list):

    filtered_tab_list = [line for line in second_tab_list if line.count('\t') == 2]

    return filtered_tab_list


def change_to_tabdel_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\tab_dels")


def write_tabdel_txtfile(file_nr, filtered_tab_list):

    file_name = 'tab_del_file_'
    file_nr_str = str(file_nr)
    file_type = '.txt'
    full_file_name = file_name + file_nr_str + file_type
    with open(full_file_name, 'w', encoding='utf-8') as f:
        for item in filtered_tab_list:
            f.write(item)


def loop_create_tabdels(file_list):
    for file_nr, file in enumerate(file_list):
        read_lines = read_in_txt(file)
        filtered_list = filter_out(read_lines)
        first_tab_list = first_tab(filtered_list)
        second_tab_list = second_tab(first_tab_list)
        filtered_tab_list = filter_in_twotabs(second_tab_list)
        change_to_tabdel_dir()
        write_tabdel_txtfile(file_nr, filtered_tab_list)

"Make file tab delimited like Adams"
"Save to other directory"


"Create empty master_df with columns Datetime, User and Message"
"For every tab delimited file txt in tab_del directory"
"read in txt file to df"
"Append txt file to master_df"
"Remove duplicates based on Datetime and Message"
"Adjust user names to one user per person"


"Make name columns for first and last name"
"""Add Year, Month, Weekday and Hour column based on Datetime"""

"Make columns for nr words"
"Make columns to check if message contains certain topics"

"Save file to csv"
"Create pickle"




# logging.debug(read_in_txt.__name__)
# logging.debug(type(read_lines))
# logging.debug(len(read_lines))
# logging.debug(read_lines[:3])
# logging.debug('------------------------')
#
#
# logging.debug(filter_out.__name__)
# logging.debug(type(filtered_list))
# logging.debug(len(filtered_list))
# logging.debug(f'Records deleted: {len(read_lines) - len(filtered_list)}')
# logging.debug(filtered_list[:3])
# logging.debug('------------------------')
#
#
#
# logging.debug(first_tab.__name__)
# logging.debug(type(first_tab_list))
# logging.debug(len(first_tab_list))
# logging.debug(first_tab_list[:3])
# logging.debug('------------------------')
#
#
# logging.debug(second_tab.__name__)
# logging.debug(type(second_tab_list))
# logging.debug(len(second_tab_list))
# logging.debug(second_tab_list[:3])
# logging.debug('------------------------')
#
#
# logging.debug(filter_out.__name__)
# logging.debug(type(filtered_tab_list))
# logging.debug(len(filtered_tab_list))
# logging.debug(f'Records deleted: {len(second_tab_list) - len(filtered_tab_list)}')
# logging.debug(filtered_tab_list[:3])
# logging.debug('------------------------')
#
#
#
#
#
file_list = get_raw_txtfiles()
loop_create_tabdels(file_list)