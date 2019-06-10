import os
from os import listdir
from os.path import isfile, join
import logging
import re

LOG_FORMAT = '%(asctime)s:%(funcName)s:%(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w'
                    )


def get_raw_txtfiles():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\1_whatsapp_exports"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list


def _change_to_waexport_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\1_whatsapp_exports")


def _read_in_txt(file_name):
    "Read in txt file"
    file = open(file_name, encoding='utf-8')
    read_lines = file.readlines()  # This makes it a list with an item per record

    return read_lines


def _filter_out_singles(read_lines):
    "Filter in rows with more than one ':' characters (eg: Koen heeft groep aangemaakt is being fileterd out)"
    filtered_list = [line for line in read_lines if line.count(':') > 1]

    return filtered_list


def _make_first_sep(filtered_list):
    first_sep_list = []
    for line in filtered_list:
        first_tab_index = line.find(' - ')  # Find first tab index
        new_line = line[:first_tab_index] + ';' + line[first_tab_index + 3:]  # create line with first tab
        if new_line[0] == '[':
            new_line = new_line[1:]
        first_sep_list.append(new_line)

    # count_first_tab = len(first_tab_list)
    return first_sep_list


def _make_second_sep(first_tab_list):
    second_tab_list = []
    for line in first_tab_list:
        second_tab_index = line.find(': ')  # Find second tab index
        new_line = line[:second_tab_index] + ';' + line[second_tab_index + 2:]  # create line with second tab
        second_tab_list.append(new_line)

    return second_tab_list


def _filter_in_twoseps(second_tab_list):

    filtered_sep_list = [line for line in second_tab_list if line.count(';') == 2]

    log_topic = filtered_sep_list
    logging.debug(len(log_topic))

    return filtered_sep_list


def _check_first_col_for_date(sec_sep_list):
    date_filtered_list = []
    for line in sec_sep_list:
        my_regex = re.compile(r'\d\d-\d\d-\d\d \d\d:\d\d')  # regex to check if line start with datetime
        mo = my_regex.search(line)
        if mo:  # Check if line has two tabs and start with datetime
            date_filtered_list.append(line)

    log_topic = date_filtered_list
    logging.debug(len(log_topic))

    return date_filtered_list


def _change_to_temp_dir():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\2_temp_csv_fact\\android")


def _write_csv_file(file_nr, filtered_tab_list):

    file_name = 'temp_facttable_'
    file_nr_str = str(file_nr)
    file_type = '.csv'
    full_file_name = file_name + file_nr_str + file_type
    with open(full_file_name, 'w', encoding='utf-8') as f:
        for item in filtered_tab_list:
            f.write(item)


def loop_create_csvs(file_list):
    for file_nr, file in enumerate(file_list):
        _change_to_waexport_dir()
        read_lines = _read_in_txt(file)
        filtered_list = _filter_out_singles(read_lines)
        first_tab_list = _make_first_sep(filtered_list)
        second_tab_list = _make_second_sep(first_tab_list)
        filtered_tab_list = _filter_in_twoseps(second_tab_list)
        date_filtered_list = _check_first_col_for_date(filtered_tab_list)
        _change_to_temp_dir()
        _write_csv_file(file_nr, date_filtered_list)


file_list = get_raw_txtfiles()
loop_create_csvs(file_list)
