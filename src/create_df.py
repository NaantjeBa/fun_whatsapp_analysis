import os
from os import listdir
from os.path import isfile, join


def get_raw_txtfiles():
    "Get list with every txt file"
    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list

"For every export txt file in directory"

def read_in_txt():
    "Read in txt file"
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports")
    file = open("raw_jesse.txt", encoding='utf-8')
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

read_in_txt()