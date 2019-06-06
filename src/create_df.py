import os
from os import listdir
from os.path import isfile, join

"Get list with every txt file"
def get_raw_txtfiles():

    mypath = "C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports"
    file_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    return file_list

"For every export txt file in directory"
"Read in txt file"
def read_in_txt():
    os.chdir("C:\\Users\\jniens\\GoogleDrive\\Python_Projects\\fun_whatsapp_analysis\\data_files\\whatsapp_exports")
    file = open("raw_jesse.txt", encoding='utf-8')
    read_lines = file.readlines()  # This makes it a list with an item per record

    return read_lines

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