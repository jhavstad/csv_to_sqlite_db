#! /usr/bin/env python

import pandas
from pysqlite2 import dbapi2 as pysqldb
import os
import sys

def create_state_abbreviations_table(connection, state_abbreviations_path, state_abbreviations_filename):
    state_abbreviations_tablename = 'state_abbreviations'
    state_abbreviations_df = pandas.read_csv(state_abbreviations_path + '/' + state_abbreviations_filename)
    cursor = connection.cursor()
    create_state_abbreviations_table_query = 'CREATE TABLE ' + state_abbreviations_tablename + ' ( State varchar(255), Abbreviation character(2) )'
    print 'SQL Query Text: ' + create_state_abbreviations_table_query
    try:
        cursor.execute(create_state_abbreviations_table_query)
        connection.commit()
    except pysqldb.OperationalError as oe:
        print str(oe)
    for index, data_row in state_abbreviations_df.iterrows():
        add_abbreviation_row_query = 'INSERT INTO ' + state_abbreviations_tablename + ' VALUES( \'' + data_row.State + '\', \'' +  data_row.Abbreviation + '\' )'
        print 'SQL Query Text: ' + add_abbreviation_row_query
        try:
            cursor.execute(add_abbreviation_row_query)
            connection.commit()
        except pysqldb.OperationalError as oe:
            print str(oe)

    cursor.close()

def create_states_counties_tables(connection, states_and_counties_list):
    create_count = 0

    cursor = connection.cursor()
    for state_name, state_df in states_and_counties_list:

        create_state_counties_table_query = 'CREATE TABLE ' + '\'' + state_name + '\'' + ' ( county varchar(255) )'
        print 'SQL Query Text: ' + create_state_counties_table_query
        try:
            cursor.execute(create_state_counties_table_query)
            connection.commit()
            create_count += 1
        except pysqldb.OperationalError as oe:
            print str(oe)
            print 'Press any key to continue: '
            user_in = raw_input()

        key = 'County' if 'County' in state_df else 'Parish' if 'Parish' in state_df else 'Borough'
        for county in state_df[key]:
            add_county_row_query = 'INSERT INTO ' + '\'' + state_name + '\'' + ' VALUES( \'' + county + '\' )'
            print 'SQL Query Text: ' + add_county_row_query
            try:
                cursor.execute(add_county_row_query)
                connection.commit()
            except pysqldb.OperationalError as oe:
                print str(oe)

    cursor.close()

    return create_count

def create_states_and_counties_list(states_and_counties_dir):
    dir_listing = os.listdir(states_and_counties_dir)
    states_and_counties_list = list()
    for state_csv in dir_listing:
        state_name = state_csv[0:state_csv.index('-')]
        print 'Adding state: ' + state_name

        state_df = pandas.read_csv(states_and_counties_dir + '/' + state_csv)

        states_and_counties_list.append([state_name, state_df])

    return states_and_counties_list

def main(argv):
    # Set the database filename
    database_filename = 'states_and_counties.db'

    # Delete the existing database so as not to create multiple entries
    try:
        os.remove(database_filename)
    except OSError as error:
        print database_filename + ' does not exist'
        pass
    # Establish a new database connection
    connection = pysqldb.Connection(database_filename)

    # Create States and Counties database table
    base_dir = os.getenv('HOME') + '/Documents/workspace/JER/data_tables'
    states_and_counties_dir = base_dir + '/states_and_counties'
    states_and_counties_list = create_states_and_counties_list(states_and_counties_dir)
    states_with_counties_created = create_states_counties_tables(connection, states_and_counties_list)

    # Create States and Counties base dir
    state_abbreviations_dir = base_dir
    state_abbreviations_filename = 'state_abbreviations.csv'
    create_state_abbreviations_table(connection, state_abbreviations_dir, state_abbreviations_filename)

    print
    print '-' * 50
    print 'Number of states: ' + str(len(states_and_counties_list))
    print 'States with counties created: ' + str(states_with_counties_created)

    connection.close()

if __name__ == '__main__':
    main(sys.argv)
