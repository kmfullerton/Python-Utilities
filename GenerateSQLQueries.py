# Script to query BQ, return list of columns, remove columns that are not applicable to SVM validation, and return the table anme and list of columns to be input into the SVM validator.

# Created 01/15/18 by SK Fullerton

import os,os.path
from google.oauth2 import service_account
from google.cloud import bigquery
import re
from pathlib import Path
import tkinter.filedialog

def queryBQ(projectID, bq_db_name, bq_final_table_name) :
    # Function 1: Query BQ with table name and return table Schema
    credentials = service_account.Credentials.from_service_account_file(
        '/Users/sarahkfullerton/Documents/pr-finance-serv-account.json')

    bigquery_client = bigquery.Client(project=projectID, credentials=credentials)
    dataset = bigquery_client.dataset(bq_db_name)
    table = dataset.table(bq_final_table_name)

    table.reload()
    #print(table.schema)
    schema = [field.name for field in table.schema]

    return schema

def cleanCols(schema):
    # Function 2: using reg ex, strip schema of commonly un-interesting columns
    # If you would like to use a column with one of these flags, enter it manually during the writeSQLQuery function
    rejection_strings = ['NBR', 'YR', 'TS', 'FLG', 'TYP', 'DT','PCT','ID','TEXT', 'RSN', 'DESC', 'IND','TXT'] #if column name contains these strings, it is not of interest

    col_list = []
    keep_col = 1
    print('The columns in the table are: ')
    print(schema)

    for col_name in schema:
    #Iterate through the schema for each column
        for rej_str in rejection_strings:
            #Iterate through the rejection strings to test each column name
            search_str = re.compile(rej_str, re.IGNORECASE)
            match_flag = search_str.search(col_name)
            if match_flag:
                # Set a tag for this column equal to zero if it contains a rejection strings
                keep_col = 0

        if (keep_col):
            # if the column does not contain a rejection string, append it to the column list.
            col_list.append(col_name)
        # Reset the keep col flag to default of 1 for next iteration
        keep_col = 1

    return col_list

def writeSQLQuery(projectID, bq_db_name, td_table_name, bq_raw_table_name, bq_final_table_name):

    full_cols = queryBQ(projectID, bq_db_name, bq_final_table_name)

    metric_cols = cleanCols(full_cols)
    print('The columns of interest are:')
    print(metric_cols)
    group_col = input('Please enter the column used to group the sql query: ')
    print('Your query will be grouped on column: '+ group_col)


    # Subsection: build stem of sql query
    group_col = group_col.upper()
    if group_col in metric_cols:
        metric_cols.remove(group_col)
    print(group_col)
    sql_query_p1 = 'SELECT ' + group_col + ', '

    # Subsection: build the final sum statement without a comma
    col_name = metric_cols.pop()
    sql_query_p3 =  'SUM(' + col_name + ') AS ' + col_name

    # Subsection: build the sum columns for the query
    sql_query_p2 = ''
    for col_name in metric_cols:
        sql_query_p2 = sql_query_p2 + 'SUM(' + col_name + ') AS ' + col_name + ', '

    # Subsection: create from and group by statments
    sql_query_p4_td = ' FROM PR_US_FINANCE_VIEW.' + td_table_name
    sql_query_p4_bqraw = ' FROM TD_FINANCE_DATA_HIST.' + bq_raw_table_name
    sql_query_p4_bqfinal = ' FROM FINANCE_DATA_HIST.' + bq_final_table_name
    sql_query_p6 = ' GROUP BY 1 ORDER BY 1'



    # Subsection: build the full queries
    td_query = sql_query_p1 + sql_query_p2 + sql_query_p3 + sql_query_p4_td + sql_query_p6
    bq_raw_query = sql_query_p1 + sql_query_p2 + sql_query_p3 + sql_query_p4_bqraw + sql_query_p6
    bq_final_query = sql_query_p1 + sql_query_p2 + sql_query_p3 + sql_query_p4_bqfinal + sql_query_p6

    query_list = [td_query, bq_raw_query, bq_final_query]
    return query_list


def main():
    projectID = 'pr-finance-thd'
    bq_db_name = 'FINANCE_DATA'
    td_table_name = 'STRSC_U_SNRCT_M'
    bq_raw_table_name = 'STRSC_U_SNRCT_M'
    bq_final_table_name = 'STRSK_U_SNRCT'

    query_list = writeSQLQuery(projectID, bq_db_name, td_table_name, bq_raw_table_name, bq_final_table_name)
    td_query = query_list[0]
    bq_raw_query = query_list[1]
    bq_final_query = query_list[2]

    print('Teradata Source Query')
    print(td_query)

    print('BQ Raw Query')
    print(bq_raw_query)

    print('BQ Final Query')
    print(bq_final_query)

main()