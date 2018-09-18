from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

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

def classifyColumns(schema):
    print('Interactively classify each column of the table as a Key, Metric, or column to be Ignored')
    classified_cols = pd.DataFrame()
    for col in schema:
        classification = input('Please classify this column as K (key), M (metric), or I (ignore): ', col)
        classified_cols.append([col, classification])

    print('The column classifications are:')
    print(classified_cols)
    return classified_cols

def main():
    projectID = 'np-finance-thd'
    bq_db_name = 'SKU_PROFITABILITY'
    bq_final_table_name = 'LNDC_DRVR_DC_W'

    schema = queryBQ(projectID, bq_db_name, bq_final_table_name)
    classified_cols = classifyColumns(schema)

main()