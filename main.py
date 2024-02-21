import numpy as np
import pandas as pd
import json
import os

from pandas import json_normalize

def read_customerdata():
    try:
        # User a breakpoint in the code line below to debug your script.
        df = pd.read_csv("./input_data/starter/customers.csv")
        return df
    except Exception as e:
        print("Failed to fetch customer data ", e)
        raise

def read_productData():
    try:
        df = pd.read_csv("./input_data/starter/products.csv")
        return df
    except Exception as e:
        print("Failed to fetch product data ", e)
        raise


def read_transactionJsonfiles():
    try:
        # Open the JSON file
        file_list = []
        df = pd.DataFrame()
        for root, dirs, files in os.walk('./input_data/starter/transactions/'):
            for file in files:
                file_list.append(os.path.join(root, file))
        #print(file_list)
        for file in file_list:
            with open(file, 'r') as f:
                data = [json.loads(line) for line in f]
                #df=df._append(df)
                df = df._append(pd.json_normalize(data))

        df.reset_index(inplace = True)
        df['loss'] = 1
        # explode all columns with lists of dicts
        df = df.apply(lambda x: x.explode()).reset_index(drop=True)
        # df.apply(pd.Series.explode).reset_index(drop=True) also works

        # list of columns with dicts
        cols_to_normalize = ['basket']

        # if there are keys, which will become column names, overlap with excising column names
        # add the current column name as a prefix
        normalized = list()
        for col in cols_to_normalize:
            d = pd.json_normalize(df[col], sep='_')
            d.columns = [f'{col}_{v}' for v in d.columns]
            normalized.append(d.copy())

        # combine df with the normalized columns
        df = pd.concat([df] + normalized, axis=1).drop(columns=cols_to_normalize)
        df = df.drop('loss', axis=1)
        df.rename(columns={'basket_product_id': 'product_id'}, inplace=True)
        return df
    except Exception as e:
        print("Failed to fetch Transaction data ", e)
        raise

def computeCustomerinfo(df_customer, df_product, df_transaction):
    try:
        if not df_customer.empty and not df_product.empty and not df_transaction.empty:
            df = pd.DataFrame()
            #df_customer = df_customer.reset_index()

            df3 = pd.merge(df_transaction, df_customer, on="customer_id", how="inner")
            df4 = pd.merge(df3, df_product, on="product_id", how="inner")

            print(df4.groupby(['customer_id','product_id','product_category','loyalty_score'])['customer_id'].count().reset_index(name='Purchase_count'))
            customer_data_df_new= df4.groupby(['customer_id','product_id','product_category','loyalty_score'])['customer_id'].size().reset_index(name='Purchase_count')
            # customer_data_df_new = pd.DataFrame(customer_data_df)
            return customer_data_df_new
        else:
            raise Exception('Customer, Product and Transaction data is not available to do further process !!!') from e
    except Exception as e:
        print("Failed to Compute data ", e)
        raise

def writejsonfile(customer_data_df):
    try:
        if not customer_data_df.empty:
            customer_data_df.to_json('CustomerPurchaseData.json', orient='records')
        else:
            print("Data not available to create JSON file !!!")
    except Exception as e:
        print("Failed to write data ", e)
        raise


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        df_customer = read_customerdata()
        print('Here')
        df_product = read_productData()
        df_transaction = read_transactionJsonfiles()
        if df_customer is not None and df_product is not None and df_transaction is not None:
            customer_data_df=computeCustomerinfo(df_customer, df_product, df_transaction)
        if customer_data_df is not None:
            writejsonfile(customer_data_df)
    except Exception as e:
        print("Fail to process files", e)

