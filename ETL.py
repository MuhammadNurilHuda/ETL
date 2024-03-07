import pandas as pd
import numpy as np
import os

try:
    import mysql.connector
except ImportError:
    print("mysql.connector not found. Installing...")
    try:
        import subprocess
        subprocess.call(['pip', 'install', 'mysql-connector-python'])
    except Exception as e:
        print(f"Error during installation: {e}")
    else:
        print("mysql.connector has been successfully installed.")
finally:
    import mysql.connector

try:
    import pandas as pd
except ImportError:
    print("pandas not found. Installing...")
    try:
        import subprocess
        subprocess.call(['pip', 'install', 'pandas'])
    except Exception as e:
        print(f"Error during installation: {e}")
    else:
        print("pandas has been successfully installed.")
finally:
    import pandas as pd


def connect_mysql(host, user, password, database):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
        )
    
    cursor = conn.cursor()

    # ------------------------------------------
    # productlines
    # ------------------------------------------
    cursor.execute("SELECT * FROM productlines")
    result = cursor.fetchall()
    productlines = pd.DataFrame(result, columns = ['productLine', 'textDescription', 'htmlDescription', 'image'])

    # ------------------------------------------
    # products
    # ------------------------------------------
    cursor.execute("SELECT * FROM products")
    result = cursor.fetchall()
    products = pd.DataFrame(result, columns = ['productCode', 'productName', 'productLine', 'productScale', 'productVendor', 'productDescription', 'quantityInStock', 'buyPrice', 'MSRP'])

    # ------------------------------------------
    # offices
    # ------------------------------------------
    cursor.execute("SELECT * FROM offices")
    result = cursor.fetchall()
    offices = pd.DataFrame(result, columns = ['officeCode', 'city', 'phone', 'addressLine1', 'addressLine2', 'state', 'country', 'postalCode', 'territory'])

    # ------------------------------------------
    # employees
    # ------------------------------------------
    cursor.execute("SELECT * FROM employees")
    result = cursor.fetchall()
    employees = pd.DataFrame(result, columns = ['employeeNumber', 'lastName', 'firstName', 'extension', 'email', 'officeCode', 'reportsTo', 'jobTitle'])

    # ------------------------------------------
    # customers
    # ------------------------------------------
    cursor.execute("SELECT * FROM customers")
    result = cursor.fetchall()
    customers = pd.DataFrame(result, columns = ['customerNumber', 'customerName', 'contactLastName', 'contactFirstName', 'phone', 'addressLine1', 'addressLine2', 'city', 'state', 'postalCode', 'country', 'salesRepEmployeeNumber', 'creditLimit'])

    # ------------------------------------------
    # payments
    # ------------------------------------------
    cursor.execute("SELECT * FROM payments")
    result = cursor.fetchall()
    payments = pd.DataFrame(result, columns = ['customerNumber', 'checkNumber', 'paymentDate', 'amount'])

    # ------------------------------------------
    # orders
    # ------------------------------------------
    cursor.execute("SELECT * FROM orders")
    result = cursor.fetchall()
    orders = pd.DataFrame(result, columns = ['orderNumber', 'orderDate', 'requiredDate', 'shippedDate', 'status', 'comments', 'customerNumber'])

    # ------------------------------------------
    # orderdetails
    # ------------------------------------------

    cursor.execute("SELECT * FROM orderdetails")
    result = cursor.fetchall()
    orderdetails = pd.DataFrame(result, columns = ['orderNumber', 'productCode', 'quantityOrdered', 'priceEach', 'orderLineNumber'])

    conn.close()

    return extract(productlines, products, offices, employees, customers, payments, orders, orderdetails)

def extract(productlines, products, offices, employees, customers, payments, orders, orderdetails):
    # productlines, products, offices, employees, customers, payments, orders, orderdetails = connect_mysql()
    # -----------------------------------------
    df_temp_1 = pd.merge(products, productlines, on='productLine', how='outer')

    # -----------------------------------------
    df_temp_2 = pd.merge(employees, offices, on='officeCode', how='outer')

    # -----------------------------------------
    df_temp_3 = pd.merge(df_temp_2, df_temp_2, on='employeeNumber', how='outer', suffixes=('', '_reportsTo'))
    drop_col = ['reportsTo', 'lastName_reportsTo', 'firstName_reportsTo', 'extension_reportsTo', 'email_reportsTo', 'officeCode_reportsTo', 'jobTitle_reportsTo', 'city_reportsTo', 'phone_reportsTo',
                'addressLine1_reportsTo', 'addressLine2_reportsTo', 'state_reportsTo','country_reportsTo', 'postalCode_reportsTo', 'territory_reportsTo']
    df_temp_3 = df_temp_3.drop(columns=drop_col, axis=1)
    df_temp_3 = df_temp_3.rename(columns={'reportsTo_reportsTo':'reportsTo'})

    # -----------------------------------------
    customers['salesRepEmployeeNumber'] = pd.to_numeric(customers['salesRepEmployeeNumber'], errors='coerce').astype('Int64')
    df_temp_4 = pd.merge(customers, df_temp_3, left_on='salesRepEmployeeNumber', right_on='employeeNumber',how='outer', suffixes=('_x', ''))
    drop_col = ['phone_x', 'addressLine1_x', 'addressLine2_x', 'city_x', 'state_x',
                'postalCode_x', 'country_x']
    df_temp_4 = df_temp_4.drop(columns=drop_col, axis=1)

    # -----------------------------------------
    df_temp_5 = pd.merge(payments, df_temp_4, on='customerNumber',how='outer')

    # -----------------------------------------
    df_temp_6 = pd.merge(orders, df_temp_5, on='customerNumber',how='outer')

    # -----------------------------------------
    df_temp_6['orderNumber'] = pd.to_numeric(df_temp_6['orderNumber'], errors='coerce').astype('Int64')
    df_temp_7 = pd.merge(orderdetails, df_temp_6, on='orderNumber',how='outer')

    # -----------------------------------------
    df_temp_8 = pd.merge(df_temp_1, df_temp_7, on='productCode', how='outer')

    df_used = df_temp_8
    del df_temp_1, df_temp_2, df_temp_3, df_temp_4, df_temp_5, df_temp_6, df_temp_7, df_temp_8

    return transform(df_used)

def transform(df_used):
    df_transform = df_used

    df_transform = df_transform.drop(columns=['state', 'productCode', 'textDescription', 'htmlDescription', 
                                              'image', 'comments', 'checkNumber', 'phone', 'addressLine1', 'addressLine2'])
    df_transform = df_transform.dropna(subset=df_transform.columns.difference(['shippedDate']))
    df_transform['shippedDate'] = pd.to_datetime(df_transform['shippedDate'], errors='coerce')
    df_transform[['orderDate', 'requiredDate', 'paymentDate']] = df_transform[['orderDate', 
                                                                               'requiredDate', 'paymentDate']].apply(pd.to_datetime, format='%Y-%m-%d')
    df_transform[['buyPrice', 'MSRP', 'priceEach', 'amount', 'creditLimit']] = df_transform[['buyPrice', 'MSRP', 'priceEach', 
                                                                                             'amount', 'creditLimit']].astype('float64')
    df_transform['quantityInStock'] = df_transform['quantityInStock'].astype('int64')

    return load(df_transform)

def load(df_transform):
    dir = os.getcwd()
    path = os.path.join(dir, 'cleaned_data.csv')
    df_transform.to_csv(path, index=False)
    print("File telah disimpan di direktori saat ini.")

if __name__ == "__main__":
    host = input("Masukkan host MySQL: ")
    user = input("Masukkan username MySQL: ")
    password = input("Masukkan password MySQL: ")
    database = input("Masukkan nama database MySQL: ")
    connect_mysql(host, user, password, database)