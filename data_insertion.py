import mysql.connector
import pandas as pd


# Load cleaned data
df = pd.read_csv("/home/dhyanendra/Desktop/Pyhton_sql_projact/ecommerce_data_cleaned.csv")

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="ecommerce_db"
)
cursor = conn.cursor()

# Insert data
for index, row in df.iterrows():
    sql = """INSERT INTO customer_orders (Customer_ID, Purchase_Date, Product_Category, Product_Price_USD, Quantity, Total_Purchase_Amount_INR, Payment_Method, Customer_Age, Returns, Customer_Name, Gender, Churn)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (row["Customer ID"], row["Purchase Date"], row["Product Category"], row["Product_Price_USD"], row["Quantity"],
              row["Total_Purchase_Amount_INR"], row["Payment Method"], row["Customer Age"], row["Returns"],
              row["Customer Name"], row["Gender"], row["Churn"])
    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()
print("Data inserted into MySQL successfully!")