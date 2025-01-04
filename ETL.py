import pandas as pd
from sqlalchemy import create_engine
DATABASE_URL = "mssql+pyodbc:///?odbc_connect=" + \
    "Driver={ODBC Driver 17 for SQL Server};" + \
    "Server=Toka;" + \
    "Database=TEST2;" + \
    "Trusted_Connection=yes;"

engine = create_engine(DATABASE_URL)
################# EXTRACT #######################
feedback_path="C:/Users/PCCV/Documents/Mentorship_project/feedback_dataset.csv"
df_feedback_full = pd.read_csv(feedback_path)
df_feedback=df_feedback_full[['feedback_id', 'feedback_score'] ]

orderitem_path = "C:/Users/PCCV/Documents/Mentorship_project/order_item_dataset.csv"
df_orderitem_full = pd.read_csv(orderitem_path)
df_orderitem=df_orderitem_full[['order_id', 'product_id', 'price']]

seller_path = "C:/Users/PCCV/Documents/Mentorship_project/seller_dataset.csv"
df_seller = pd.read_csv(seller_path)

product_path = "C:/Users/PCCV/Documents/Mentorship_project/products_dataset.csv"
df_product = pd.read_csv(product_path)

order_path = "C:/Users/PCCV/Documents/Mentorship_project/order_dataset.csv"
df_order_full = pd.read_csv(order_path)
df_order=df_order_full[['order_id','order_status','user_name']]
customer_path = "C:/Users/PCCV/Documents/Mentorship_project/user_dataset.csv"
df_customer = pd.read_csv(customer_path)
df_OrderCust = pd.merge(df_order, df_customer, on='user_name', how='left')
df_OrderCust = df_OrderCust[['order_id','order_status','user_name', 'customer_zip_code', 'customer_city', 'customer_state']].rename(columns={'user_name': 'customer_id','order_status':'order_state'})
payment_path = "C:/Users/PCCV/Documents/Mentorship_project/payment_dataset.csv"
df_payment_full = pd.read_csv(payment_path)
df_OrderUser_payment=pd.merge(df_OrderCust,df_payment_full,on='order_id',how='left')


################## TRANSFORM ###################### >>>>>>>>>
print(f"Total number of rows orderUSER: {df_OrderUser_payment.shape[0]}")
duplicate_count = df_OrderUser_payment.duplicated().sum()
print(f"Number of duplicate rows: {duplicate_count}")
df_OrderUser_payment = df_OrderUser_payment.drop_duplicates()
print(f"Total number of rows: {df_OrderUser_payment.shape[0]}")



## FACT ##
df_FactOrders1=pd.merge(df_order_full[['order_id']],df_orderitem_full[['order_id','order_item_id','price','shipping_cost']], on='order_id', how='left')

#Dim product
query = "SELECT product_id, product_id_sk FROM Dim_product"
dim_product_df = pd.read_sql(query, engine)
df_FactOrders2=pd.merge(df_orderitem.drop(columns=['price']),dim_product_df, on='product_id', how='left')
df_FactTotal=pd.merge(df_FactOrders1,df_FactOrders2,on='order_id',how='left')
df_FactTotal=df_FactTotal[['order_id',  'order_item_id','price', 'product_id_sk','shipping_cost']]

#Dim feedback
query = "SELECT feedback_id, feedback_sk FROM Dim_feedback"
dim_feedback_df = pd.read_sql(query, engine)
df_order_feedback=pd.merge(df_feedback_full,dim_feedback_df,on='feedback_id',how='left').drop(columns=['feedback_id','feedback_score'])
for col in df_order_feedback[['feedback_form_sent_date', 'feedback_answer_date']].columns:
    df_order_feedback[col] = pd.to_datetime(df_order_feedback[col]).dt.strftime('%Y%m%d')
df_FactTotal=pd.merge(df_FactTotal,df_order_feedback,on='order_id',how='left')

#Dim seller 
query = "SELECT seller_id, seller_id_sk FROM Dim_seller"
dim_seller_df = pd.read_sql(query, engine)
df_order_seller=pd.merge(df_orderitem_full[['order_id','seller_id']],dim_seller_df,on='seller_id',how='left').drop(columns='seller_id')
df_FactTotal=pd.merge(df_FactTotal,df_order_seller,on='order_id',how='left')

#Dim orderUser_payment
query = "SELECT order_id, order_id_sk FROM Dim_orderUser_payment"
dim_order_df = pd.read_sql(query, engine)
df_FactTotal=pd.merge(df_FactTotal,dim_order_df,on='order_id',how='left')

#dates
df_order_orderitem=pd.merge(df_orderitem_full,df_order_full,on='order_id',how='left').drop(columns=[ 'order_item_id', 'product_id', 'seller_id','price', 'shipping_cost', 'user_name','order_status'])
df_datesOnly=df_order_orderitem.drop(columns='order_id')
df = pd.DataFrame(df_datesOnly)
df_datesOnly = pd.DataFrame()
df_timesOnly = pd.DataFrame()
i=0
for col in df.columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')  # Handle invalid formats gracefully
    
    df_datesOnly[col] = df[col].dt.strftime('%Y%m%d')  # Safely extract the date part
    
    df_timesOnly[col]= df[col].dt.strftime('%H%M%S')  # Safely extract the time part

df_datesOnly.rename(columns={'estimated_time_delivery':'estimated_date_delivery'},inplace=True)
df_timesOnly = df_timesOnly.rename(columns=lambda x: x.replace('date', 'time'))
df_order_orderitem=pd.concat([df_order_orderitem['order_id'],df_datesOnly,df_timesOnly],axis=1)
df_FactTotal=pd.merge(df_FactTotal,df_order_orderitem,on='order_id',how='left')
# print(df_FactTotal.columns,df_order_orderitem.columns)



print(f"Total number of rows df_FactTotal:>>>>>>> {df_FactTotal.shape[0]}")
duplicate_count = df_FactTotal.duplicated().sum()
print(f"Number of duplicate rows:>>>>>>>> {duplicate_count}")
df_FactTotal = df_FactTotal.drop_duplicates()
print(f"Total number of rows df_FactTotal:>>>>>>>>> {df_FactTotal.shape[0]}")

df_FactTotal.rename(columns={
    'order_id_sk': 'orderuser_fk',
    'order_item_id': 'orderitem_ID',
    'product_id_sk': 'product_fk',
    'feedback_sk': 'feedback_fk',
    'seller_id_sk': 'seller_fk',
    'pickup_limit_date': 'pickup_limit_date',
    'order_date': 'order_date',
    'order_approved_date': 'order_approved_date',
    'pickup_date': 'pickup_date',
    'delivered_date': 'delivery_date',
    'delivered_time': 'delivery_time',

}, inplace=True)

# Step 2: Rearrange columns in the specified order
df_FactTotal = df_FactTotal[['orderuser_fk', 'orderitem_ID', 'product_fk', 'feedback_fk', 'seller_fk',
                             'feedback_form_sent_date', 'feedback_answer_date', 'order_date', 'order_approved_date',
                             'pickup_date', 'delivery_date', 'estimated_date_delivery', 'pickup_limit_date', 
                              'order_time', 'order_approved_time', 'pickup_time', 'delivery_time', 'estimated_time_delivery',
                              'pickup_limit_time', 'price', 'shipping_cost']]


df_FactTotal = df_FactTotal.fillna(0)  # Replace NaN with 0
df_FactTotal.replace([float('inf'), float('-inf')], 0, inplace=True)
# df_FactTotal = df_FactTotal.astype(int)

pd.set_option('display.max_columns', None)

print(df_FactTotal.head())
########################### LOAD #########################
table_name = 'Dim_feedback' 
df_feedback.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)

table_name = 'Dim_seller' 
df_seller.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)

print(df_OrderUser_payment.head())
table_name = 'Dim_orderUser_payment' 
df_OrderUser_payment.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)

table_name = 'Dim_product' 
df_product.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)

table_name = 'Dim_payment' 
df_payment.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)

table_name = 'Fact_orders' 
df_FactTotal.to_sql(
    table_name,
    con=engine,
    if_exists='append', 
    index=False
)
print("DONE!")