# %% [markdown]
import pandas as pd
from datetime import datetime, timedelta, date
import numpy as np

from datetime import datetime
import tempfile
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, BlobLeaseClient


#connecting to azure storage and container
connectionString1 = "DefaultEndpointsProtocol=https;AccountName=nfinsdata;AccountKey=fkIjwa6uiAZBanIeeK8MtqdrGg/04sXKgCGCCHDX0yM+D6h/slJO2cRs5l1kb/k+14wyJtG2elvs+AStl1f7vQ==;EndpointSuffix=core.windows.net"
blobConnect = BlobServiceClient.from_connection_string(connectionString1)
missTestContainer = "miss-test"
containerConnect = blobConnect.get_container_client(missTestContainer)
misstestOutputContainer = "miss-test-outputs"

miss_test_items_binBlob = "customsearch_miss_test_items_bin.csv"
miss_test_items_receivedBlob = "customsearch_miss_test_items_received.csv"
miss_test_open_salesBlob = "customsearch_miss_test_open_sales_orde_2.csv"
miss_test_picking_dataBlob = "customsearch_miss_test_picking_data.csv"
miss_test_prod_invBlob = "customsearch_miss_test_prod_inv_search.csv"
miss_test_sales_detailsBlob = "customsearch_miss_test_sales_details.csv"

miss_test_items_bin_client = containerConnect.get_blob_client(miss_test_items_binBlob)
miss_test_items_received_client = containerConnect.get_blob_client(miss_test_items_receivedBlob)
miss_test_open_sales_client = containerConnect.get_blob_client(miss_test_open_salesBlob)
miss_test_picking_data_client = containerConnect.get_blob_client(miss_test_picking_dataBlob)
miss_test_prod_inv_client = containerConnect.get_blob_client(miss_test_prod_invBlob)
miss_test_sales_details_client = containerConnect.get_blob_client(miss_test_sales_detailsBlob)

miss_test_items_bin_data = miss_test_items_bin_client.download_blob().content_as_text()
miss_test_items_received_data = miss_test_items_received_client.download_blob().content_as_text()
miss_test_open_sales_data = miss_test_open_sales_client.download_blob().content_as_text()
miss_test_picking_data_data = miss_test_picking_data_client.download_blob().content_as_text()
miss_test_prod_inv_data = miss_test_prod_inv_client.download_blob().content_as_text()
miss_test_sales_details_data = miss_test_sales_details_client.download_blob().content_as_text()


df_All_Items_In_Bins_Status = pd.read_csv(StringIO(miss_test_items_bin_data))
df_NF_Items_Received = pd.read_csv(StringIO(miss_test_items_received_data))
df_All_Open_Sales_Orders = pd.read_csv(StringIO(miss_test_open_sales_data))
df_FEZ_Picking_Data_YST = pd.read_csv(StringIO(miss_test_picking_data_data), low_memory=False)
df_EZ_Production_INV_Search_YST = pd.read_csv(StringIO(miss_test_prod_inv_data), low_memory=False)
df_ez_gr_sales = pd.read_csv(StringIO(miss_test_sales_details_data))

df_FEZ_Picking_Data_YST.head()


def date_array():
    
    if datetime.now().weekday() == 0: 
        
        yesterday_date = datetime.now()- timedelta(3)
        yesterday = yesterday_date.strftime('%m/%d/%Y') 
        
        today_date = datetime.now()-timedelta(2)
        today = today_date.strftime('%m/%d/%Y')
        
        four_days_ago_date = today_date - timedelta(6)
        four_days_ago = four_days_ago_date.strftime('%m/%d/%Y')
        
    else:
        
        today_date = datetime.now()
        today = today_date.strftime('%m/%d/%Y')


        yesterday_date = today_date - pd.offsets.BDay(1)
        yesterday = yesterday_date.strftime('%m/%d/%Y')

        four_days_ago_date = today_date - pd.offsets.BDay(4)
        four_days_ago = four_days_ago_date.strftime('%m/%d/%Y')
    

    date_array = {"today": today, "yesterday": yesterday, "four days ago": four_days_ago}
    
    return date_array


date_array()
date_array()["today"]
date_array()["yesterday"]
date_array()["four days ago"]



def date_array_Todays_Misses():
    
    if datetime.now().weekday() == 4: 
        
        yesterday_date = datetime.now()
        yesterday = yesterday_date.strftime('%m/%d/%Y') 
        
        today_date = datetime.now()+timedelta(1)
        today = today_date.strftime('%m/%d/%Y')
        
        four_days_ago_date = today_date - timedelta(4)
        four_days_ago = four_days_ago_date.strftime('%m/%d/%Y')
        
    else:
        
        today_date = datetime.now() + pd.offsets.BDay(1)
        today = today_date.strftime('%m/%d/%Y')


        yesterday_date = today_date - pd.offsets.BDay(1)
        yesterday = yesterday_date.strftime('%m/%d/%Y')

        four_days_ago_date = today_date - pd.offsets.BDay(4)
        four_days_ago = four_days_ago_date.strftime('%m/%d/%Y')
    

    date_array = {"today": today, "yesterday": yesterday, "four days ago": four_days_ago}
    
    return date_array


date_array_Todays_Misses()




pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 5)


def CreateUID(DocNum, SKU):
    UID = str(DocNum) + str(SKU)
    return UID

def UID_Column (df,DOCUMENT_num,SKU):
    df['UID']=df[DOCUMENT_num]+df[SKU]
    return df

def UID_FEZ_Picking ():
    temp = []
    for created_from in df_FEZ_Picking_Data_YST['Created From']:
        Sales_Order = created_from[13:41]
        temp.append(Sales_Order)
    df_FEZ_Picking_Data_YST["Sales_Order"]=pd.Series(temp)
    
    UID_Column(df_FEZ_Picking_Data_YST, "Sales_Order","Item" )
        
    



UID_FEZ_Picking ()
df_FEZ_Picking_Data_YST


UID_Column(df_ez_gr_sales,'Document Number','Product SKU')
UID_Column(df_All_Open_Sales_Orders,'Document Number','Item')
df_FEZ_Picking_Data_YST


df_All_Items_In_Bins_Status

df_All_Open_Sales_Orders


def AJ_Customer(df):
    temp = [] 
    hard_code_priority_customers = ['Grainger - Drop Ship','Grainger - Branch Office','Grainger - Sourcing','Amazon.com Services, Inc','16558 Fix Customer to USA Sealing','17305 Amazon Direct Fulfillment', 'Zoro - Drop Ship']
    for customer in df_ez_gr_sales['Customer']:
        if customer == hard_code_priority_customers[0]:
            temp.append('Grainger Drop')
        
        elif customer == hard_code_priority_customers[1]:
            temp.append('Grainger Branch')
        
        elif customer == hard_code_priority_customers[2]:
            temp.append('Grainger Sourcing')
        
        elif customer == hard_code_priority_customers[3]:
            temp.append('Amazon DC')
        
        elif customer == hard_code_priority_customers[4]:
            temp.append( 'Fix Supply')
        
        elif customer == hard_code_priority_customers[5]:
            temp.append('Amazon Drop')
        
        elif customer == hard_code_priority_customers[6]:
            temp.append("Zoro")
        
        else:
            temp.append('Cust Not Found')
    df_ez_gr_sales['AJ_Customer']=pd.Series(temp)
    
    return df
    


AJ_Customer(df_ez_gr_sales)


from datetime import datetime


def picked_in_range(df_FEZ_Picking_Data_YST, yesterday, four_days_ago):
    
    # Convert strings to datetime.date objects if they are not already
    if isinstance(yesterday, str):
        yesterday = datetime.strptime(yesterday, '%m/%d/%Y').date()
    if isinstance(four_days_ago, str):
        four_days_ago = datetime.strptime(four_days_ago, '%m/%d/%Y').date()

    temp = []
    
    # Loop through the "Short Date" column and check if each date falls within the range
    for date_str in df_FEZ_Picking_Data_YST['Short Date']:
        try:
            date_object = datetime.strptime(date_str, '%m/%d/%Y').date()
            if date_object == yesterday or date_object >= four_days_ago:
                temp.append("YES")
            else:
                temp.append("")
        except ValueError:
            # Handle invalid date strings
            temp.append("error")
    
    # Create a new column called "Picked in Range" with the results
    df_FEZ_Picking_Data_YST['Picked in Range'] = pd.Series(temp)
    
    
    return df_FEZ_Picking_Data_YST





picked_in_range(df_FEZ_Picking_Data_YST,date_array_Todays_Misses()['yesterday'], date_array_Todays_Misses()['four days ago'])


df_FEZ_Picking_Data_YST["Picked in Range"][249991]







def Amazon_Pick_Check():
    temp = []

    for UID in df_ez_gr_sales['UID']:
        result = df_FEZ_Picking_Data_YST.loc[df_FEZ_Picking_Data_YST['UID']==UID, "Picked in Range" ]
        
        amazon = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'AJ_Customer'].iloc[0]
        if "Amaz" in amazon:
            if len(result) > 0 or result.empty != True:
                result_value = result.iloc[0]
                temp.append(result_value)
            else:
                temp.append("NO")
        else:
            temp.append("")
        
        
    df_ez_gr_sales['Amazon Pick Check']=pd.Series(temp)


    return df_ez_gr_sales

    


Amazon_Pick_Check()


df_ez_gr_sales['Amazon Pick Check'][7476]


# 
# 

# %% [markdown]
# # [ez_gr_sales-T] Days_Late

# %% [markdown]
# 

# %%



def Days_Late(today):

    today = datetime.strptime(today, '%m/%d/%Y').date()

    temp = []
    
    # Loop through the "Short Date" column and check if each date falls within the range
    df_ez_gr_sales['Requested Ship Date'] = df_ez_gr_sales['Requested Ship Date'].astype(str)
    for date_str in df_ez_gr_sales['Requested Ship Date']:
        if date_str == "nan":
            temp.append(" ")
        else:
            date_object = datetime.strptime(date_str, '%m/%d/%Y').date()

            days_late = (today- date_object).days
            temp.append(days_late)
        
    # Create a new column called "Picked in Range" with the results
    df_ez_gr_sales['Days Late'] = pd.Series(temp)
    
    
    #debug function


    return df_ez_gr_sales
    
        

        
    

# %%
Days_Late(date_array_Todays_Misses()['today'])

def late():
    temp = []
    merge_df = pd.merge(df_ez_gr_sales, df_FEZ_Picking_Data_YST, left_on="UID",right_on="UID", how='left')
    
    for UID in merge_df['UID']:
        freight_status = merge_df.loc[merge_df['UID'] == UID, 'Freight Helper'].iloc[0]
        days_late = merge_df.loc[merge_df['UID'] == UID, 'Days Late'].iloc[0]
        amazon_pick_check = merge_df.loc[merge_df['UID'] == UID, 'Amazon Pick Check'].iloc[0]
        if ((freight_status != "Freight/Packed") and (days_late == 1) and (amazon_pick_check != 'YES')): 
            temp.append("MISS")
        else:
            temp.append("N/A")
    
    merge_df['Late'] = pd.Series(temp)

    mapping = dict(merge_df[['UID','Late']].values)
    df_ez_gr_sales['Late'] = df_ez_gr_sales.UID.map(mapping)
            
    

    return df_ez_gr_sales





# %%
late()


# %%
df_ez_gr_sales['Late'][6336]  


count =0
for uid in df_ez_gr_sales['UID']:
    if df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Late"].iloc[0] == "MISS":
        count = count+1
        






def Quantity_On_Hand():
    
    temp = []
    merge_df = pd.merge(df_ez_gr_sales, df_EZ_Production_INV_Search_YST, left_on="Product SKU",right_on="Item", how='left')
    for sku in merge_df['Product SKU']:
        if (merge_df.loc[merge_df['Product SKU']==sku, 'On Hand'].iloc[0] == ""):
            temp.append(0)
        else:
            temp.append(merge_df.loc[merge_df['Product SKU']==sku, 'On Hand'].iloc[0])
    merge_df['Quantity On Hand'] = pd.Series(temp)
    mapping = dict(merge_df[['Product SKU','Quantity On Hand']].values)
    df_ez_gr_sales['Quantity On Hand'] = df_ez_gr_sales['Product SKU'].map(mapping)
            
    df_ez_gr_sales["Quantity On Hand"] = df_ez_gr_sales["Quantity On Hand"].fillna(0)
    
    return df_ez_gr_sales
    

# %%
Quantity_On_Hand()

# %%
df_ez_gr_sales["Quantity On Hand"][6339]


# %%
def sum_of_picked(): 

    sum_by_condition = df_All_Open_Sales_Orders.groupby('Item',sort='False')['Quantity Picked'].sum().reset_index()
    sum_by_condition = sum_by_condition.rename(columns={'Quantity Picked': 'Picked_sum'})  # rename the column
    result = pd.merge(df_All_Open_Sales_Orders, sum_by_condition, on='Item',how='outer')
    
    
    result['Sum of Picked'] = result['Picked_sum'].fillna(0) 
    temp =[]
    for uid in df_All_Open_Sales_Orders['UID']:
        temp.append(result.loc[result['UID']==uid, 'Sum of Picked'].iloc[0])
        
    df_All_Open_Sales_Orders['Sum of Picked'] = pd.Series(temp) 
    
    return df_All_Open_Sales_Orders
   

# %%
sum_of_picked()

df_All_Open_Sales_Orders['Sum of Picked'][123]

def Rolling_QOH():
    temp = []
    check = []
    merge_df = pd.merge(df_ez_gr_sales, df_All_Open_Sales_Orders, left_on="UID",right_on="UID", how='left')
    merge_df['Quantity On Hand'] = merge_df['Quantity On Hand'].fillna(0)
    merge_df['Sum of Picked'] = merge_df['Sum of Picked'].fillna(0)
    
    for UID in merge_df['UID']:
        late = merge_df.loc[merge_df['UID']==UID, 'Late'].iloc[0]
        qty_on_hand = merge_df.loc[merge_df['UID']== UID, "Quantity On Hand"].iloc[0]
        sum_of_picked = merge_df.loc[merge_df['UID']== UID, "Sum of Picked"].iloc[0]
        if (late != "MISS"):
            temp.append("")
        elif (late == "MISS"):
#         else:
            rolling_qoh = qty_on_hand - sum_of_picked
            temp.append(rolling_qoh)
            check.append(str(qty_on_hand)+" - "+str(sum_of_picked)+"types:  "+str(type(qty_on_hand))+" "+str(type(sum_of_picked)))
        
            
    merge_df['Rolling QOH'] = pd.Series(temp)
    merge_df['Rolling QOH_check_print'] = pd.Series(check)
    
    
    mapping = dict(merge_df[['UID','Rolling QOH']].values)
    df_ez_gr_sales['Rolling QOH'] = df_ez_gr_sales.UID.map(mapping)
    
    mapping2 = dict(merge_df[['UID','Rolling QOH_check_print']].values)
    df_ez_gr_sales['Rolling QOH_check_print'] = df_ez_gr_sales.UID.map(mapping2)
    
    return df_ez_gr_sales


# %%
Rolling_QOH()

# %%
df_ez_gr_sales["Rolling QOH"][6987]

# %%
df_ez_gr_sales

def is_quarantine():
    temp = []
    merge_df = pd.merge(df_ez_gr_sales,df_All_Items_In_Bins_Status,left_on="Product SKU",right_on="Item",how="left")
    for sku in merge_df['Product SKU']:
        if (merge_df.loc[merge_df['Product SKU']==sku, "Status_y"].iloc[0] == "Quarantine"):
            temp.append("YES")

        else:
            temp.append("NO")
    merge_df["Is Quarantine"] =pd.Series(temp)
    mapping = dict(merge_df[['Product SKU','Is Quarantine']].values)
    df_ez_gr_sales['Is Quarantine'] = df_ez_gr_sales['Product SKU'].map(mapping)
    
    return df_ez_gr_sales
    

# %%
is_quarantine()


# %%
df_All_Items_In_Bins_Status


# %%
df_ez_gr_sales['Is Quarantine'][350]



def Is_Picked():
    temp = []
    debug = []
    merge_df = pd.merge(df_ez_gr_sales,df_All_Open_Sales_Orders, left_on='UID',right_on='UID',how='left')
    for UID in merge_df["UID"]:
        quantity_picked = merge_df.loc[merge_df['UID'] == UID, 'Quantity Picked'].iloc[0]
        quantity = merge_df.loc[merge_df['UID'] == UID, 'Quantity'].iloc[0]
        quantity_fulfilled_received = merge_df.loc[merge_df['UID'] == UID, 'Quantity Fulfilled/Received_x'].iloc[0]
        
        if np.isnan(np.float64(quantity_picked)) == True:
            quantity_picked = 0

        if quantity_picked == quantity_fulfilled_received:
            debug.append("NO"+"==")
            temp.append("NO")
        elif (quantity_picked != quantity_fulfilled_received): 
        
            if (quantity_picked >= (quantity - quantity_fulfilled_received)):
                debug.append("YES"+":  >= ")
                temp.append("YES")
            elif (quantity_picked < (quantity - quantity_fulfilled_received)):
                debug.append("NO"+":  < ")
                temp.append("NO")
        
        else:
            debug.append("NO"+"Not Found")
            temp.append("NO")
        
            
    
    merge_df['Is Picked'] = pd.Series(temp, name='Is Picked')
    mapping = dict(merge_df[['UID','Is Picked']].values)
    df_ez_gr_sales['Is Picked'] = df_ez_gr_sales['UID'].map(mapping)
    
    merge_df['Is Picked_DEBUG'] = pd.Series(debug, name='Is Picked_DEBUG')
    mapping = dict(merge_df[['UID','Is Picked_DEBUG']].values)
    df_ez_gr_sales['Is Picked_DEBUG'] = df_ez_gr_sales['UID'].map(mapping)
    
    return df_ez_gr_sales



# %%
Is_Picked()


# %%
df_ez_gr_sales['Is Picked'][420]



def fulfillment_miss():
    temp = []
    for UID in df_ez_gr_sales['UID']:
        late = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'Late'].iloc[0]
        is_quarantine = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID,'Is Quarantine'].iloc[0]
        is_picked = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'Is Picked'].iloc[0]
        quantity = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'Quantity'].iloc[0]
        quantity_f_r = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'Quantity Fulfilled/Received'].iloc[0]
        rolling_qoh = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==UID, 'Rolling QOH'].iloc[0]
        if late == "N/A":
            temp.append("N/A")
        elif (late != "N/A" and is_quarantine == "YES"):
            temp.append("N/A")
        else:
            if(late != "N/A" and is_quarantine != "YES" and is_picked == "YES"):
                temp.append("YES")
            else:
                if quantity-quantity_f_r > rolling_qoh:
                    temp.append("NO")
                elif rolling_qoh >= quantity-quantity_f_r:
                    temp.append("YES")
                else:
                    temp.append("excpetion")
    df_ez_gr_sales['Fulfillment Miss'] = pd.Series(temp)
    

    return df_ez_gr_sales
            
        

# %%
fulfillment_miss()

# %%
df_ez_gr_sales['Fulfillment Miss'][3682]

# %%
df_EZ_Production_INV_Search_YST


def parent():
    temp = []
    merge_df = pd.merge(df_ez_gr_sales, df_EZ_Production_INV_Search_YST, left_on='Product SKU', right_on='Item', how='left')
    
    for UID, fulfillment_miss in zip(merge_df["UID"], merge_df['Fulfillment Miss']):
        if fulfillment_miss == "NO":
            temp.append(merge_df.loc[merge_df['UID'] == UID, "RM Master Parent"].iloc[0])
        else:
            temp.append("")
    
    merge_df['Parent'] = pd.Series(temp, name='Parent')
    mapping = dict(merge_df[['UID', 'Parent']].values)
    df_ez_gr_sales['Parent'] = df_ez_gr_sales['UID'].map(mapping)
    
    
    return df_ez_gr_sales

    

# %%
parent()






def date_received():
    temp = []
    debug = []

    for sku, late, product_class, date_received, parent in zip(df_ez_gr_sales['Product SKU'],df_ez_gr_sales['Late'],df_ez_gr_sales['Product Class'],df_NF_Items_Received['Date Received'],df_ez_gr_sales['Parent']):
        
        if late == "N/A":
            temp.append("")
            debug.append(""+"LATE IS N/A")
            
        elif late == "MISS" and (product_class == "Parent" or product_class == "Pass Through"):
            
            parent_dates = df_NF_Items_Received.loc[df_NF_Items_Received['SKU'] == sku, 'Date Received']
#             temp.append(parent_dates)
            if not parent_dates.empty:
        
                try:
                    date_conversion = datetime.strptime(parent_dates.iloc[0],'%m/%d/%Y %I:%M %p').strftime('%m/%d/%Y')
                    temp.append(datetime.strptime(date_conversion,'%m/%d/%Y').date())
                except:
                    temp.append(datetime.strptime(parent_dates.iloc[0],'%m/%d/%Y').date())
                debug.append("Conv parent"+str(parent))
                
            else:
                temp.append("")
                debug.append("parent/passthrough not found")
            
        elif late == "MISS" and product_class == "Converted":
            parent_dates = df_NF_Items_Received.loc[df_NF_Items_Received['SKU'] == parent, 'Date Received']
#             temp.append(parent_dates)
            if not parent_dates.empty:
                try:
                    date_conversion = datetime.strptime(parent_dates.iloc[0],'%m/%d/%Y %I:%M %p').strftime('%m/%d/%Y')
                    temp.append(datetime.strptime(date_conversion,'%m/%d/%Y').date())
                except:
                    temp.append(datetime.strptime(parent_dates.iloc[0],'%m/%d/%Y').date())
                debug.append("Conv parent"+str(parent))
                
            else:
                temp.append("")
                debug.append("cov parent not found")
        else:
            temp.append("")
            debug.append("exception of all conditions")

    df_ez_gr_sales['Debug']=pd.Series(debug)
    df_ez_gr_sales['Date Received']=pd.Series(temp)

    return df_ez_gr_sales


    
    




# %%
date_received()



from datetime import datetime


def receiving_miss(today, four_days_ago):
    temp = []
    
    if isinstance(four_days_ago, str):
        four_days_ago = datetime.strptime(four_days_ago, '%m/%d/%Y').date()
    if isinstance(today, str):
        today = datetime.strptime(today, '%m/%d/%Y').date()
        
    for uid in df_ez_gr_sales['UID']:
        late = df_ez_gr_sales.loc[df_ez_gr_sales['UID'] == uid, 'Late'].iloc[0]
        quantity = float(df_ez_gr_sales.loc[df_ez_gr_sales['UID'] == uid, 'Quantity'].iloc[0])
        date_received = df_ez_gr_sales.loc[df_ez_gr_sales['UID'] == uid, 'Date Received'].iloc[0]
        rolling_qoh = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'Rolling QOH'].iloc[0]
        
        if isinstance(date_received, str) and date_received.strip():
            date_received = datetime.strptime(date_received.strip(), '%m/%d/%Y').date()
        if isinstance(rolling_qoh, np.ndarray):
            if len(rolling_qoh) != 0:
                rolling_qoh = float(rolling_qoh[0])
            else:
                continue  # skip this iteration as 'rolling_qoh' is empty
        elif isinstance(rolling_qoh, str):
            rolling_qoh = float(rolling_qoh) if rolling_qoh.strip() else None
        else:
            rolling_qoh = float(rolling_qoh)

        if rolling_qoh is not None and rolling_qoh < quantity:
            if late == "N/A":
                temp.append("N/A")
            elif late != "N/A":
                
                if ((date_received == "") or (date_received == " ") or (date_received < four_days_ago)) and late != "N/A":
                    temp.append("NO")
                elif (date_received == today or date_received >= four_days_ago) and late != "N/A":
                    temp.append("YES")
                
        else:
            temp.append("N/A")
    
    df_ez_gr_sales['Receiving Miss'] = pd.Series(temp)
    
    mapping = dict(df_ez_gr_sales[['UID','Receiving Miss']].values)
    df_ez_gr_sales['Receiving Miss'] = df_ez_gr_sales.UID.map(mapping)
    
    return df_ez_gr_sales


            
            


# %%
receiving_miss(date_array_Todays_Misses()['today'], date_array_Todays_Misses()['four days ago'])


def parent_availible():
    temp = []
    for parent in df_ez_gr_sales['Parent']:
        Available = df_EZ_Production_INV_Search_YST.loc[df_EZ_Production_INV_Search_YST['Item'] == parent, 'Maximum of Available'].fillna(0) 
        if Available.empty or Available.iloc[0] == "" or Available.iloc[0] == " ":
            temp.append(0)
        else:
            temp.append(Available.iloc[0])
            
    df_ez_gr_sales['Parent Availible'] = pd.Series(temp)
    
    mapping = dict(df_ez_gr_sales[['UID','Parent Availible']].values)
    df_ez_gr_sales['Parent Availible'] = df_ez_gr_sales.UID.map(mapping)
    
    return df_ez_gr_sales

# %%
parent_availible()



def RM_Master_BOM():
    temp = []
    for uid in df_ez_gr_sales['UID']:
        
        fulfill_miss = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'Fulfillment Miss'].iloc[0]
        product_class = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Product Class'].iloc[0]
        sku = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Product SKU"].iloc[0]
        RM_master_bom = df_EZ_Production_INV_Search_YST.loc[df_EZ_Production_INV_Search_YST['Item']==sku, 'RM Master Units for BOM']
        
        if (fulfill_miss == "NO") and (product_class == "Converted"):
            if (len(RM_master_bom) != 0) or (RM_master_bom.iloc[0] != ""):
                temp.append(RM_master_bom.iloc[0])
            else:
                temp.append(" ")
        else:
            temp.append(" ")
        
        
    df_ez_gr_sales['RM Master Units for BOM'] = pd.Series(temp)
    
    return df_ez_gr_sales

        
    


# %%
RM_Master_BOM()


def parent_needed():
    temp = []
    debug =[]
    for uid in df_ez_gr_sales['UID']:
        fulfill_miss = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'Fulfillment Miss'].iloc[0]
        product_class = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Product Class'].iloc[0]
        quantity = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Quantity'].iloc[0]
        quantity_f_r = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'Quantity Fulfilled/Received'].iloc[0]
        RM_master_bom = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'RM Master Units for BOM'].iloc[0]
        
        if fulfill_miss=="NO" and product_class =="Converted" and RM_master_bom != " " and RM_master_bom != "":
            temp.append(RM_master_bom*(quantity-quantity_f_r))
        else:
            temp.append(0)
    df_ez_gr_sales["Parent Needed"] = pd.Series(temp)
    
    mapping = dict(df_ez_gr_sales[['UID','Parent Needed']].values)
    df_ez_gr_sales['Parent Needed'] = df_ez_gr_sales.UID.map(mapping)
    
    return df_ez_gr_sales
            
        

# %%
parent_needed()

def Inventory_Miss():
    temp = []
    for uid in df_ez_gr_sales['UID']:
        fulfill_miss = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, 'Fulfillment Miss'].iloc[0]
        product_class = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Product Class'].iloc[0]
        receiving_miss = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Receiving Miss'].iloc[0]
        parent_needed = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Parent Needed'].iloc[0]
        parent_availible = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid,'Parent Availible'].iloc[0]
        
        if isinstance(parent_needed, np.ndarray) and parent_needed.size:
            parent_needed = parent_needed[0]
        if isinstance(parent_availible, np.ndarray) and parent_availible.size:
            parent_availible = parent_availible[0]
            
        parent_needed = float(parent_needed) if isinstance(parent_needed, str) and parent_needed.strip() else parent_needed
        parent_availible = float(parent_availible) if isinstance(parent_availible, str) and parent_availible.strip() else parent_availible

        if parent_availible == "" or   parent_availible == " ":
            parent_availible = 0

            
        if fulfill_miss != "NO" or receiving_miss == "YES":
            temp.append("N/A")
        elif product_class in ["Pass Through", "Parent"] or parent_needed > parent_availible:
            temp.append("YES")
        elif parent_needed <= parent_availible:
            temp.append("NO")
        else:
            temp.append("fails all")

    df_ez_gr_sales['Inventory Miss'] = pd.Series(temp)
    

    return df_ez_gr_sales
        
        

# %%
Inventory_Miss()


def manufacturing_miss():
    temp = []
    for inventory_miss in df_ez_gr_sales['Inventory Miss']:
        if inventory_miss == "NO":
            temp.append("YES")
        else:
            temp.append("N/A")
    df_ez_gr_sales['Manufacturing Miss'] = pd.Series(temp)
    
  
    return df_ez_gr_sales


# %%
manufacturing_miss()



def miss_type():
    temp = []
    for _, row in df_ez_gr_sales.iterrows():
        sku = row['Product SKU']
        late = row['Late']
        receiving_miss = row['Receiving Miss']
        fulfill_miss = row['Fulfillment Miss']
        inventory_miss = row['Inventory Miss']
        is_quarantine = row['Is Quarantine']
        manufacture_miss = row['Manufacturing Miss']

        if late =="N/A":
            temp.append(" ")
        elif receiving_miss == "YES":
            temp.append("Receiving")
        elif fulfill_miss == "YES":
            temp.append("Fulfillment")
        elif inventory_miss == "YES":
            temp.append("Inventory")
        elif manufacture_miss =="YES":
            temp.append("Manufacturing")
        elif is_quarantine == "YES":
            temp.append("Quarantine")
        else:
            temp.append("N/A")  # You may need to customize this fallback value

    df_ez_gr_sales['Miss Type'] = pd.Series(temp)
    
   
    return df_ez_gr_sales

    

# %%
miss_type()


# %%

def max_bin():
    
    max_bin_uid = []
    max_bin_number = []
    
    item_max_bin = []
    
    df_All_Items_In_Bins_Status["UID"] = df_All_Items_In_Bins_Status['Bin Number'] + df_All_Items_In_Bins_Status['Item']+df_All_Items_In_Bins_Status['Status']
    
    df_All_Items_In_Bins_Status["UID"].fillna("Bin Not Found")
    
    df_All_Items_In_Bins_Status["item_qty"] = df_All_Items_In_Bins_Status['Item'] + df_All_Items_In_Bins_Status['On Hand'].astype(str)

    for item in df_All_Items_In_Bins_Status['Item']:
        try:
            max_value = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['Item']==item,"On Hand"].max()
            current_value = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['Item']==item,"On Hand"].iloc[0]
            max_bin_uid.append(str(item)+str(max_value))
        except:
            max_bin_uid.append("Error")
            
        
        
    df_All_Items_In_Bins_Status["Max Bin UID"] = pd.Series(max_bin_uid)
    
    for uid in df_All_Items_In_Bins_Status['UID']:
        try:
            bin_number = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['UID']==uid,"Bin Number"].iloc[0]
            max_bin = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['UID']==uid,"Max Bin UID"].iloc[0]
            bin_id = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['UID']==uid,"item_qty"].iloc[0]
            item = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['UID']==uid,"Item"].iloc[0]
            
            if bin_id == max_bin:
                max_bin_number.append(bin_number)
                item_max_bin.append(item)
            else:
                max_bin_number.append("Not Max Bin")
                item_max_bin.append("N/A")
        except:
            max_bin_number.append("Bin Not Found")

    df_All_Items_In_Bins_Status["Max Bin Number"] = pd.Series(max_bin_number)
    df_All_Items_In_Bins_Status["Item Max Bin"] = pd.Series(item_max_bin)
    
    return df_All_Items_In_Bins_Status


# %%
max_bin()

# %%
def output():
    
    output_df = pd.DataFrame()
    
    UID = []
    sku = []
    so = []
    qty_on_so = []
    miss_type_list = []
    description = []
    customer = []
    max_bin = []
    qty_in_max_bin = []
    
    
    for uid in df_ez_gr_sales["UID"]:

        miss_type = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Miss Type"].iloc[0]
        
        if miss_type == "Fulfillment" or miss_type == "Manufacturing" or miss_type == "Quarantine":
            product = df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Product SKU"].iloc[0]
            try:
                max_bin_num = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['Item Max Bin']==product,"Bin Number"].iloc[0]
                qty_in_bin = df_All_Items_In_Bins_Status.loc[df_All_Items_In_Bins_Status['Item Max Bin']==product,"On Hand"].iloc[0]
                max_bin.append(max_bin_num)
                qty_in_max_bin.append(qty_in_bin)
            
            except:
                max_bin.append("Not Found")
                qty_in_max_bin.append("Not Found")
                
            
            UID.append(uid)
            sku.append(product)
            so.append(df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Document Number"].iloc[0])
            qty_on_so.append(df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Quantity"].iloc[0])
            
            miss_type_list.append(df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Miss Type"].iloc[0])
            description.append(df_EZ_Production_INV_Search_YST.loc[df_EZ_Production_INV_Search_YST['Item']==product,"Description"].iloc[0])
            customer.append(df_ez_gr_sales.loc[df_ez_gr_sales['UID']==uid, "Customer"].iloc[0])
            
        else:
            continue
    
    output_df['UID']  =pd.Series(UID) 
    output_df['SKU Missed']  =pd.Series(sku) 
    output_df['Sales Order']  =pd.Series(so)
    output_df['Quantity on SO']  =pd.Series(qty_on_so)
    output_df['Max Bin']  =pd.Series(max_bin)
    output_df['Qty in Max Bin']  =pd.Series(qty_in_max_bin)
    output_df['Reason Missed']  =pd.Series(miss_type_list)
    output_df['Description']  =pd.Series(description)
    output_df['Customer']  =pd.Series(customer)

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        output_df.to_csv(temp_file.name,index=False)
        outputPath = temp_file.name

    uploadBlobClient = blobConnect.get_blob_client(container=misstestOutputContainer, blob="miss_test_op_today.csv")

    with open(file=outputPath,mode="rb") as outputData:
            uploadBlobClient.upload_blob(outputData, overwrite=True)

    return output_df

output()
