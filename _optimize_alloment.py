import pandas as pd 
import numpy as np
import math
import re
import warnings
# import os
# import glob

warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
pd.set_option('mode.chained_assignment', 'raise')

'''DisSale'''

path   =   "C:/Users/admin/Desktop/optimize/"
path_stockHO="C:/Users/admin/Desktop/optimize/StockHO.xlsx"

min_day=    38
max_day=    45

def drop_sub_columns_pool1(df):
    df=df.drop(['StockQuantity','SO1','SO2','SO3',
                'SO4','AvgSO','SellPower','DOS',
                'Statement']
                ,axis=1)
    return df

def drop_sub_columns_pool2(df):
    df=df.drop(['SellPower','DOS',
                'Statement','Balance_num']
                ,axis=1)
    return df

def sort_by_balance(df):    
    df=df.sort_values(by='Balance_num'
                      ,ascending=False)
    return df

def DOS_Classify (condition):
    if condition < min_day:
        return "Thiếu hàng"
    if condition > max_day:
        return "Thừa hàng"
    if min_day <= condition <= max_day:
        return "Đủ hàng"  

def allotment(df_stock_HO_raw,df_2):

    # if hasattr(df_stock_HO_raw,
    #             'attr_name') and hasattr(df_2,
    #             'attr_name'):

    list_pool_1= list()
    list_pool_2= list()
    list_sheet_name_pool_1=list()
    list_sheet_name_pool_2=list()
    pool1_tong_hop= pd.DataFrame()
    pool2_tong_hop= pd.DataFrame()
    '''cmt'''
    for df in df_2:

        data=df.loc[:,['Area','storeId','storeName',
                    'productId','productName'
                    ,'SO1','SO2','SO3','SO4',
                    'AvgSO','StockQuantity']].copy()
        
        data = data.fillna(np.nan).replace( [np.nan], 0)
        data['AvgSO'] = data['AvgSO'].clip(lower=0)

        #Sell power
        data['SellPower'] = round(data['AvgSO'].div(7),3)
        #DOS: return the quantity fit with sell power
        data['DOS'] = round(data['StockQuantity'] / data['SellPower'],0)

        #DROP residual column df.drop([index])
        data = data.sort_values(by=['DOS'],
                                ascending = False)
        data['Statement']=data['DOS'].apply(lambda x: DOS_Classify(x))
        data['Balance_num'] = '...'

        ## 1st Loop to navigate statement

        for i,row in data.iterrows():
            val=row['Statement']

            if val == "Thừa hàng":
                data.at[i,'Balance_num']=      math.ceil( data['StockQuantity'][i] 
                                                        - data['SellPower'][i] *max_day)   
            if val == "Thiếu hàng":
                data.at[i,'Balance_num'] =     round(-data['StockQuantity'][i]  
                                                    +data['SellPower'][i] *min_day,0)
            if val == "Đủ hàng":
                data.at[i,'Balance_num'] = 0

        # Initialize pool_1
        pool_1=data.loc[data['Statement'] ==
                            'Thừa hàng']
        pool_1=sort_by_balance(pool_1)

        ## Initialize pool_2
        pool_2=data.loc[data['Statement'] ==
                            'Thiếu hàng'] 
        pool_2=sort_by_balance(pool_2)

        if list(data.iterrows())[0][1]['Area'] == 'HồChíMinh' or list(data.iterrows())[0][1]['Area'] == 'HàNội':
        
            #Setup new pool_1_transfer_clone ; pool_2_get_clone
            pool_1_transfer_clone=pool_1
            pool_2_get_clone=pool_2
        
            # Identify
            # First Initialize cache_transaction
            cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                                pool_1_transfer_clone.iloc[0]['productId']]
            
            # print(cache_transaction)
            # Transaction dataframe
            ## Rule: Compare the productId and Balance number
            transaction=pd.DataFrame(columns=['storeId_transfer','storeName_transfer',
                                            'storeId_receive','storeName_receive',
                                            'productId','productName',
                                            'Quantity'])
            
            pool_3_move_to_HO=pd.DataFrame(columns=['storeId_HO','storeName_HO',
                                                    'productId','productName',
                                                    'Quantity'])
            
            while True:
                # The amount of goods exceeded pool_1
                n   =   pool_1_transfer_clone.iloc[0]['Balance_num']
                if len(cache_transaction.index)==0  :
                    d = {'storeId_HO':  pool_1_transfer_clone.iloc[0]['storeId'],
                        'storeName_HO': pool_1_transfer_clone.iloc[0]['storeName'], 

                        'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                        'productName':        pool_1_transfer_clone.iloc[0]['productName'],
                        'Quantity':           n}
                    
                    pool_3_move_to_HO=pd.concat([pool_3_move_to_HO,
                                                pd.DataFrame(data=d,index=[0])],
                                                ignore_index=False)
                    pool_1_transfer_clone.drop(index=
                                            pool_1_transfer_clone.iloc[0:1,:].index,
                                            inplace=True)
                    
                    if len(pool_1_transfer_clone.index)==0:
                        break

                    cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                                        pool_1_transfer_clone.iloc[0]['productId']]
                    continue
                # The amount of goods lacked pool_2 
                m   =   cache_transaction.iloc[0]['Balance_num']

                ## Case : if the exceed > lack
                if  n    >=   m :

                    transfering =    n  -   m
                # This dataframe is storing the transaction

                    c = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
                    'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'], 
                    'storeId_receive':    cache_transaction.iloc[0]['storeId'],
                    'storeName_receive':  cache_transaction.iloc[0]['storeName'],

                    'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                    'productName':        pool_1_transfer_clone.iloc[0]['productName'],
                    'Quantity': m}
                    transaction =    pd.concat([transaction,pd.DataFrame(data=c,
                                                index=[0])],
                                            ignore_index=False)

                    #   Migrate the value    
                    # pool_1_transfer_clone.at[0,'Balance_num'] =    transfering

                    pool_1_transfer_clone.loc[pool_1_transfer_clone.index[0],
                                            ['Balance_num']] = transfering       
                    pool_2_get_clone.drop(index=cache_transaction.iloc[0:1,:].index,
                                        inplace=True)

                    #   Refreah
                    pool_1_transfer_clone=sort_by_balance(pool_1_transfer_clone)
                    pool_2_get_clone=sort_by_balance(pool_2_get_clone)

                    #   Update cache_transaction 
                    if len(pool_2_get_clone.index)==0:
                        break
                    cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                                        pool_1_transfer_clone.iloc[0]['productId']]
                    continue
                #Case 2 lack > exceed
                if m > n  :
    
                    transfering =    m  - n 

                    # This dataframe is storing the transaction

                    e = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
                    'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'], 
                    'storeId_receive':    cache_transaction.iloc[0]['storeId'],
                    'storeName_receive':  cache_transaction.iloc[0]['storeName'],

                    'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                    'productName':        pool_1_transfer_clone.iloc[0]['productName'],
                    'Quantity': n}

                    transaction =    pd.concat([transaction,pd.DataFrame(data=e,index=[0])],
                                            ignore_index=False)
                    
                    #   Migrate values and delete perious row
                    pool_2_get_clone.loc[cache_transaction.index[0],
                                        ['Balance_num']] = transfering
                
                    pool_1_transfer_clone.drop(index=pool_1_transfer_clone.iloc[0:1,:].index
                                            ,inplace=True)
                    
                    #   Refreah 
                    pool_1_transfer_clone=sort_by_balance(pool_1_transfer_clone)
                    pool_2_get_clone=sort_by_balance(pool_2_get_clone)

                    #   Update cache_transaction 
                    if len(pool_1_transfer_clone.index)==0 :
                        break
                    cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==  
                                                        pool_1_transfer_clone.iloc[0]['productId']]
                    continue

            transaction=transaction[transaction['Quantity']>0]
            pool_3_move_to_HO=pool_3_move_to_HO[pool_3_move_to_HO['Quantity']>0]
            out_path = f"{path}{data.iloc[0]['Area']}.xlsx"

            writer = pd.ExcelWriter(out_path , 
                                    engine='xlsxwriter')
            transaction.to_excel(writer,
                                sheet_name=f"transfer_{data.iloc[0]['Area']}",
                                encoding='utf-16',
                                index=False)
            drop_sub_columns_pool2(pool_2_get_clone).to_excel(writer, 
                                                            sheet_name=f"Statement_{data.iloc[0]['Area']}_thiếuhàng",
                                                            encoding='utf-16',
                                                            index=False)
            pool_3_move_to_HO.to_excel(writer, 
                                    sheet_name=f"Statement_{data.iloc[0]['Area']}_thừahàng",
                                    encoding='utf-16',
                                    index=False)
            
            pool1_tong_hop=pd.concat([pool1_tong_hop,
                                    pool_3_move_to_HO],
                                    ignore_index=True)
            
            pool2_tong_hop=pd.concat([pool2_tong_hop,
                                    drop_sub_columns_pool2(pool_2_get_clone)],
                                    ignore_index=True)
            writer.save()
        else: 
            list_pool_1.append(drop_sub_columns_pool1(pool_1.rename(columns={'Balance_num':'Quantity',
                                                                            'storeId':'storeId_HO',
                                                                            'storeName':'storeName_HO'}))) #Thừa hàng
            list_pool_2.append(drop_sub_columns_pool2(pool_2)) #Thiếu hàng

            list_sheet_name_pool_1.append(f"{data.iloc[0]['Area']}_HO") #Name for sheet_name in pool_1
            list_sheet_name_pool_2.append(f"HO_{data.iloc[0]['Area']}") #Name for sheet_name in pool_2

        '''cmt'''

    '''cmt'''
    writer_pool_1 = pd.ExcelWriter( f"{path}danh sách store tỉnh thừa hàng.xlsx" ,
                                    engine='xlsxwriter')
    writer_pool_2 = pd.ExcelWriter( f"{path}danh sách store tỉnh thiếu hàng.xlsx" ,
                                    engine='xlsxwriter')

    for pool1,name_pool1,pool2,name_pool2 in zip(list_pool_1,list_sheet_name_pool_1,
                                                list_pool_2,list_sheet_name_pool_2):
        
        pool1.to_excel( writer_pool_1, 
                        sheet_name= name_pool1,
                        encoding='utf-16',
                        index=False )

        pool2.to_excel( writer_pool_2, 
                        sheet_name= name_pool2,
                        encoding='utf-16',
                        index=False )
        
        pool1=pool1.drop(['Area'],
                        axis=1)
        
        pool1_tong_hop=pd.concat([pool1_tong_hop,
                                  pool1],
                                  ignore_index=True)
        
        pool2_tong_hop=pd.concat([pool2_tong_hop,
                                  pool2],
                                  ignore_index=True)

    writer_pool_1.save()
    writer_pool_2.save()
    '''cmt'''

    pool1_tong_hop.to_excel(f'{path}danh sách tất cả store thừa hàng.xlsx',
                            encoding='utf-16',
                            index=False)
    pool2_tong_hop.to_excel(f'{path}danh sách tất cả store thiếu hàng.xlsx',
                            encoding='utf-16',
                            index=False)
    pool1_tong_hop=pool1_tong_hop.rename(columns={'storeId_HO':'storeId',
                                                  'storeName_HO':'storeName',
                                                  'Quantity':'StockQuantity'})

    '''cmt'''

    df_stock_HO=pd.concat([df_stock_HO_raw,pool1_tong_hop],
                           ignore_index=True)
               
    df_stock_HO=df_stock_HO.groupby(by=['productId',
                                        'productName'],
                                        )['StockQuantity'].sum()
    df_stock_HO=df_stock_HO.reset_index()
    df_stock_HO['storeId']='88003'
    df_stock_HO['storeName']='KHO_DP2'
    df_stock_HO['Area']='KHO_DP2'
    df_stock_HO['SO1']=0
    df_stock_HO['SO2']=0
    df_stock_HO['SO3']=0
    df_stock_HO['SO4']=0
    df_stock_HO['AvgSO']=0
    df_stock_HO=df_stock_HO.reindex(columns=['Area','storeId','storeName','productId',
                            'productName','SO1','SO2','SO3','SO4',
                            'AvgSO','StockQuantity'])

    df_stock_HO.to_excel(f'{path}KHO_HO_sau_khi_nhận_hàng.xlsx',
                            encoding='utf-16',
                            index=False)
    #######
    df_stock_HO=pd.concat([df_stock_HO,pool2_tong_hop],
                            ignore_index=True)

    df_stock_HO = df_stock_HO.fillna(np.nan).replace( [np.nan], 0)
    df_stock_HO['AvgSO'] = df_stock_HO['AvgSO'].clip(lower=0)

    #Sell power
    df_stock_HO['SellPower'] = round(df_stock_HO['AvgSO'].div(7),3)
    #DOS: return the quantity fit with sell power
    df_stock_HO['DOS'] = round(df_stock_HO['StockQuantity'] / df_stock_HO['SellPower'],0)

    #DROP residual column df.drop([index])
    df_stock_HO = df_stock_HO.sort_values(by=['DOS'],
                            ascending = False)
    df_stock_HO['Statement']=df_stock_HO['DOS'].apply(lambda x: DOS_Classify(x))
    df_stock_HO['Balance_num'] = 0

        ## 1st Loop to navigate statement

    for i,row in df_stock_HO.iterrows():
        val=row['Statement']

        if val == "Thừa hàng":
            df_stock_HO.at[i,'Balance_num']=      math.ceil( df_stock_HO['StockQuantity'][i] 
                                                    - df_stock_HO['SellPower'][i] *max_day)   
        if val == "Thiếu hàng":
            df_stock_HO.at[i,'Balance_num'] =     round(-df_stock_HO['StockQuantity'][i]  
                                                +df_stock_HO['SellPower'][i] *min_day,0)
        if val == "Đủ hàng":
            df_stock_HO.at[i,'Balance_num'] = 0

    # Initialize pool_1
    pool_1=df_stock_HO.loc[df_stock_HO['Statement'] ==
                        'Thừa hàng']
    pool_1=sort_by_balance(pool_1)

    ## Initialize pool_2
    pool_2=df_stock_HO.loc[df_stock_HO['Statement'] ==
                        'Thiếu hàng'] 
    pool_2=sort_by_balance(pool_2)

    #Setup new pool_1_transfer_clone ; pool_2_get_clone
    pool_1_transfer_clone=pool_1
    pool_2_get_clone=pool_2

    # Identify
    # First Initialize cache_transaction
    cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                        pool_1_transfer_clone.iloc[0]['productId']]
    
    # print(cache_transaction)
    # Transaction dataframe
    ## Rule: Compare the productId and Balance number
    transaction=pd.DataFrame(columns=['storeId_transfer','storeName_transfer',
                                    'storeId_receive','storeName_receive',
                                    'productId','productName',
                                    'Quantity'])
    
    pool_3_move_to_HO=pd.DataFrame(columns=['storeId_HO','storeName_HO',
                                            'productId','productName',
                                            'Quantity'])
    
    while True:
        # The amount of goods exceeded pool_1
        n   =   pool_1_transfer_clone.iloc[0]['Balance_num']
        if len(cache_transaction.index)==0  :
            d = {'storeId_HO':  pool_1_transfer_clone.iloc[0]['storeId'],
                'storeName_HO': pool_1_transfer_clone.iloc[0]['storeName'], 

                'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                'productName':        pool_1_transfer_clone.iloc[0]['productName'],
                'Quantity':           n}
            
            pool_3_move_to_HO=pd.concat([pool_3_move_to_HO,
                                        pd.DataFrame(data=d,index=[0])],
                                        ignore_index=False)
            pool_1_transfer_clone.drop(index=
                                    pool_1_transfer_clone.iloc[0:1,:].index,
                                    inplace=True)
            
            if len(pool_1_transfer_clone.index)==0:
                break

            cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                                pool_1_transfer_clone.iloc[0]['productId']]
            continue
        # The amount of goods lacked pool_2 
        m   =   cache_transaction.iloc[0]['Balance_num']

        ## Case : if the exceed > lack
        if  n    >=   m :

            transfering =    n  -   m
        # This dataframe is storing the transaction

            c = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
            'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'], 
            'storeId_receive':    cache_transaction.iloc[0]['storeId'],
            'storeName_receive':  cache_transaction.iloc[0]['storeName'],

            'productId':          pool_1_transfer_clone.iloc[0]['productId'],
            'productName':        pool_1_transfer_clone.iloc[0]['productName'],
            'Quantity': m}
            transaction =    pd.concat([transaction,pd.DataFrame(data=c,
                                        index=[0])],
                                    ignore_index=False)

            #   Migrate the value    
            # pool_1_transfer_clone.at[0,'Balance_num'] =    transfering

            pool_1_transfer_clone.loc[pool_1_transfer_clone.index[0],
                                    ['Balance_num']] = transfering       
            pool_2_get_clone.drop(index=cache_transaction.iloc[0:1,:].index,
                                inplace=True)

            #   Refreah
            pool_1_transfer_clone=sort_by_balance(pool_1_transfer_clone)
            pool_2_get_clone=sort_by_balance(pool_2_get_clone)

            #   Update cache_transaction 
            if len(pool_2_get_clone.index)==0:
                break
            cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==
                                                pool_1_transfer_clone.iloc[0]['productId']]
            continue
        #Case 2 lack > exceed
        if m > n  :

            transfering =    m  - n 

            # This dataframe is storing the transaction

            e = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
            'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'], 
            'storeId_receive':    cache_transaction.iloc[0]['storeId'],
            'storeName_receive':  cache_transaction.iloc[0]['storeName'],

            'productId':          pool_1_transfer_clone.iloc[0]['productId'],
            'productName':        pool_1_transfer_clone.iloc[0]['productName'],
            'Quantity': n}

            transaction =    pd.concat([transaction,pd.DataFrame(data=e,index=[0])],
                                    ignore_index=False)
            
            #   Migrate values and delete perious row
            pool_2_get_clone.loc[cache_transaction.index[0],
                                ['Balance_num']] = transfering
        
            pool_1_transfer_clone.drop(index=pool_1_transfer_clone.iloc[0:1,:].index
                                    ,inplace=True)
            
            #   Refreah 
            pool_1_transfer_clone=sort_by_balance(pool_1_transfer_clone)
            pool_2_get_clone=sort_by_balance(pool_2_get_clone)

            #   Update cache_transaction 
            if len(pool_1_transfer_clone.index)==0 :
                break
            cache_transaction=pool_2_get_clone.loc[pool_2_get_clone['productId'] ==  
                                                pool_1_transfer_clone.iloc[0]['productId']]
            continue

    transaction=transaction[transaction['Quantity']>0]
    pool_3_move_to_HO=pool_3_move_to_HO[pool_3_move_to_HO['Quantity']>0]
    out_path = f"{path}KHO_HO_FINAL.xlsx"

    writer = pd.ExcelWriter(out_path , 
                            engine='xlsxwriter')
    transaction.to_excel(writer,
                        sheet_name=f"transfer_KHO_HO_FINAL",
                        encoding='utf-16',
                        index=False)
    drop_sub_columns_pool2(pool_2_get_clone).to_excel(writer, 
                                                    sheet_name=f"Statement_KHO_FINAL_thiếuhàng",
                                                    encoding='utf-16',
                                                    index=False)
    pool_3_move_to_HO.to_excel(writer, 
                            sheet_name=f"KHO_FINAL_thừahàng",
                            encoding='utf-16',
                            index=False)
    
    pool1_tong_hop=pd.concat([pool1_tong_hop,
                            pool_3_move_to_HO],
                            ignore_index=True)
    
    pool2_tong_hop=pd.concat([pool2_tong_hop,
                            drop_sub_columns_pool2(pool_2_get_clone)],
                            ignore_index=True)
    writer.save()
# ''''cmt'''

    return True

df=pd.read_excel(f'{path}DisSale2703.xlsx')

'''
This part is relate to clean the dataframe to make it synchrone Area column.
The returned list_region_2 is single unique value in Area_new
'''

list_region=df['Area'].values.tolist()
Area_new=[re.sub(r'\d+',"",
          re.sub(r"\s+","",region)) 
          for region in list_region]

df=df.drop(['Area'],
           axis=1)
df.insert(0,'Area',
          Area_new)

list_region_2=df['Area'].drop_duplicates().values.tolist()

'''
This part is create mutiple dataframe which is filtered from main dataframe
Making the loop to past out each parameter in the list before
return the list of df
'''
df_1=pd.read_excel(path_stockHO)
df_2=[df[df.Area==list_region_2[region]] 
      for region in range(0,len(list_region_2))]
allotment(df_1,df_2)