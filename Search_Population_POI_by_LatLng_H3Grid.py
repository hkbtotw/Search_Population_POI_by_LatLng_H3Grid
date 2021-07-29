from h3 import h3
from Database_Population import *
from datetime import datetime, date, timedelta
from math import radians, cos, sin, asin, sqrt
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from csv_join_tambon import Reverse_GeoCoding, Reverse_GeoCoding_CenterGrid, Reverse_GeoCoding_5km2
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob
from sys import exit
import warnings
import requests
import swifter
from tqdm import *

warnings.filterwarnings('ignore')

#enable tqdm with pandas, progress_apply
tqdm.pandas()

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

  
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def GetCenterHex_Lat(hex_id):
    return h3.h3_to_geo(hex_id)[0]

def GetCenterHex_Lng(hex_id):
    return h3.h3_to_geo(hex_id)[1]

def Read_H3_Grid_Lv8_Province_PAT(province):
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT  [hex_id]
                     ,[Latitude]
                     ,[Longitude]
                     ,[population]
                     ,[population_youth]
                     ,[population_elder]
                     ,[population_under_five]
                     ,[population_515_2560]
                     ,[population_men]
                     ,[population_women]
                     ,[geometry]
                     ,[p_name_t]
                     ,[a_name_t]
                     ,[t_name_t]
                     ,[s_region]
                     ,[prov_idn]
                     ,[amphoe_idn]
                     ,[tambon_idn]
                     ,[DBCreatedAt]
              FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Read_H3_Grid_RWI_Lv8_Province(province):
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT [hex_id]
                ,[Latitude]
                ,[Longitude]
                ,[rwi]
                ,[geometry]
                ,[p_name_t]
                ,[a_name_t]
                ,[t_name_t]
                ,[s_region]
                ,[prov_idn]
                ,[amphoe_idn]
                ,[tambon_idn]
                ,[DBCreatedAt]
            FROM [TSR_ADHOC].[dbo].[H3_Grid_RWI_Lv8_Province]
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    includeList=['hex_id','rwi']
    dfout=dfout[includeList]
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def normalize_min_max(x,x_min, x_max):
    return (x-x_min)/(x_max-x_min)

def RWI_normalize(dfIn):
    dfIn['norm_rwi']=dfIn.swifter.apply(lambda x: normalize_min_max(x['rwi'],dfIn['rwi'].min(),dfIn['rwi'].max()),axis=1)
    return dfIn

def Seek_Quantile_Position(x,d):   
    try:    
        if x<= d[0.25]:
            return 1
        elif x<=d[0.50]:
            return 2
        elif x<=d[0.75]:
            return 3
        else:
            return 4
    except:
        return 999

## Get population on grid
def Get_Facebook_Population_General(dfIn, hex_id):
    #######################################################################################################
    #print(' ---> ',hex_id)
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population'].values[0]
        else:
            #print( 'xxxxxxx ')
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_Youth(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_youth'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_Elder(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_elder'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_Under_Five(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_under_five'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_515_2560(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_515_2560'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_Men(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_men'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population
def Get_Facebook_Population_Women(dfIn, hex_id):
    #######################################################################################################
    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        #dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        dfDummy=dfIn[dfIn['hex_id']==hex_id].copy()        
        del dfIn
        if(len(dfDummy)>0):
            #print(dfDummy.columns,' ---population general-- ',dfDummy.head(5))
            population=dfDummy['population_women'].values[0]
        else:
            population=0
    else:
        #print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        population=0        
    ##########################################################################################
    #print(' population general  :: ',population)
    return population

## Get external data from sandbox
### Read external data from sandbox
def Read_Ext_711_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_711\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_711\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Retail_Shop_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_retailshop\" where p_name_t = '"""+str(prv_input)+"""' and type_ in ('Convenience store','CP','Family Mart','Lawson 108','Freshmart','108 Shop') """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_retailshop\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Residential_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_residential\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_residential\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout

### Only restaurant with Alcohols (liquer, beer, wine)
def Read_Ext_Restaurant_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                #sql = """SELECT * FROM public.\"th_ext_restaurant\" where p_name_t = '"""+str(prv_input)+"""' and left(goodfors,4) in ('จานด','เดลิ') """
                sql = """SELECT * FROM public.\"th_ext_restaurant\" where p_name_t = '"""+str(prv_input)+"""' and alcohols in ('เหล้า','คอกเทล','เบียร์','ไวน์') """
        else:
                #print(' ALL ****************************************************** ') 
                #sql = """SELECT * FROM public.\"th_ext_restaurant\" where left(goodfors,4) in ('จานด','เดลิ')  """
                sql = """SELECT * FROM public.\"th_ext_restaurant\" where alcohols in ('เหล้า','คอกเทล','เบียร์','ไวน์') """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Education_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_education\" where p_name_t = '"""+str(prv_input)+"""' and cate in ('มหาวิทยาลัย') """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_education\" where cate in ('มหาวิทยาลัย')  """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Hotel_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_hotel\" where p_name_t = '"""+str(prv_input)+"""' """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_hotel\"   """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout

### Get Store on Store grid
def Get711Store_rev2(df711, hex_id,h3_level):
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtRetailShop_rev2(df711, hex_id,h3_level):
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtResidential_rev2(df711, hex_id,h3_level):
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtRestaurant_rev2(df711, hex_id,h3_level):
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtEducation_rev2(df711, hex_id,h3_level):
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtHotel_rev2(df711, hex_id,h3_level):    
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store

### Get #Store on 5km2 around store grid
def Get711Store_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:Get711Store_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtRetailShop_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtRetailShop_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtResidential_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtResidential_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtRestaurant_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtRestaurant_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtEducation_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtEducation_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtHotel_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtHotel_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]


### Get #Store on 5km2 around store center grid + fix boundary grid
def Get711Store_Around_CenterGrid_rev2(dfIn, prvIn, hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)

    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)

    resultDf=pd.DataFrame()

    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_711_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: Get711Store_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]
def GetExtRetailShop_Around_CenterGrid_rev2(dfIn, prvIn,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)
    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)
    resultDf=pd.DataFrame()
    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_Retail_Shop_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: GetExtRetailShop_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]
def GetExtResidential_Around_CenterGrid_rev2(dfIn, prvIn, hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)
    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)
    resultDf=pd.DataFrame()
    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_Residential_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: GetExtResidential_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]
def GetExtRestaurant_Around_CenterGrid_rev2(dfIn, prvIn, hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)
    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)
    resultDf=pd.DataFrame()
    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_Restaurant_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: GetExtRestaurant_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]
def GetExtEducation_Around_CenterGrid_rev2(dfIn, prvIn, hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)
    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)
    resultDf=pd.DataFrame()
    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_Education_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: GetExtEducation_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]
def GetExtHotel_Around_CenterGrid_rev2(dfIn, prvIn, hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    #print('hex :',hexagons1)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)
    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)
    resultDf=pd.DataFrame()
    for province in provinceList:
        if(province==prvIn):
            dfShop=dfIn
        else:
            dfShop=Read_Ext_Hotel_Prv(province)   
        dfDummy=dfHex[dfHex['p_name_t']==province].copy()
        dfDummy['stores']=dfDummy.apply(lambda x: GetExtHotel_rev2(dfShop, x['hex_id'],h3_level),axis=1 )
        #print(' dfDummy : ',dfDummy)
        resultDf=resultDf.append(dfDummy).reset_index(drop=True)

    includeList=['stores']
    resultDf=resultDf[includeList].copy()
    #print(' result : ',resultDf)
    dfSum=resultDf.sum()
    #print(' Sum --- ',dfSum)   
    del dfHex, hexagons1, kRing, dfShop, dfDummy, resultDf
    return dfSum[0]


##### Get popolation 5km2 area
def GetPopulation_Around_CenterGrid(dfDummy,hex_id):
       hexagons1=[]
       hexagons1.append(hex_id)
       # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
       # 0 is no neighbor
       # 1 is 1 level around center grid and so on
       kRing = h3.k_ring(hexagons1[0], 1)
       hexagons1=list(set(list(hexagons1+list(kRing))))

       dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
       #print(' --- ',dfHex)
       if(len(dfDummy)>0):
           #print(' merge ')
           dfHex=dfHex.merge(dfDummy, how="left", on=["hex_id"])
           #print(' --- hex : ',dfHex,' :: ',dfHex.columns)
           includeList=['hex_id', 'population', 'population_youth', 'population_elder', 'population_under_five', 'population_515_2560', 'population_men', 'population_women']
           dfHex=dfHex[includeList].copy()
           #print(' --- hex : ',dfHex,' :: ',dfHex.columns)
           dfSum=dfHex.sum()
           #print(' --- Sum : ',dfSum,' :: ',dfSum[1], ' ::  ',type(dfSum[2]))
           pop_general=dfSum[1]
           pop_youth=dfSum[2]
           pop_elder=dfSum[3]
           pop_under_five=dfSum[4]
           pop_515_2560=dfSum[5]
           pop_men=dfSum[6]
           pop_women=dfSum[7]
           population=str(pop_general)+'_'+str(pop_youth)+'_'+str(pop_elder)+"_"+str(pop_under_five)+"_"+str(pop_515_2560)+"_"+str(pop_men)+"_"+str(pop_women)
           del dfSum, pop_515_2560, pop_elder, pop_general, pop_men, pop_women, pop_youth, pop_under_five
           
       else:
           #print(' not merge ')
           population=str(0)+'_'+str(0)+'_'+str(0)+"_"+str(0)+"_"+str(0)+"_"+str(0)+"_"+str(0)
       del dfHex, hexagons1
       return population

## Get population in 5km2 area + fix boundary grid
def GetPopulation_Around_CenterGrid_rev2(hex_id):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id']) 
    #print(' --- ',dfHex)    
    dfHex['h_Latitude']=dfHex.apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1 )
    dfHex['h_Longitude']=dfHex.apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1 )
    dfHex=Reverse_GeoCoding_5km2(dfHex)    
    #print(' 2 --- ',dfHex)

    provinceList=list(dfHex['p_name_t'].unique())
    #print(' provinceList : ',provinceList)

    resultDf=pd.DataFrame()
    for province in provinceList:
        dfDummy=Read_H3_Grid_Lv8_Province_PAT(province)
        dfPop=dfHex[dfHex['p_name_t']==province].copy()
        if(len(dfDummy)>0):
            #print(' merge ')
            dfPop=dfPop.merge(dfDummy, how="left", on=["hex_id"])
            #print(' --- hex : ',dfHex,' :: ',dfHex.columns)
            includeList=['hex_id', 'population', 'population_youth', 'population_elder', 'population_under_five', 'population_515_2560', 'population_men', 'population_women']
            dfPop=dfPop[includeList].copy()
            #print(' --- pop : ',dfPop,' :: ',dfPop.columns)
            
            resultDf=resultDf.append(dfPop)
            
        else:
            print(' not merge ')
            
    #print(' result :', resultDf)
    dfSum=resultDf.sum()
    #print(' --- Sum : ',dfSum,' :: ',dfSum[1], ' ::  ',type(dfSum[2]))
    pop_general=dfSum[1]
    pop_youth=dfSum[2]
    pop_elder=dfSum[3]
    pop_under_five=dfSum[4]
    pop_515_2560=dfSum[5]
    pop_men=dfSum[6]
    pop_women=dfSum[7]
    population=str(pop_general)+'_'+str(pop_youth)+'_'+str(pop_elder)+"_"+str(pop_under_five)+"_"+str(pop_515_2560)+"_"+str(pop_men)+"_"+str(pop_women)
    del pop_515_2560, pop_elder, pop_general, pop_men, pop_women, pop_youth, pop_under_five
    
    del dfHex, hexagons1, resultDf, dfPop, dfSum
    return population


def Assign_Population_General_CenterGrid(x):
       return float(x.split("_")[0])
def Assign_Population_Youth_CenterGrid(x):
       return float(x.split("_")[1])
def Assign_Population_Elder_CenterGrid(x):
       return float(x.split("_")[2])
def Assign_Population_underFive_CenterGrid(x):
       return float(x.split("_")[3])
def Assign_Population_515_2560_CenterGrid(x):
       return float(x.split("_")[4])
def Assign_Population_Men_CenterGrid(x):
       return float(x.split("_")[5])
def Assign_Population_Women_CenterGrid(x):
       return float(x.split("_")[6])

### Compute #Shop based on specified radius from location
def ComputeElementdistance(s_lat, s_lng, p_lat, p_lng):
    return haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

def ComputeNumberShop_by_Distance_rev2(s_lat, s_lng, df711, shop_distance):
    df711['distance']=df711.apply(lambda x: ComputeElementdistance(s_lat, s_lng, x['lat'], x['lng']),axis=1)
    return len(df711[df711['distance']<=shop_distance].reset_index(drop=True))

#### Reverse geocoing with Longdo map
def ReverseGeocoding_Longdo(lat,lng):
    # Use URL from opendata website
    url = 'https://api.longdo.com/map/services/address?'  
    stringSearch='lon=%s&lat=%s&noelevation=1&key=%s'%(lng,lat,longdo_api)
    url=url+stringSearch
    #print(' url : ',url)

    response = requests.get(url)
    
    try:
        result=response.json()
        output_string=result['road']+' '+result['subdistrict']+' '+result['district']+' '+result['province']+' '+result['country']+' '+result['geocode']
    except:
        output_string=''
    #print(' result :: ', output_string)
    return output_string

########################################################################################################
######  Input ----  ####################################################################################
# SQL connection for writing data to database
conn = connect_tad

# level 8 covers approx 1 km2
h3_level=8   

# Specify distance to seach for shops
shop_distance=1

# working directory
current_path=os.getcwd()
print(' -- current directory : ',current_path)  # Prints the current working directory
boundary_path=current_path+'\\boundary_data\\'
input_path=current_path+'\\'
# temp_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\temp\\'
# write_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\shapefile\\'
# qgis_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\qgis_shapefile\\'

# input filename
#input_name='Store_master.xlsx'    ### KFC Store   797,799,806,816
input_name='Location.xlsx'     ### Location   798, 800, 807, 817
#output_name='Store_Population_POI.xlsx'     ### KFC Store
output_name='Location_Population_POI.xlsx'   #### Location

# id column
id_column_name='Store_ID'
#######################################################################################################
cvt={id_column_name:str}
#dfIn=pd.read_excel(input_name, sheet_name='Store_Master',converters=cvt)    ### KFC Store
dfIn=pd.read_excel(input_name, sheet_name='Sheet1',converters=cvt)    ### Location
print(len(dfIn),' ======= ',dfIn.head(10))

### for testing ###############
#dfIn=dfIn.head(1)
##############################

dfIn['hex_id']=dfIn.progress_apply(lambda x: GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)
dfIn['Center_Latitude']=dfIn.progress_apply(lambda x: GetCenterHex_Lat(x['hex_id']),axis=1)
dfIn['Center_Longitude']=dfIn.progress_apply(lambda x: GetCenterHex_Lng(x['hex_id']),axis=1)


#includeList=['hex_id','Store_ID','Latitude','Longitude']   ### KFC Store
includeList=['hex_id','Name','Latitude','Longitude','Center_Latitude','Center_Longitude']     ### Location
dfHex=dfIn[includeList].copy().reset_index(drop=True)
#print(len(dfHex),'  ----  ',dfHex)

#### Find province name
dfHex=Reverse_GeoCoding(dfHex)
dropList=['geometry','index_right', 'p_code', 'a_code', 't_code', 'prov_idn','amphoe_idn', 'tambon_idn', 'area_sqm', 'BS_IDX']
dfHex.drop(columns=dropList,inplace=True)
#print(dfHex.columns, ' ======= ',dfHex)

## Find center grid province name
dfHex=Reverse_GeoCoding_CenterGrid(dfHex)
#print(dfHex.columns, ' ==== ****************** === ',dfHex)

mainDf=pd.DataFrame()

provinceList=list(dfHex['Center_Province'].unique())
province_bar=tqdm(provinceList)
for province in province_bar:  #[:2]:
    ################# format : file_name='boundary_ชลบุรี.data'
    province_bar.set_description("Processing %s" % province)

    print(' 1. Population on Grid ')   
    dfPop=Read_H3_Grid_Lv8_Province_PAT(province)
    #print(len(dfPop),' ---- ', dfPop.head(3), ' ::  ',dfPop.columns)
    dfDummy=dfHex[dfHex['Center_Province']==province].copy()
    # get Address by Reverse Geocoing by Longdo api
    dfDummy['address']=dfDummy.apply(lambda x: ReverseGeocoding_Longdo(x['Latitude'],x['Longitude']),axis=1)

    #print(' ==> ',dfDummy)    
    dfDummy['population_general']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_General(dfPop, x['hex_id']),axis=1)
    dfDummy['population_1625']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_Youth(dfPop, x['hex_id']),axis=1)
    dfDummy['population_60up']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_Elder(dfPop, x['hex_id']),axis=1)
    dfDummy['population_under_five']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_Under_Five(dfPop, x['hex_id']),axis=1)
    dfDummy['population_515_2660']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_515_2560(dfPop, x['hex_id']),axis=1)
    dfDummy['population_Men']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_Men(dfPop, x['hex_id']),axis=1)
    dfDummy['population_Women']=dfDummy.swifter.apply(lambda x: Get_Facebook_Population_Women(dfPop, x['hex_id']),axis=1)

    ### Count store numbers on store grid
    print(' 2. POI on grid and 5 km2 ')   
    dfShop=Read_Ext_711_Prv(province)     
    dfDummy['ext_711_073']=dfDummy.swifter.apply(lambda x: Get711Store_rev2(dfShop, x['hex_id'],h3_level),axis=1)               
    ### Version 1  ## did not fix missing information on bounary grid
    #dfDummy['ext_711_5']=dfDummy.swifter.apply(lambda x: Get711Store_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)    
    dfDummy['ext_711_5']=dfDummy.swifter.apply(lambda x: Get711Store_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    # #dfDummy['ext_711_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)

    dfShop=Read_Ext_Retail_Shop_Prv(province)   
    dfDummy['ext_Retail_073']=dfDummy.swifter.apply(lambda x: GetExtRetailShop_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfDummy['ext_Retail_5']=dfDummy.swifter.apply(lambda x: GetExtRetailShop_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    #dfDummy['ext_Retail_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)
    
    dfShop=Read_Ext_Residential_Prv(province)  
    dfDummy['ext_Residential_073']=dfDummy.swifter.apply(lambda x: GetExtResidential_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfDummy['ext_Residential_5']=dfDummy.swifter.apply(lambda x: GetExtResidential_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    #dfDummy['ext_Residential_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)
    
    dfShop=Read_Ext_Restaurant_Prv(province)  
    dfDummy['ext_Restaurant_073']=dfDummy.swifter.apply(lambda x: GetExtRestaurant_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfDummy['ext_Restaurant_5']=dfDummy.swifter.apply(lambda x: GetExtRestaurant_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    #dfDummy['ext_Restaurant_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)
    
    dfShop=Read_Ext_Education_Prv(province)
    dfDummy['ext_Education_073']=dfDummy.swifter.apply(lambda x: GetExtEducation_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfDummy['ext_Education_5']=dfDummy.swifter.apply(lambda x: GetExtEducation_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    #dfDummy['ext_Education_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)
    
    dfShop=Read_Ext_Hotel_Prv(province) 
    dfDummy['ext_Hotel_073']=dfDummy.swifter.apply(lambda x: GetExtHotel_rev2(dfShop, x['hex_id'],h3_level),axis=1)  
    dfDummy['ext_Hotel_5']=dfDummy.swifter.apply(lambda x: GetExtHotel_Around_CenterGrid_rev2(dfShop, province, x['hex_id'], h3_level),axis=1)
    #dfDummy['ext_Hotel_by_radius']=dfDummy.apply(lambda x: ComputeNumberShop_by_Distance_rev2(x['Latitude'],x['Longitude'],dfShop, shop_distance),axis=1)
    print(' 3. Population on 5km2 area ')   
    dfDummy['Population_C']=dfDummy.swifter.apply(lambda x:GetPopulation_Around_CenterGrid_rev2( x['hex_id']),axis=1)
    dfDummy['population_general_5']=dfDummy.swifter.apply(lambda x: Assign_Population_General_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_1625_5']=dfDummy.swifter.apply(lambda x: Assign_Population_Youth_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_60up_5']=dfDummy.swifter.apply(lambda x: Assign_Population_Elder_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_under_five_5']=dfDummy.swifter.apply(lambda x: Assign_Population_underFive_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_515_2660_5']=dfDummy.swifter.apply(lambda x: Assign_Population_515_2560_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_men_5']=dfDummy.swifter.apply(lambda x: Assign_Population_Men_CenterGrid(x['Population_C']),axis=1)
    dfDummy['population_women_5']=dfDummy.swifter.apply(lambda x: Assign_Population_Women_CenterGrid(x['Population_C']),axis=1)
    dfDummy.drop(columns=['Population_C'], inplace=True)
    print(' 4. RWI , normalized in province ')
    dfShop=Read_H3_Grid_RWI_Lv8_Province(province)
    dfShop=RWI_normalize(dfShop)
    quantiles = dfShop['norm_rwi'].quantile(q=[0.25,0.50,0.75])
    quantiles = quantiles.to_dict()
    #print(' quantile : ',quantiles)
    dfShop['rwi_quartile']=dfShop.swifter.apply(lambda x:Seek_Quantile_Position(x['norm_rwi'],quantiles),axis=1)
    dfDummy=dfDummy.merge(dfShop[['hex_id','norm_rwi','rwi_quartile']], on='hex_id', how='left')
    #print(' ------ ',dfDummy)

    mainDf=mainDf.append(dfDummy).reset_index(drop=True)

print(mainDf.head(10))
dropList=['Center_Latitude','Center_Longitude','a_name_e','t_name_e']
mainDf.drop(columns=dropList, inplace=True)
mainDf.rename(columns={'Center_Province':'GridCenter_Province'},inplace=True)
mainDf.to_excel(output_name)

conn.close()
del dfIn, dfHex, dfPop, dfDummy, dfShop
del mainDf

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')