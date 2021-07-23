# Use geo_env_2 environment

# -*- coding: utf-8 -*-
# import os, sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import datetime


def Reverse_GeoCoding(Inputdata):
    #---------------------INPUT SHAPE---------------------
    #path = 'D:/LAB/geopandas_tuitor/'
    path= 'C:\\Users\\70018928\\Documents\\Project2021\\TBR\\SHAPE\\'
    # Importing Thailand ESRI Shapefile 
    th_boundary = gpd.read_file(path+'TH_tambon_boundary.shp')
    #A_boundary = gpd.read_file(path+'TH_amphoe_border.shp')
    #P_boundary = gpd.read_file(path+'TH_province.shp')

    #---------------------INPUT POINT---------------------

    cvm_geo = [Point(xy) for xy in zip(Inputdata['Longitude'].astype(float),Inputdata['Latitude'].astype(float))]
    Inputdata = gpd.GeoDataFrame(Inputdata, geometry = cvm_geo)
    Inputdata.set_crs(epsg=4326, inplace=True)
    Inputdata = Inputdata.to_crs(epsg=32647)
    #cvm_point.plot()

    #--------------------- Spatial Join------------------
    output = gpd.sjoin(Inputdata,th_boundary, how = 'inner', op = 'intersects')
    output=output.reset_index(drop=True)
    #---------------------- SAVE FILE ------------------
    return output


def Reverse_GeoCoding_CenterGrid(Inputdata):
    #---------------------INPUT SHAPE---------------------
    #path = 'D:/LAB/geopandas_tuitor/'
    path= 'C:\\Users\\70018928\\Documents\\Project2021\\TBR\\SHAPE\\'
    # Importing Thailand ESRI Shapefile 
    th_boundary = gpd.read_file(path+'TH_tambon_boundary.shp')
    #A_boundary = gpd.read_file(path+'TH_amphoe_border.shp')
    #P_boundary = gpd.read_file(path+'TH_province.shp')

    #---------------------INPUT POINT---------------------

    cvm_geo = [Point(xy) for xy in zip(Inputdata['Center_Longitude'].astype(float),Inputdata['Center_Latitude'].astype(float))]
    Inputdata = gpd.GeoDataFrame(Inputdata, geometry = cvm_geo)
    Inputdata.set_crs(epsg=4326, inplace=True)
    Inputdata = Inputdata.to_crs(epsg=32647)
    #cvm_point.plot()    

    #--------------------- Spatial Join------------------
    #print(' input : ',Inputdata, ' ======> ',Inputdata.columns)
    #print(' thboundary ', th_boundary,' ------- ',th_boundary.columns)
    output = gpd.sjoin(Inputdata,th_boundary, how = 'inner', op = 'intersects')

    nameDict1={ 'p_name_t_left':'p_name_t', 'p_name_e_left':'p_name_e', 'a_name_t_left':'a_name_t',
       'a_name_e_left':'a_name_e', 't_name_t_left':'t_name_t', 't_name_e_left':'t_name_e', 's_region_left':'s_region',
       'p_name_t_right':'Center_Province' }

    output.rename(columns=nameDict1,inplace=True)
    dropList=['a_name_t_right', 'a_name_e_right', 't_name_t_right', 't_name_e_right','p_name_e_right',
       's_region_right', 'prov_idn', 'amphoe_idn', 'tambon_idn', 'area_sqm', 'geometry', 'index_right', 'BS_IDX', 's_region', 'p_code', 'a_code', 't_code']
    output.drop(columns=dropList,inplace=True)
    #print(' output -------> ', output, ' ::  ',output.columns)

    output=output.reset_index(drop=True)
    #---------------------- SAVE FILE ------------------
    return output
