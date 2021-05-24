import rasterio
import geopandas as gpd
import numpy
import os
import sys
from osgeo import ogr
from osgeo import gdal
from osgeo import osr
import pandas as pd
from rasterio.windows import Window
from rasterio.windows import from_bounds
from rasterio.mask import mask
import tqdm.notebook as tqdm
import numpy.ma as ma
import numpy as np
def Exposure(input_zone, input_value_raster,Ear_Table_PK,agg_col=None):
    vector=ogr.Open(input_zone)
    lyr =vector.GetLayer()
    feat = lyr.GetNextFeature()
    geom = feat.GetGeometryRef()
    geometrytype=geom.GetGeometryName()
    if (geometrytype== 'POLYGON' or geometrytype== 'MULTIPOLYGON'):
        return zonalPoly(input_zone,input_value_raster,Ear_Table_PK,agg_col=agg_col)
        
    elif(geometrytype=='POINT' or geometrytype=='MULTIPOINT'):
        return zonalLine(input_zone,input_value_raster,Ear_Table_PK,agg_col=agg_col)
        
    elif(geometrytype=='LINESTRING' or geometrytype=='MULTILINESTRING'):
        return zonalPoint(lyr,input_value_raster,Ear_Table_PK,agg_col=agg_col)
def zonalPoly(input_zone_data,input_value_raster,Ear_Table_PK,agg_col):
    raster=rasterio.open(input_value_raster)
    data=gpd.read_file(input_zone_data)
    df=pd.DataFrame()
    for ind,row in tqdm.tqdm(data.iterrows(),total=data.shape[0]):
        maska,transform=rasterio.mask.mask(raster, [row.geometry], crop=True,nodata=0)
        zoneraster = ma.masked_array(maska, mask=maska==0)
        len_ras=zoneraster.count()
        #print(len_ras)
        if len_ras==0:
            continue

        unique, counts = np.unique(zoneraster, return_counts=True)
        if ma.is_masked(unique):
            unique=unique.filled(0)
            idx=np.where(unique==0)[0][0]
            #print(idx)
            ids=np.delete(unique, idx)
            cus=np.delete(counts, idx)
        else:
            ids=unique
            cus=counts
        frequencies = np.asarray((ids, cus)).T
        for i in range(len(frequencies)):
            frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
        #print(frequencies)
        df_temp= pd.DataFrame(frequencies, columns=['class','exposed'])
        df_temp['geom_id']=row[Ear_Table_PK]
        if agg_col != None :
            df_temp['admin_unit']=row[agg_col]  
            df_temp['areaOrLen']=row.geometry.area
        df=df.append(df_temp,ignore_index=True)
    
    raster=None 
    return df
def zonalLine(lyr,input_value_raster,Ear_Table_PK,agg_col):
    tempDict={}
    featlist=range(lyr.GetFeatureCount())
    raster = gdal.Open(input_value_raster)
    
    projras = osr.SpatialReference(wkt=raster.GetProjection())
    epsgras=projras.GetAttrValue('AUTHORITY',1)
    
    projear=lyr.GetSpatialRef()
    epsgear=projear.GetAttrValue('AUTHORITY',1)
    #print(epsgear,epsgras)
    if not epsgras==epsgear:
        toEPSG="EPSG:"+str(epsgear)
        output_raster=input_value_raster.replace(".tif","_projected.tif")
        gdal.Warp(output_raster,input_value_raster,dstSRS=toEPSG)
        raster=None
        raster=gdal.Open(output_raster)
    else:
        pass
    # Get raster georeference info
    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromWkt(raster.GetProjectionRef())
    gt=raster.GetGeoTransform()
    xOrigin = gt[0]
    yOrigin = gt[3]
    pixelWidth = gt[1]
    pixelHeight = gt[5]
    rb=raster.GetRasterBand(1)
    

    df = pd.DataFrame()
    for FID in featlist:
        feat = lyr.GetFeature(FID)
        geom = feat.GetGeometryRef()
        extent = geom.GetEnvelope()
        xmin = extent[0]
        xmax = extent[1]
        ymin = extent[2]
        ymax = extent[3]
        
        xoff = int((xmin - xOrigin)/pixelWidth)
        yoff = int((yOrigin - ymax)/pixelWidth)
        xcount = int((xmax - xmin)/pixelWidth)+1
        ycount = int((ymax - ymin)/pixelWidth)+1
        
        target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, 1, gdal.GDT_Byte)
        target_ds.SetGeoTransform((
            xmin, pixelWidth, 0,
            ymax, 0, pixelHeight,
        ))
        # Create for target raster the same projection as for the value raster
        target_ds.SetProjection(raster_srs.ExportToWkt())
        
        gdal.RasterizeLayer(target_ds, [1], lyr, burn_values=[1])

        # Read raster as arrays
        banddataraster = raster.GetRasterBand(1)
        dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

        bandmask = target_ds.GetRasterBand(1)
        datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)
        # Mask zone of raster
        zoneraster = numpy.ma.masked_array(dataraster,  numpy.logical_not(datamask))
       #print(zoneraster)
        (unique, counts) = numpy.unique(zoneraster, return_counts=True)
        unique[unique.mask] = 9999
        if 9999 in unique:
            falsedata=numpy.where(unique==9999)[0][0]
            ids=numpy.delete(unique, falsedata)
            cus=numpy.delete(counts, falsedata)
        else:
            ids=unique
            cus=counts
        #print(ids)
        frequencies = numpy.asarray((ids, cus)).T
        len_ras=zoneraster.count()
        for i in range(len(frequencies)):
            frequencies[i][1]=(frequencies[i][1]/len_ras)*100  
            
        df_temp= pd.DataFrame(frequencies, columns=['class','exposed'])
        df_temp['geom_id'] = feat[Ear_Table_PK]
        #df_temp['exposure_id'] = exposure_id
        #df_temp['admin_unit'] =feat[agg_col]
        df=df.append(df_temp,ignore_index=True)
    raster=None 
    return df
def zonalPoint(lyr,input_value_raster,Ear_Table_PK,agg_col):
    df=gpd.read_file(lyr)
    src=rasterio.open(input_value_raster)
    print("Calculating via centroid")
    coords = [(x,y) for x, y in tqdm.tqdm(zip(df.geometry.x, df.geometry.y))]
    a =src.sample(coords)
    exposure=[]
    for value in a:
        exposure.append(value[0])
    df['class']=exposure
    df['exposed']=100
    df.loc[(df['class']<0)|(df['class']>1000), 'class'] = 0
    df.loc[df['class']==0, 'exposed'] = 0
    df=df.rename(columns={Ear_Table_PK:'geom_id'})
    if agg_col != None :
        df_sel=df[['class','exposed','geom_id','admin_unit']]    
    else:
        df_sel=df[['class','exposed','geom_id']]
        
    src=None 
    return df_sel
def saveshp(df,outputname):
    df.to_file(outputname+".shp")
def savecsv(df,outputname):
    df.drop('geometry',axis=1).to_csv(outputname+".csv") 
def ComputeExposure(ear,hazard,ear_key,outputdir,outputformat="csv"):
    exposure=Exposure(ear, hazard,ear_key)
    #import geopandas as gpd
    ear=gpd.read_file(ear)
    exposure_merged=ear.merge(exposure, how='right', left_on=[ear_key], right_on=['geom_id'])
    if outputformat=="shp":
        saveshp(exposure_merged,outputdir)
    if outputformat=="csv":
        savecsv(exposure_merged,outputdir)
def aggregate(df,agg_col):
    try:
        df['exposed_areaOrLen']=df['exposed']*df['areaOrLen']/100
        df_aggregated=df.groupby(['admin_unit','class'],as_index=False).agg({'exposed_areaOrLen':'sum','exposed':'count'})
    except:
        df_aggregated=df.groupby(['admin_unit','class'],as_index=False).agg({'exposed':'count'})
    return df_aggregated
def ComputeExposureAgg(ear,hazard,ear_key,admin_unit,agg_col,outputname,outputformat="csv"):
    ear_data=gpd.read_file(ear)
    admin_data=gpd.read_file(admin_unit)
    ear_temp=gpd.overlay(ear_data, admin_data, how='intersection', make_valid=True, keep_geom_type=True)
    #ear_temp=ear_temp.rename(columns={agg_col:'admin_unit'})
    tempfile=ear.replace(".shp","_tempadmin.shp")
    ear_temp.to_file(tempfile)
    ear=tempfile
    
    exposure=Exposure(ear, hazard,ear_key,agg_col)
    agg_exposure=aggregate(exposure,agg_col)
    #import geopandas as gpd
    #admin=gpd.read_file(ear)
    exposure_merged=admin_data.merge(agg_exposure, how='right', left_on=[agg_col], right_on=['admin_unit'])
    #return exposure_merged
    if outputformat=="shp":
        saveshp(exposure_merged,outputname)
    if outputformat=="csv":
        savecsv(exposure_merged,outputname)