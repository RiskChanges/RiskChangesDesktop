from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import os
import sys
import copy
import numpy as np
import rasterio
import geopandas as gpd
import pandas as pd
def reclassify(in_image,out_image,base,stepsize):
    input_image=rasterio.open(in_image)
    intensity_data=input_image.read(1)
    maxval=np.max(intensity_data)
    #print(maxval)
    prev=base
    i=1
    thresholds=np.arange(start=base,stop=maxval+stepsize,step=stepsize)[1:].tolist()
    #print(thresholds)
    intensity_data[intensity_data<base]=input_image.get_nodatavals()[0]
    intensity_data_classified=np.copy(intensity_data)
    for threshold in thresholds:
        #mean=intensity_data[((intensity_data<threshold) & (intensity_data>=prev))].mean()
        intensity_data_classified[((intensity_data<threshold) & (intensity_data>=prev))]=i
        mean=intensity_data_classified[((intensity_data<threshold) & (intensity_data>=prev))].mean()
        #print(mean)
        i+=1
        prev=threshold

    with rasterio.Env():
        profile = input_image.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(intensity_data_classified, 1)
        dst=None
    input_image=None
def ClassifyHazard(hazard_file,base,stepsize):
    infile=hazard_file
    outfile=hazard_file.replace(".tif","_reclassified.tif")
    reclassify(infile,outfile,base,stepsize)
def MatchProjection(in_image,invector):
    
    raster = gdal.Open(in_image)
    vector=ogr.Open(invector)
    lyr =vector.GetLayer()
    projras = osr.SpatialReference(wkt=raster.GetProjection())
    epsgras=projras.GetAttrValue('AUTHORITY',1)
    
    projear=lyr.GetSpatialRef()
    epsgear=projear.GetAttrValue('AUTHORITY',1)
    #print(epsgear,epsgras)
    #return 0
    if int(epsgras)!=int(epsgear):
        toEPSG="EPSG:"+str(epsgear)
        out_image=in_image.replace(".tif","_projected.tif")
        gdal.Warp(out_image,in_image,dstSRS=toEPSG)
        raster=None
        return f"Raster reprojected and saved as {out_image} with epsg {toEPSG}"
    else:
        raster=None
        return "Both are in same projection system, no projection required"
def ProjectVector(invector,epsg):
    outputShapefile=invector.replace(".shp","_projected.shp")
    data=gpd.read_file(invector)
    toepsg='epsg:'+str(epsg)
    data_reprojected=data.to_crs({'init': toepsg})
    data_reprojected.to_file(outputShapefile)
    return f"the file is reprojected and saved as f{outputShapefile}"
    
def ProjectRaster(in_image,epsg):
    raster = gdal.Open(in_image)
    projras = osr.SpatialReference(wkt=raster.GetProjection())
    epsgras=projras.GetAttrValue('AUTHORITY',1)
    print(epsgras,epsg)
    if int(epsgras)!=int(epsg):
        toEPSG="EPSG:"+str(epsg)
        out_image=in_image.replace(".tif","_projected.tif")
        gdal.Warp(out_image,in_image,dstSRS=toEPSG)
        raster=None
        return f"Raster reprojected and saved as {out_image} with epsg {toEPSG}"
    else:
        raster=None
        return "Both are in same projection system, no projection required"
def CheckProjectionRaster(inraster):
    raster = gdal.Open(inraster)
    projras = osr.SpatialReference(wkt=raster.GetProjection())
    epsgras=projras.GetAttrValue('AUTHORITY',1)
    raster = None
    return f"The projection system EPSG code fo the Raster image {inraster} is {epsgras} "
    
def CheckProjectionVector(invector):
    try:
        dataset = ogr.Open(invector)
    except:
        return "Dataset Dosenot Exist"
    lyr=dataset.GetLayer()
    projear=lyr.GetSpatialRef()
    epsgear=projear.GetAttrValue('AUTHORITY',1)
    dataset=None
    return f"The projection system EPSG code fo the vector {invector} is {epsgear} "
def CheckUniqueTypes(input_ear,type_coln):
    data=gpd.read_file(input_ear)
    unique=data[type_coln].unique()
    nunique=data[type_coln].nunique()
    data=None
    return f"There are {nunique} unique columns in your EAR file {input_ear} and they are {unique}"
def LinkVulnerability(input_ear,type_coln,type_vuln):
    if 'csv' in input_ear:
        data=pd.read_csv(input_ear)
    elif 'shp' in input_ear:
        data=gpd.read_file(input_ear)
    else:
         raise TypeError('The format of the exposure must be either shp or csv')
    data["vulnfile"] = ""
    for key, value in type_vuln.items():
        data['vulnfile'].loc[data[type_coln] == key]=value
    if 'shp' in input_ear:
        outputname=input_ear.replace(".shp","_linked.shp")
        data.to_file(outputname)
    elif 'csv' in input_ear:
        outputname=input_ear.replace(".csv","_linked.csv")
        data.to_csv(outputname)
    data=None
    return f"The input EAR file has linked the vulnerability curve and stored at {outputname}"
def ComputeCentroid(ear):
    df=gpd.read_file(ear)
        
    def keep_first(geo):
        if geo.geom_type == 'Polygon':
            return geo
        elif geo.geom_type == 'MultiPolygon':
            return geo[0]
    df.geometry = df.geometry.apply(lambda _geo: keep_first(_geo))
    df2=df
    df.geometry=df2.centroid
    outputname_centroid=ear.replace(".shp","_centroid.shp")
    df.to_file(outputname_centroid)
    return f"The input EAR file has been converted to centroid and saved at {outputname_centroid}"