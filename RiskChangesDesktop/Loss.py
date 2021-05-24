import psycopg2
import pandas as pd
import numpy
import geopandas as gpd
import random
import string
import os.path
# Importing the libraries 
import numpy as np 
import pandas as pd 
def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
#from .classifyVulnerability import classifyVulnerability
def getExposure(exposure):
    
    assert os.path.isfile(exposure), f"the file {exposure} do not exist, please check the directory again"
    if 'csv' in exposure:
        exposureData=pd.read_csv(exposure)
    
    elif 'shp' in exposure:
        exposureData=gpd.read_file(exposure)
        exposureData = pd.DataFrame(exposureData.drop(columns='geometry'))
    else:
        raise TypeError('The format of the exposure must be either shp or csv')
        
    return exposureData
def mergeGeometry(loss,ear,ear_id):
    #assert os.path.isfile(loss), f"the file {loss} do not exist, please check the directory again"
    assert os.path.isfile(ear), f"the file {ear} do not exist, please check the directory again"
    earData=gpd.read_file(ear)
    
    losstable= pd.merge(left=loss, right=earData, how='left', left_on=['geom_id'], right_on=[ear_id],right_index=False)
    losstable=gpd.GeoDataFrame(losstable, crs=earData.crs, geometry=losstable.geometry)
    return losstable

def mergeGeometryAdmin(loss,ear,ear_id):
    assert os.path.isfile(loss), f"the file {loss} do not exist, please check the directory again"
    assert os.path.isfile(ear), f"the file {ear} do not exist, please check the directory again"
    earData=gpd.read_file(ear)
    
    losstable= pd.merge(left=loss, right=earData, how='left', left_on=['admin_unit'], right_on=[ear_id],right_index=False)
    losstable=gpd.GeoDataFrame(losstable, crs=earData.crs, geometry=losstable.geometry)
    return losstable
def getHazardMeanIntensity(exposuretable,stepsize,base):
    stepsize=stepsize#5 #import from database
    base=base#0 #import from database
    half_step=stepsize/2
    exposuretable['meanHazard']=base+exposuretable['class']*stepsize-half_step
    return exposuretable
def checknconvert(exposuretable,hazunit,vulnunit,multiplyfactor=1):
    #if vulnerability and hazard do not have same units convert mean hazard to same values..  
    if hazunit==vulnunit:
        exposuretable['meanHazard']=exposuretable['meanHazard']
    else:
        mulfactor=multiplyfactor
        exposuretable['meanHazard']=exposuretable['meanHazard']*mulfactor  
    return exposuretable
import numpy as np
import pandas as pd
import geopandas as gpd
def getVulnerability(exposuretable,vulnColumn,vulndir,haztype):
        if haztype=="Intensity":  
            final_df=pd.DataFrame()
            for i in exposuretable[vulnColumn].unique():
                #print(i)
                vuln_file=vulndir+'/'+str(i)+'.csv'
                assert os.path.isfile(vuln_file), f"the file {vuln_file} do not exist, please check the directory again"
                vulnerbaility=pd.read_csv(vuln_file)
                vulnerbaility['mean_x'] =vulnerbaility.apply(lambda row: (row.hazintensity_from+row.hazintensity_to)/2, axis=1)
                y=vulnerbaility.vulnAVG.values 
                x=vulnerbaility.mean_x.values 

                subset_exp=exposuretable[exposuretable[vulnColumn]==i]
                subset_exp["vuln"]=np.interp(subset_exp.meanHazard, x, y, left=0, right=1)
                final_df=final_df.append(subset_exp, ignore_index = True)
                final_df.loc[final_df.vuln<0,'vuln':]=0
                final_df.loc[final_df.vuln>1,'vuln':]=1
                
            exposuretable=None
            exposuretable=final_df
        elif haztype=="susceptibility":
            for i in exposuretable[vulnColumn].unique():
                vuln_file=vulndir+'/'+str(i)+'.csv'
                assert os.path.isfile(vuln_file), f"the file {vuln_file} do not exist, please check the directory again"
                vulnerbaility=pd.read_csv(vuln_file)
                subset_exp=exposuretable[exposuretable[vulnColumn]==i]
                #subset_exp["vuln"]
                subset_exp=pd.merge(left=subset_exp, right=vulnerbaility[['vulnAVG', 'hazIntensity_to']], how='left', left_on=['class'], right_on=['hazIntensity_to'],right_index=False)
                subset_exp.drop(columns= ['hazIntensity_to'])
                subset_exp.rename(columns={"vulnAVG": "vuln"})
                final_df=final_df.append(subset_exp, ignore_index = True)   
            exposuretable=None
            exposuretable=final_df
        else:
            raise TypeError('The Only susceptibility and intensity type of hazards are supported by RiskChanges')
        return exposuretable
    
def calculateLoss(exposuretable,aggregation,costColumn,spprob):
    if aggregation:
        if 'admin_unit' not in exposuretable.columns:
            raise ValueError("Aggregation must required in the exposure table")
        exposuretable['loss'] = exposuretable.apply(lambda row: row[costColumn]*row.exposed*row.vuln*spprob/100, axis=1)
        losstable=exposuretable.groupby(["geom_id"],as_index=False).agg({'loss':'sum'})
        losstableAgg=seexposuretable.groupby(['admin_unit'],as_index=False).agg({'loss':'sum'})
    else:
        exposuretable['loss'] = exposuretable.apply(lambda row: row[costColumn]*row.exposed*row.vuln*spprob/100, axis=1)
        losstable=exposuretable.groupby(["geom_id"],as_index=False).agg({'loss':'sum'})
    return losstable
def saveshp(df,outputname):
    df.to_file(outputname+".shp")
def savecsv(df,outputname):
    df.drop('geometry',axis=1).to_csv(outputname+".csv") 
def computeLoss(exposure,ear,ear_key,costcol,vulncol,vulndir,haztype,outputfile,outputformat,
                spprob=1,stepsize=1,base=0,hazunit=None,vulnunit=None,multiplyfactor=1,aggregation=False):
    exposure_data=getExposure(exposure)
    exposure_data=getHazardMeanIntensity(exposure_data,stepsize,base)
    exposure_data=checknconvert(exposure_data,hazunit,vulnunit,multiplyfactor)
    exposure_data=getVulnerability(exposure_data,vulncol,vulndir,haztype)
    loss=calculateLoss(exposure_data,aggregation,costcol,spprob)
    #return loss,exposure_data
    loss_geom=mergeGeometry(loss,ear,ear_key)
    if outputformat=="shp":
        saveshp(loss_geom,outputfile)
        return f"the loss calculation is complete and resutls are saved to {outputfile}.shp"
    if outputformat=="csv":
        savecsv(loss_geom,outputfile)
        return f"the loss calculation is complete and resutls are saved to {outputfile}.csv"
def computeLossAgg(exposure,ear,ear_key,costcol,vulncol,vulndir,haztype,adminunit,admin_key,outputfile,outputformat,
                spprob=1,stepsize=1,base=0,hazunit=None,vulnunit=None,multiplyfactor=1,aggregation=True):
    
    ear_data=gpd.read_file(ear)
    admin_data=gpd.read_file(adminunit)
    ear_temp=gpd.overlay(ear_data, admin_data, how='intersection', make_valid=True, keep_geom_type=True)
    exposure_data=getExposure(exposure)
    ear_temp = pd.DataFrame(ear_temp.drop(columns='geometry'))
    
    exposure_data=pd.merge(left=exposure_data, right=ear_temp[[admin_key, ear_key]], how='left', left_on=['geom_id'], right_on=[ear_key],right_index=False)
    exposure_data.rename(columns={admin_key: "admin_unit"})
    
    exposure_data=getHazardMeanIntensity(exposure_data,stepsize,base)
    exposure_data=checknconvert(exposure_data,hazunit,vulnunit,multiplyfactor)
    exposure_data=getVulnerability(exposure_data,vulncol,vulndir,haztype)
    loss=calculateLoss(exposure_data,aggregation,costcol,spprob)
    
    loss_geom=mergeGeometryAdmin(loss,adminunit,admin_key)
    if outputformat=="shp":
        saveshp(loss_geom,outputfile)
        return f"the loss calculation is complete and resutls are saved to {outputfile}.shp"
    if outputformat=="csv":
        savecsv(loss_geom,outputfile)
        return f"the loss calculation is complete and resutls are saved to {outputfile}.csv"