import pandas as pd
import geopandas as gpd
from sklearn.metrics import auc
import os
def load_loss(losscomb):
    print(f"calculating risk for {losscomb['Hazard'].nunique()}")
    assert losscomb['Hazard'].nunique()==1, "Only multiple return periods of single hazard is supported"
    colnames=[]
    probs=[]
    for index, row in losscomb.iterrows():
        #print(row)
        lossindex=row['RiskCombinationId']
        losstable=row['loss_file']
        returnperiod=row['return_period']
        prob=1/returnperiod
        colname='loss_rp_'+str(returnperiod)
        #print(colname)

        lossdf_temp=pd.read_csv(losstable)
        lossdf_temp=lossdf_temp[['geom_id','loss']]
        lossdf_temp=lossdf_temp.rename(columns={'loss': colname})
        if index == 0:
            lossdf=lossdf_temp
        else:
            lossdf=lossdf.merge(lossdf_temp, on='geom_id',right_index=False)
        colnames.append(colname)
        probs.append(prob)
        #print(self.lossdf)
    return colnames,probs,lossdf
def dutch_method(xx,yy):
    #compute risk based on dutch method where xx is value axis and yy is probability axis
    AAL=auc(yy,xx)+(xx[0]*yy[0])
    return AAL
def calculaterisk(lossdf,colnames,probs):
    for index, row in lossdf.iterrows():
        xx=row[colnames].values.tolist()
        yy=probs
        aal=dutch_method(xx,yy)
        #print('ear',aal)
        ear_id=row['geom_id']
        new_row = {'geom_id':[ear_id], 'AAL':[aal]}
        #append row to the dataframe
        if index==0:
            new_row = {'geom_id':[ear_id], 'AAL':[aal]}
            risktable=pd.DataFrame.from_dict(new_row)
        else:
            new_row = {'geom_id':ear_id, 'AAL':aal}
            risktable = risktable.append(new_row, ignore_index=True)
    return risktable
def computeSingleRisk(losscombination):
    colnames,probs,lossdf=load_loss(losscombination)
    print(colnames,probs)
    risk=calculaterisk(lossdf,colnames,probs)
    return risk
def saveshp(df,outputname):
    df.to_file(outputname+".shp")
def savecsv(df,outputname):
    df.drop('geometry',axis=1).to_csv(outputname+".csv") 
def mergeGeometry(loss,ear,ear_id):
    #assert os.path.isfile(exposure), f"the file {exposure} do not exist, please check the directory again"
    assert os.path.isfile(ear), f"the file {ear} do not exist, please check the directory again"
    earData=gpd.read_file(ear)
    
    losstable= pd.merge(left=loss, right=earData, how='left', left_on=['geom_id'], right_on=[ear_id],right_index=False)
    losstable=gpd.GeoDataFrame(losstable, crs=earData.crs, geometry=losstable.geometry)
    return losstable
def mergeGeometryAdmin(risk,ear,ear_id):
    #assert os.path.isfile(exposure), f"the file {exposure} do not exist, please check the directory again"
    assert os.path.isfile(ear), f"the file {ear} do not exist, please check the directory again"
    earData=gpd.read_file(ear)
    
    risktable= pd.merge(left=risk, right=earData, how='left', left_on=['admin_unit'], right_on=[ear_id],right_index=False)
    risktable=gpd.GeoDataFrame(risktable, crs=earData.crs, geometry=losstable.geometry)
    return losstable
def computeRisk(riskcombofile):
    riskcombinations=pd.read_csv(riskcombofile)
    combinations=riskcombinations.RiskCombinationId.unique()
    for riskcombinationid in combinations:
        subset_cominations=riskcombinations[riskcombinations.RiskCombinationId==riskcombinationid]
        outputfilename=subset_cominations.outputname[0]
        outputfileformat=subset_cominations.outputformat[0]
        ear=subset_cominations.ear[0]
        ear_id=subset_cominations.ear_key[0]
        risk=computeSingleRisk(subset_cominations)
        risk=mergeGeometry(risk,ear,ear_id)
        if outputfileformat=="shp":
            saveshp(risk,outputfilename)
            return f"the risk calculation is complete and resutls are saved to {outputfilename}.shp"
        if outputfileformat=="csv":
            savecsv(risk,outputfilename)
            return f"the risk calculation is complete and resutls are saved to {outputfilename}.shp"
            print(risk)

def computeRiskAgg(riskcombofile):
    riskcombinations=pd.read_csv(riskcombofile)
    combinations=riskcombinations.RiskCombinationId.unique()
    for riskcombinationid in combinations:
        subset_cominations=riskcombinations[riskcombinations.RiskCombinationId==riskcombinationid]
        outputfilename=subset_cominations.outputname[0]
        outputfileformat=subset_cominations.outputformat[0]
    
    
    
        ear=subset_cominations.ear[0]
        ear_id=subset_cominations.ear_key[0]
        adminunit=subset_cominations.adminunit[0]
        admin_key=subset_cominations.adminunit_key[0]
        
        ear_data=gpd.read_file(ear)
        admin_data=gpd.read_file(adminunit)
        ear_temp=gpd.overlay(ear_data, admin_data, how='intersection', make_valid=True, keep_geom_type=True)
        
        risk=computeSingleRisk(subset_cominations)

      
        ear_temp = pd.DataFrame(ear_temp.drop(columns='geometry'))
        risk=pd.merge(left=risk, right=ear_temp[[admin_key, ear_id]], how='left', left_on=['geom_id'], right_on=[ear_key],right_index=False)
        risk.rename(columns={admin_key: "admin_unit"})
        risk=risk.groupby(['admin_unit'],as_index=False).agg({'AAL':'sum'})
        risk=mergeGeometryAdmin(risk,adminunit,admin_key)
        if outputfileformat=="shp":
            saveshp(risk,outputfilename)
            return f"the risk calculation is complete and resutls are saved to {outputfilename}.shp"
        if outputfileformat=="csv":
            savecsv(risk,outputfilename)
            return f"the risk calculation is complete and resutls are saved to {outputfilename}.shp"
            #print(risk)
def mul_independent(individual_risks,hazards,triggering=None,probability_independent=None):
    hazards_list=list(hazards)
    columns_hazard_only=[hazard + "_AAL" for hazard in hazards_list]
    columns_hazard=columns_hazard_only
    columns_hazard.append('Unit_ID')
    individual_filtered=individual_risks[columns_hazard]
    individual_filtered['independent']=individual_filtered[columns_hazard_only].sum(axis=1)
    independent_risk=individual_filtered.drop(columns=columns_hazard_only)
    return independent_risk

    #return combination of risks

def mul_compounding(individual_risks,value,triggering=None,probability_compounding=None):
    hazards_list=list(hazards)
    columns_hazard_only=[hazard + "_AAL" for hazard in hazards_list]
    columns_hazard=columns_hazard_only
    columns_hazard.append('Unit_ID')
    columns_hazard.append('maxvalue')
    individual_filtered=individual_risks[columns_hazard]

    individual_filtered['compounding_nobound']=individual_filtered[columns_hazard_only].sum(axis=1)
    individual_filtered['compounding']=individual_filtered[['compounding_nobound','maxvalue']].min(axis=1)

    columns_hazard_only.append('compounding_nobound')
    columns_hazard_only.append('maxvalue')

    compounding_risk=individual_filtered.drop(columns=columns_hazard_only)
    return compounding_risk
    #return combination of risks

def mul_coupled(individual_risks,value,triggering=None,probability_coupled=None):
    hazards_list=list(hazards)
    columns_hazard_only=[hazard + "_AAL" for hazard in hazards_list]
    columns_hazard=columns_hazard_only
    columns_hazard.append('Unit_ID')
    individual_filtered=individual_risks[columns_hazard]

    individual_filtered['coupled']=individual_filtered[columns_hazard_only].max(axis=1)

    coupled_risk=individual_filtered.drop(columns=columns_hazard_only)
    return coupled_risk

    #return combination of risks

def mul_cascading(individual_risks,value, triggering, probability_cascade=1):
    hazards_list=list(hazards)
    columns_hazard_only=[hazard + "_AAL" for hazard in hazards_list]
    columns_hazard=columns_hazard_only
    columns_hazard.append('Unit_ID')
    individual_filtered=individual_risks[columns_hazard]
    triggering_AAL=triggering+ "_AAL"
    columns_hazard_only.remove(triggering_AAL)

    individual_filtered['cascading_AAL']=probability_cascade*(individual_filtered[columns_hazard_only].sum(axis=1))+[individual_filtered]
    individual_filtered['cascading']=individual_filtered[['cascading_AAL','maxvalue']].min(axis=1)

    cascading_risk=individual_filtered.drop(columns=columns_hazard_only)
    cascading_risk=cascading_risk.drop(columns=triggering_AAL)
    return cascading_risk
    #return combination of risks

def mul_conditional(individual_risks,value,triggering,probability_conditional=1):
    hazards_list=list(hazards)
    columns_hazard_only=[hazard + "_AAL" for hazard in hazards_list]
    columns_hazard=columns_hazard_only
    columns_hazard.append('Unit_ID')
    individual_filtered=individual_risks[columns_hazard]
    triggering_AAL=triggering+ "_AAL"
    columns_hazard_only.remove(triggering_AAL)
    individual_filtered['conditional']=probability_conditional*(individual_filtered[columns_hazard_only].sum(axis=1))+[individual_filtered]       
    conditional_risk=individual_filtered.drop(columns=columns_hazard_only)
    conditional_risk=conditional_risk.drop(columns=triggering_AAL)
    return conditional_risk
    #return combination of risks
        
        
def computeAllRisk(hazardcombinations):
    combinations=hazardcombinations.Hazard.unique()
    i=0
    for singlehazard in combinations:
        subset_cominations=hazardcombinations[hazardcombinations.Hazard==singlehazard]
        risk=computeSingleRisk(subset_cominations)
        if i==0:
            risk=risk.rename(columns={'AAL': singlehazard+'_AAL','geom_id':'Unit_ID'})
            allrisk=risk
            i+=1
        else:
            risk=risk.rename(columns={'AAL': singlehazard+'_AAL','geom_id':'Unit_ID'})
            allrisk=pd.merge(left=allrisk, right=risk, how='left', left_on=['Unit_ID'], right_on=['Unit_ID'],right_index=False)
    return allrisk
def mul_combineall(listed_risks):
    #return combination of risks
    hazards_df_cols=list(listed_risks.columns)
    hazards_df_cols.remove('Unit_ID')
    listed_risks['AAL']=listed_risks[hazards_df_cols].sum(axis=1)
    hazards_df=listed_risks.drop(hazards_df_cols)
    multihazard_risk=hazards_df
    return multihazard_risk
def mergeGeometryMh(loss,ear,ear_id):
    #assert os.path.isfile(exposure), f"the file {exposure} do not exist, please check the directory again"
    assert os.path.isfile(ear), f"the file {ear} do not exist, please check the directory again"
    earData=gpd.read_file(ear)
    
    losstable= pd.merge(left=loss, right=earData, how='left', left_on=['Unit_ID'], right_on=[ear_id],right_index=False)
    losstable=gpd.GeoDataFrame(losstable, crs=earData.crs, geometry=losstable.geometry)
    return losstable
def ComputeRiskMh(riskcombofile,hazard_interaction):
    first=True

    switcher = {'independent': mul_independent, 'compounding': mul_compounding,'coupled': mul_coupled,
                'cascading': mul_cascading,'conditional': mul_conditional}
    riskcombinations=pd.read_csv(riskcombofile)
    combinations=riskcombinations.RiskCombinationId.unique()
    for riskcombinationid in combinations:
        subset_cominations=riskcombinations[riskcombinations.RiskCombinationId==riskcombinationid]
        
        outputfilename=subset_cominations.outputname[0]
        outputfileformat=subset_cominations.outputformat[0]
        ear=subset_cominations.ear[0]
        ear_id=subset_cominations.ear_key[0]
        
        all_risk=computeAllRisk(subset_cominations)
        
        relevant_interactions=hazard_interaction[hazard_interaction.RiskCombinationId==riskcombinationid]
        
        for index, rows in relevant_interactions.iterrows():
            key=rows.interactiontype
            func_mulhaz=switcher.get(key, lambda: "Invalid Hazard interaction, make sure all the combinations are spelled correctly in all lowercase")
            combination=(rows.haz1,rows.haz2)
        
            if key=='cascading':
                triggering=rows.haz1prob
                probability=rows.haz2prob

            elif key=='conditional':
                triggering=rows.haz1prob
                probability=rows.haz2prob
            else:
                triggering=None
                probability=None

            mulhaz_val=func_mulhaz(allrisk,combination,triggering,probability)
            colname=key+"_"+str(index+1)
            mulhaz_val=mulhaz_val.rename(columns={key: colname})
            if first:
                listed_risks=mulhaz_val
            else:
                listed_risks=listed_risks.merge(mulhaz_val, on='Unit_ID')
            
        multihazard_risk=mul_combineall(listed_risks)
        risk=mergeGeometry(multihazard_risk,ear,ear_id)
        if outputfileformat=="shp":
            saveshp(risk,outputfilename)
            return f"the  multi hazard risk calculation is complete and resutls are saved to {outputfilename}.shp"
        if outputfileformat=="csv":
            savecsv(risk,outputfilename)
            return f"the multi hazard risk calculation is complete and resutls are saved to {outputfilename}.shp"

def ComputeRiskMhAgg(riskcombofile,hazard_interaction):
    first=True

    switcher = {'independent': mul_independent, 'compounding': mul_compounding,'coupled': mul_coupled,
                'cascading': mul_cascading,'conditional': mul_conditional}
    riskcombinations=pd.read_csv(riskcombofile)
    combinations=riskcombinations.RiskCombinationId.unique()
    for riskcombinationid in combinations:
        subset_cominations=riskcombinations[riskcombinations.RiskCombinationId==riskcombinationid]
        
        outputfilename=subset_cominations.outputname[0]
        outputfileformat=subset_cominations.outputformat[0]
        ear=subset_cominations.ear[0]
        ear_id=subset_cominations.ear_key[0]
        adminunit=subset_cominations.adminunit[0]
        admin_key=subset_cominations.adminunit_key[0]
        ear_data=gpd.read_file(ear)
        admin_data=gpd.read_file(adminunit)
        ear_temp=gpd.overlay(ear_data, admin_data, how='intersection', make_valid=True, keep_geom_type=True)
        
        
        all_risk=computeAllRisk(subset_cominations)
        
        relevant_interactions=hazard_interaction[hazard_interaction.RiskCombinationId==riskcombinationid]
        
        for index, rows in relevant_interactions.iterrows():
            key=rows.interactiontype
            func_mulhaz=switcher.get(key, lambda: "Invalid Hazard interaction, make sure all the combinations are spelled correctly in all lowercase")
            combination=(rows.haz1,rows.haz2)
        
            if key=='cascading':
                triggering=rows.haz1prob
                probability=rows.haz2prob

            elif key=='conditional':
                triggering=rows.haz1prob
                probability=rows.haz2prob
            else:
                triggering=None
                probability=None

            mulhaz_val=func_mulhaz(allrisk,combination,triggering,probability)
            colname=key+"_"+str(index+1)
            mulhaz_val=mulhaz_val.rename(columns={key: colname})
            if first:
                listed_risks=mulhaz_val
            else:
                listed_risks=listed_risks.merge(mulhaz_val, on='Unit_ID')
            
        multihazard_risk=mul_combineall(listed_risks)
        multihazard_risk=pd.merge(left=multihazard_risk, right=ear_temp[[admin_key, ear_id]], how='left', left_on=['Unit_ID'], right_on=[ear_key],right_index=False)
        multihazard_risk=multihazard_risk.drop(columns='Unit_ID')
        multihazard_risk.rename(columns={admin_key: "Unit_ID"})
        multihazard_risk=multihazard_risk.groupby(['Unit_ID'],as_index=False).agg({'AAL':'sum'})
        risk=mergeGeometry(multihazard_risk,adminunit,admin_key)
        if outputfileformat=="shp":
            saveshp(risk,outputfilename)
            return f"the  multi hazard risk calculation is complete and resutls are saved to {outputfilename}.shp"
        if outputfileformat=="csv":
            savecsv(risk,outputfilename)
            return f"the multi hazard risk calculation is complete and resutls are saved to {outputfilename}.shp"

