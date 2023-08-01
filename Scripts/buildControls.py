from datetime import datetime
import numpy as np
import pandas as pd
import os
import sys

#############################################################################
##   Load files
#############################################################################
VISION_YEAR = 2035

parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value', 'Notes']

WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
dataDir = os.path.join(WORKING_DIR, 'Data')
dataPopSim_Dir = os.path.join(dataDir, 'PopSim')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'Data')

baseYear = int(parameters[parameters.Key == 'baseYear']['Value'].item().strip(' '))
Cube_P = float(parameters[parameters.Key == 'Cube_P']['Value'].item().strip(' '))
adjPOP = float(parameters[parameters.Key == 'adjPOP']['Value'].item().strip(' '))
adjEMP = float(parameters[parameters.Key == 'adjEMP']['Value'].item().strip(' '))
adjVacRate = float(parameters[parameters.Key == 'adjVacRate']['Value'].item().strip(' '))
adjUrban = float(parameters[parameters.Key == 'adjUrban']['Value'].item().strip(' '))

try:
    targetYear = int(sys.argv[2])
except:
    targetYear = int(parameters[parameters.Key == 'targetYear']['Value'].item().strip(' '))

print('\r\n--- BUILD GROWTH CONTROLS ---')
print('Start time '+str(datetime.now()))
#print("\r\n--- YEAR " + str(targetYear) + " ---\r\n")

#############################################################################
##   Set growth targets
#############################################################################
# Analyze base data
Base_MAZ = pd.read_csv(os.path.join(dataDir, "Base_MAZ_2019.csv"))
hhsize_avg = Base_MAZ['HH_POP'].sum()/Base_MAZ['Base_HH'].sum()
vacrate_avg = (Base_MAZ['Base_HU'].sum()-Base_MAZ['Base_HH'].sum())/Base_MAZ['Base_HU'].sum()
vacrate_adj = np.clip(vacrate_avg+adjVacRate*(min(VISION_YEAR,targetYear)-baseYear)/(VISION_YEAR-baseYear),0.025,0.3)

HH_factor = Base_MAZ['Base_HU'].sum()*(1-vacrate_adj)/Base_MAZ['Base_HH'].sum()
#print("\r\nBase vacancy rate: " + str(vacrate_avg))
#print("Adjusted vacancy rate: " + str(vacrate_adj))
#print("HH factor for new vacrate: " + str(HH_factor))

Base_MAZ['Base_HH'] = Base_MAZ['Base_HH']*HH_factor
Base_MAZ['HH_POP'] = Base_MAZ['HH_POP']*HH_factor
Base_MAZ['Base_GQ'] = Base_MAZ['Base_DORM']+Base_MAZ['Base_MEDICAL']+Base_MAZ['Base_PRISON']+Base_MAZ['Base_MILITARY']
Base_MAZ['Base_SCHL'] = Base_MAZ['Base_ELEM']+Base_MAZ['Base_HS']+Base_MAZ['Base_COLLEGE']

Base_SOI = Base_MAZ.groupby('SOI', as_index = False).agg({'HH_POP':'sum','Base_HH':'sum','Base_HU':'sum','Base_EMP':'sum','Base_AGR':'sum','Base_GQ':'sum','Base_SCHL':'sum'})
Base_SOI['VacRate'] = (Base_SOI['Base_HU']-Base_SOI['Base_HH'])/Base_SOI['Base_HU']
#Base_SOI.rename(columns={'SOI':'AGENCY'}, inplace=True)

Base_SOI = Base_SOI.append({'SOI':'Fresno County','Base_HU':Base_SOI['Base_HU'].sum(),'Base_EMP':Base_SOI['Base_EMP'].sum(),'Base_AGR':Base_SOI['Base_AGR'].sum(),'Base_GQ':Base_SOI['Base_GQ'].sum(),'Base_SCHL':Base_SOI['Base_SCHL'].sum(),'Base_HH':Base_SOI['Base_HH'].sum(),'HH_POP':Base_SOI['HH_POP'].sum(),'HH_SIZE':hhsize_avg,'VacRate':vacrate_adj} , ignore_index=True)

#print(Base_SOI)
#print(hhsize_avg, vacrate_avg)

# Agency controls
Demographic_Forecast = pd.read_csv(os.path.join(dataDir, "Demographic_Forecast.csv"))
Forecast_Base = Demographic_Forecast.loc[Demographic_Forecast['YEAR']==baseYear].filter(items=['SOI','YEAR','POP_HH','POP_GRP','POP_SCHL','HH_TOT','EMP_TOT','EMP_EDU','EMP_FOO','EMP_GOV','EMP_IND','EMP_MED','EMP_OFC','EMP_OTH','EMP_RET','EMP_AGR'])
Forecast_Base.columns = ['SOI','BASE_YEAR','HHPOP_BASE','GQPOP_BASE','SCHL_BASE','HH_BASE','EMP_BASE','EDU_BASE','FOO_BASE','GOV_BASE','IND_BASE','MED_BASE','OFC_BASE','OTH_BASE','RET_BASE','AGR_BASE']
Forecast = Demographic_Forecast.loc[Demographic_Forecast['YEAR']==targetYear].filter(items=['SOI','YEAR','POP_HH','POP_GRP','POP_SCHL','POP_TOT','RACE_White','HH_TOT','EMP_TOT','EMP_EDU','EMP_FOO','EMP_GOV','EMP_IND','EMP_MED','EMP_OFC','EMP_OTH','EMP_RET','EMP_AGR'])
Forecast.columns = ['SOI','TARGET_YEAR','HHPOP_TARGET','GQPOP_TARGET','SCHL_TARGET','POP_TOT_TARGET','POP_White_TARGET','HH_TARGET','EMP_TARGET','EDU_TARGET','FOO_TARGET','GOV_TARGET','IND_TARGET','MED_TARGET','OFC_TARGET','OTH_TARGET','RET_TARGET','AGR_TARGET']

Forecast = Forecast.merge(Forecast_Base, how = 'left', on = 'SOI')
Forecast = Forecast.merge(Base_SOI, how = 'left', on = 'SOI')
Forecast['SOI_HH_Target'] = (1+adjPOP)*(Forecast['HH_TARGET']-Forecast['HH_BASE'])
Forecast['SOI_HU_Target'] = Forecast['SOI_HH_Target']/(1-Forecast['VacRate'])
Forecast['SOI_HHPOP_Target'] = (1+adjPOP)*(Forecast['HHPOP_TARGET']-Forecast['HHPOP_BASE'])
Forecast['SOI_GQ_Target'] = (1+adjPOP)*(Forecast['GQPOP_TARGET']-Forecast['GQPOP_BASE'])
Forecast['SOI_SCHL_Target'] = (1+adjPOP)*(Forecast['SCHL_TARGET']-Forecast['SCHL_BASE'])
Forecast['SCHL_Factor'] = (Forecast['SOI_SCHL_Target']+Forecast['Base_SCHL'])/Forecast['Base_SCHL']
Forecast['HH_SIZE'] = Forecast['SOI_HHPOP_Target']/Forecast['SOI_HH_Target']

Forecast['SOI_EDU_Target'] = (1+adjEMP)*(Forecast['EDU_TARGET']-Forecast['EDU_BASE'])
Forecast['SOI_FOO_Target'] = (1+adjEMP)*(Forecast['FOO_TARGET']-Forecast['FOO_BASE'])
Forecast['SOI_GOV_Target'] = (1+adjEMP)*(Forecast['GOV_TARGET']-Forecast['GOV_BASE'])
Forecast['SOI_IND_Target'] = (1+adjEMP)*(Forecast['IND_TARGET']-Forecast['IND_BASE'])
Forecast['SOI_MED_Target'] = (1+adjEMP)*(Forecast['MED_TARGET']-Forecast['MED_BASE'])
Forecast['SOI_OFC_Target'] = (1+adjEMP)*(Forecast['OFC_TARGET']-Forecast['OFC_BASE'])
Forecast['SOI_OTH_Target'] = (1+adjEMP)*(Forecast['OTH_TARGET']-Forecast['OTH_BASE'])
Forecast['SOI_RET_Target'] = (1+adjEMP)*(Forecast['RET_TARGET']-Forecast['RET_BASE'])
Forecast['SOI_AGR_Target'] = (1+adjEMP)*(Forecast['AGR_TARGET']-Forecast['AGR_BASE'])
Forecast['SOI_EMP_Target'] = (1+adjEMP)*(Forecast['EMP_TARGET']-Forecast['EMP_BASE']-Forecast['SOI_AGR_Target'])
Forecast.to_csv(os.path.join(outputDir,"controls_soi.csv"), index = False)

Forecast_dev = Forecast.filter(items=['SOI','SOI_HU_Target','SOI_EMP_Target'])
row_region = Forecast_dev.loc[Forecast_dev['SOI'] == 'Fresno County']
HU_region = row_region['SOI_HU_Target'].sum()
EMP_region = row_region['SOI_EMP_Target'].sum()

#print('\r\nRegional growth targets: '+str(int(round(HU_region,0)))+' HU, '+str(int(round(EMP_region)))+' EMP')
#print(Forecast_dev)

# TAZ controls
Cube_Growth = pd.read_csv(os.path.join(dataDir, "CubeGrowth_19_35.csv"))
Cube_Growth.rename(columns={'Cube_HU_TAZ':'TAZ_HU_Target'}, inplace=True)
Cube_Growth.rename(columns={'Cube_EMP_TAZ':'TAZ_EMP_Target'}, inplace=True)

Cube_COMMUNITY = pd.read_csv(os.path.join(dataDir, "Communities.csv"))
Cube_COMMUNITY = Cube_COMMUNITY.filter(items=['COMMUNITY','SOI_HU_P','SOI_EMP_P'])

#Cube_COMMUNITY = Cube_Growth.groupby('COMMUNITY', as_index = False).agg({'SOI':'first','TAZ_HU_Target':'sum','TAZ_EMP_Target':'sum'})
#Cube_COMMUNITY['SOI_HU_P'] = Cube_COMMUNITY['TAZ_HU_Target']/Cube_COMMUNITY['TAZ_HU_Target'].sum()
#Cube_COMMUNITY['SOI_EMP_P'] = Cube_COMMUNITY['TAZ_EMP_Target']/Cube_COMMUNITY['TAZ_EMP_Target'].sum()
#print(Cube_COMMUNITY)

Cube_SOI = Cube_Growth.groupby('SOI', as_index = False).agg({'TAZ_HU_Target':'sum','TAZ_EMP_Target':'sum'})
Cube_SOI = Cube_SOI.merge(Forecast_dev, how = 'left', on = 'SOI')
Cube_SOI['HU_adj'] = Cube_SOI['SOI_HU_Target']/Cube_SOI['TAZ_HU_Target']
Cube_SOI['EMP_adj'] = Cube_SOI['SOI_EMP_Target']/Cube_SOI['TAZ_EMP_Target']
Cube_SOI = Cube_SOI.filter(items=['SOI','HU_adj','EMP_adj'])

Cube_Growth = Cube_Growth.merge(Cube_SOI, how = 'left', on = 'SOI')
Cube_Growth['TAZ_HU_Target'] = Cube_P*Cube_Growth['TAZ_HU_Target']*Cube_Growth['HU_adj']
Cube_Growth['TAZ_EMP_Target'] = Cube_P*Cube_Growth['TAZ_EMP_Target']*Cube_Growth['EMP_adj']
Cube_Growth = Cube_Growth.merge(Cube_COMMUNITY, how = 'left', on = 'COMMUNITY')
Cube_Growth = Cube_Growth.filter(items=['TAZ','TAZ_HU_Target','TAZ_EMP_Target','SOI_HU_P','SOI_EMP_P'])
Cube_Growth.replace(np.nan, 0, inplace=True)

Cube_Growth.to_csv(os.path.join(outputDir,"controls_taz.csv"), index = False)

#############################################################################
##   End of script
#############################################################################

print('\r\n--- Script ran successfully! ---\r\n')
print('End time '+str(datetime.now()))
