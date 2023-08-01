from datetime import datetime
import pandas as pd
import os
import sys

#############################################################################
##   Load files
#############################################################################
VISION_YEAR = 2035
NO_DEV = 9999

parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value', 'Notes']

WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
dataDir = os.path.join(WORKING_DIR, 'Data')
dataPopSim_Dir = os.path.join(dataDir, 'PopSim')
dataABM_Dir = os.path.join(dataDir, 'ABM')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'Data')
popsimDir = os.path.join(WORKING_DIR, 'Setup', 'Outputs')

baseYear = int(parameters[parameters.Key == 'baseYear']['Value'].item().strip(' '))
Cube_P = float(parameters[parameters.Key == 'Cube_P']['Value'].item().strip(' '))

try:
    targetYear = int(sys.argv[2])
except:
    targetYear = int(parameters[parameters.Key == 'targetYear']['Value'].item().strip(' '))

#print('\r\n--- LAND USE ALLOCATION ---')
#print('Start time '+str(datetime.now()))
#print("\r\n--- YEAR " + str(targetYear) + " ---\r\n")

Base_MAZ = pd.read_csv(os.path.join(dataDir, "Base_MAZ_2019.csv"))
maz_new = pd.read_csv(os.path.join(outputDir, "maz_new.csv"))
#Forecast = pd.read_csv(os.path.join(outputDir, "controls_soi.csv"))


#############################################################################
##   Calculate performance measures
#############################################################################
print('\r\nCalculating performance measures...')

maz_new = maz_new.merge(Base_MAZ, how = 'left', on = 'MAZ')
maz_new['HU_NET'] = maz_new['HU']-maz_new['Base_HU']
maz_new['EMP_NET'] = maz_new['EMP']-maz_new['Base_EMP']

HU_Tot = maz_new['HU_NET'].sum()
EMP_Tot = maz_new['EMP_NET'].sum()

pm_geo_area = maz_new.groupby('GEO_AREA', as_index = False).agg({'HU_NET':'sum','EMP_NET':'sum'})

DevTable = pd.read_csv(os.path.join(outputDir, "devtable.csv")).filter(items=['parcelid','DEV'])
DevTable = DevTable[DevTable['DEV']<=targetYear]
DevTable = DevTable.merge(pd.read_csv(os.path.join(outputDir, "parcels.csv")), how = 'left', on = 'parcelid')

HU_Tot_Dev = DevTable['HU_NET'].sum()
EMP_Tot_Dev = DevTable['EMP_NET'].sum()

#DevTable = DevTable[DevTable['DEV']==1]
#DevTable.rename(columns={'SOI':'SOI'}, inplace=True)
#DevTable.to_csv(os.path.join(outputDir,"test.csv"), index = False)

ACRES_Tot = (DevTable['Vacant']*DevTable['ACRES']*DevTable['PLANNED']).sum()
ACRES_Res = DevTable[DevTable['HU_NET'] > 0]['ACRES'].sum()
#ACRES_Res = DevTable.loc[DevTable['HU_NET'] > 0,'ACRES'].sum

# TOD, DT, MF
if HU_Tot > 0:
    pm_tod_hu = (DevTable['HU_NET']*DevTable['TOD']).sum()/HU_Tot
    pm_dt_hu = (DevTable['HU_NET']*DevTable['DT']).sum()/HU_Tot
    pm_mf = (DevTable['HU_NET']*(DevTable['HU_MF_P'])).sum()/HU_Tot
else:
    pm_tod_hu = 0
    pm_dt_hu = 0
    pm_mf = 0

if EMP_Tot > 0:
    pm_tod_emp = (DevTable['EMP_NET']*DevTable['TOD']).sum()/EMP_Tot
    pm_dt_emp = (DevTable['EMP_NET']*DevTable['DT']).sum()/EMP_Tot
else:
    pm_tod_emp = 0
    pm_dt_emp = 0

# MU, redev
if HU_Tot+EMP_Tot > 0:
    pm_mu = (DevTable['MU']*(DevTable['HU_NET']+DevTable['EMP_NET'])).sum()/(HU_Tot+EMP_Tot)
    pm_redev = (DevTable['Developed']*(DevTable['HU_NET']+DevTable['EMP_NET'])).sum()/(HU_Tot+EMP_Tot)
    pm_geo_area['PERC'] = 100*(pm_geo_area['HU_NET']+pm_geo_area['EMP_NET'])/(HU_Tot+EMP_Tot)
else:
    pm_mu = 0
    pm_redev = 0
    pm_geo_area['PERC'] = 0

# RES DEN
if ACRES_Res > 0:
    pm_res_den = HU_Tot_Dev/ACRES_Res
else:
    pm_res_den = 0

# Farmland acres
pm_impfarm = (DevTable['ACRES']*DevTable['Vacant']*(1-DevTable['Incorporated'])*(DevTable['FMMP_P']+DevTable['FMMP_S']+DevTable['FMMP_U'])).sum()
pm_farm = (DevTable['ACRES']*DevTable['Vacant']*(DevTable['FMMP_P']+DevTable['FMMP_S']+DevTable['FMMP_U'])).sum()

# Fresno Infill %
DevTable_Fresno = DevTable[DevTable['SOI']=='Fresno']
HU_Tot_Fresno = DevTable_Fresno['HU_NET'].sum()
if HU_Tot_Fresno > 0:
    pm_infill = DevTable_Fresno[DevTable_Fresno['Infill']==0]['HU_NET'].sum()/HU_Tot_Fresno
else:
    pm_infill = 0

print('                           2018')
#print('TOTAL     ' + str(HU_Tot) + ', ' + str(EMP_Tot))
print('ACRES     ' + '{:.1f}'.format(ACRES_Tot))
print('TOD       ' + '{:.1%}'.format(pm_tod_hu) + ', ' + '{:.1%}'.format(pm_tod_emp) + '     24% 36%')
print('DT        ' + '{:.1%}'.format(pm_dt_hu) + ', ' + '{:.1%}'.format(pm_dt_emp))
print('MU        ' + '{:.1%}'.format(pm_mu))
print('RES DEN   ' + '{:.2f}'.format(pm_res_den) + '             7.4')
print('MF        ' + '{:.1%}'.format(pm_mf) + '            39%')
print('FARM AC   ' + '{:.1f}'.format(pm_farm))
print('FARM IMP  ' + '{:.1f}'.format(pm_impfarm) + '             38.2')
print('Redev%    ' + '{:.1%}'.format(pm_redev))
print('Infill    ' + '{:.1%}'.format(pm_infill) + '            50%')

print('\r\nDevelopment by Geographic Area')
print(pm_geo_area)

#DevTable = DevTable.merge(Forecast.filter(items=['AGENCY','HH_SIZE','VacRate','SCHL_Factor']), how = 'left', on = 'AGENCY')
#DevTable = DevTable[DevTable['DEV']<=targetYear].filter(items=['parcelid'])
#DevTable.to_csv(os.path.join(outputDir,"DevTable",targetYear,".csv"), index = False)


#############################################################################
##   End of script
#############################################################################

print('\r\n--- Script ran successfully! ---\r\n')
print('End time '+str(datetime.now()))
