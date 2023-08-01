from datetime import datetime
import pandas as pd
import os
import sys

#############################################################################
##   Load files
#############################################################################
parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value', 'Notes']

WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
dataDir = os.path.join(WORKING_DIR, 'Data')
dataPopSim_Dir = os.path.join(dataDir, 'PopSim')
dataABM_Dir = os.path.join(dataDir, 'ABM')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'Data')
popsimDir = os.path.join(WORKING_DIR, 'Setup', 'Outputs')

try:
    targetYear = int(sys.argv[2])
except:
    targetYear = int(parameters[parameters.Key == 'targetYear']['Value'].item().strip(' '))

print('\r\n--- GENERATE ABM INPUTS ---')
print('Start time '+str(datetime.now()))


#############################################################################
##   Generate maz_parks
#############################################################################
MAZ_New = pd.read_csv(os.path.join(outputDir, 'maz_new.csv'))
maz_parks = pd.read_csv(os.path.join(dataABM_Dir, 'maz_2019_parks.csv')).filter(items=['parcelid','xcoord_p','ycoord_p','sqft_p','taz_p','block_p','parkdy_p','parkhr_p','ppricdyp','pprichrp'])
maz_parks['MAZ'] = maz_parks['parcelid']
maz_parks = maz_parks.merge(MAZ_New, how = 'left', on = 'MAZ')
maz_parks.rename(columns={'HH':'hh_p','ELEM':'stugrd_p','HS':'stuhgh_p','COLLEGE':'stuuni_p','EMP_EDU':'empedu_p','EMP_FOO':'empfoo_p','EMP_GOV':'empgov_p','EMP_IND':'empind_p','EMP_MED':'empmed_p','EMP_OFC':'empofc_p','EMP_RET':'empret_p','EMP_OTH':'empsvc_p','EMP_AGR':'empoth_p','EMP':'emptot_p'}, inplace=True)
maz_parks = maz_parks.filter(items=['parcelid','xcoord_p','ycoord_p','sqft_p','taz_p','block_p','hh_p','stugrd_p','stuhgh_p','stuuni_p','empedu_p','empfoo_p','empgov_p','empind_p','empmed_p','empofc_p','empret_p','empsvc_p','empoth_p','emptot_p','parkdy_p','parkhr_p','ppricdyp','pprichrp'])
maz_parks = maz_parks[['parcelid','xcoord_p','ycoord_p','sqft_p','taz_p','block_p','hh_p','stugrd_p','stuhgh_p','stuuni_p','empedu_p','empfoo_p','empgov_p','empind_p','empmed_p','empofc_p','empret_p','empsvc_p','empoth_p','emptot_p','parkdy_p','parkhr_p','ppricdyp','pprichrp']]
maz_parks.to_csv(os.path.join(popsimDir,'maz_'+str(targetYear)+'_parks.csv'), index = False)
del maz_parks


#############################################################################
##   Generate se_detail
#############################################################################
TAZ_New = pd.read_csv(os.path.join(outputDir, 'taz_new.csv'))
se_detail = pd.read_csv(os.path.join(dataABM_Dir, 'FC19_Base_SE_Detail.csv')).filter(items=['; TAZ','COUNTY','CITY'])#,'sqft_p','taz_p','block_p','parkdy_p','parkhr_p','ppricdyp','pprichrp'])
se_detail['TAZ'] = se_detail['; TAZ']
se_detail = se_detail.merge(TAZ_New, how = 'left', on = 'TAZ')
se_detail.rename(columns={'HH':'TOTHH','POP':'TOTPOP','EMP':'TOTEMP','EMP_EDU':'EMPEDU','EMP_FOO':'EMPFOO','EMP_GOV':'EMPGOV','EMP_IND':'EMPIND','EMP_MED':'EMPMED','EMP_OFC':'EMPOFC','EMP_OTH':'EMPOTH','EMP_RET':'EMPRET','EMP_AGR':'EMPAGR'}, inplace=True)
#se_detail['TOTHH'] = se_detail['HH']
#se_detail['TOTPOP'] = se_detail['POP']
#se_detail['TOTEMP'] = se_detail['EMP']
#se_detail['EMP_EDU'] = se_detail['EMP_FOO']+se_detail['EMP_RET']
#se_detail['EMP_FOO'] = se_detail['EMP_AGR']
#se_detail['EMP_GOV'] = se_detail['EMP_OTH']/2
#se_detail['EMP_IND'] = se_detail['EMP_IND']/3
#se_detail['EMP_MED'] = se_detail['EMP_OTH']/2
#se_detail['EMP_OFC'] = 0
#se_detail['TRANSP'] = se_detail['EMP_IND']/3
#se_detail['WHLSALE'] = se_detail['EMP_IND']/3
#se_detail['FINANCE'] = se_detail['EMP_OFC']
#se_detail['EDUGOV'] = se_detail['EMP_EDU']+se_detail['EMP_GOV']+se_detail['EMP_MED']
se_detail = se_detail.filter(items=['; TAZ','COUNTY','CITY','TOTHH','TOTPOP','TOTEMP','EMPEDU','EMPFOO','EMPGOV','EMPIND','EMPMED','EMPOFC','EMPOTH','EMPRET','EMPAGR'])
se_detail.fillna(0, inplace = True)
se_detail.to_csv(os.path.join(popsimDir,'FC'+str(targetYear%100)+'_Base_SE_Detail.csv'), index = False)
del se_detail

#############################################################################
##   End of script
#############################################################################

print('\r\n--- Script ran successfully! ---\r\n')
print('End time '+str(datetime.now()))
