from datetime import datetime
import subprocess
import pandas as pd
import os
import sys


#############################################################################
##   Set variables, load files
#############################################################################
BASE_YEAR = 2019
TARGET_YEARS = [2020,2035]
HORIZON_YEAR = 2046
ITER = 3

parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value', 'Notes']

WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
dataDir = os.path.join(WORKING_DIR, 'Data')
setupDir = os.path.join(WORKING_DIR, 'Setup')
dataPopSim_Dir = os.path.join(dataDir, 'PopSim')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'Data')
scenarioDir = os.path.join(WORKING_DIR, 'Setup', 'Data')

print('\r\n--- BUILD INDUCED DEMAND BATCH FILE ---')
print('Start time '+str(datetime.now()))
#print('\r\n--- YEAR ' + str(targetYear) + ' ---\r\n')


#############################################################################
##   Start writing batch file
#############################################################################
bat_lines = [
    '@ECHO OFF',
    'ECHO --%startTime%%Time%'
#    'SET WORKING_DIR=%~dp0',
#    'SET SCENARIO=%1',
#    'IF [%SCENARIO%] == [] (',
#    '	ECHO --Error: Please specify scenario name',
#    '	GOTO EOF',
#    ')',
#    'SET "SCENARIO_DIR=%WORKING_DIR%Scenarios\\%SCENARIO%"',
]


#############################################################################
##   Main loop
#############################################################################
currentYear = BASE_YEAR + ITER
TARGET_YEARS.append(HORIZON_YEAR)
TARGET_YEARS.sort()
nextTarget = TARGET_YEARS.pop(0)
override_str = " Y"
done = False


while not done:
    if currentYear >= nextTarget:
        currentYear = nextTarget
        if TARGET_YEARS:
            nextTarget = TARGET_YEARS.pop(0)
        else:
            nextTarget = 0

    bat_lines.extend([
        'ECHO ---- ' + str(currentYear) + ' ----',
        'ECHO --%startTime%%Time%: Creating growth controls...',
        'CALL python Scripts\\buildControls.py Setup\\parameters.csv ' + str(currentYear) + ' > Setup\\logs\\buildControls.log 2>&1',
        'ECHO --%startTime%%Time%: Running skims...',
        #PUT NAGENDRA'S CODE HERE
        'ECHO --%startTime%%Time%: Processing skims...',
        'CALL python scripts\\processSkims.py Setup\\parameters.csv ' + str(currentYear) + ' > Setup\\logs\\processSkims.log 2>&1',
        'ECHO --%startTime%%Time%: Calculating development scores...',
        'CALL python scripts\\calcDevScores.py Setup\\parameters.csv ' + str(currentYear) + override_str + ' > Setup\\logs\\calcDevScores.log 2>&1',
        'ECHO --%startTime%%Time%: Allocating new growth...',
        'CALL python scripts\\allocateGrowth.py Setup\\parameters.csv ' + str(currentYear) + ' > Setup\\logs\\allocateGrowth.log 2>&1'
    ])

    override_str = ""

    if nextTarget:
        currentYear += ITER
    else:
        done = True


#############################################################################
##   Wrap up and write batch file
#############################################################################
bat_lines.extend([
    'COPY Setup\\Data\\devtable.csv Setup\\Outputs\\devtable.csv',
    ':EOF',
    'ECHO --%startTime%%Time%: Batch complete'
])

f = open(os.path.join(WORKING_DIR, 'RunTemp.bat'), 'w')
f.write('\n'.join(bat_lines))
f.close()

#############################################################################
##   End of script
#############################################################################

print('\r\n--- Script ran successfully! ---\r\n')
print('End time '+str(datetime.now()))
