#%%
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 18:06:04 2018

@author: bcubrich


SUMMARY
--------------------------------
This code takes audit files for gaseous data and collects the audit and 
indicated measurements, then outputs a pipe delimited text file called
 'QA_output.txt' that can be directly uploaded to AQS.

INDEX
-------------------------------
1. Imports
2. Global Vars
3. Functions
    -functions to get filenames and directories    

4. Analysis Main

3. Write to file 
    -Write the above df to a file

"""

'''---------------------------------------------------------------------------
                                1. Imports
----------------------------------------------------------------------------'''

#need most of these imports for the code to work

import pandas as pd
import numpy as np         
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import os
from tkinter import *
import pyodbc 
import datetime as dt  
import time
import sys
from bs4 import BeautifulSoup
#from tkinter.filedialog import asksavefilename
from tkinter.filedialog import askdirectory


'''---------------------------------------------------------------------------
                                2. Global Vars
----------------------------------------------------------------------------'''

global you
global counter
global auto_run
counter=0

'''---------------------------------------------------------------------------
                                3. Functions
----------------------------------------------------------------------------'''

def get_db_dat(start_date,end_date):
    #get data from MS SQL server, AVdata table.
    #This is where we keep the AV data at DEQ,
    #and I use this function to get a df of data off of the 
    #server.
    
    
    driver_names = pyodbc.drivers()           #get drivers that will work in the current env.
    db='AVData.Reporting.CalibrationDataFull' #this is where the data comes from
    
    #Need to login to the server with these
    username='gis'
    password='Axd!35jkl'
    
    #create a connection to the server
    cnxn = pyodbc.connect(r'DRIVER={'+driver_names[0]+'};'
                            r'SERVER=168.178.43.244;'
                            r'DATABASE=AVData;'
                            r'UID='+username+';PWD='+password+';'
                        r'timeout=10')
    
    #This is a SQL query that is saved inside of this program as a 
    #giant block of text. This only thing that needs to be updated are the 
    #dates over which the query will be performed. You can see that this is the only
    #black text in the giant block below.
    query="""SELECT 'QA' AS TransactionType, 'I' AS ActionIndicator, '1-Point QC' AS AssessmentType, 
    1113 As PerformingAgencyCode,
    AqsStateCode AS State, 
    AqsCountyTribalCode AS County,
    AqsSiteCode AS Site,
    AqsParameterCode AS ParameterCode,
    AqsParameterOccuranceCode AS POC,
    StartDate AS StartDate,
    CAST(CONVERT(varchar,StartDate,112) AS INT) AS AssessmentDate,
    --FORMAT(StartDate, 'yyyyMMdd', 'en-US'),
    --EndDate,
    '1' As AssessmentNumber,
    ParameterAqsMethodCode AS MethodCode,
    AqsUnitCode AS ReportingUnit,
    Value,
    ExpectedValue
    , 
    CASE WHEN (Value < (ExpectedValue*0.93)) THEN  '7%Low' ELSE '' END AS 'Low7Test',
    CASE WHEN (Value < (ExpectedValue*0.9)) THEN  '10%Low' ELSE '' END AS 'Low10Test',
    CASE WHEN (Value < (ExpectedValue*0.85)) THEN  '15%Low' ELSE '' END AS 'Low15Test',
    CASE WHEN (Value > (ExpectedValue*1.07)) THEN  '7%High' ELSE '' END AS 'High7Test', 
    CASE WHEN (Value > (ExpectedValue*1.1)) THEN  '10%High' ELSE '' END AS 'High10Test', 
    CASE WHEN (Value > (ExpectedValue*1.15)) THEN  '15%High' ELSE '' END AS 'High15Test', 
    PhaseName
    FROM AVData.Reporting.CalibrationDataFull
    
    
    WHERE SiteAbbreviation IN ('BV','ED','ES','MA','BR','HV','O2','SM','LN','NR','RS','SF','V4','H3','HW','P2','AI','BI','SA','CV','EN','HC','RP')
    AND StartDate > '"""+start_date+"""'
    AND StartDate < '"""+end_date+"""'
    
    AND ParameterEnabled = 1
    AND PhaseEnabled = 1
    AND AqsParameterCode != 88313
    --AND ExcludeFromReporting = 0
    --AND NOT (SiteAbbreviation ='RS' AND ParameterName = 'O3')
    --AND ParameterAqsMethodCode != '199'
    AND (
      PhaseName Like '%[Pp][Rr][Ee]%' 
      OR ((PhaseName Like '%[Ss][Pp][Aa][Nn]%' OR PhaseName Like '[Ss][Pp][Aa][Nn]%' OR PhaseName Like '[Nn][Oo][_][Ss][Pp][Aa][Nn]%') 
      OR PhaseName Like '%[Zz][Ee][Rr][Oo]%'
      AND AqsParameterCode != 42603
      )
    )
    ORDER BY AqsStateCode, 
    AqsCountyTribalCode,
    AqsSiteCode,
    AqsParameterCode,
    StartDate"""
    
    #use pandas to execute the query and use the connection to the database we 
    #created, storing the results in the DataFrame 'df'
    df = pd.read_sql_query(query, cnxn)
    
    #some of the df needs to be as strings for the rest of the code to work.
    #This is largely because of leading zeros being dropped when the location
    #codes are converted to numbers, but also due to some logic later on.
    converters={'PerformingAgencyCode':str,'State':str,
            'County':str, 'Site':str, 'ParameterCode':str, 
            'POC':str, 'MethodCode':str, 'ReportingUnit':str,
            'ParameterCode':str, 'AssessmentDate':str}
    
    #apply the converters
    df=df.astype(converters)
    return df #Get back the df you want when this is called.


def Get_PZS_dat():
    #This function takes most of the runtime when pressing the button 'run'. 
    #This function works by looking at all of the PREC, ZERO, SPANs from the
    #last year prior to the end_date of the query. It drops this down to a 
    #list of unique PZSs. Of course this list probably has too many entries,
    #including those where the station or instrument has been discontinued, but 
    #I tried to be inclusive as possible.
    
    #same converters as always
    converters={'PerformingAgencyCode':str,'State':str,
                'County':str, 'Site':str, 'ParameterCode':str, 
                'POC':str, 'MethodCode':str, 'ReportingUnit':str,
                'ParameterCode':str, 'AssessmentDate':str}
    
    #Want to get data from the database up until now
    end_date=dt.datetime.now()
    
    #go back a year from the start date.
    start_date=str(end_date-pd.Timedelta('365 days'))[:-3]
    end_date=str(dt.datetime.now())[:-3]  #drop extra digits
    df=get_db_dat(start_date,end_date)    #call get_db_dat with start and end date
    df=df.astype(converters)              #apply converters
    
    #convert the date from assesment date format to 'normal datetime'
    df['date_normal']=df['AssessmentDate'].str[4:6]+'-'+df['AssessmentDate'].str[-2:]+'-'+df['AssessmentDate'].str[:4]
    #convert that to a datetime
    df['datetime']=pd.to_datetime(df['date_normal'])
    #create a column with the station name
    df['Station']=df['State'] + df['County'] + df['Site']
    #make the phase name PRES, PREC, PREZ all the same, because at some sites
    #it will get changed part way through, but we want it to be the same
    df['PhaseName']=df['PhaseName'].str.replace('PRES','PREC').str.replace('PREZ','PREC')
    df=df.fillna(value='0')   #convert nans to zeros
    #create a paramter that can be used ot drop duplicates
    df['DropDup']=df['Station']+df['PhaseName']+ df['ParameterCode']
    #drop duplicates down to each phase we want at each station
    pzs_df=df.drop_duplicates(subset=['DropDup']).copy()
    
    #create three separate dfs, on for each of PREC, ZERO, SPAN
    prec_df=pzs_df[pzs_df['PhaseName'].str.contains('PR')].copy()
    span_df=pzs_df[pzs_df['PhaseName'].str.contains('SPAN')].copy()
    zero_df=pzs_df[pzs_df['PhaseName'].str.contains('ZERO')].copy()
    #dont know what the diff zero should be, so I drop it.
    zero_df=zero_df[zero_df['ParameterCode']!='42612']

    return (prec_df,span_df,zero_df)       #return the three the dfs 


def out_dir():        
    #get the directory where we want to save some files at some point.
    filename = askdirectory(title = "Select Save File Path")      # Open single file
    return filename


'''--------------------------------------------------------------------------
                             4. Analysis Main
                         
This section focuses on the pandas df 'output_df'. I use this df to store up
all the info needed for an AQS upload that can be easily saved to a pipe 
delimited csv, and to create a df we can explore for any pzs fails, etc.
----------------------------------------------------------------------------'''



def pzs_main():
    print('Started')
    global you
    global report_out_path

    '''--------------------------------------------------------------------------
                                 4a. Setup needed information
    ----------------------------------------------------------------------------'''                            


    
    converters={'PerformingAgencyCode':str,'State':str,        #convert some of df to string
                'County':str, 'Site':str, 'ParamterCode':str, 
                'POC':str, 'MethodCode':str, 'ReportingUnit':str,
                'ParameterCode':str, 'AssessmentDate':str}
    prec_dict={'42101':10, '44201':7, '42401':10,'42601':15,          #what s allowable prec %disc.
               '42602':15,'42600':15,'42603':15, '42612':15}
    span_dict={'42101':10, '44201':7, '42401':10,'42601':10,          #what s allowable span %disc.
               '42602':10, '42600':10, '42603':10, '42612':10}
    param_dict={'42101':'CO', '44201':'O3', '42401':'SO2','42601':'NO',   #convert from parameter code to analyte
               '42602':'NO2', '42600':'NOy', '42603':'NOx', '42612':'NOy-NO Diff'}
    zero_dict_24={'42101':0.41, '44201':0.0031, '42401':0.0031,'42601':0.0031, #allowable zeros values (ppm)
               '42602':0.0031,'42600':0.0031,'42603':0.0031, '42612':0.0031}
    zero_dict={'42101':0.61, '44201':0.0051, '42401':0.0051,'42601':0.0051,    #14 day allowable
               '42602':0.0051,'42600':0.0051,'42603':0.0051, '42612':0.0051}
    
    prec_warn_dict={'42101':0.7, '44201':0.7, '42401':0.7,'42601':0.667,   #Fraction of allowable considered a warning
               '42602':0.667,'42600':0.667,'42603':0.667, '42612':0.667}
    span_warn_dict={'42101':0.7, '44201':0.7, '42401':0.7,'42601':0.7,
               '42602':0.7, '42600':0.7, '42603':0.7, '42612':0.7}
    zero_warn_dict={'42101':0.7, '44201':0.7, '42401':0.7,'42601':0.7,
               '42602':0.7, '42600':0.7, '42603':0.7, '42612':0.7}    
    
    #Station run's by site symbol instead of station code
    run_dict={'BV':'Kati', 'ED':'Kati','ES':'Kati','MA':'Kati',
              'BR':'John','HV':'John','O2':'John', 'SM':'John',
              'LN':'Shauna', 'NR':'Shauna','RS':'Shauna', 'SF':'Shauna', 'V4':'Shauna',
              'H3':'Luke','HW':'Luke','P2':'Luke','AI':'Luke','BI':'Luke','SA':'Luke',
              'CV':'Thad','EN':'Thad','HC':'Thad','RP':'Thad'}
    
    #Use this to map the operator to the station. Will need to be updated yearly
    station_run_dict={'490110004':'Kati', '490450004':'Kati','490170006':'Kati','490351007':'Kati',
          '490030003':'John','490571003':'John','490570002':'John', '490050007':'John',
          '490494001':'Shauna', '490354002':'Shauna','490130002':'Shauna', '490495010':'Shauna', '490471004':'Shauna',
          '490353013':'Luke','490353006':'Luke','490071003':'Luke','490116001':'Luke','490456001':'Luke','490353005':'Luke',
          '490352005':'Thad','490210005':'Thad','490530007':'Thad','490353010':'Thad'}

    #I use the following to get information about the site.
    site_text="""index,SITE NAME,Site Symbol,State Code,County Code,Site Code,Parameter,Analyt,POC,Method,Unit
    0,Brigham City,BR,49,003,0003,44201,(O3),1,087,007
    1,Smithfield,SM,49,005,0007,44201,(O3),1,087,007
    2,Price #2,P2,49,007,1003,44201,(O3),1,087,007
    3,Bountiful #2,BV,49,011,0004,44201,(O3),1,087,007
    4,Roosevelt,RS,49,013,0002,,,0,000,000
    5,Escalante,ES,49,017,0006,44201,(O3),1,087,007
    6,Enoch,EN,49,021,0005,44201,(O3),1,087,007
    7,Copperview,CV,49,035,2005,42101,(CO),1,554,007
    8,Hawthorne,HW,49,035,3006,44201,(O3),1,087,007
    9,Rose Park,RP,49,035,3010,42101,(CO),1,054,007
    10,Herriman,H3,49,035,3013,44201,(O3),1,087,007
    11,Erda,ED,49,045,0004,44201,(O3),1,087,007
    12,Vernal,V4,49,047,1004,44201,(O3),1,047,007
    13,Lindon,LN,49,049,4001,42101,(CO),2,593,007
    14,Spanish Fork,SF,49,049,5010,44201,(O3),1,087,007
    15,Ogden,O2,49,057,0002,42101,(CO),1,054,007
    16,Harrisville,HV,49,057,1003,44201,(O3),1,087,007
    17,Hurricane,HC,49,053,0007,44201,(O3),1,087,007
    18,Antelope Island,AI,49,011,6001,61101,(WS),1,050,012
    19,Saltair,SA,49,035,3005,61101,(WS),1,050,012
    20,Near Road,NR,49,035,4002,44201,(O3),1,087,007
    21,Magna,MA,49,035,1007,44201,(O3),1,087,007
    22,Badger Island,BI,49,045,6001,44201,(O3),1,087,007"""
    
    #sites_df is information about the site that we currently have. If they aren't
    #in this df then I can't print the name and sybmbol of the site late.
    #The other information is just extra because I used this in another script
    sites_df=pd.DataFrame()
    for line in site_text.split('\n'):              #Split site_text into lines and create a df out of them
        temp_df=pd.DataFrame(line.split(',')).T
        sites_df=sites_df.append(temp_df)           #append each line in site_text to sites_df
        
    sites_df=sites_df.set_index(0)
    sites_df.columns=sites_df.loc['index',:]
    sites_df=sites_df.drop('index', axis='index')
    
    #need to check if the script was triggered by a human or automaticallty (at 5:30 am)
    if auto_run==0:    #human ran
        #if it is human ran then we will assume that we want to look a month on either
        #side of thier query for long lastin PZS gaps. In the end the script
        #will look at PZS failures in the user identified date range, but 
        #will look at the date range +/-30 days for PZS gaps.
        av_start=str(pd.to_datetime(av_date1)-pd.Timedelta('30 days'))+'.000'
        av_end=str(pd.to_datetime(av_date2)+pd.Timedelta('30 days'))+'.000'
    elif auto_run==1:  #autoran
        #if autoran we want to look for >= two week times gaps a month before
        #the query started. The script already makes av_date1 two weeks before
        #todays date. We will go back 17 days to cover a '31 day month'
        #in the end, the script will look at two weeks of PZS fails and 
        #a month of two week gaps when autoran
        av_start=str(av_date1-pd.Timedelta('17 days'))[:-3]
        av_end=str(av_date2)[:-3]
    
    
    output_df=get_db_dat(av_start,av_end) #query database for PZS data
    
    #same as above. Convert dates to 'normal' american style, get a datetime
    output_df['date_normal']=output_df['AssessmentDate'].str[4:6]+'-'+output_df['AssessmentDate'].str[-2:]+'-'+output_df['AssessmentDate'].str[:4]
    output_df['datetime']=pd.to_datetime(output_df['date_normal'])
    #get a unique station id for each site.
    output_df['Station']=output_df['State'] + output_df['County'] + output_df['Site']
    
    #Do you want to organize the output by run?
    if run_org==1: #yes, organize by run
        #going to need to use the current 'station_run_dict' to see what runs
        #belong to whom. We can use this later to sort the df, and print who's
        #run is who's in the email.
        output_df['Run']=output_df['Station'].map(station_run_dict)
    else: #don't organize by run? We'll just organize by station ID then.
        output_df['Run']=output_df['Station']
    
    #doing the replacement again so that any spell of Precision will now be PREC
    output_df['PhaseName']=output_df['PhaseName'].str.replace('PRES','PREC').str.replace('PREZ','PREC')
    
    
    
    '''----------------------------------------------------------------------------
                            4b.  Check for failures and Add to df
    ---------------------------------------------------------------------------'''
        
    output_df=output_df.fillna(value='0')
    
    #max_pzs means the absolute value of the %disc between obs and exp
    #that the EPA will allow for us to consider the data valid. Need to use
    #dictionaries to apply this. That way later we can do a simple pandas+
    #np.where() statement to see if the current PZS passed it's allowed value. 
    #Of course in the case of zeros you need to look at the ABS value allowed,
    #not a % disc.The next 10 line for loop + if statements handles this.
    max_pzs=[]    #list to store the max allowed for each line in df
    for param_code, ps in zip(output_df['ParameterCode'],output_df['PhaseName']):
        if 'prec' in ps.lower() or 'pres' in ps.lower() or 'prez' in ps.lower():
            max_pzs.append(prec_dict.get(param_code))
        elif 'span' in ps.lower():
            max_pzs.append(span_dict.get(param_code))
        elif 'zero' in ps.lower():
            max_pzs.append(zero_dict_24.get(param_code))
        else: 
            max_pzs.append('error!!!')
    
    #append the max allowable list created above to the df
    output_df['MaxAllowed']=max_pzs
    #simply calculate the percent disc.
    output_df['PZS_diff']=(output_df['Value']-output_df['ExpectedValue'])/output_df['ExpectedValue']*100
    #also calculate it by rounding. at the 1's place. Bo may choose to argue that
    #This is the prefered value.
    output_df['PZS_diff_round']=np.round(output_df['PZS_diff'], decimals=0)
    
    #float that biz so we can do math. Mathematical!!
    output_df['MaxAllowed']=output_df['MaxAllowed'].apply(float)
    
    #some values are in ppb. This causes issues with zero checking. Need 
    #to check the "UNIT" code to see if it in ppb or ppm.
    output_df['Value']=np.where((output_df['ReportingUnit'].str.contains('008'))&
             (output_df['PhaseName'].str.contains('ZERO')),
                output_df['Value']/1000,output_df['Value'])
    
    
    #now we need to check if the PZS is passed or not. This should be doable
    #in a doubly nested np.where() statement, but I couldn't get it to work. 
    #this messy looking set of if's in a for loop does the same job.
    
    pzs_check=[]   #list to store pass/fails for each line in output_df
    #gonna loop each line in output_df, but just need to look at a few params
    #A flossier way to do this might be by using df.iterrows.
    #sort of annoying cuz the whole point of creating 'output_df['MaxAllowed']'
    #was to do this with np.where, which is what I used to do, but now we need
    #to recursilvely check for failures, then for warnings, then call it a pass.
    for phase, value, diff, allowed, param in zip(output_df['PhaseName'], 
                                           output_df['Value'],
                                           output_df['PZS_diff'],
                                           output_df['MaxAllowed'],
                                           output_df['ParameterCode']):
        #again, if we could do np.where this would be easy, but now we need to 
        #check what kind of PZS each entry is, and use dictionaries to check for
        #passes and fails.
        if 'prec' in phase.lower(): 
            if np.abs(diff)>=allowed: 
                pzs_check.append('FAIL!!!')
            elif np.abs(diff)>=allowed*prec_warn_dict.get(param):   #multiply by fraction to get correct warning levels
                pzs_check.append('WARNING')
            else:
                pzs_check.append('PASS')
        elif 'span' in phase.lower():
            if np.abs(diff)>=allowed: 
                pzs_check.append('FAIL!!!')
            elif np.abs(diff)>=allowed*span_warn_dict.get(param):   #multiply by fraction to get correct warning levels
                pzs_check.append('WARNING')
            else:
                pzs_check.append('PASS')
        elif 'zero' in phase.lower():
            if np.abs(value)>=allowed: 
                pzs_check.append('FAIL!!!')
            elif np.abs(value)>=allowed*zero_warn_dict.get(param): 
                pzs_check.append('WARNING')
            else:
                pzs_check.append('PASS')
        else:
            pzs_check.append('No Match')
    
    #append pzs_check to the df
    output_df['PZS_Check']= pzs_check    
    #double check checks if we could pass the pzs of we used rounding instead of 
    #the raw value.
    output_df['PZS_DoubleCheck']=np.where((np.abs(output_df['PZS_diff_round'])>output_df['MaxAllowed'])&\
             (~output_df['PhaseName'].str.contains('ZERO')),'FAIL!!!','PASS')
    
    output_df['PZS_DoubleCheck']=np.where((np.abs(output_df['Value'])>output_df['MaxAllowed'])&\
             (output_df['PhaseName'].str.contains('ZERO')),'FAIL!!!',output_df['PZS_DoubleCheck'])
    
    
    #pzs_fail is a list of failed PZSs, so we can report them later.
    pzs_fail_df=output_df[output_df['PZS_Check']=='FAIL!!!'].copy()
    
    #next split of the failures into each kind. This will be useful when 
    #we go to write the automated email later.
    prec_fail_df=pzs_fail_df[pzs_fail_df['PhaseName'].str.contains('PR')].copy()
    span_fail_df=pzs_fail_df[pzs_fail_df['PhaseName'].str.contains('SPAN')].copy()
#    span_fail_df=span_fail_df[span_fail_df['ParameterCode']!='42612'].copy()
    zero_fail_df=pzs_fail_df[pzs_fail_df['PhaseName'].str.contains('ZERO')].copy()
    zero_fail_df=zero_fail_df[zero_fail_df['ParameterCode']!='42612']
    
    
    #we also need a list of all the warning by PZS for the same reason.
    #Also, I drop NO-NOy diff zeros cuz that value is calculated.
    pzs_warn_df=output_df[output_df['PZS_Check']=='WARNING'].copy()
    prec_warn_df=pzs_warn_df[pzs_warn_df['PhaseName'].str.contains('PR')].copy()
    span_warn_df=pzs_warn_df[pzs_warn_df['PhaseName'].str.contains('SPAN')].copy()
    zero_warn_df=pzs_warn_df[pzs_warn_df['PhaseName'].str.contains('ZERO')].copy()
    zero_warn_df=zero_warn_df[zero_warn_df['ParameterCode']!='42612']
    
    
    
    
    '''-------------------------------------------------------------------------
                       4c.  Any Two Week Gap in Last month check
    ---------------------------------------------------------------------------'''
    
    #pass_df contains all the passing PZSs
    pass_df=output_df[output_df['PZS_Check']!='FAIL!!!'].sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate']).copy()
    #per BO's request I also have to keep track of cases where rounding saves the day
    pass_round_df=output_df[output_df['PZS_DoubleCheck']!='FAIL!!!'].copy()
    
    #this text variable is how we will write the email at the end
    two_week_gaps_text='<br><b><u>TWO WEEK GAPS IN PAST MONTH</u></b><br><br>'
    
    #technically as long as we get a PZS anytime on the 14th day it count's 
    #so a two week gap is really 15 days.
    gap = pd.Timedelta('15 days')
    
    #I want to know how much time we lose due to PZS fails, and if we cna save time by rounding
    gap_count=0
    save_count=0
    save_time=0
    lost_time=0
    
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    #for loop is going ot look at each station one at a time. Then, within that 
    #it needs to look at each "phasename', which is eahc PZS for each parameter
    for site in pass_df['Station'].unique():
        PZS_temp=pass_df[(pass_df['Station']==site)].copy()
        site_name=sites_df[(sites_df['County Code']==PZS_temp['County'].values[0])&(sites_df['Site Code']==site[-4:])]
        site_name='{} ({}, '.format(site_name['SITE NAME'].values[0].strip(),site_name['Site Symbol'].values[0])
        
        #this is the loop for each phasename
        for param in pass_df['PhaseName'].unique():
            #sort the dates within the passing PZSs 
            PZS_date=pass_df[(pass_df['PhaseName']==param) & (pass_df['Station']==site)].StartDate.sort_values().copy()
            
            #loop through each date in the sorted list and see if the timedelta
            #is greater than 15.
            for date1, date2 in zip(PZS_date, PZS_date[1:]):
                date1=date1
                date2=date2
                delta=pd.to_datetime(date2)-pd.to_datetime(date1)
                if delta>gap:
                    
                    #don't need these print statements for the exe version
                    
                    gap_count+=1
                    run=station_run_dict.get(site)
                    if run=='Kati':
                        kati_counter+=1
                    if kati_counter==1:
                            two_week_gaps_text+=("   <br>Kati's Run<br>")
                    
                    if run=='John':
                        john_counter+=1
                        if john_counter==1:
                            two_week_gaps_text+=("   John's Run<br>")
                    
                    if run=='Shauna':
                        shauna_counter+=1
                        if shauna_counter==1:
                            two_week_gaps_text+=("   <br>Shauna's Run<br>")
            
                    if run=='Luke':
                        luke_counter+=1
                        if luke_counter==1:
                            two_week_gaps_text+=("   <br>Luke's Run<br>")
            
                    if run=='Thad':
                        thad_counter+=1
                        if thad_counter==1:
                            two_week_gaps_text+=("   <br>Thad's Run<br>")
                    
                    two_week_gaps_text+=r'   At {} {}) the parameter {} ({}) has a gap '\
                         r'of {:.2f} days between {} and {}. Null '\
                         r'hourly data between these dates.<br>'\
                         .format(site_name,site, param, param_dict.get(param),(delta.days + delta.seconds/86400), date1, date2)
                    round_test_df=pass_round_df[(pass_round_df['StartDate']>=pd.to_datetime(date1))&
                                          (pass_round_df['StartDate']<=pd.to_datetime(date2))&
                                          (pass_round_df['Station']==site)&
                                          (pass_round_df['PhaseName']==param)]
                    
                    exact_test_df=pass_df[(pass_df['StartDate']>=pd.to_datetime(date1))&
                                          (pass_df['StartDate']<=pd.to_datetime(date2))&
                                          (pass_df['Station']==site)&
                                          (pass_df['PhaseName']==param)]
                    
                    lost_time+=delta.days + delta.seconds/86400
#                    if len(round_test_df)==len(exact_test_df): print('\n')
                    
                    losses=0
                    if len(round_test_df)>len(exact_test_df):
                        save_time+=delta.days + delta.seconds/86400
                        PZS_date=pass_round_df[(pass_round_df['PhaseName']==param) & (pass_round_df['Station']==site)].StartDate.sort_values().copy()
#                        print('--Some data in this interval can be saved by scientific rounding.')
                        two_week_gaps_text+='   --Some data in this interval can be saved by scientific rounding.<br>'
                        
                        for date1, date2 in zip(PZS_date, PZS_date[1:]):
                            date1=date1
                            date2=date2
                            delta2=pd.to_datetime(date2)-pd.to_datetime(date1)
#                            saved_days=delta.days-delta2.days
                            
                            if delta2>gap:
#                                print(r'**The data from {} to {} cannot be saved'\
#                                      r' by scientific rounding. Still, {} days were lost in this range.'\
#                                      .format(date1,date2,delta2))
                                save_time+=-(delta2.days + delta2.seconds/86400)
                                two_week_gaps_text+=(r'   **The data from {} to {} cannot be saved'\
                                      r' by scientific rounding. Still, {} days were lost in this range.<br>'.format(date1,date2,delta2))
                                
                                losses+=1
                        if losses==0:
#                            print('**In fact, all data in this range can be saved. {} days saved.'.format(delta))
                            two_week_gaps_text+='**In fact, all data in this range can be saved. {} days saved.'.format(delta)
                        save_count+=1
#                        print('\n')
                        
    #                print(delta)
    
#    if gap_count>0:
#        print ('WARNING!!!!!!!!')
#        print ("Some stations contain gaps greater than 14 days, be sure to null the appropriate data")
        
#    if float(lost_time)>0: saved='{:.2f}%'.format(save_time/(lost_time)*100)
    
    
    '''----------------------------------------------------------------------------
                                 5.  Create Email Messages
    ---------------------------------------------------------------------------'''
    
        
    
    '''                          5a.  Create Span Email Text                    ''' 
    '''-------------------------------------------------------------------------'''                             
    
    ########################
    #Repeat starts here     
    ########################                        
    #Now we wa gonna email out all the failed entries so that can be further QCed
    span_text='<b><u>SPAN FAILURES AND WARNINGS</u></b><br><br>'
    
    #In some cases we extended the dfs to much further than the interval of 
    #interest, so we gotta chop it back down. We also gotta sort by run.
    span_fail_df=span_fail_df[span_fail_df['datetime']>=pd.to_datetime(cut_date1)]
    span_fail_df=span_fail_df[span_fail_df['datetime']<=pd.to_datetime(cut_date2)]
    span_fail_df=span_fail_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    #need this so we can print who's run it is only once, instead of n_fail+n_warn times per operator
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    #loop through all the failures and write an email line for each one.
    
    for state, county, site, parameter, phase, date, diff, obs, exp, allow, run in zip(span_fail_df['State'], 
                                                    span_fail_df['County'], 
                                                    span_fail_df['Site'], 
                                                    span_fail_df['ParameterCode'],
                                                    span_fail_df['PhaseName'],
                                                    span_fail_df['StartDate'],
                                                    span_fail_df['PZS_diff'],
                                                    span_fail_df['Value'],
                                                    span_fail_df['ExpectedValue'],
                                                    span_fail_df['MaxAllowed'],
                                                    span_fail_df['Run']):
        #one way to get the site symbol of each site
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        #yeah, so. All these if's are katies fault, and are needed to print out who's 
        #run the email is on. One day I need to think of a good way to generalize
        #all of this. Probably you could have an enrty feild where you can 
        #input all the run operators and their fields and then just go through the
        #list one by one with a for loop that is as long as there are operatores.
        #once again, the if's just make sure that the run operators name is printed once and ony once.
        

        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                span_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                span_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                span_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                span_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                span_text+=("   <br>Thad's Run<br>")
        
        #the 'ifs' here are so that failures today will be highlighted in the email.
        #This is the section of code that actually write the email statement for each line
        
        color='#ffb3b3'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ff3333'
        span_text+='   <span style="background-color: '+color+'">'
        span_text+=('   <b>{}</b> - {} ({}) fail {:.2f}%. (of \u00B1{:.2f}) at {}. Obs.={:.4f}, Exp.={:.4f}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, diff, allow, date.strftime("%Y-%m-%d %H:%S"), obs, exp))
    
    ########################
    #Repeat ends here     
    ########################   
    
    ########################################
    #see repeat above for following 56 lines
    ########################################
    span_warn_df=span_warn_df[span_warn_df['datetime']>=pd.to_datetime(cut_date1)]
    span_warn_df=span_warn_df[span_warn_df['datetime']<=pd.to_datetime(cut_date2)]
    span_warn_df=span_warn_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    span_text+='<br>'
   
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
 
    for state, county, site, parameter, phase, date, diff, obs, exp, allow, run in zip(span_warn_df['State'], 
                                                    span_warn_df['County'], 
                                                    span_warn_df['Site'], 
                                                    span_warn_df['ParameterCode'],
                                                    span_warn_df['PhaseName'],
                                                    span_warn_df['StartDate'],
                                                    span_warn_df['PZS_diff'],
                                                    span_warn_df['Value'],
                                                    span_warn_df['ExpectedValue'],
                                                    span_warn_df['MaxAllowed'],
                                                    span_warn_df['Run']):
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                span_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                span_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                span_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                span_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                span_text+=("   <br>Thad's Run<br>")
        
        color='#ffffcc'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ffff00'
        span_text+='   <span style="background-color: '+color+'">'
        span_text+=('   <b>{}</b> - {} ({}) warning {:.2f}%. (of \u00B1{:.2f}) at {}. Obs.={:.4f}, Exp.={:.4f}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, diff, allow, date.strftime("%Y-%m-%d %H:%S"), obs, exp))
    
        
    #%%
    '''                      5b.  Create Precision Email Text                   ''' 
    '''-------------------------------------------------------------------------''' 
    ########################################
    #see repeat above for following 56 lines
    ########################################
    prec_fail_df=prec_fail_df[prec_fail_df['datetime']>=pd.to_datetime(cut_date1)]
    prec_fail_df=prec_fail_df[prec_fail_df['datetime']<=pd.to_datetime(cut_date2)]
    prec_fail_df=prec_fail_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    prec_text='<br><b><u>PRECISION FAILURES AND WARNINGS</u></b><br><br>'
    
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    for state, county, site, parameter, phase, date, diff, obs, exp, allow,run in zip(prec_fail_df['State'], 
                                                    prec_fail_df['County'], 
                                                    prec_fail_df['Site'], 
                                                    prec_fail_df['ParameterCode'],
                                                    prec_fail_df['PhaseName'],
                                                    prec_fail_df['StartDate'],
                                                    prec_fail_df['PZS_diff'],
                                                    prec_fail_df['Value'],
                                                    prec_fail_df['ExpectedValue'],
                                                    prec_fail_df['MaxAllowed'],
                                                    prec_fail_df['Run']):
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                prec_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                prec_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                prec_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                prec_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                prec_text+=("   <br>Thad's Run<br>")
                
        color='#ffb3b3'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ff3333'
        prec_text+='   <span style="background-color: '+color+'">'
        prec_text+=('<b>{}</b> - {} ({}) fail {:.2f}%. (of \u00B1{:.2f}) at {}. Obs.={:.4f}, Exp.={:.4f}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, diff, allow, date.strftime("%Y-%m-%d %H:%M"), obs, exp))
    
    ########################################
    #see repeat above for following 56 lines
    ########################################
    prec_warn_df=prec_warn_df[prec_warn_df['datetime']>=pd.to_datetime(cut_date1)]
    prec_warn_df=prec_warn_df[prec_warn_df['datetime']<=pd.to_datetime(cut_date2)]
    prec_warn_df=prec_warn_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    prec_text+='<br>'
    
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    for state, county, site, parameter, phase, date, diff, obs, exp, allow,run in zip(prec_warn_df['State'], 
                                                    prec_warn_df['County'], 
                                                    prec_warn_df['Site'], 
                                                    prec_warn_df['ParameterCode'],
                                                    prec_warn_df['PhaseName'],
                                                    prec_warn_df['StartDate'],
                                                    prec_warn_df['PZS_diff'],
                                                    prec_warn_df['Value'],
                                                    prec_warn_df['ExpectedValue'],
                                                    prec_warn_df['MaxAllowed'],
                                                    prec_warn_df['Run']):
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                prec_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                prec_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                prec_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                prec_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                prec_text+=("   <br>Thad's Run<br>")
        
        color='#ffffcc'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ffff00'
        prec_text+='   <span style="background-color: '+color+'">'
        prec_text+=('<b>{}</b> - {} ({}) warning {:.2f}%. (of \u00B1{:.2f}) at {}. Obs.={:.4f}, Exp.={:.4f}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, diff, allow, date.strftime("%Y-%m-%d %H:%M"), obs, exp))
    '''-------------------------------------------------------------------------'''     
    '''                      5c.  Create Zero Email Text                   ''' 
    '''-------------------------------------------------------------------------''' 
    ########################################
    #see repeat above for following 56 lines
    ########################################
    zero_fail_df=zero_fail_df[zero_fail_df['datetime']>=pd.to_datetime(cut_date1)]
    zero_fail_df=zero_fail_df[zero_fail_df['datetime']<=pd.to_datetime(cut_date2)]
    zero_fail_df=zero_fail_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    zero_text='<br><b><u>ZERO FAILURES AND WARNINGS</u></b><br><br>'
    
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    for state, county, site, parameter, phase, date, diff, obs, exp, allow, run in zip(zero_fail_df['State'], 
                                                    zero_fail_df['County'], 
                                                    zero_fail_df['Site'], 
                                                    zero_fail_df['ParameterCode'],
                                                    zero_fail_df['PhaseName'],
                                                    zero_fail_df['StartDate'],
                                                    zero_fail_df['PZS_diff'],
                                                    zero_fail_df['Value'],
                                                    zero_fail_df['ExpectedValue'],
                                                    zero_fail_df['MaxAllowed'],
                                                    zero_fail_df['Run']):
        
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                zero_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                zero_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                zero_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                zero_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                zero_text+=("   <br>Thad's Run<br>")
                
        
        color='#ffb3b3'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ff3333'
        zero_text+='   <span style="background-color: '+color+'">'
        zero_text+=('<b>{}</b> - {} ({}) fail {:.6f}. (of \u00B1{:.4f}) at {}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, obs, zero_dict.get(parameter), date))
        
    kati_counter=0
    john_counter=0
    shauna_counter=0
    luke_counter=0
    thad_counter=0
    
    ########################################
    #see repeat above for following 56 lines
    ########################################
    zero_warn_df=zero_warn_df[zero_warn_df['datetime']>=pd.to_datetime(cut_date1)]
    zero_warn_df=zero_warn_df[zero_warn_df['datetime']<=pd.to_datetime(cut_date2)]
    zero_warn_df=zero_warn_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
    
    zero_text+='<br>'
    
    for state, county, site, parameter, phase, date, diff, obs, exp, allow, run in zip(zero_warn_df['State'], 
                                                    zero_warn_df['County'], 
                                                    zero_warn_df['Site'], 
                                                    zero_warn_df['ParameterCode'],
                                                    zero_warn_df['PhaseName'],
                                                    zero_warn_df['StartDate'],
                                                    zero_warn_df['PZS_diff'],
                                                    zero_warn_df['Value'],
                                                    zero_warn_df['ExpectedValue'],
                                                    zero_warn_df['MaxAllowed'],
                                                    zero_warn_df['Run']):
        site_name=sites_df[(sites_df['County Code']==county)&(sites_df['Site Code']==site)]
        site_name=site_name['Site Symbol'].values[0]
        
        
        if run=='Kati':
            kati_counter+=1
            if kati_counter==1:
                zero_text+=("   <br>Kati's Run<br>")
        
        if run=='John':
            john_counter+=1
            if john_counter==1:
                zero_text+=("   John's Run<br>")
        
        if run=='Shauna':
            shauna_counter+=1
            if shauna_counter==1:
                zero_text+=("   <br>Shauna's Run<br>")

        if run=='Luke':
            luke_counter+=1
            if luke_counter==1:
                zero_text+=("   <br>Luke's Run<br>")

        if run=='Thad':
            thad_counter+=1
            if thad_counter==1:
                zero_text+=("   <br>Thad's Run<br>")
        
        
        color='#ffffcc'
        if str(date)[:10]==str(dt.datetime.now())[:10]: color='#ffff00'
        zero_text+='   <span style="background-color: '+color+'">'
        zero_text+=('<b>{}</b> - {} ({}) warning {:.6f}. (of \u00B1{:.4f}) at {}</span><br>'
             .format(site_name, param_dict.get(parameter),phase, obs, zero_dict.get(parameter), date))
    
    
    '''----------------------------------------------------------------------------
            6.  Check another way for missings and Add to df
    ---------------------------------------------------------------------------'''
    
    
    #df_all = xls_df.merge(zero_df, on=['Station','PhaseName'], 
    #                   how='right', indicator=True)
    def final_gap_check(df,start, end_date, header_text):
        '''#This use of this function is to see if gaps can be found even 
        #though we haven' measured another good PZS. The way the previous 
        #gap checker works is too look for large gaps between two good PZSs.
        #If, however, we are in the midst of an instrument being down, there
        #won't be a good PZS after the data needs to be nulled yet, so we will 
        #miss it during that months uploads. This method, however, attempts
        #to catch those gaps by looking at the rate of PZS (Number of PZS/Time)
        #and looking for sections where the rate is less than necessary for EPA
        #this function takes a few important args. First, it needs a df. Since
        #This is a gap check I pass the 'pass_round_df,' which has all the passing
        #pzss includiung the rounded ones. It also takes the 'start' arg. This
        #is a text string that indicates how many days before the specified end 
        #date you want to look. The 'end_date' is a datetime which is the last date you
        #want to look at. Finally, this function has to be run recursilvely, twice,
        #in each of the ways in which is it implemented in the current setup. These
        #ways are 1) automatic, and 2) manually triggered. The 'header_text' arg
        #is a html formatted string that will display the header for this
        #section of the email.'''
        prec_df,span_df,zero_df=Get_PZS_dat()
        param_list=prec_df
        param_list=param_list.append(span_df)
        param_list=param_list.append(zero_df)
        param_list['Run']=param_list['Station'].map(station_run_dict)
        param_list=param_list.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
        start_date=pd.to_datetime(end_date)-pd.Timedelta(start)
    #    end_date=end_date)
        gap_df=df[df['datetime']>=start_date]
        gap_df=gap_df[gap_df['datetime']<=end_date]
        gap_df=gap_df.sort_values(['Run','Site','ParameterCode','PhaseName','AssessmentDate'])
        time_delta=end_date-start_date
        
        final_gap_text=header_text
        kati_counter=0
        john_counter=0
        shauna_counter=0
        luke_counter=0
        thad_counter=0        
        #The way this code actually works is a bit odd. It takes each line in 
        #each dataframe (prec_df, zero_df, span_df), and creates a temporary
        #df out of a single line called 'line'. Remember that these dfs contain
        #all of the possible prec, zeros, and spans that should be run each week/day
        #Then it performs a merge on the station and and phasename with the 
        #psasing df. This returns a df with only the passing lines from the 
        #passing df. By counting these I can calculate the rate of pzs (test/day).
        #If there was 1 every two weeks, as there should be, then the rate would
        #be 0.0714 (1/14) per day, so anything less than this is failing. 
        #if the user triggers the code then it will look at both of the two week 
        #periods in the last month. If autotriggered then it will look at the
        #last 7 days and the last 14 days.
        
        for x,y in param_list.iterrows():
            line=pd.DataFrame(y).T #turn the line into df
            #merge it
            pzs_list=line.merge(gap_df, on=['Station','PhaseName'], how='inner', indicator=True)
            pzs_count=len(pzs_list) #count it
            pzs_rate=pzs_count/(time_delta.days+time_delta.seconds) #get rate
            
            #check for gap and write email line if there is
            if pzs_rate<0.0714:
                site_name=sites_df[(sites_df['County Code']==line['County'].values[0])&
                                   (sites_df['Site Code']==line['Site'].values[0])]
                
                site_sym=site_name['Site Symbol'].values[0]
                site_name=site_name['SITE NAME'].values[0]
                
                run=station_run_dict.get(line['Station'].values[0])
                if run=='Kati':
                    kati_counter+=1
                    if kati_counter==1:
                        final_gap_text+=("   <br>Kati's Run<br>")
                
                if run=='John':
                    john_counter+=1
                    if john_counter==1:
                        final_gap_text+=("   John's Run<br>")
                
                if run=='Shauna':
                    shauna_counter+=1
                    if shauna_counter==1:
                        final_gap_text+=("   <br>Shauna's Run<br>")
        
                if run=='Luke':
                    luke_counter+=1
                    if luke_counter==1:
                        final_gap_text+=("   <br>Luke's Run<br>")
        
                if run=='Thad':
                    thad_counter+=1
                    if thad_counter==1:
                        final_gap_text+=("   <br>Thad's Run<br>")

                final_gap_text+=(r'   <b>{}</b> ({}, {}) appears to have {} gap between {} and {}<br>'\
                      .format(site_sym,site_name,line['Station'].values[0],
                              line['PhaseName'].values[0],
                              start_date.strftime('%m/%d/%Y'),end_date.strftime('%m/%d/%Y')))

        return final_gap_text
    
    #check how the event was triggered. If this were a class this would be
    #much easier. The next lines change the text and start and end dates
    #based on whether the user or the auto_trigger launched the analysis.
    if auto_run==1:
        final_gap_text=final_gap_check(pass_round_df,
                                       '14 days',
                                       dt.datetime.now(),
                                       '<br><b><u>PZS GAPS IN LAST 14 DAYS</u></b><br><br>')
        final_gap_text2=final_gap_check(pass_round_df,
                                        '7 days',
                                        dt.datetime.now(),
                                        '<br><b><u>PZS GAPS IN LAST 7 DAYS</u></b><br><br>')
    elif auto_run==0:
        final_gap_text=final_gap_check(pass_round_df,
                                       '14 days',
                                       pd.to_datetime(av_date2),
                                       '<br><b><u>PZS GAPS IN FIRST 14 DAYS OF THE MONTH</u></b><br><br>')
        final_gap_text2=final_gap_check(pass_round_df,
                                        '14 days',
                                        (pd.to_datetime(av_date2)-pd.Timedelta('14 days')),
                                        '<br><b><u>PZS GAPS IN LAST 14 DAYS OF THE MONTH</u></b><br><br>')        
    #since there are two runs in each mode we need to concat them
    final_gap_text+=final_gap_text2
    
    #%%
    '''----------------------------------------------------------------------------
                                 6.  Send Email
    ---------------------------------------------------------------------------'''
    #concat all the text vars into one email body
    final_text=two_week_gaps_text+final_gap_text+span_text+prec_text+zero_text
    
    
    # Import smtplib for the actual sending function
    import smtplib
    
    # Import the email modules we'll need
    from email.mime.text import MIMEText
    
    #html is the email body. We are gonna wrap some text around the analysis text
    html=''
    html = """\
    <html>
      <head></head>
      <body>
       <p>All,<br><br>Below is a list of span, precision, 
       and zero warnings and failures from {} to {}.  
       Please let me know if you need any more information.<br><br><br>""".format(pd.to_datetime(av_date1).strftime('%m/%d/%Y'),pd.to_datetime(av_date2).strftime('%m/%d/%Y'))
    
    #shove the analysis text in the middle
    html+=final_text
    
    html+="""\
        
       <br>Thanks,<br><br>
       <font color="blue">Bart Cubrich</font><br>
       <font color="blue">Environmental Scientist</font><br>
       <font color="blue">Technical Analysis Section</font><br>
       <font color="blue">Office (801) 536-4146</font><br>
       <font color="blue">Email bcubrich@utah.gov</font><br>
       </p>
      </body>
    </html>
    """
    #this is just silly, why not jsut use html?
    final_text=html
    #this uses MIMEtext the create an email message
    msg = MIMEText(final_text, 'html')
#    me = 'bcubrich@gmail.com'
    me = 'pzs@utah.gov'
    #you=['bcubrich@utah.gov']
    
    
    #the email has some attributes we can edit, like the subject
    msg['Subject'] = '(ALL SITES) - '+pd.to_datetime(cut_date1).strftime('%m/%d/%Y')+' - '+pd.to_datetime(cut_date2).strftime('%m/%d/%Y')+' PZS Warnings, Failures, and Incompletes' 
    msg['From'] = me
    msg['To'] = ", ".join(you)   #create a string of recipients from a list
    
    #open a server conection
    server = smtplib.SMTP('send.utah.gov', 25)
    #if the user wants to or the email is autoatic send it out
    if send_email==1: server.sendmail(me, you, msg.as_string())
    #close the server conection
    server.quit()
    
    #edit the final text so that it can also be written as a text file
    report_text=final_text.replace('<br>','\n')
    report_text=report_text.replace('</br>','\n')
    soup=BeautifulSoup(report_text,'lxml')
#    print(soup.text)
    
    #write the report file to the appropraite location
    if auto_run==1:
        out_path =str(report_out_path)+'/PZS_Report_' #get user selected output path
        out_path+=str(dt.datetime.now())[:10]+'.txt'
        f= open(out_path,"w+")
        f.write(soup.text)
        f.close()
    
    '''----------------------------------------------------------------------------
                                 7.  Output Cleanup
    ---------------------------------------------------------------------------'''
    #this is only if the user want to create and AQS report. Ideally this is 
    #run once a month to make sure that the correct PZSs are uploaded to 
    #AQS
    
    #gonna clean up the output here so that the final result is an 
    
    #get rid of all the fails and the zero, which we don't report.
    output_df['PZS_Check_Final']=np.where(output_df['PZS_Check']!='FAIL!!!',output_df['PZS_Check'],
             output_df['PZS_DoubleCheck'])
    output_df=output_df[output_df['PZS_Check_Final']!='FAIL!!!'].copy()
    output_df=output_df[~output_df['PhaseName'].str.contains('ZERO')].copy()
    #Drop the data back down to the reporting month, data currently has a 2 week buffer
    #on either side to make sure that the PZSs have been run frequently enough.
    output_df=output_df[output_df['datetime']>=pd.to_datetime(cut_date1)]
    output_df=output_df[output_df['datetime']<=pd.to_datetime(cut_date2)]
    
    #this is a string with a list of the columns we need to keep
    columns=r'TransactionType|ActionIndicator|AssessmentType|PerformingAgencyCode'\
            r'|State|County|Site|ParameterCode|POC|AssessmentDate'\
            r'|AssessmentNumber|MethodCode|ReportingUnit|Value|ExpectedValue'
    #turn the string into a list
    columns_to_keep=columns.split('|')
    #drop down to only the columns that are needed for an AQS upload
    output_df=output_df[columns_to_keep] 
    
    '''       -------------Duplicate Check-------------------------------       
    AQS won't accept 2 entries for the same instrument at the same station on the 
    same day, so we need to remove any duplicates like that.'''
    
    #This is a passing df, so we can safely drop duplicates and take
    #just 1 passing PZS for each day.
    subset=output_df.columns[:10] #subset of df columns to perform duplicate check on
    output_df=output_df.sort_values(['State', 'County', 'Site', 'ParameterCode',
                                     'AssessmentDate']).drop_duplicates(subset =subset, keep ='last') #drop duplicates, take second entry
    
    
    
    
    '''----------------------------------------------------------------------------
                                 8.  Write to file
    ---------------------------------------------------------------------------'''
    #This writes an AQS file if the user checks that box. Only want this if the 
    #the user launches the analysis because no one will be around to choose a file save
    #location otherwise, and we don't need an AQS upload file every day.
    if write_file==1 and auto_run==0:
        filename=out_dir()
        out_path =filename+'/AQS_PZS_upload_'+cut_date1+'_'+cut_date2+'.txt' #get user selected output path
        
        
        output_df=output_df.set_index('TransactionType') #need to get rid of index
        output_df.to_csv(out_path, sep='|')    #write to pipe file
        
        
        '''---------
        The following whole bit is used to add a '#' to the first line of the file. 
        Seems like a lot of code just to add a hashtag to the file, but I like having 
        the header info right in the file, in case someone only sees the text file.
        ------'''
        appendText='#'
        text_file=open(out_path,'r')
        text=text_file.read()
        text_file.close()
        text_file=open(out_path,'w')
        text_file.seek(0,0)
        text_file.write(appendText+text)
        text_file.close()
    print('Completed')
#    tick()
        

'''--------------------------------------------------------------------------
                                9. GUI
---------------------------------------------------------------------------'''
'''Now Everything is all setup and ready to go. As such, we can write the 
GUI that will handle all the diferent cases.'''



#main tkinter window=master
master = Tk()
master.title('PZS Manager')

#eventually I'll figure out how to add a cool icon to this app.
#the below works in an IDE, but no tin the stand alone package
#master.iconbitmap('U:\PLAN\BCUBRICH\Python\Daily_PZS_Fail_Test\icon.ico')

#this is useful when running through an IDE, but not if you want the stand alone
#package to run on its own and not be annoying. Just keep the window alwayy on top.
#master.attributes("-topmost", True)      #makes the dialog appear on top


'''------------------
b) create some lists for the buttons'''
    
dates=[str('0'+str(x))[-2:] for x in np.arange(1,32)]      #need days of the month
years=[str(x) for x in np.arange(2016,2030)] #need years
hours=[str('0'+str(x))[-2:] for x in np.arange(0,24)]

mon_dict={'January':'01','February':'02', 'March':'03','April':'04', 'May':'05',
               'June':'06', 'July':'07', 'August':'08', 'Spetember':'09', 'October':'10', 
               'November':'11', 'December':'12'}
mon__len_dict={'January':'31','February':'28', 'March':'31','April':'30', 'May':'31',
               'June':'30', 'July':'31', 'August':'31', 'September':'30', 'October':'31', 
               'November':'30', 'December':'31'}
'''------------------
c) create buttons and dropdowns   
  -each w is a tk object, just got w from the code I copied from the internet
  -each variable is a field in the tk windown. variables1, 2, and 3 are the 
   fields for the start date: Month, day, year respectively
  -I used the grid method to indicate the placement of each button, as it is
   more precise than pack
    ''' 
#start date month
variable1 = StringVar(master)
variable1.set("January") # default value
w = OptionMenu(master, variable1, 'January','February', 'March','April', 'May',
               'June', 'July', 'August', 'September', 'October', 'November', 
               'December')
w.grid(row=1, sticky='W', column=0)

#w.config(width=12, height=1)

#start date day of month
variable2 = StringVar(master)
variable2.set('01') # default value
w2 = OptionMenu(master, variable2, *dates)
w2.grid(row=1, sticky=W, column=1)

#start date year
variable3 = StringVar(master)
variable3.set('2019') # default value
w3 = OptionMenu(master, variable3, *years)
w3.grid(row=1, sticky=W, column=2)

#end date month
variable4 = StringVar(master)
variable4.set('January') # default value
w4 = OptionMenu(master, variable4, 'January','February', 'March','April', 'May',
               'June', 'July', 'August', 'September', 'October', 'November', 
               'December')
w4.grid(row=3, sticky=W, column=0)

#end date day of month
variable5 = StringVar(master)
variable5.set('31') # default value
w5 = OptionMenu(master, variable5, *dates)
w5.grid(row=3, sticky=W, column=1)

#end date year
variable6 = StringVar(master)
variable6.set('2019') # default value
w6 = OptionMenu(master, variable6, *years)
w6.grid(row=3, sticky=W, column=2)

#start hour
variable7 = StringVar(master)
variable7.set('00') # default value
w6 = OptionMenu(master, variable7, *hours)
w6.grid(row=1, sticky=W, column=3)

#end hour
variable8 = StringVar(master)
variable8.set('00') # default value
w6 = OptionMenu(master, variable8, *hours)
w6.grid(row=3, sticky=W, column=3)

#If analysis is user generated then send an email?
variable9 = IntVar(master)
Checkbutton(master, text="Send Email", variable=variable9).grid(row=5, sticky=W, column=1)

#create a check button to organize results by run and automatically sets it's default to checked
variable12 = IntVar(master)
Checkbutton(master, text="Organize by Run", variable=variable12).grid(row=5, sticky=W, column=3)
variable12.set(1)

#create radio button option between preview and send 
variable10 = IntVar(master)
variable10.set(1)
r1 = Radiobutton(master, text='Preview',
                        variable=variable10, value=1)
r1.grid(row=6, sticky=W, column=1)

r2 = Radiobutton(master, text='Send',
                        variable=variable10, value=2)
r2.grid(row=7, sticky=W, column=1)

'''------------------
d) get data from GUI'''

def auto_get():
    variable4.set(variable1.get())
    variable5.set(mon__len_dict.get(variable1.get()))
    variable6.set(variable3.get()) # default value

#originally this function ran the analysis then quit the GUI. Now it 
#actually just runs the analysis when the user presses 'Run' and leaves the GUI open 
def quitit():
    global cut_date1
    global cut_date2
    global av_date1
    global av_date2
    global send_email
    global preview
    global write_file
    global you
    global auto_run
    global preview_email
    global report_out_path
    global run_org
    auto_run=0   #indicates that a user triggered this event
    
    #get the vars from the GUi
    run_org=variable12.get()   #organize by operator?
    you= preview_email.get()   #who to email if preview?
    preview=variable10.get()   #preview email or send it?
    report_out_path=report_path.get()    #where to put auto-generated text message report
    you=str(you).split(',')              #if we are using preview, need to split the string
    send_email=variable9.get()           #if user gnerated send email? 
    write_file=variable11.get()          #write AQS pipe file for upload?
    
    #if send is chosen this is the default send list.
    if preview == 2:
        you=['bcubrich@utah.gov','smward@utah.gov','lleclair@utah.gov',
             'jmcoombs@utah.gov', 'tbaldwin@utah.gov','kchachere@utah.gov',
             'bcluster@utah.gov','pharrison@utah.gov','khart@utah.gov',
             'kkreykes@utah.gov','bcubrich@utah.gov', 'adarnold@utah.gov',
             'bocall@utah.gov', 'jkarmazyn@utah.gov','ksymons@utah.gov']
        
    #setup the date range if user triggered
    cut_date1=mon_dict.get(variable1.get()) +'-'+ variable2.get() +'-'+ variable3.get()
    cut_date2= mon_dict.get(variable4.get()) +'-'+ variable5.get() +'-'+ variable6.get()
    av_date1=variable3.get() +'-'+ mon_dict.get(variable1.get()) +'-'+ variable2.get() +' '+ variable7.get() +':00:00.000'
    av_date2= variable6.get() +'-'+ mon_dict.get(variable4.get()) +'-'+ variable5.get()  +' '+ variable8.get() +':00:00.000'
    
    #error message if user chooses backward date range
    if pd.to_datetime(av_date2)<pd.to_datetime(av_date1):
        messagebox.showerror('Datetime Error', 'Please pick an end date that is after the start date')
    else:
        #run analysis
        pzs_main()

#actually operates the quit button
def quitit2():
    master.quit()
    master.destroy()
    sys.exit("Script No Longer Running")

#allows user to navigate to file path and sets that as the report outpput filepath
def report_out():
    report_path.set(out_dir())
 
#create a login window so that not just anyone can send automatic emails
def login():
    global pword
    global login_label 
    window=Tk()
    pword=StringVar(window)
    
    def quits(entry=''):
        global password
        password=pword.get()
        
        #create a password status thingy
        if password=='Admin123!@#':
            login_label=Label(master,text='Admin Mode \n Active', bg='#33ff77').grid(row=0,column=3)
        else: 
            login_label=Label(master,text='Admin Mode \n Inactive', bg='red').grid(row=0,column=3)
        window.destroy()
    
    #elements in login window
    pass_label=Label(window,text='Password').pack()
    pass_entry=Entry(window,textvariable=pword).pack()
#    window.bind('<Return>', quits)
    pass_ok=Button(window,text='login', command=quits).pack()
    

        
    
    
    
    
#automatically get the end of the month as the end date. Useful for 
#end of the month review of PZS data    
Autoset = Button(master, text="Set End Date \n to End of \n Month", command=auto_get)
Autoset.grid(row=6, sticky=W, column=0)


#Label for choosing start date
label1=Label(master, text="Choose Start Date", height=4)
label1.grid(row=0, sticky='SW', column=0)

#Label for choosing end date
label1=Label(master, text="Choose End Date", height=4)
label1.grid(row=2, sticky='SW', column=0)

#Run code button
button = Button(master, text="       Run       ", command=quitit, bg="cyan")
button.grid(row=11, sticky='E', column=3)

#Quit the program button
button2 = Button(master, text="       Quit       ", command=quitit2, bg='red')
button2.grid(row=12, sticky='E', column=3)

#Send email checkbox
variable9 = IntVar(master)
Checkbutton(master, text="Send Email", variable=variable9).grid(row=5, sticky=W, column=1)

#Write AQS output file checkbox
variable11 = IntVar(master)
Checkbutton(master, text="Write AQS \n to File", variable=variable11).grid(row=5, sticky=W, column=2)

#Label that creates the clock display. Helps tell the app is running
clock = Label(master, font=('times', 12, 'bold'))
clock.grid(row=0, column=1, columnspan=2)

#label preview email
label3 = Label(master,text="Preview Email:").grid(row=8,sticky=W,column=1)

#variable and entry box used for getting list of people to email on preview
global preview_email
preview_email=StringVar(master)
preview_email.set('bcubrich@utah.gov')
entry1=Entry(master, textvariable=preview_email).grid(row=9, sticky=W, column=1, columnspan=2)

#Entry box and variable for file path to write the daily PZS report email as a string
global report_path
report_path=StringVar(master)
report_path.set('U:/PLAN/AMC/AIRS/DAILY PZS REPORTS')
label3 = Label(master,text="Report File Output Path").grid(row=10,sticky=W,column=0)
entry2=Entry(master, textvariable=report_path, width = 50).grid(row=11, column=0, columnspan=3, sticky='W')

#Admin login button for checking password
button4 = Button(master, text="  Admin Login  ", command=login, bg='green')
button4.grid(row=12, column=1, columnspan=2)

#Button to allow user to choose a folder to output the daily report to
button5 = Button(master, text="Choose Report Folder", command=report_out, bg='#ffff99')
button5.grid(row=12, sticky='W', column=0)


def tick():
    #'tick()' checks the time every 200ms. At each tick it updates the clock
    #but it also check to see if the time matches the time when it is 
    #supposed to send an email. The first time there is a match it sends the email.
    #This may be a little cheesey, but it is handled by the counter var. This
    #var is reset to zero whenever it is not the correct 'HH:MM' to send the email.
    #When it is the right 'HH:MM' then the counter is incremented by 1 every tick (200ms)
    #at the first incriment the pzs_main() analysis functoin is triggered.
    global counter
    global cut_date1
    global cut_date2
    global av_date1
    global av_date2
    global send_email
    global preview
    global write_file
    global you
    global auto_run
    global password
    global run_org
    global report_out_path
    run_org=variable12.get()
    report_out_path=report_path.get()

    # get the current local time from the PC
    time1 = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    #update the string
    clock.config(text=time1)
    time2 = dt.datetime.now().strftime('%H:%M')
    #if not 5:30AM then counter is 0
    if str(time2)!='05:30': counter=0
    #if it is 5:30 am then get ready to send the email. The first time send
    #the email out. After, just keep counting until it goes back to zero
    if str(time2)=='05:30':
        counter+=1
        if counter<2:
            auto_run=1
#            you=['bcubrich@utah.gov']
            you=['bcubrich@utah.gov','smward@utah.gov','lleclair@utah.gov',
                 'jmcoombs@utah.gov', 'tbaldwin@utah.gov','kchachere@utah.gov',
                 'bcluster@utah.gov','pharrison@utah.gov','khart@utah.gov',
                 'kkreykes@utah.gov','bcubrich@utah.gov', 'adarnold@utah.gov',
                 'bocall@utah.gov', 'jkarmazyn@utah.gov','ksymons@utah.gov']
            preview=2
            send_email=1
            write_file=0
            cut_date2= dt.datetime.now()
            cut_date1=dt.datetime.now()-pd.Timedelta('14 days')
            
            av_date1=dt.datetime.now()-pd.Timedelta('14 days')
            av_date2= dt.datetime.now()
            
            #won't send the email unless the right password is supplied
            if password=='Admin123!@#':
                pzs_main()
    
    # calls itself every 200 milliseconds
    # to update the time display as needed
    # could use >200 ms, but display gets jerky
    clock.after(200, tick)
#run tick the first time, afterwards it runs itself every 200ms
tick()
#keep GUI running
mainloop()



