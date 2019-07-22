import requests
import pyodbc
import connection
from datetime import datetime , timezone
import time

#TODO: Bind to a different port other than 80. It makes things all freaky if using default portr 80 (http traffic).

#TODO: Add graphana to SQL Database: https://grafana.com/docs/features/datasources/mssql/

#TODO: ADD GUID column to link jobs together


"""
Created an ETL to get JSON data from printer over to SQL so we can do analythics
LINK: https://artemiorimando.com/2017/03/25/extract-transform-and-load-yelp-data-using-python-and-microsoft-sql-server/


This functions job is to pull date from the API, transform it into a respected datatype then load it into a SQL database.
"""
RECORDINGDATE = datetime.now()
RECORDINGDATE = str(RECORDINGDATE)

# All possible GET commands  0-11 btw

def job():
    response = requests.get('http://'+connection.Domain+CALLS[0], headers=headers)
    response = response.json()

    ts = time.time()
    RECORDINGDATE = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    jobName = response['job']['file']['name']
    jobSize = response['job']['file']['size']
    jobCreateDate = response['job']['file']['date']
    jobTime = response['job']['estimatedPrintTime']
    jobFilamentLength = response['job']['filament']['tool0']['length']
    jobFilamentVolume = response['job']['filament']['tool0']['volume']

    jobName = str(jobName)
    jobSize = str(jobSize)
    jobCreateDate = str(jobCreateDate)
    jobTime = str(jobTime)
    jobFilamentLength = str(jobFilamentLength)
    jobFilamentVolume = str(jobFilamentVolume)

    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+connection.Domain+';DATABASE='+connection.Database+';UID='+connection.Username+';PWD='+connection.Password
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO [dbo].[jobList] (
                            RecordingDate,
                            jobName, 
                            jobSize, 
                            jobCreateDate, 
                            jobTime, 
                            jobFilamentLength, 
                            jobFilamentVolume) 
                            values (?, ?, ?, ?, ?, ?, ?)'''
        , RECORDINGDATE,jobName,jobSize,jobCreateDate,jobTime,jobFilamentLength,jobFilamentVolume)

    cursor.commit()
    return

def API_Caller():

    #TODO: Make sure columns ruely represent what data is being pulled

    #TODO: Maybe transform the data even more to repesent what type it is... like WTF is 720.4516325984414 for job time


    response = requests.get('http://'+connection.Domain+CALLS[0], headers=headers)
    response = response.json()

    # ts = time.time()
    # RECORDINGDATE = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


    RECORDINGDATE = datetime.now(timezone.utc)

    progressCompletion = response['progress']['completion']
    progressPrintTime = response['progress']['printTime']
    progressPrintTimeLeft =  response['progress']['printTimeLeft']
    jobSate = response['state']

    response = requests.get('http://'+connection.Domain+CALLS[1], headers=headers)
    response = response.json()

    flags_Cancelling = response['state']['flags']['cancelling']
    flags_ClosedOrError = response['state']['flags']['closedOrError']
    flags_Error = response['state']['flags']['error']
    flags_Finishing = response['state']['flags']['finishing']
    flags_Operational = response['state']['flags']['operational']
    flags_Paused = response['state']['flags']['paused']
    flags_Pausing = response['state']['flags']['pausing']
    flags_Printing = response['state']['flags']['printing']
    flags_Ready = response['state']['flags']['ready']

    bedTempActual = response['temperature']['bed']['actual']
    bedTempTarget = response['temperature']['bed']['target']    
    bedTempOffset = response['temperature']['bed']['offset']
    tool0TempActual = response['temperature']['tool0']['actual']
    tool0TempTarget = response['temperature']['tool0']['target']    
    tool0TempOffset = response['temperature']['tool0']['offset']


    # Job Details TRANSFORM

    #TODO: Optimize for better data types

    progressCompletion = float(progressCompletion)
    progressPrintTime = int(progressPrintTime)
    progressPrintTimeLeft = int(progressPrintTimeLeft)
    jobSate = str(jobSate)

    flags_Cancelling = str(flags_Cancelling)
    flags_ClosedOrError = str(flags_ClosedOrError)
    flags_Error = str(flags_Error)
    flags_Finishing = str(flags_Finishing)
    flags_Operational = str(flags_Operational)
    flags_Paused = str(flags_Paused)
    flags_Pausing = str(flags_Pausing)
    flags_Printing = str(flags_Printing)
    flags_Ready = str(flags_Ready)

    bedTempActual = float(bedTempActual)
    bedTempTarget = float(bedTempTarget)
    bedTempOffset = float(bedTempOffset)
    tool0TempActual = float(tool0TempActual)
    tool0TempTarget = float(tool0TempTarget)
    tool0TempOffset = float(tool0TempOffset)


    # Function wide LOAD
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+connection.Domain+';DATABASE='+connection.Database+';UID='+connection.Username+';PWD='+connection.Password
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    cursor.execute('''INSERT INTO [dbo].[progress](
                            RecordingDate,
                            progressCompletion,
                            progressPrintTime,
                            progressPrintTimeLeft,
                            jobSate)
                            values (?,?, ?, ?, ?)'''
        ,RECORDINGDATE,progressCompletion,progressPrintTime,progressPrintTimeLeft,jobSate)

    cursor.execute('''INSERT INTO [dbo].[flags](
                            RecordingDate,
                            flags_Cancelling,
                            flags_ClosedOrError,
                            flags_Error,
                            flags_Finishing,
                            flags_Operational,
                            flags_Paused,
                            flags_Pausing,
                            flags_Printing,
                            flags_Ready)
                            values (?,?,?,?,?,?,?,?,?,?)'''
        ,RECORDINGDATE,flags_Cancelling,flags_ClosedOrError,flags_Error,flags_Finishing,flags_Operational,flags_Paused,flags_Pausing,flags_Printing,flags_Ready)
    
    cursor.execute('''INSERT INTO [dbo].[temps](
                            RecordingDate,
                            bedTempActual,
                            bedTempTarget,
                            bedTempOffset,
                            tool0TempActual,
                            tool0TempTarget,
                            tool0TempOffset)
                            values (?,?,?,?,?,?,?)'''
        ,RECORDINGDATE,bedTempActual,bedTempTarget,bedTempOffset,tool0TempActual,tool0TempTarget,tool0TempOffset)
    cursor.commit()
    return

if __name__ == "__main__":

    headers = {
    'Host': connection.host,
    'X-API-KEY': connection.API_KEY,
    } 

    CALLS = ['/api/job','/api/printer','/api/printer/tool','/api/printer/bed','/api/printer/chamber','/api/printer/sd',
            '/api/printerprofiles','/api/settings','/api/settings/templates','/api/slicing','/api/system/commands','/api/timelapse']

    Keeprunning = 1
    jobjob = 0
    startLogging  = 0
    while Keeprunning == 1:
        start = requests.get('http://'+connection.Domain+CALLS[1], headers=headers) #checks to see if 3DP is running
        json = start.json()
        #TODO: Change logic to now have the waiting in own section
        print ("waiting...")
        time.sleep(.500)
        if start.status_code == 200:
            if (jobjob == 0 and json['state']['flags']['printing'] == 1):
                print("Job Found - Starting to log...")
                job()
                jobjob = 1
                startLogging = 1

            if (startLogging == 1):
                print(start.status_code)
                API_Caller()
                time.sleep(1)
        else:
            print(start.status_code)
            print("Error Connecting")
            time.sleep(1)
            pass 