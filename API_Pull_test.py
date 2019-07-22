import requests
import pyodbc


"""
Purpose is to get the basic funstionality of the ETL working
LINK: https://artemiorimando.com/2017/03/25/extract-transform-and-load-yelp-data-using-python-and-microsoft-sql-server/
"""


headers = {
    'Host': '10.0.0.87',
    'X-API-KEY': '61F51626190E46DF9DF9DA17F53A457F',
    } 
r = requests.get('http://24.21.47.40/api/job', headers=headers)


################## Job Status ####################

response = r.json()

# Job Details EXTRACT 
jobName = response['job']['file']['name']
jobSize = response['job']['file']['size']
jobCreateDate = response['job']['file']['date']
jobTime = response['job']['estimatedPrintTime']
jobFilamentLength = response['job']['filament']['length']
jobFilamentVolume = response['job']['filament']['volume']

# Job Details TRANSFORM
jobName = str(jobName)
jobSize = str(jobSize)
jobCreateDate = str(jobCreateDate)
jobTime = str(jobTime)
jobFilamentLength = str(jobFilamentLength)
jobFilamentVolume = str(jobFilamentVolume)

# Job Details LOAD
datasource_name = '24.21.47.40'
database = 'Prusa_Analytics'
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=24.21.47.40;DATABASE=Prusa_Analytics;UID=SA;PWD=Plane12#$%'
connection = pyodbc.connect(connection_string)

cursor = connection.cursor()

cursor.execute('''INSERT INTO [dbo].[jobList] (
                         jobName, 
                         jobSize, 
                         jobCreateDate, 
                         jobTime, 
                         jobFilamentLength, 
                         jobFilamentVolume) 
                         values (?, ?, ?, ?, ?, ?)''', jobName,jobSize,jobCreateDate,jobTime,jobFilamentLength,jobFilamentVolume)
 
cursor.commit()
