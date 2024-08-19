# This code is provided for you to check that you can connect to your remote database and then carry out a simple python command.
# The code relates to the set up tutorial in week 1

import pyodbc

server = 'tcp:mcruebs04.isad.isadroot.ex.ac.uk' 
database = 'BEM2040_YOURDATABASENAME'
username = 'YOURUSERNAME' 
password = 'YOURPASSWORD'

# Driver for own machine.  Comment out when on windows machine.
# serverstring = 'DRIVER={/opt/homebrew/cellar/msodbcsql18/18.1.2.1/lib/libmsodbcsql.18.dylib};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;Encrypt=no;'

# print(serverstring)

# cnxn = pyodbc.connect(serverstring)

cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;Encrypt=no;')

cursor = cnxn.cursor()

print('Reading data from table')
tsql = 'SELECT * FROM Student'
with cursor.execute(tsql):
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + ' '+str(row[1]))
        row = cursor.fetchone()
