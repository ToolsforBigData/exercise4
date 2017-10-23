import sqlite3
# Connect to the database
conn = sqlite3.connect("data/northwind.db")
# For encoding purpose.
#conn.text_factory = lambda x: str(x, 'latin1')
#Cursor to the Northwind database
cursor = conn.cursor()


#------------Exercise 4_1 ------------#

#The query. The inner select query combines on natural join the relationship 
# table Order-Details and the Table Order based on the customerID = ALFKI 
# The outer select query combines on natural join the Products table with 
# the result on the inner select query and displays the columns results
# OrderID, ProductID and ProductName.
query = """SELECT E.LastName, T.TerritoryDescription
			FROM Employees as E
			LEFT JOIN EmployeeTerritories as ET
			on E.EmployeeID = ET.EmployeeID
			LEFT JOIN Territories as T
			on ET.TerritoryID = T.TerritoryID
			order by T.TerritoryDescription desc
			"""

# Run the query and enter the where clause statement with ALFKI
cursor.execute(query,[])

#Fetching the results
data = cursor.fetchall()
# Print the result.
print('Results for exercise 4.3 - (a)')
print('Territory - LastName \n--------------------' )
for col1,col2, in data:
	print('{} - {}'.format(col2.rstrip(),col1.rstrip()))


#------------Exercise 4_2 ------------#
#
#Similar query as above only we look for those order ids 
# that have distinct products 2 or above.
query2 = """
		SELECT E.EmployeeID, E.LastName, T.TerritoryDescription
			FROM Employees as E
			LEFT JOIN EmployeeTerritories as ET
			on E.EmployeeID = ET.EmployeeID
			LEFT JOIN Territories as T
			on ET.TerritoryID = T.TerritoryID 
			where T.TerritoryDescription LIKE 'B%'
			order by T.TerritoryDescription desc
		"""
# Run the query and enter the where clause statement with ALFKI
cursor.execute(query2,[])

#Fetching the results
data = cursor.fetchall()
# Print the result.
print('\nResults for exercise 4.3 - (b)')
print('EmployeeID - LastName - Territory')
for col1,col2,col3 in data:
	print('{} - {} - {}'.format(col1,col2,col3))



