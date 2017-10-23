import sqlite3
# Connect to the database
conn = sqlite3.connect("data/northwind.db")
# For encoding purpose.
conn.text_factory = lambda x: str(x, 'latin1')
#Cursor to the Northwind database
cursor = conn.cursor()


#------------Exercise 4_1 ------------#

#The query. The inner select query combines on natural join the relationship 
# table Order-Details and the Table Order based on the customerID = ALFKI 
# The outer select query combines on natural join the Products table with 
# the result on the inner select query and displays the columns results
# OrderID, ProductID and ProductName.
query = """SELECT o.OrderID, p.ProductID, ProductName
			FROM Products p
			NATURAL JOIN [Order Details] od
			NATURAL JOIN [Orders] o
			WHERE o.CustomerID = ?
			"""

# Run the query and enter the where clause statement with ALFKI
cursor.execute(query,[("ALFKI")])

#Fetching the results
data = cursor.fetchall()
# Print the result.
print('Results for exercise 4.1 - all products for ALFKI')
print('OrderID\t\tProductID\tProductName')
for col1,col2,col3 in data:
	print('{}\t\t{}\t\t{}'.format(col1,col2,col3))


#------------Exercise 4_2 ------------#
#
#Similar query as above only we look for those order ids 
# that have distinct products 2 or above.
query2 = """SELECT o.OrderID, p.ProductID, ProductName
			FROM Products p
			NATURAL JOIN [Order Details] od
			NATURAL JOIN [Orders] o
			WHERE o.OrderID IN (
				SELECT o.OrderId
				FROM Products p
				NATURAL JOIN [Order Details] od
				NATURAL JOIN [Orders] o
				WHERE o.CustomerID = ?
				GROUP BY o.OrderID
				HAVING count(distinct od.ProductID) >= 2
			)
		"""
# Run the query and enter the where clause statement with ALFKI
cursor.execute(query2,[("ALFKI")])

#Fetching the results
data = cursor.fetchall()
# Print the result.
print('\nResults for exercise 4.2 - contain at least 2 different products')
print('OrderID\t\tProductID\tProductName')
for col1,col2,col3 in data:
	print('{}\t\t{}\t\t{}'.format(col1,col2,col3))