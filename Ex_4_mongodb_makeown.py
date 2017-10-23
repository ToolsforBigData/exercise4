import pymongo
# pprint library is used to make the output look more pretty
from pprint import pprint

# 1. Connect to MongoDB instance running on localhost
connection = pymongo.MongoClient('localhost', 27017)
# Connect to the Northwind database
db = connection['Northwind']

#----------------------Ex 4.3 (a)----------------------------#

# Using pipelines and the aggregate function to create one match stage,
# two lookup and one porject stage in the pipelines
result = db['employee-territories'].aggregate([
	{ "$lookup":
		{ "from":"territories", 
		  "localField":"TerritoryID", 
		  "foreignField":"TerritoryID",
		  "as":"Terr_Join"
		}
	},
	{ "$lookup":
		{ "from":"employees", 
		  "localField":"EmployeeID",
		  "foreignField":"EmployeeID",
		  "as":"TerrProd"
		 }
	},
	{ "$sort": 
		{ "Terr_Join.TerritoryDescription": -1} 
	},
	{ "$project":
		{ "Terr_Join.TerritoryDescription":1,
		  "TerrProd.LastName":1,
		  "_id":0
		} 
	}

])

#Printing the results
print('Results for exercise 4.3 - (a)')
print('Territory - LastName\n--------------------')
for territory in result:
	terrname = territory['Terr_Join'][0]['TerritoryDescription']
	for terrprod in territory['TerrProd']:
		last_name = terrprod['LastName']
 		print('{} - {}'.format(terrname, last_name.encode('utf-8')))

#----------------------Ex 4.3 (b)----------------------------#
#
## Using pipelines and the aggregate function to create one match stage,
# two lookup and one porject stage in the pipelines
result2 = db['employee-territories'].aggregate([
	{ "$lookup":
		{ "from":"territories", 
		  "localField":"TerritoryID", 
		  "foreignField":"TerritoryID",
		  "as":"Terr_Join"
		}
	},
	{ "$lookup":
		{ "from":"employees", 
		  "localField":"EmployeeID",
		  "foreignField":"EmployeeID",
		  "as":"TerrProd"
		 }
	},
	{ "$sort": 
		{ "Terr_Join.TerritoryDescription": -1} 
	},
	{ "$project":
		{ "Terr_Join.TerritoryDescription":1,
		  "TerrProd.LastName":1,
		  "TerrProd.EmployeeID":1,
		  "_id":0
		} 
	},
	{"$match":
		{ "Terr_Join.TerritoryDescription":  {'$regex': '^B'} }
	}
])

#Printing the results
print('\nResults for exercise 4.3 - (b)')
print('EmployeeID - LastName - Territory\n--------------------')
for territory in result2:
	terrname = territory['Terr_Join'][0]['TerritoryDescription']
	empid = territory['TerrProd'][0]['EmployeeID']

	for terrprod in territory['TerrProd']:
		last_name = terrprod['LastName']
		print('{} - {} - {}'.format(empid, last_name.encode('utf-8'),terrname))

