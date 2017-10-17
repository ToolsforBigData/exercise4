import pymongo
# pprint library is used to make the output look more pretty
from pprint import pprint

# 1. Connect to MongoDB instance running on localhost
connection = pymongo.MongoClient('localhost', 27017)
# Connect to the Northwind database
db = connection['Northwind']

# Print the collections available --> 
# print(db.collection_names(include_system_collections=False))

#----------------------Ex 4.1 ----------------------------#

# Using pipelines and the aggregate function to create one match stage,
# two lookup and one porject stage in the pipelines
result = db.orders.aggregate([
	{ "$match":
		{"CustomerID": "ALFKI"}
	},
	{ "$lookup":
		{ "from":"order-details", 
		  "localField":"OrderID", 
		  "foreignField":"OrderID",
		  "as":"OrderJoinDetails"
		}
	},
	{ "$lookup":
		{ "from":"products", 
		  "localField":"OrderJoinDetails.ProductID",
		  "foreignField":"ProductID",
		  "as":"OrdProd"
		 }
	},
	{ "$project":
		{ "OrderJoinDetails.OrderID":1,
		  "OrdProd.ProductID":1,
		  "OrdProd.ProductName":1,
		  "_id":0
		} 
	}
])

#Printing the results
print('Results for exercise 4.1 - all products for ALFKI')
print('OrderID \t ProductID \t ProductName')
for order in result:

    ord_id = order['OrderJoinDetails'][0]['OrderID']

    for orderprod in order['OrdProd']:
    	prod_id = orderprod['ProductID']
    	prod_name = orderprod['ProductName']
    	print('{} \t\t {} \t\t {}'.format(ord_id,prod_id,prod_name.encode('utf-8')))
    print('----- \t\t -- \t\t ---------')


#----------------------Ex 4.2 ----------------------------#
# Using pipelines and the aggregate function to create two match stage,
# two lookup and one porject stage in the pipelines
result2 = db.orders.aggregate([
	{ "$match":
		{"CustomerID": "ALFKI"}
	},
	{ "$lookup":
		{ "from":"order-details",
		  "localField":"OrderID",
		  "foreignField":"OrderID",
		  "as":"OrderJoinDetails"
		}
	},
	{ "$lookup":
		{ "from":"products",
		  "localField":"OrderJoinDetails.ProductID", 
		  "foreignField":"ProductID",
		  "as":"OrdProd"
		}
	},
	{ "$project":
		{"OrderJoinDetails.OrderID":1, 
		 "OrdProd.ProductID":1,
		 "OrdProd.ProductName":1,
		 "Pro_count": { "$size": {"$setUnion":[[],"$OrdProd.ProductName"]}},
		 "_id":0
		}
	},
	{ "$match": {"Pro_count": { "$gte": 2 }}}
])

#Printing the results
print('\nResults for exercise 4.2 - contain at least 2 different products')
print('OrderID \t ProductID \t ProductName')
for order in result2:

    ord_id = order['OrderJoinDetails'][0]['OrderID']

    for orderprod in order['OrdProd']:
    	prod_id = orderprod['ProductID']
    	prod_name = orderprod['ProductName']
    	print('{} \t\t {} \t\t {}'.format(ord_id,prod_id,prod_name.encode('utf-8')))
    print('----- \t\t -- \t\t ---------')