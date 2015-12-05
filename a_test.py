import sys
import numpy as np
import unittest

import analyze_main
import psycopg2
from database import *

class Tests(unittest.TestCase):
	def test_cost_array_math(self):
		"test if the basic cost calculations are working, both for 'straight-up' and transposed trips matrices"
		
		#create some test arrays
		base_trips=np.array(([  [ 0,   1,    2,   3],  
				                                      [1,   11, 12, 13],      
				                                      [2,   21, 22, 23],       
				                                      [3,   31, 32, 33]   ]))
		base_costs=np.array(([   
				                                    [0,    1,    2,    3], 
				                                    [1, .10, .20, .30],   
				                                    [2, .40, .50, .60],  
				                                    [3, .70, .80, .90]    ]))
		
		#We expect the costs to be cell-wise multiplication of base_trips(i,j) * base+_cost(i,j)
		
		transpose=False    #use transposed version of trips file (for return leg)
		hov_adj=0               #multiplies time costs for hov highway trips
				
		#test the get_costs() routine
		costs, trips_used = analyze_main.get_costs(base_costs, base_trips)
		
		#expected values for trips and costs
		exp_trips=np.array(([[11, 12, 13],                              #same as base trips
				                                [21, 22, 23],
				                                [31, 32, 33]]))
		 
		exp_costs=np.array(([[  1.1,   2.4,   3.9],                    #[11*.1     12*.2    13*.3]
				                                    [  8.4,  11. ,  13.8],                  #[21*.4     22*.5    23*.6]
				                                    [ 21.7,  25.6,  29.7]]))            #[31*.7     32*.8    33*.9]
		
		#number of trips actually used and the cell-wise costs shoul dbe as exected	
		self.assertTrue( np.isclose(trips_used, exp_trips).all())
		self.assertTrue( np.isclose(costs, exp_costs).all())
		
		#If we're asking to use a transposed trips array, costs should be base_trips(j, i) * bases_costs(i,j)
		
		#expected values for trips and costs
		exp_trips_T = np.array(([[ 11.,  21.,  31.],
				                                         [ 12.,  22.,  32.],
				                                         [ 13.,  23.,  33.]  ]))
		
		exp_costs_T=np.array(([[  1.1,   4.2,   9.3],                   #[11*.1     21*.2    31*.3]
				                                        [  4.8,  11. ,  19.2],                 #[12*.4     22*.5    32*.6]
				                                        [  9.1,  18.4,  29.7]]))            #[13*.7     23*.8    33*.9]
		
		transpose=True    #use transposed version of trips file (for return leg)
		costs, trips_used = analyze_main.get_costs(base_costs, base_trips, transpose)
		
		#make sure we're really using the transposed trips matrix and the cell-wise costs are right
		self.assertTrue( np.isclose(trips_used, exp_trips_T).all())
		self.assertTrue( np.isclose(costs, exp_costs_T).all())
		
	def test_cons_surplus_calcs(self):
				
		#spin up a couple o'test arrays (same as above)
		base_trips=np.array(([  [ 0,   1,    2,   3],  
				                                      [1,   11, 12, 13],      
				                                      [2,   21, 22, 23],       
				                                      [3,   31, 32, 33]   ]))
		base_cost_per_trip=np.array(([   
				                                    [0,    1,    2,    3], 
				                                    [1, .10, .20, .30],   
				                                    [2, .40, .50, .60],  
				                                    [3, .70, .80, .90]    ]))
		
		#create a test case with triple trips, each at half price		
		test_trips=base_trips*3
		test_cost_per_trip=base_cost_per_trip* 1/2
		
		#We expect the benefits to be a function of cost and quantity, calculated cell-wise
		#              .5 * (base_trips + test_trips)*(base_costs - test_costs)
		
		exp_cs_delta=np.array(([[  1.1,   2.4,   3.9],                    #[.5*(11+11*3)*(.10-.10/2),     .5*(12+12*3)*(.20-.20/2),    .5*(13+13*3)*(.30-.30/2)]
				                                        [  8.4,  11. ,  13.8],                    #[.5*(21+21*3)*(.40-.40/2),     .5*(22+22*3)*(.50-.50/2),    .5*(23+23*3)*(.60-.60/2)]
				                                        [21.7,  25.6,  29.7]]))              #[.5*(31+31*3)*(.70-.70/2),     .5*(32+32*3)*(.80-.80/2),    .5*(33+33*3)*(.90-.90/2)]]
		
		#run get_costs() on both the base and test case - calculates cell-wise trips*cost/trip
		base_costs,  base_trips_used = analyze_main.get_costs(base_cost_per_trip, base_trips)
		test_costs,    test_trips_used = analyze_main.get_costs(test_cost_per_trip, test_trips)
		
		#calculate the consumer surplus delta with get_cs_delta(), cell-wise
		cs_delta = analyze_main.get_cs_delta(base_trips_used, test_trips_used, base_cost_per_trip[1:,1:], test_cost_per_trip[1:,1:])
		
		#do we have the expected values?		
		self.assertTrue( np.isclose(cs_delta, exp_cs_delta).all())
				
		
		#So far so good.  Now, we'll roll up these benefits by Zone - that's our unit of analysis to assign bennies by race.  
				#For each origin Zone, total benefits will be a sum of the benefits derived by trips to each destination.  In other words, the row sum.
								
		exp_benefits_by_zone=np.array(([[   7.4,                        #  1.1 +    2.4    +   3.9
		                                                                        33.2,                       #  8.4 +   11. , + 13.8
		                                                                        77.0]]))                  #  21.7 + 25.6,   29.7
		
		pct_hb = 100   #percent of benefits accruing to originating (home) Zone
		benefits_by_zone=analyze_main.calculate_benefits(cs_delta, pct_hb)
		self.assertTrue( np.isclose(benefits_by_zone, exp_benefits_by_zone).all())
		
		
		#For some trips (highway trips at night or mid-day), only half of the benefits accrue to the originating Zone.  The other half  accrue to the destination Zone.
		#  In these cases, the originating zone gets the benefits of half the trips to the destination (half the row sum).  The destination Zone gets the other half.  To 
		#   operationalize this, we'll give the origin zone half the row sum (each row is an origin); and we'll give the destination zone half the column sum.    Since any zone
		#  can be both an origin and a destination, we'll add up the 'Zone as origin' and 'Zone as destination' values for attirubtion to each zone:
				
		#Or should we count the 'destination' part of this at all?  *Someone* gets the benefit - I'd argue that if Chinese food is delivered to your Zone someone there
		#     benefits from a reduced-cost trip (cheaper food) or less time (hotter food).  On the other hand, the benefit might accue to the origin (lower labor cost
		#     for delivery guy, more repeat business from timely delivery).  Hmmmm.
		
																												#   credit for being origin + credit for being dest
		exp_benefits_by_zone=np.array(([[ 19.3,                        #  (1.1 +    2.4    +   3.9) /2     + (1.1 + 8.4 +3.9) /2         = 1/2 row sum row 1 + 1/2 col sum col 1
		                                                                       36.1,                       #  (8.4 +   11. , + 13.8)  /2     + (2.4 + 11. +13.8) /2      = 1/2 row sum row 2 + 1/2 col sum col 2
		                                                                       62.2]]))                  #  (21.7 + 25.6,   29.7) /2     + (1.1 +26.6 +29.7) /2     = 1/2 row sum row 3 + 1/2 col sum col 3
		
		pct_hb = 50   #percent of benefits accruing to originating (home) Zone
		benefits_by_zone=analyze_main.calculate_benefits(cs_delta, pct_hb)
		self.assertTrue( np.isclose(benefits_by_zone, exp_benefits_by_zone).all())
		
		#make sure that the total benefits are accounted for
		self.assertTrue(np.sum(benefits_by_zone) == np.sum(cs_delta))
				
	
class TestUtils(unittest.TestCase):
	def test_accrete_col_to_np_array(self):
		"tests utility to tack on a new column to a numpy array"
		#add a column to an empty np array
		vector=[1, 2, 3, 4]
		npa = analyze_main.accrete_col_to_np_array(vector=vector, max_index_val=len(vector))
		self.assertTrue ( np.array_equal(npa, np.array(([[ 1.,  1.],  [ 2.,  2.],  [ 3.,  3.],  [ 4.,  4.]])))  )
		#add a new column
		vector=[666,667,668,669]
		npa = analyze_main.accrete_col_to_np_array(npa=npa, vector=vector, max_index_val=len(vector))
		self.assertTrue( np.array_equal( npa, np.array(([[ 1.,  1., 666],  [ 2.,  2., 667],  [ 3.,  3., 668],  [ 4.,  4., 669]])))  )
	
	def test_sum_col_to_np_array(self):
		#test adding a vector the last column of the npa array; first don't provide an array
		vector=[1,2,3,4]
		npa = analyze_main.sum_col_to_np_array(vector=vector, max_index_val=len(vector))
		self.assertTrue( np.array_equal(npa, np.array(([[ 1.,  1.],  [ 2.,  2.],  [ 3.,  3.],  [ 4.,  4.]]))) )
		
		#add the value of a new column
		vector = [10,10,10,10]
		npa = analyze_main.sum_col_to_np_array(npa=npa, vector=vector, max_index_val=len(vector))
		self.assertTrue(  np.array_equal(npa, np.array(([[ 1.,  11.],  [ 2.,  12.],  [ 3.,  13.],  [ 4.,  14.]]))) )
		
	def test_database_updates(self):
		"""makes sure that the database storage routine does all of the following:
		       - creates a new database if none exists
		       - adds new tables as needed
		       - adds new columns as needed and populates them with the right data.
		"""
		db='test_db'
		
		#test1:  will it store/retrieve a single array?
		
		create_new_tables=True
		
		test_data_1=np.array(( [[1,11], [2, 12], [3,13]] ))
		column='col1'
		table='table1'
		
		#create a fake dict with a bit of data	
		arr_dict={}
		arr_dict['test1']={ 'data': test_data_1,
				            'column': column,
				            'table':table
				            }		
		analyze_main.store_data(arr_dict=arr_dict, db = db, create_new_tables=create_new_tables, zones=3)
		
		#make a db connection (let main app create the db if needed)
		conn = psycopg2.connect(database = db,
				                user=login_info['user'],
				                password=login_info['password']
				                )
		
		curs = conn.cursor() 
		
		curs.execute('SELECT * from {}'.format(table))
		conn.commit()
		
		#do we get our single array?
		self.assertTrue( np.isclose(np.array(curs.fetchall()), test_data_1).all())
		
		
		#***Now try it with two columns in one table and two in another:
		test_data_1=np.array(( [[1,11], [2, 12], [3,13]] ))
		test_data_2=np.array(( [[1,110], [2, 120], [3,130]] ))
		
		
		column1='col1'
		column2='col2'
		table1='table1'
		table2='table2'
		
		#create a fake dict 	
		arr_dict={}
		arr_dict['test1']={ 'data': test_data_1,
				            'column': column1,
				            'table':table1
				            }	
		arr_dict['test2']={ 'data': test_data_1,
				            'column': column2,
				            'table':table1
				            }	
		arr_dict['test3']={ 'data': test_data_2,
				            'column': column1,
				            'table':table2
				            }	
		arr_dict['test4']={ 'data': test_data_2,
				            'column': column2,
				            'table':table2
				            }	
		analyze_main.store_data(arr_dict=arr_dict, db = db, create_new_tables=create_new_tables, zones=3)
		
		exp_table1=np.array([(1.0, 11.0, 11.0), (2.0, 12.0, 12.0), (3.0, 13.0, 13.0)])
		exp_table2=np.array([(1.0, 110.0, 110.0), (2.0, 120.0, 120.0), (3.0, 130.0, 130.0)])
		
		curs.execute('SELECT * from {}'.format(table1))
		actual_table1=curs.fetchall()
		
		curs.execute('SELECT * from {}'.format(table2))
		actual_table2=curs.fetchall()
		
		self.assertTrue( np.isclose(exp_table1, actual_table1).all())
		self.assertTrue ( np.isclose(exp_table2, actual_table2).all())


if __name__=='__main__':
	unittest.main()