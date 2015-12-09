
from  database import *
import psycopg2
import csv

def sum_costs(test=False):
		if test:
			from test_all import scenarios, map_file, DB
		else:
		   from mappings import scenarios, map_file
		   from mappings import DB 
		

		
		#grab all the data tables from the map file
		reader=csv.DictReader(open(map_file))
		cs_tables=set()
		totals_tables=set()
		
		for line in reader:
			cs_tables.add(line['dbtable'])
			totals_tables.add(line['rollup_dbtable'])
			
		
		
		#make a db connection (let main app create the db if needed)
		conn = psycopg2.connect(database = DB,
						            user=login_info['user'],
						            password=login_info['password']
						            )
		
		curs = conn.cursor() 
		
		for s in scenarios[1:]:
			name=s['name']
			print('\nCosts deltas for {}: (relative to the "no highway, no Red Line" case)\n'.format(name))
			for table_set in [cs_tables, totals_tables]:
				a=1
				for table in sorted(table_set):
						print('{}'.format(table))
						tname=name+"_"+table.lower()
						sql="SELECT column_name FROM information_schema.columns WHERE table_name ='{}'".format(tname)
						curs.execute(sql)
						cols =curs.fetchall()
						for col_output  in sorted(cols[1:]):
							col=col_output[0]
							sql="SELECT SUM({}) FROM {}".format(col, tname)
							#print(sql)
							curs.execute(sql)
							total=curs.fetchall()
							curs.execute("END")
							print("\t{}\t\t{}".format(col, total[0][0]))
						print()
		
if __name__=='__main__':
				sum_costs(test=False)
