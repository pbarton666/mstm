"""generate_carl_tables.py

NB:  This code is horrible and NEEDS to be refactored before any re-use (it done in a hurry, etc.).

TODO:

- Field names (column) names should come from the database, not the input files.  This is not robust if 
  the database fails to contain all the intended trip/cost pairs.  A series of assert statements screen for this
  possibility, but it's far from the ideal.  The table names should also come from the database - presently
  they're hard-coded to match the current incarnation of the map_file.csv, the setup file specified with the
  current incarnation of mappings.py.
  
  -The if __name__='__main__' block needs to be refactored, moving ensuring that the output file names can
   come from an input file and the SQL documentation file production is integrated with the data output
   production.
  
  -Beyond the assert statements there are not tests.  'Plan A' should to build them into test_all.py.

"""
from  database import *
import psycopg2
import csv
import shutil
import os


def sum_costs(test=False):
	sql_file=open("for_the_record_sql_statements_used.txt", 'w')
	if test:
		from test_all import scenarios, map_file, DB
	else:
		from mappings import scenarios, map_file
		from mappings import DB 



	#grab all the data tables from the map file
	reader=csv.DictReader(open(map_file))
	cs_tables=set()
	totals_tables=set()
	all_cols=set()
	
	

	for line in reader:
		cs_tables.add(line['dbtable'])
		totals_tables.add(line['rollup_dbtable'])
		all_cols.add(line["aggregate_to"])

	sorted_cols=sorted(all_cols)

	#make a db connection (let main app create the db if needed)
	conn = psycopg2.connect(database = DB,
		                    user=login_info['user'],
		                    password=login_info['password']
		                    )

	curs = conn.cursor() 

	for s in scenarios[1:]:
		name=s['name']
		#print('\nCosts deltas for {}: (relative to the "no highway, no Red Line" case)\n'.format(name))
		for table_set in [cs_tables, totals_tables]:
			
			
			#Tables all begin with income, like "inc1"
			sorted_tables = sorted(table_set)
			table_set_name = "_".join(sorted_tables[0].split("_")[1:])
			#so do column names
			cols_inc1=[]
			cols_inc2=[]
			cols_inc3=[]
			cols_inc4=[]
			cols_inc5=[]
			
			for col in sorted_cols: #create ordered column lists
				if 'inc1' in col.lower(): cols_inc1.append(col)
				if 'inc2'in  col.lower(): cols_inc2.append(col)
				if 'inc3' in col.lower(): cols_inc3.append(col)
				if 'inc4' in col.lower(): cols_inc4.append(col)
				if 'inc5' in col.lower(): cols_inc5.append(col)		
		
			cols_list=[cols_inc1, cols_inc2, cols_inc3, cols_inc4, cols_inc5]
			
	
			for costix in range(len(cols_inc1)):
				sql="SELECT\n"
				sql+="\tt1.zone,\n"			
				this_col=cols_inc1[costix]
				
				sql+="\tt1.{},\n".format( cols_inc1[costix])
				sql+="\tt2.{},\n".format( cols_inc2[costix])
				sql+="\tt3.{},\n".format( cols_inc3[costix])
				sql+="\tt4.{},\n".format( cols_inc4[costix])
				sql+="\tt5.{},\n".format( cols_inc5[costix])
				
				#check integrity
				target = "_".join(cols_inc1[costix].split("_")[1:])
				assert target in cols_inc2[costix]
				assert target in cols_inc3[costix]
				assert target in cols_inc4[costix]
				assert target in cols_inc5[costix]
				sql=sql[:-2] +'\n'
				
				#print(sql)
					
				sql+="FROM\n"
				for cix in range(len(sorted_tables)):
					sql+="\t{}_{}   {},\n".format(name,  sorted_tables[cix],  't' + str(cix+1))
				sql=sql[:-2]	+'\n'
				#print(sql)
				
				sql+="WHERE\n"
				for cix in range(len(sorted_tables)):
					sql+="\tt{}.zone=t1.zone AND\n".format(cix+1)
				sql=sql[:-4]+"\n"
				
				sql+="ORDER BY t1.zone"
				
				print(sql)
				curs.execute(sql)	
				
				result=curs.fetchall()
				
				fn=name+"_"+"_".join(this_col.split("_")[1:] ) + "_"+table_set_name+'.csv'
				print(fn)
				with open(os.path.join(outdir,fn), 'w') as ofile:
					writer=csv.writer(ofile)
					writer.writerow(['zone', 'inc1', 'inc2', 'inc3', 'inc4', 'inc5'])
					writer.writerows(result)
				
				sql_file.write('{}:\n\n'.format(fn))
				sql_file.write('{}\n\n=====================\n\n'.format(sql))
	
				#print(sql)

	sql_file.close()
	
if __name__=='__main__':
	test=True
	outdir='scratch_outdir'
	if test:
		try:
			shutil.rmtree(outdir)
			os.mkdir(outdir)
			
		except:
			pass
		#os.mkdir(outdir)			
	else:		
		#outdir='/home/pat/Dropbox/Maryland_TDM_RedLine_ProjectShare/Cost_Benefit_Tables'
		#outdir='myout'
		pass

	sum_costs(test=False)
