import pandas as pd
import csv




'''----------Violent Crimes Code----------'''

#Read csv into pandas object
data = pd.read_csv("ViolentCrimes-Pre.csv")	

#Create list of columns names		
columns = data.columns.tolist()						

#Convert wide to long, pivoting on year
table =pd.melt(data, id_vars=['Region', 'Division', 'State'], value_vars=columns[3:49], var_name='Year', value_name='Count')	

#Reindex state to the first column index, this is more of a preference
table = table.reindex(columns=['State', 'Region', 'Division', 'Year', 'Count'])

#Create ascending column values
table = table.sort_values(['State', 'Region', 'Division'], ascending=[True, True, True])

#Export to csv
table.to_csv('ViolentCrimes-Post.csv', index=True)	



'''----------School Code----------'''

#Read csv into pandas object
data = pd.read_csv("Real School-Pre.csv")	

#Abstract column headers in to a list
columns = data.columns.tolist()	

#Drop NA values. If not done, then problems arise when melting the data
data = data.dropna()

#Melts the data converting wide to long format. Rearranges years completed.
table =pd.melt(data, id_vars=['Year', 'Total', 'Age Range', 'Sex', 'Median'], value_vars=columns[2:7], var_name='Years Completed', value_name='Amount')	

#Reindex in to the prefered order
table = table.reindex(columns= ['Year','Years Completed', 'Age Range', 'Sex', 'Total', 'Amount', 'Median'])

#Sort values with some ascending and some not
table = table.sort_values(['Year', 'Years Completed', 'Age Range', 'Sex'], ascending=[False, True, True, True])

#Export to csv
table.to_csv('School-Post.csv', index=False)	



'''----------Migration Code----------'''

'''Data was initially split in to two different tables (one for percents and one for numbers) to then be combined in the end'''

#Read csv into pandas object
data = pd.read_csv("GeographicMovementNum-Pre.csv")	

#Create column titles
column_titles = ['Period', 'Population']
rough_columns = data.columns.tolist()	
columns = []

#Clean format of the strings in column headers
for col in rough_columns:
	columns.append(col.replace('\n',''))

#Melt data converting long to wide
table_num =pd.melt(data, id_vars=column_titles, value_vars=columns[2:9], var_name='Move Type', value_name='People (1000s)')	

'''Next Table'''

#Read csv into pandas object
data = pd.read_csv("GeographicMovementPerc-Pre.csv")	

rough_columns = data.columns.tolist()	
columns = []

#Clean format of the strings in column headers
for col in rough_columns:
	columns.append(col.replace('\n',''))

#Melt data converting long to wide
table_perc =pd.melt(data, id_vars=column_titles, value_vars=columns[2:9], var_name='Move Type', value_name='Percentage')	

#Convert serial column to dataframe object
tojoin = table_perc['Percentage'].to_frame()

#final = table_perc.reindex(columns=column_titles)
final_table = table_num.join(tojoin)

#Export to csv
final_table.to_csv('Migration-Post.csv', index=False)	



'''----------Income Code----------'''
'''This code was written before the usage of pandas with our code. It still could be used with similar other data sets'''


current_dolla = []								#Long data of first table. Format: [[row1], [row2],...]
old_dolla = []									#For 2nd table. Both will be iterated over when read in to
years = []
with open('Income-Pre.csv') as csvfile:
	overall = csv.reader(csvfile, delimiter=',')	#Read entire csv file into a 2d list
	count = 0									#Count variable to keep track of what row while it iterates through the overal list

	
	for row in overall:							#Systematically going through each row of our data		
		if count == 4:							#This first conditional and loop extracts the years(column headers) in to a list. It is the 5th row down in the csv file.
			for n in range(1, 66, 2):		#The year is every other column. The for loop runs and captures each year in a list 
				years.append(int(row[n][:4]))

		'''State", "Year", "Money Type", "Median Income", "Standard Error'''   #How the columns will be layed out in long format

		if 6 <= count <= 57 :					#Abstract 1st table
			state = row[0]						#Create new rows that will have the following format: "State", "Year", "Money Type", "Median Income", "Standard Error"
			new_row = [state]					#Each "new_row" created will be the new row in the long data
			year_count = 0						#The counter used to cycle through the years grabbed earlier and listed
			for n in range(1,(len(row))):			#We start at column 1 and not 0 because we already got the state name
				add = None
				if n % 2 == 1:					#Depending on our count of odd or even we know if we are grabbing median income or standard error
					new_row.append(years[year_count])	#add year. 
					new_row.append("Current")			#since we are in the current table, all Money Types will be current
					add = row[n].replace(',', '')	#Median Income
					new_row.append(int(add))
				if n % 2 == 0:	
					add = row[n].replace(',', '')	#Standard Error
					new_row.append(int(add))
					year_count = year_count + 1			#Each time we end our new row after standard error.
					current_dolla.append(new_row)		#add this new row as a list to a greater list of all the current dollars data
					new_row = [state]					#Then we will have to reset our new row as we will continue to the next row

		
		count = count +1

headers = ["State", "Year", "Money Type", "Median Income", "Standard Error"]		#Create headers for the new table


with open("Income-Post.csv", "w") as f:		#Create output for long data
	writer = csv.writer(f, delimiter=',')		#Create writer
	writer.writerow(headers)					#Add headers
	for n in range(len(current_dolla)):			#Iterate through the two long data extracted tables. Write every other.
		writer.writerow(current_dolla[n])
	
'''----------Divorce and Marriage Code--------'''

df = pd.read_csv('Divorces-Pre.csv') #import uncleaned csv file

cols1 = list(df.columns)
cols1 = [str(x)[:5] for x in cols1]

#connect to (or create if doesn’t exists) the SQLite database named db_name.db 
conn = sqlite3.connect("database.db") 
#[connection_name] = sqlite3.connect("[db_name].db") 
engine = create_engine('sqlite:///database.db') #Create engine

df2 = pd.melt(df, id_vars = ['State'], #Set Primary Column
value_vars = ['2011 Divorce Rates', '2011 Marriage Rates', #Enter Values of Variables
'2010 Divorce Rates', '2010 Marriage Rates', 
'2009 Divorce Rates', '2009 Marriage Rates', '2008 Divorce Rates', '2008 Marriage Rates',
'2007 Divorce Rates', '2007 Marriage Rates', '2006 Divorce Rates', '2006 Marriage Rates',
'2005 Divorce Rates', '2005 Marriage Rates', '2004 Divorce Rates', '2004 Marriage Rates',
'2003 Divorce Rates', '2003 Marriage Rates', '2002 Divorce Rates', '2002 Marriage Rates',
'2001 Divorce Rates', '2001 Marriage Rates', '2000 Divorce Rates', '2000 Marriage Rates',
'1999 Divorce Rates', '1999 Marriage Rates', '1995 Divorce Rates', '1995 Marriage Rates',
'1990 Divorce Rates', '1990 Marriage Rates'],
var_name = 'Year', #Set Variable Name
value_name = 'Rate') #Set Name for measure of variable

df2.sort_values(by= ['State', 'Year'], inplace = True, ascending=True)  #Sort values based on State and Year

df2.groupby('State').groups

#print(df2) #Test Print
df2.to_sql("database.db", engine) #converts datafram to SQL database
df2.to_csv('Divorces-Post.csv') #Exports clean file to the file the python program is in
print('File has been cleaned and saved to your Folder')
print(pd.read_sql_table("database.db", engine)) #Test to see database is filled

conn.close()    #close connection 
		


