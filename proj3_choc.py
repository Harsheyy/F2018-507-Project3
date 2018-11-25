import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = "DROP TABLE IF EXISTS 'Countries';"
cur.execute(statement)

statement = "DROP TABLE IF EXISTS 'Bars';"
cur.execute(statement)

statement = '''
	CREATE TABLE 'Countries' (
		'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
		'Alpha2' VARCHAR(128) NOT NULL,
		'Alpha3' VARCHAR(128) NOT NULL,
		'EnglishName' VARCHAR(128) NOT NULL,
		'Region' VARCHAR(128) NOT NULL,
		'Subregion' VARCHAR(128) NOT NULL,
		'Population' INT NOT NULL,
		'Area' REAL);'''
cur.execute(statement)	

statement = '''
	CREATE TABLE 'Bars' (
		'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
		'Company' VARCHAR(128) NOT NULL,
		'SpecificBeanBarName' VARCHAR(128) NOT NULL,
		'REF' VARCHAR(128) NOT NULL,
		'ReviewDate' VARCHAR(128) NOT NULL,
		'CocoaPercent' REAL,
		'CompanyLocationId' INT NOT NULL,
		'Rating' REAL,
		'BeanType' VARCHAR(128) NOT NULL,
		'BroadBeanOriginId' INT,
		FOREIGN KEY(CompanyLocationId) REFERENCES Countries(Id),
		FOREIGN KEY(BroadBeanOriginId) REFERENCES Countries(Id));'''
cur.execute(statement)	

with open(COUNTRIESJSON) as file:
    data = json.load(file)

for countries in data:
	name = countries['name']
	alpha2 = countries['alpha2Code']
	alpha3 = countries['alpha3Code']
	region = countries['region']
	subregion = countries['subregion']
	population = countries['population']
	area = countries['area']
	vals = (alpha2,alpha3,name,region,subregion,population,area)
	#print(vals)
	cur.execute("INSERT INTO Countries(Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)", vals)

f = open(BARSCSV,'r')
reader = csv.reader(f)
next(reader)
for row in reader:
	cocoap = row[4].split('%')
	row[4] = cocoap[0]
	cur.execute("INSERT INTO Bars(Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

#cur.execute('UPDATE Bars SET CompanyLocationId = (SELECT Id FROM Countries WHERE EnglishName = Bars.CompanyLocationId)')

conn.commit()
conn.close()
# Part 2: Implement logic to process user commands
def process_command(command):
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()
	parse = command.split()
	main = parse[0]
	params = []
	for items in range(1, len(parse)):
		params.append(parse[items])

	#Globals
	statement = ''
	area_desc = ''
	area = ''
	country_name = ''
	region_name = ''
	sortBy = 'Rating'
	sell_source = 'CompanyLocationId'
	topBot = 'top'
	limit = '10'
	fetch_alot = False
	invalid = False

	if(main == 'bars'):
		for i in range(0, len(params)):
			if(len(params[i].split('=')) == 1):
				if(params[i] == 'ratings'): 
					sortBy = 'Rating'
				elif(params[i] == 'cocoa'):
					sortBy = 'CocoaPercent'
				else:
					print('invalid command')
					invalid = True
			else:
				temp = params[i].split('=')
				if(temp[0] == 'sellcountry' or temp[0] == 'sourcecountry' or temp[0] == 'sellregion' or temp[0] == 'sourceregion'):	
					if(temp[0] == 'sellcountry' or temp[0] == 'sourcecountry'):
						statement = 'SELECT EnglishName FROM Countries WHERE Alpha2 = "' + str(temp[1]) + '"'
						cur.execute(statement)
						country_name = cur.fetchone()
						fetch_alot = False
					if(temp[0] == 'sellregion' or temp[0] == 'sourceregion'):
						statement = 'SELECT EnglishName FROM Countries WHERE Region = "' + str(temp[1]) + '" OR Subregion = "' + str(temp[1]) + '"'
						cur.execute(statement)
						country_name = cur.fetchall()
						fetch_alot = True

					if(temp[0] == 'sellcountry' or temp[0] == 'sellregion'):
						area_desc = 'CompanyLocationId'
					elif(temp[0] == 'sourcecountry' or temp[0] == 'sourceregion'):
						area_desc = 'BroadBeanOriginId'

				elif(temp[0] == 'top' or temp[0] == 'bottom'):
					topBot = temp[0]
					limit = temp[1]
				else:
					print("invalid command")
					invalid = True

		statement = 'SELECT SpecificBeanBarName, Company, CompanyLocationId, Rating, CocoaPercent, BroadBeanOriginId FROM Bars'
		if(country_name != '' and fetch_alot == False):
			statement += ' WHERE '+ str(area_desc) +' = "'+ str(country_name[0]) + '"'
		elif(country_name != '' and fetch_alot == True):
			statement += ' WHERE '+ str(area_desc) +' = "'+ str(country_name[0][0]) + '"'
			for i in range(1, len(country_name)):
				statement += 'OR '+ str(area_desc) +' = "'+ str(country_name[i][0]) + '"'
		
		if(topBot == 'top'):
			statement += ' ORDER BY ' + str(sortBy) + ' DESC LIMIT ' + str(limit)
		elif(topBot == 'bottom'):
			statement += ' ORDER BY ' + str(sortBy) + ' ASC LIMIT ' + str(limit)

	elif(main == 'companies'):
		for i in range(0, len(params)):
			if(len(params[i].split('=')) == 1):
				if(params[i] == 'ratings'):
					sortBy = 'AVG(Rating)'
				elif(params[i] == 'cocoa'):					
					sortBy = 'AVG(CocoaPercent)'
				elif(params[i] == 'bars_sold'):
					sortBy = 'COUNT(SpecificBeanBarName)'
				else:
					print('invalid command')
					invalid = True
			else:
				temp = params[i].split('=')
				if(temp[0] == 'country' or temp[0] == 'region'):
					if(temp[0] == 'country'):
						statement = 'SELECT EnglishName FROM Countries WHERE Alpha2 = "' + str(temp[1]) + '"'
						cur.execute(statement)
						country_name = cur.fetchone()
						fetch_alot = False
					if(temp[0] == 'region'):
						statement = 'SELECT EnglishName FROM Countries WHERE Region = "' + str(temp[1]) + '" OR Subregion = "' + str(temp[1]) + '"'
						cur.execute(statement)
						country_name = cur.fetchall()
						fetch_alot = True

				elif(temp[0] == 'top' or temp[0] == 'bottom'):
					topBot = temp[0]
					limit = temp[1]
				else:
					print("invalid command")
					invalid = True

		statement = 'SELECT Company, CompanyLocationId, ' + str(sortBy) + ' FROM Bars'
		if(country_name != '' and fetch_alot == False):
			statement += ' WHERE CompanyLocationId = "'+ str(country_name[0]) + '"'
		elif(country_name != '' and fetch_alot == True):
			statement += ' WHERE CompanyLocationId = "'+ str(country_name[0][0]) + '"'
			for i in range(1, len(country_name)):
				statement += ' OR CompanyLocationId = "'+ str(country_name[i][0]) + '"'

		if(topBot == 'top'):
			statement += ' GROUP BY Company HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' DESC LIMIT ' + str(limit)
		elif(topBot == 'bottom'):
			statement += ' GROUP BY Company HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' ASC LIMIT ' + str(limit)

	elif(main == 'countries'):
		for i in range(0, len(params)):
			if(len(params[i].split('=')) == 1):
				if(params[i] == 'ratings'):
					sortBy = 'AVG(Rating)'
				elif(params[i] == 'cocoa'):
					sortBy = 'AVG(CocoaPercent)'
				elif(params[i] == 'bars_sold'):
					sortBy = 'COUNT(SpecificBeanBarName)'
				elif(params[i] == 'sellers'):
					sell_source = 'CompanyLocationId'
				elif(params[i] == 'sources'):
					sell_source = 'BroadBeanOriginId'
				else:
					print('invalid command')
					invalid = True
			else:
				temp = params[i].split('=')
				if(temp[0] == 'region'):
					region_name = temp[1]
				elif(temp[0] == 'top' or temp[0] == 'bottom'):
					topBot = temp[0]
					limit = temp[1]
				else:
					print("invalid command")
					invalid = True

		statement = 'SELECT '+ str(sell_source) + ', Region, ' + str(sortBy) + ' FROM Bars JOIN Countries WHERE ' +str(sell_source)+ ' = Countries.EnglishName'
		if(region_name != ''):
			statement += ' AND Countries.Region = "'+ str(region_name) + '"'

		if(topBot == 'top'):
			statement += ' GROUP BY '+ str(sell_source) +' HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' DESC LIMIT ' + str(limit)
		elif(topBot == 'bottom'):
			statement += ' GROUP BY '+ str(sell_source) +' HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' ASC LIMIT ' + str(limit)

	elif(main == 'regions'):
		for i in range(0, len(params)):
			if(len(params[i].split('=')) == 1):
				if(params[i] == 'ratings'):
					sortBy = 'AVG(Rating)'
				elif(params[i] == 'cocoa'):
					sortBy = 'AVG(CocoaPercent)'
				elif(params[i] == 'bars_sold'):
					sortBy = 'COUNT(SpecificBeanBarName)'
				elif(params[i] == 'sellers'):
					sell_source = 'CompanyLocationId'
				elif(params[i] == 'sources'):
					sell_source = 'BroadBeanOriginId'
				else:
					print('invalid command')
					invalid = True
			else:
				temp = params[i].split('=')
				if(temp[0] == 'top' or temp[0] == 'bottom'):
					topBot = temp[0]
					limit = temp[1]
				else:
					print("invalid command")
					invalid = True

		statement = 'SELECT Region, ' + str(sortBy) + ' FROM Bars JOIN Countries WHERE ' +str(sell_source)+ ' = Countries.EnglishName'
		if(topBot == 'top'):
			statement += ' GROUP BY Region HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' DESC LIMIT ' + str(limit)
		elif(topBot == 'bottom'):
			statement += ' GROUP BY Region HAVING COUNT(SpecificBeanBarName) > 4 ORDER BY ' + str(sortBy) + ' ASC LIMIT ' + str(limit)

	elif(main == 'exit'):
		print("Bye!")
	else:
		print('invalid command')
		invalid = True

	#print(statement)
	cur.execute(statement)
	results = cur.fetchall()
	conn.commit()
	conn.close()
	if(invalid == True):
		return 0

	return results

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'help':
        	print(help_text)
        	continue
        results = process_command(response)

        '''response2 = response.split()
        if(response2[0] == 'bars'):
       		for items in results:
       			print(str(items[0]).strip()+'\t'+str(items[1]).strip()+'\t'+str(items[2]).strip()+'\t'+str(items[3]).strip()+'\t'+str(items[4]).strip()+'\t'+str(items[5]).strip())
       	elif(response2[0] == 'companies'):
       		for items in results:
       			print(str(items[0]).strip()+'\t'+str(items[1]).strip()+'\t'+str(items[2]).strip())
       	elif(response2[0] == 'countries'):
       		for items in results:
       			print(str(items[0]).strip()+'\t'+str(items[1]).strip()+'\t'+str(items[2]).strip())
       	elif(response2[0] == 'regions'):
       		for items in results:
       			print(str(items[0]).strip()+'\t'+str(items[1]).strip())'''
        print('')

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
	invalid = False
	interactive_prompt()
    
    
