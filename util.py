# -*-coding:utf-8-*-

import datetime, sqlite3, pdb
from pandas import DataFrame

def print_attr(x):
    for item in x.__dict__:
        print item

def print_dict(x):
	for (k,v) in x.items():
		print k,v

def get_today_tup():	
	d = datetime.date.today()			
	yy = str(d.year)
	mm = str(d.month)
	dd = str(d.day)
	# return a tuple of string to build url
	return (yy, mm.zfill(2), dd.zfill(2))
	
def next_day(x):
	# x is a date tuple, its element can be int or str
	# make sure your tuple is available
	d = datetime.date( int(x[0]), int(x[1]), int(x[2]))
	d += datetime.timedelta(days = 1)
	yy = str(d.year)
	mm = str(d.month)
	dd = str(d.day)

	# return a tuple of string to build url
	return (yy, mm.zfill(2), dd.zfill(2))

def next_week(x):
	# x is a date tuple, its element can be int or str
	# make sure your tuple is available
	d = datetime.date( int(x[0]), int(x[1]), int(x[2]))
	for i in range(0,7):
		d += datetime.timedelta(days = 1)
	yy = str(d.year)
	mm = str(d.month)
	dd = str(d.day)

	# return a tuple of string to build url
	return (yy, mm.zfill(2), dd.zfill(2))

def weekday(x):
	# x is a date tuple, its element can be int or str
	# make sure your tuple is available
	d = datetime.date( int(x[0]), int(x[1]), int(x[2]))
	return d.weekday()

def sql2csv(csv_writer, table_name, conn):
	# conn = sqlite3.connect(full_db_name)  
	# with open('test.csv','wb') as csv_file:
	# 	 csv_writer = csv.writer(csv_file)
		
	cs_all = conn.cursor()
	cs_all.execute("select * from %s" % table_name) 

	csv_writer.writerow([i[0] for i in cs_all.description])
	#pdb.set_trace()
	csv_writer.writerows(cs_all)
	csv_writer.writerow(" ")

	cs_all.close()

def sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = []):
	# conn = sqlite3.connect(full_db_name)  
	# with open('test.csv','wb') as csv_file:
	# 	 csv_writer = csv.writer(csv_file)
	
	cs_all = conn.cursor()
	cs_all.execute("select * from %s" % table_name) 

	csv_writer.writerow([i[0] for i in cs_all.description])

	list_of_tuple = cs_all.fetchall()
	list_of_list = [list(x) for x in list_of_tuple]
	
	def encode2printcsv(line, col_of_chn):
		for i in col_of_chn:
			#if isinstance(line[i],unicode)
			line[i] = line[i].encode('utf-8')
		return line

	list_of_encode_list = [encode2printcsv(x, col_of_chn) for x in list_of_list]
	for x in list_of_encode_list:		
		#print x	
		csv_writer.writerow(x)
		
	csv_writer.writerow(" ")
	cs_all.close()

def encode_list_write_csv(filename, list_of_list, col_of_chn):
	if isinstance(list_of_list, DataFrame):
			list_of_list = list_of_list.values.tolist()
	with open(filename, 'w', ) as f:
		csv_writer = csv.writer(f)
		def encode2printcsv(line, col_of_chn):
			for i in col_of_chn:
				#if isinstance(line[i],unicode)
				line[i] = line[i].encode('utf-8')
			return line

		list_of_encode_list = [encode2printcsv(x, col_of_chn) for x in list_of_list]
		for x in list_of_encode_list:
			csv_writer.writerow(x)

def createTABLE_URL(conn ):
	sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
			remark char)'''
	conn.execute( sql ) 
	conn.commit()

def getVisitedURL(conn):
	cs = conn.cursor()
	sql = 'select url from TABLE_URL'
	cs.execute(sql)
	visited_url = cs.fetchall()
	visited_url = [x[0] for x in visited_url]
	return visited_url

def get_URL_status(conn, url):
	cs = conn.cursor()
	sql = 'select remark from TABLE_URL where url = "%s"' % url
	cs.execute(sql)
	data = cs.fetchall()
	data = [x[0] for x in data]
	return data[0]

def rootPath():
	# you can set your own databse root path
	import os
	#s = '/home/dongcheng/my_workspace/spider_ce_data/'
	s = '/tmp/ccpm/data/'
	if not os.path.exists(s):
		os.makedirs(s)
	return s

def midfilePath():
	# you can set your own databse root path
	import os
	s = rootPath() + 'mid_file/'
	if not os.path.exists(s):
		os.makedirs(s)
	return s

if __name__ == '__main__':
	# date = datetime.date(2016, 7,7)
	# for i in range(5):
	# 	date += datetime.timedelta(days = 1)
	# 	print date
		
	# 	d = str(date.day)		
	# 	print date.year, date.month, d.zfill(2)
	#-----------------------
	# x = (2016,7,7)
	# for i in range(0,5):
	# 	print x, weekday(x)
	# 	x = next_day(x)
	#-----------------------
	# s1 = 'abcdefg'
	# s2 = 'cde'
	# print s1.find(s2)
	# print s2.find('haha')
	#-----------------------
	root_path = '/home/dongcheng/my_workspace/ccdata/data/'
	ce_name = 'cffex/'
	db_name = 'daily_info.db'
	full_db_name = root_path+ce_name+db_name

	conn = sqlite3.connect(full_db_name)  
	cs = conn.cursor() 	
	
	table_name = 'TABLE_TOTAL'
	product_id = 'TF      '
	sql = "SELECT tradingday, VOLUME FROM TABLE_TOTAL WHERE productid ='"  + product_id + "'"  
	cs.execute(sql)  
	result = (cs.fetchall())
	#print type(result)
	print len(result)
	
	#print d

	import pandas as pd
	import matplotlib.pyplot as plt
	df = pd.DataFrame(result)
	df.columns = ['date', 'volume']
	print df.head()
	df['date'] = pd.to_datetime(d)
	plot_idx = df.set_index('date')
	plot_idx['volume'].plot()
	#plt.legend(loc='what')
	plt.show()

	company_list = [u'银河期货',u'国泰君安',u'永安期货',u'五矿经易',
u'海通期货',u'中信期货',u'中国国际',u'广发期货',
u'鲁证期货',u'东证期货',u'光大期货',

u'弘业期货',u'中粮期货',u'华泰期货',u'广永期货',u'申银万国',
u'混沌天成',u'招商期货',u'国元期货',u'国信期货',u'金瑞期货',
u'中融汇信',u'天津金谷',u'山金期货',

u'国投中谷',u'格林大华',u'大有期货',u'渤海期货',u'宏源期货',
u'国贸期货',u'南华期货',u'锦泰期货',u'华信万达',u'浙商期货',
u'海航东银',u'安粮期货',u'信达期货',u'财达期货',u'英大期货',
u'兴业期货',u'东海期货',u'国海良时',u'东吴期货',u'西南期货',
u'兴证期货',u'摩根大通',u'东航期货',u'国联期货',u'建信期货',u'民生期货',

u'中信建投',u'中大期货',u'中银国际',u'广州期货',u'方正中期',
u'中原期货',u'和合期货',u'迈科期货',u'华融期货',u'东兴期货',
u'长江期货',u'瑞达期货',u'中投天琪',u'西部期货',u'宝城期货',
u'上海中期',u'华闻期货',u'华西期货',u'平安期货',u'中航期货',
u'中钢期货',u'大地期货',u'新湖期货',u'中国中金',u'倍特期货',
u'金友期货',u'华安期货',u'上海浙石',u'北京首创',u'长安期货',
u'国都期货',u'华鑫期货',u'红塔期货',u'首创京都']