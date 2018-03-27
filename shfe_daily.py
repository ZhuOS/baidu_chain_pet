# -*-coding:utf-8-*-
import os, sys, pdb
import requests, sqlite3, csv
import util, time

def create_table(conn,flag = '', table_name ='' ):
	cs = conn.cursor()
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s (	pkey char primary key,			
				tradingday DATE, productid char,  instrumentid char, 

				presettlementprice real, openprice real, 	highestprice real,   lowestprice real, 
				closeprice real,         settlementprice real,	change1 real,    change2 real,				
				volume INT ,	openinterest INT, openinterest_change INT)'''  %  table_name
		
		cs.execute(sql)
	if flag == 'init' :
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
		tradingday DATE, productid varchar(10) ,
		volume INT, openinterest INT, openinterest_change INT)''' 
		cs.execute(sql)

		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 
	conn.commit()

#-------------------------------
def string2database_shfe_daily(conn, myDict,  d):
	cs = conn.cursor()
	pid = ''
	dict_of_dict = myDict['o_curinstrument']	
	for dic in dict_of_dict:
		if dic['OPENPRICE'] != '':
			productid = dic['PRODUCTID'].replace('_f','').replace(' ','').upper()
			contrcatid = productid+dic['DELIVERYMONTH']
			pkey = ''.join(d) + '-' + contrcatid
			tmp = [pkey, '-'.join(d), productid, contrcatid,] +[dic['PRESETTLEMENTPRICE'], dic['OPENPRICE'],
				dic['HIGHESTPRICE'], dic['LOWESTPRICE'], dic['CLOSEPRICE'], dic['SETTLEMENTPRICE'],
				dic['ZD1_CHG'], dic['ZD2_CHG'], dic['VOLUME'], dic['OPENINTEREST'], dic['OPENINTERESTCHG']	]
			tb = 'TABLE_' + productid.upper()
			#-----check table exist---------
			sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
			cs.execute(sql)
			if len(cs.fetchall()) == 0:
				create_table(conn, flag = 'item', table_name = tb)	
			#-------------------------------
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?)''' % tb,  tmp) 
			pid = productid
		else:
			if dic["DELIVERYMONTH"] == "小计":
				tb = 'TABLE_TOTAL' 
				pid = dic['PRODUCTID'].replace('_f','').replace(' ','').upper()
				pkey = ''.join(d) + '-' + pid
				tmp = [pkey, '-'.join(d), pid, ] + [dic['VOLUME'], dic['OPENINTEREST'], dic['OPENINTERESTCHG']]
				cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?, ?,?,?)''' % tb,  tmp) 

	conn.commit()	
	return 
#end string 2 database	
#---------------------------------		
def startSpider(conn, url_info):	
	raw_dat_file = util.midfilePath() + 'shfe_daily_raw.dat'
		
	exception_url_file = util.midfilePath() + 'shfe_daily_exception_url.txt'
	f1 = open(exception_url_file, 'w')
	
	cs = conn.cursor()	
	t1 = time.time()
	count = 0
	for (url,d) in url_info:
		count += 1			
		if util.weekday(d) >= 5:				
			remark = 'weekend'
		else:		
			try:
				r = requests.get(url)
			except requests.ConnectionError:
				print r.status_code
				f1.write(url)
				f1.write('\n')
				remark = 'error'				
			else:
				# insert to database, no need to encode								
				with open(raw_dat_file,'wb') as f:
			 		f.write(r.content)
			 	
			 	myDict = r.content
			 	if myDict[:3] == '<!D':
			 		remark = 'holiday'
			 	else:			 		
			 		myDict = eval(myDict)
			 		dict_of_dict = myDict['o_curinstrument']
			 		#print len(dict_of_dict )
			 		if len(dict_of_dict ) == 0:
			 			remark = 'holiday'
			 		else:
			 			remark = 'tradingday'
			 			string2database_shfe_daily(conn, myDict, d)			 	
			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :
			print "---%s seconds---" %(time.time() - t1)	
	f1.close()		
	# end for
#end  startSpider

def getURL_list(conn, end_date):	
	
	#start_date = ('2010', '04', '16')
	start_date = ('2010', '01', '01')
	# end_date   = ('2016', '01', '02')	

	#start_date = ('2016', '07', '06')
	if end_date == '':
		end_date   = ('2016','07','07')
	num1 = int( ''.join(start_date) )
	num2 = int(''.join(end_date))
	if num2 < num1:
		print 'error: invalid end_date, should be bigger'

	visited_url = util.getVisitedURL(conn)
	url_info = []
	d = start_date
	while d != end_date:					
		url = 'http://www.shfe.com.cn/data/dailydata/kx/kx%s.dat' % ''.join(d)
		
		if url in visited_url:
			visited_url_status = util.get_URL_status(conn, url)
			#print visited_url_status
			if visited_url_status == 'error':
				url_info.append((url, d))			
		else:
			url_info.append((url, d))
		d = util.next_day(d)	
	print 'unvisited url :', len(url_info)
	return url_info

def test_shfe_daily(conn):	
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	#pdb.set_trace()
	with open(util.midfilePath() + 'test_shfe_daily.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])

def main_func(end_date = '', see_csv = False):
	ce_name = 'shfe/'
	db_name = 'shfe_daily_info.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  
	create_table(conn, 'init')

	url_info = getURL_list( conn, end_date = end_date )
	startSpider(conn, url_info)

	if see_csv:	
		test_shfe_daily(conn)
	conn.close()

if __name__ == '__main__':
	main_func(see_csv = True)		