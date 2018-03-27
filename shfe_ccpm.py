# -*-coding:utf-8-*-
import os, sys, pdb
import requests, sqlite3, csv
import util, time

def create_table(conn,flag = '', table_name ='' ):
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
		tradingday DATE, productid char, instrumentid char,   rank INT, 
		company_name1 nchar, volume1 INT, variation1 INT,
		company_name2 nchar, volume2 INT, variation2 INT,
		company_name3 nchar, volume3 INT, variation3 INT) ''' % table_name
		conn.execute(sql)
	if flag == 'init':
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
				tradingday DATE, productid varchar(10) , instrumentid, 
				volume1 INT, variation1 INT, volume2 INT, variation2 INT,volume3 INT, variation3 INT)'''	
		conn.execute(sql)
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 
	conn.commit()

#-------------------------------
def string2database_shfe_ccpm(conn, myDict,  d):
	def get_item_month(s):			
		for i in range(0, len(s)):
			if s[i].isdigit():
				break			
		return s[0:i], s[i:]

	cs = conn.cursor()
	pid = ''
	dict_of_dict = myDict['o_cursor']	
	for dic in dict_of_dict:
		if dic['RANK'] >= 1 and dic['RANK'] <= 20:			
			contrcatid = dic['INSTRUMENTID'].upper().replace(' ','')
			if contrcatid.find('ACTV') >=0:
				contrcatid = productid + '-all'
			else:
				productid, month = get_item_month(contrcatid)
			pkey = ''.join(d) + '-' + contrcatid + '-' + str(dic['RANK'])
			tmp = [pkey, '-'.join(d), productid, contrcatid,] + [dic['RANK'], 
				dic["PARTICIPANTABBR1"].replace(' ',''), dic['CJ1'], dic["CJ1_CHG"],
				dic["PARTICIPANTABBR2"].replace(' ',''), dic['CJ2'], dic["CJ2_CHG"],
				dic["PARTICIPANTABBR3"].replace(' ',''), dic['CJ3'], dic["CJ3_CHG"],		]
			tb = 'TABLE_' + productid.upper()
			#-----check table exist---------
			sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
			cs.execute(sql)
			if len(cs.fetchall()) == 0:
				create_table(conn, flag = 'item', table_name = tb)	
			#-------------------------------
			tmp[5] = tmp[5].decode('utf-8')
			tmp[8] = tmp[8].decode('utf-8')
			tmp[11] = tmp[11].decode('utf-8')
						
			# if d == ('2016','07','06') and contrcatid == 'NI1607':
				
			# 	print dic["PARTICIPANTABBR1"], dic['CJ1'], dic["CJ1_CHG"]
			# 	pdb.set_trace()
			for i in [5,8,11]:
				if tmp[i] == '':
					tmp[i] = '-'
					tmp[i+1] = 0 
					tmp[i+2] = 0
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,?,  ?,?,?,  ?,?,?,  ?,?,?)''' % tb,  tmp) 
			pid = productid
		else:
			if dic["RANK"] == 999:
				tb = 'TABLE_TOTAL' 
				pkey = ''.join(d) + '-' + contrcatid
				tmp = [pkey, '-'.join(d), productid, contrcatid,] + [ dic['CJ1'], dic["CJ1_CHG"],
						 dic['CJ2'], dic["CJ2_CHG"],	 dic['CJ3'], dic["CJ3_CHG"],		]
				cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,   ?,?,?,?,?,?)''' % tb,  tmp) 

	conn.commit()	
	return 
#end string 2 database	
#---------------------------------		
def startSpider(conn, url_info):	
	raw_dat_file = util.midfilePath() + 'shfe_ccpm_raw.dat'
		
	exception_url_file = util.midfilePath() + 'shfe_ccpm_exception_url.txt'
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
			 		tmp = repr(myDict)	
			 		#pdb.set_trace()			 		
			 		tmp = tmp[1:-1]		 
			 		tmp = tmp.replace('\\r','').replace('\\n','')		
			 		pattern = '# -*-coding:utf-8-*-\n%s' % tmp	 					 		
			 		myDict = eval(pattern	)
			 		dict_of_dict = myDict['o_cursor']
			 		#print len(dict_of_dict )
			 		if len(dict_of_dict ) == 0:
			 			remark = 'holiday'
			 		else:
			 			remark = 'tradingday'
			 			string2database_shfe_ccpm(conn, myDict, d)			 	
			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) 
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
	# start_date = ('2016', '01', '01')
	# end_date   = ('2016', '01', '02')	

	#start_date = ('2016', '07', '07')
	if end_date == '':
		end_date   = ('2016','07','08')	
	num1 = int( ''.join(start_date) )
	num2 = int(''.join(end_date))
	if num2 < num1:
		print 'error: invalid end_date, should be bigger'

	visited_url = util.getVisitedURL(conn)
	url_info = []
	d = start_date
	while d != end_date:			
		
		url = 'http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat' % ''.join(d)
		
		if url in visited_url:
			visited_url_status = util.get_URL_status(conn, url)
			#print visited_url_status
			if visited_url_status == 'error':
				url_info.append((url, d))			
		else:
			url_info.append((url, d))
		d = util.next_day(d)	
	return url_info

def test_shfe_ccpm(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	#pdb.set_trace()
	with open(util.midfilePath() + 'test_shfe_ccpm.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			if table_name == 'TABLE_TOTAL' or table_name == 'TABLE_URL':
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
			else:
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [5,8,11])

def main_func(end_date, see_csv):
	ce_name = 'shfe/'
	db_name = 'shfe_ccpm.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  
	create_table(conn, 'init')

	url_info = getURL_list( conn, end_date = end_date )
	print 'unvisited url :', len(url_info)
	startSpider(conn, url_info)

	if see_csv :
		test_shfe_ccpm(conn)
	conn.close()

if __name__ == '__main__':
	d = util.get_today_tup()
	print d
	main_func(end_date = d, see_csv = True)		
	