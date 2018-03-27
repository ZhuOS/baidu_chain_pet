# -*-coding:utf-8-*-
#czce daily
import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, time

def get_item_month(s):
	for i in range(0, len(s)):
		if s[i].isdigit():
			break
		
	return s[0:i], '1'+s[i:]

def create_table(conn, flag = '', table_name = ''):
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
		tradingday DATE, productid char, instrumentid char , 
		presettlementprice real, openprice real, highestprice real, lowestprice real, 
		settlementprice real, closeprice real, change1 real, change2 real,
		volume INT, openinterest INT, openinterest_change INT, turnover real )''' % table_name
		conn.execute( sql ) 
	if flag == 'init':
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
				tradingday DATE, productid varchar(10) ,
				volume INT, openinterest INT, openinterest_change INT, turnover real)'''
		conn.execute( sql ) 
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 	
	conn.commit()

def getURL_list(conn, end_date = ''):		
	#start_date = ('2012', '07', '16')
	start_date = ('2012','07','16')
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
		#czce_url['dfs'] = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/2016/20160706/FutureDataHolding.txt'
		#czce_url['exc'] = 'http://www.czce.com.cn/portal/exchange/2015/datatradeholding/20150309.txt'
		date_str = int(d[0]+d[1]+d[2])
		if date_str >= 20150921:
			url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%d/FutureDataDaily.txt' % (d[0], date_str)
		else:		
			url = 'http://www.czce.com.cn/portal/exchange/%s/datadaily/%d.txt' % (d[0], date_str)
		
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

def string2database_czce_daily(conn, s,  d):
	date_str = int(d[0]+d[1]+d[2])
	
	cs = conn.cursor()
	if date_str >= 20150921:
		s = s[2:-1] # parse the title
	else:
		s = s[1:-1]
	list_of_list = []
	for line in s:				
		if date_str >= 20150921:
			line = line.replace(' ','').replace('\n','').replace(',','')
			x = line.split('|')
		else:
			line = line.replace(' ','').replace('\n','')
			x = line.split(',')
		x = x[:-1] # parse jiao ge jie suan jia		
		x = ' '.join(x)
		new_row = x.split()		
		list_of_list.append(new_row)

	def get_item_month(s):			
		for i in range(0, len(s)):
			if s[i].isdigit():
				break			
		return s[0:i], s[i:]

	pid =''
	contractid = ''
	count1 = 0
	count2 = 0
	for x in list_of_list:
		
		if len(x) == 13:
			productid, settlementMonth = get_item_month(x[0])			
			pkey = ''.join(d) + '-' + x[0] 
			tmp = [pkey, '-'.join(d), productid,] + x # 3 + 13 = 16
			tb = 'TABLE_' + productid
			#-----check table exist---------
			sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
			cs.execute(sql)
			if len(cs.fetchall()) == 0:
				create_table(conn, flag = 'item', table_name = tb)	
			#-------------------------------
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?)''' % tb,  tmp) # 3+11=14
			pid = productid
			contractid = x[0]
			count1 += 1			
		
		if	x[0].find(u'小计') >= 0:
			#pdb.set_trace()
			pkey = ''.join(d) + '-' + pid
			tmp = [pkey, '-'.join(d), pid] + x[1:] # 3 + 4 = 7
			tb = 'TABLE_TOTAL'
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,  ?,?,?,?)''' % tb,  tmp) # 3+3 = 6
			count2 += 1
			
	#print()
	conn.commit()	
	return count1, count2

def startSpider(conn, url_info ):	
	department = 'czce'		
	exception_url_file = util.midfilePath() + '%s_daily_exception_url.txt' % department
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
				# do nothing
			else:
				# insert to database
				r.encoding = 'gbk'	
				text_stream = r.text.encode('utf-8')
				raw_txt_file = util.midfilePath() + '%s_daily_raw.txt' % department
				with open(raw_txt_file,'wb') as f2:
					f2.write(text_stream)

				f2 = codecs.open(raw_txt_file,'r', 'utf-8')
				s = f2.readlines()
				f2.close()
				#print s[0][:3]
				#pdb.set_trace() 							 
			 	if s[0][:3] != '<ht':	
			 		remark = 'tradingday'			 		
			 		string2database_czce_daily(conn, s, d)
			 	else:
			 		remark = 'holiday'
			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :
			print "---%s seconds---" %(time.time() - t1)	
	f1.close()		
	# end for
#end  startSpider


def test_czce_daily(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	
	with open(util.midfilePath() + 'test_czce_daily.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])

def main_func(end_date = '', see_csv = False):
	ce_name = 'czce/'
	db_name = 'czce_daily_info.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  
	create_table(conn, flag = 'init')
	
	url_info = getURL_list(conn, end_date = end_date)	
	startSpider(conn, url_info)		

	if see_csv:
		test_czce_daily(conn)
	conn.close()
			
if __name__ == '__main__':
	main_func(see_csv = True)
