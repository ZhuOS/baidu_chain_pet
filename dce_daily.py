# -*-coding:utf-8-*-

import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, time


def create_table(conn,flag = '', table_name ='' ):
	cs = conn.cursor()
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s (	pkey char primary key,			
				tradingday DATE, productid char,  instrumentid char, 

				presettlementprice real, openprice real, 	highestprice real,   lowestprice real, 
				closeprice real,         settlementprice real,	change1 real,    change2 real,				
				volume INT ,	openinterest INT, openinterest_change INT, turnover real)'''  %  table_name
		
		cs.execute(sql)
	if flag == 'init' :
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
		tradingday DATE, productid varchar(10) ,
		volume INT, openinterest INT, openinterest_change INT, turnover real)''' 
		cs.execute(sql)

		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 
	conn.commit()

def string2database_dce_daily(conn, s, d):
	date_str = ''.join(d)		
	cs = conn.cursor()
	pid =''
	contractid = ''
	count1 = 0
	count2 = 0
	for i in range(0, len(s)):
		x = s[i].split()
		for i in range(0, len(x)):
			if x[i] == '-':
				x[i] = 0	
		#print 'len:', len(x)
		if len(x) == 14:	
			if count1 == 0:
				count1+=1
				continue					

			productid = x[0].upper()
			instrumentid =  productid + x[1]
			pkey = ''.join(d) + '-' + instrumentid 
			tmp = [pkey, '-'.join(d), productid, instrumentid] + x[2:] # 4 + 12 = 16
			tb = 'TABLE_' + productid
			#-----check table exist---------
			sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
			cs.execute(sql)
			if len(cs.fetchall()) == 0:
				create_table(conn, flag = 'item', table_name = tb)	
			#-------------------------------
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?)''' % tb,  tmp) 
			pid = productid
			contractid = x[0]
			count1 += 1			
		
		if	len(x) >0 and x[0].find('小计') >= 0:
			#pdb.set_trace()
			pkey = ''.join(d) + '-' + pid
			tmp = [pkey, '-'.join(d), pid] + x[1:] # 3 + 4 = 7
			tb = 'TABLE_TOTAL'
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,  ?,?,?,?)''' % tb,  tmp) # 3+3 = 6
			count2 += 1
	conn.commit()
	# end for
	
	return count1, count2


def getURL_list(conn, end_date = ''):	
	param = {}
	#param['action'] = 'Pu00021_result'
	param['action'] = 'Pu00012_download'
	param['Pu00011_Input.variety'] = 'all'
	param['Pu00011_Input.trade_type'] = 0
	site = 'http://www.dce.com.cn/PublicWeb/MainServlet'	
	

	start_date = ('2012','01','01')
	if end_date == '':
		end_date   = ('2016','07','07')
	#d = ('2016','07','13')
	d = start_date
	visited_url = util.getVisitedURL(conn)
	url_info = []
	while d!= end_date:
		
		url = ''.join(d)	
		param['Pu00011_Input.trade_date'] = url
		p = copy.deepcopy(param)
		tup = (url, d, site, p)
		if url in visited_url:
			visited_url_status = util.get_URL_status(conn, url)
			#print visited_url_status
			if visited_url_status == 'error':
				url_info.append(tup)
		else:
			url_info.append(tup)
		
		d = util.next_day(d)
	print 'unvisited url :', len(url_info)
	return url_info

def startSpider(conn, url_info, ):
	cs = conn.cursor()
	item_code = [(u'豆一',u'a'),(u'豆二',u'b'),(u'胶合板',u'bb'),(u'玉米淀粉',u'cs'),(u'玉米',u'c'),
				(u'纤维板',u'fb'),(u'铁矿石',u'i'),(u'焦炭',u'j'),(u'鸡蛋',u'jd'),(u'焦煤',u'jm'),(u'聚乙烯',u'l'),
				(u'豆粕',u'm'),(u'棕榈油',u'p'),(u'聚丙烯',u'pp'),(u'聚氯乙烯',u'v'),(u'豆油',u'y'),]
	department = 'dce'
	exception_url_file = util.midfilePath() + '%s_daily_exception_url.txt' % department
	f1 = open(exception_url_file, 'w')
	t1 = time.time()
	count = 0
	for date_str, d, site, param in url_info:		
		if util.weekday(d) >= 5:		
			remark = 'weekend'			
		else:	
			try:
				r = requests.get(site, params = param)	
			except requests.ConnectionError:
					print r.status_code
					f1.write(date_str)
					f1.write('\n')
					remark = 'error'
			else:			
				if r.encoding == 'GBK':
					remark = 'holiday'
				else:
					remark = 'tradingday'
					r.encoding = 'gbk'		
					text_stream = r.text.encode('utf-8')
					raw_txt_file = util.midfilePath() + '%s_daily_raw.txt' % department
					with  open(raw_txt_file, 'wb') as f2:
						f2.write(text_stream)
					with codecs.open(raw_txt_file, 'r', 'utf-8') as f2:
						s = f2.read()				
					for a,b in item_code:
						s = s.replace(a,b)
					with  open(raw_txt_file, 'wb') as f2:
						f2.write(s.encode('utf-8'))
					with  open(raw_txt_file, 'r') as f2:
						s = f2.readlines()
					string2database_dce_daily(conn, s, d)
		count += 1
		print d, remark
		if count % 10 == 0:		
			print "---%s seconds---" %(time.time() - t1)			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (date_str, remark)) # 3+3 = 6
		conn.commit()	
		
	# end for
	f1.close()
#end startSpider

def test_dce_daily(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	with open(util.midfilePath() + 'test_dce_daily.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])

def main_func(end_date = '', see_csv = False):
	ce_name = 'dce/'
	db_name = 'dce_daily_info.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  
	create_table(conn, flag = 'init')

	url_info = getURL_list(conn, end_date = end_date)
	startSpider(conn, url_info)

	if see_csv:
		test_dce_daily(conn)
	conn.close()

if __name__ == '__main__':
	main_func(see_csv = True)		