# -*-coding:utf-8-*-

import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, time
from bs4 import BeautifulSoup

def create_table(conn, flag = '', table_name = ''):
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
				tradingday DATE, productid char, instrumentid char , 		
				settlementprice real, one_side_market INT, one_side_market_duration INT,
				long_margin_ratio real, short_margin_ratio real, trade_fee real, deliivery_fee real,
				close_today_reduce_by_half INT )''' % table_name
		conn.execute( sql ) 
	if flag == 'init':		
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 	
	conn.commit()

def getURL_list(conn, end_date = ''):		
	
	start_date = ('2012','07','16')
	# end_date = ('2012','07','17')
	#start_date = ('2015','09','17')
	# end_date = ('2015','09','25')
	if end_date == '':
		end_date   = ('2016','07','07')
		
	num1 = int( ''.join(start_date) )
	num2 = int( ''.join(end_date))
	if num2 < num1:
		print 'error: invalid end_date, should be bigger'

	visited_url = util.getVisitedURL(conn)
	url_info = []
	d = start_date
	while d != end_date:			
		date_str = int(d[0]+d[1]+d[2])
		if date_str >= 20150921:
			url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%d/FutureDataClearParams.txt' % (d[0], date_str)
		else:		
			url = 'http://www.czce.com.cn/portal/exchange/%s/dataclearparams/%d.htm' % (d[0], date_str)
		
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

def get_item_month(s):			
	for i in range(0, len(s)):
		if s[i].isdigit():
			break			
	return s[0:i], s[i:]

def string2database_czce_param(conn, s,  d):
	date_str = int(d[0]+d[1]+d[2])
	
	cs = conn.cursor()	
	s = s[2:-1] # parse the title
	
	list_of_list = []
	for line in s:						
		line = line.replace(' ','').replace('\n','').replace(',','')
		x = line.split('|')		
		# erase the blank	
		x = ' '.join(x)
		x = x.replace('Y','1').replace('N','0') # Yes or No
		new_row = x.split()		
		list_of_list.append(new_row)	
	
	for x in list_of_list:		
		if len(x) == 9:
			productid, settlementMonth = get_item_month(x[0])			
			pkey = ''.join(d) + '-' + x[0] 
			x[4] = str(float(x[4])/100)
			x[5] = str(float(x[5])/100)
			tmp = [pkey, '-'.join(d), productid,] + x # 3 + 13 = 16
			tb = 'TABLE_' + productid
			#-----check table exist---------
			sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
			cs.execute(sql)
			if len(cs.fetchall()) == 0:
				create_table(conn, flag = 'item', table_name = tb)	
			#-------------------------------
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,?, ?,?, ?,?, ?,?, ?)''' % tb,  tmp) 
								
	conn.commit()	
def string2database_czce_param_before20150921(conn, s, d):
	date_str = ''.join(d)		
	cs = conn.cursor()

	soup = BeautifulSoup(s)	
	pattern = 'table'
	table =  soup.find_all(pattern)	

	fp = codecs.open(util.midfilePath() + 'czce_param_soup.txt','w','utf-8')	
	for x in table:			
		if len(x.attrs) !=7:
			continue		
		if x['id'] != 'senfe':
			continue		
		rows = x.find_all('tr')
		title_flag = True
		for row in rows:
			arr = row.find_all('td')	
			arr = [ele.text.strip() for ele in arr]							
			new_line = ' '.join(arr) 
			new_line = new_line.replace('\r','').replace('\n','').replace('Y','1').replace('N','0')  
			arr = new_line.split()
			if len(arr)	== 9 or len(arr) == 6:								
				if title_flag:
					title_flag = False					
				else:					
					instrumentid = arr[0]	
					productid, month = get_item_month(instrumentid)
					pkey = ''.join(d) + '-' + instrumentid
										
					arr[4] = float(arr[4])/100
					arr[5] = float(arr[5])/100
					if len(arr)	== 9:			
						tmp = [pkey, '-'.join(d), productid] + arr
					if len(arr) == 6:
						tmp = [pkey, '-'.join(d), productid] + arr + [0, 0, -1]

					tb = 'TABLE_' + productid.upper()
					#-----check table exist---------
					sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
					cs.execute(sql)
					if len(cs.fetchall()) == 0:
						create_table(conn, flag = 'item', table_name = tb)	
					#-------------------------------					
					cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?, ?,?,?,?,  ?,?,?,?)''' % tb,  tmp) 				
					
					fp.write(new_line)
					fp.write('\n')	
	fp.close()
	conn.commit()

def startSpider(conn, url_info ):	
	department = 'czce'		
	exception_url_file = util.midfilePath() + '%s_param_exception_url.txt' % department
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
				r.encoding = 'gbk'	
				text_stream = r.text.encode('utf-8')
				raw_txt_file = util.midfilePath() + '%s_param_raw.txt' % department
				with open(raw_txt_file,'wb') as f2:
						f2.write(text_stream)

				if int(d[0]+d[1]+d[2]) >= 20150921:
					with codecs.open(raw_txt_file,'r', 'utf-8') as f2:
						s = f2.readlines()					
											 
				 	if s[0][:3] != '<ht':	
				 		remark = 'tradingday'			 		
				 		string2database_czce_param(conn, s, d)
				 	else:
				 		remark = 'holiday'
				else:
					with codecs.open(raw_txt_file, 'r', 'utf-8') as f2:
						s = f2.read()		
					with codecs.open(raw_txt_file, 'r', 'utf-8') as f2:								
						cout_line_s = f2.readlines()																
					
					if len(cout_line_s) < 100:
						remark = 'holiday'
					else:
						remark = 'tradingday'
						string2database_czce_param_before20150921(conn, s, d)
			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :
			print "---%s seconds---" %(time.time() - t1)	
	f1.close()		
	# end for
#end  startSpider

def test_czce_param(conn):		
	
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	
	with open(util.midfilePath() + 'test_czce_param.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])

def main_func( end_date = '', see_csv = False):
	
	ce_name = 'czce/'
	db_name = 'czce_param.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  

	create_table(conn, flag = 'init')
	url_info = getURL_list(conn, end_date = end_date, )
	startSpider(conn, url_info)
	if see_csv:
		test_czce_param(conn)

	conn.close()		

if __name__ == '__main__':
	main_func(see_csv = True)