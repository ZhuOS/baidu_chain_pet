# -*-coding:utf-8-*-
import os, sys, pdb
import requests, sqlite3, csv
import util, time

def create_table(conn, table_name_list):
	for table_name in table_name_list:				
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
		tradingday DATE, productid char, instrumentid char , 
		openprice real, highestprice real, lowestprice real, 
		volume INT, turnover real, openinterest INT,
		settlementprice real, closeprice real, change1 real, change2 real)''' % table_name
		#print sql
		conn.execute( sql ) 

	sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
			tradingday DATE, productid varchar(10) ,
		volume INT, turnover real, openinterest INT)'''
	conn.execute( sql ) 

	sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
			remark char)'''
	conn.execute( sql ) 
	conn.commit()
#-------------------------------
# raw_csv to utf-8 txt
def csv2txt(raw_csv_file, raw_txt_file):	
	fp = open(raw_txt_file,'w')	
	with  open(raw_csv_file,'r') as f2:
		s = f2.read()
		#print type(s)
		u = s.decode('gbk')
		str1 = u.encode('utf-8')
		fp.write(str1)

	fp.close()
	
	fp = open(raw_txt_file,'r',)    
	s = fp.readlines()	
	# return a list of line
	return s
#-----------------------
def string2database_cffex_daily(conn, s,  d):
	def helper1(row):
		tmp = []
		for x in row:
			if (x =='' or x == '--'):
				a = 'do nothing'
			else:
				tmp.append(x)
		return tmp

	cs = conn.cursor()
	s = s[1:-1] # parse the title
	list_of_list = []
	for line in s:				
		line = line.replace(' ','').replace('\n','')
		x = line.split(',')
		new_row = helper1(x)
		#print new_row
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
		#print len(x),
		if len(x) == 11:
			productid, settlementMonth = get_item_month(x[0])			
			pkey = ''.join(d) + '-' + x[0] 
			tmp = [pkey, '-'.join(d), productid,] + x
			tb = 'TABLE_' + productid
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,  ?,?,?,?, ?,?,?, ?,?,?,?)''' % tb,  tmp) # 3+11=14
			pid = productid
			contractid = x[0]
			count1 += 1
					
		if	x[0].find('小计') >= 0:
			#pdb.set_trace()
			pkey = ''.join(d) + '-' + pid
			tmp = [pkey, '-'.join(d), pid] + x[1:]
			tb = 'TABLE_TOTAL'
			cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,  ?,?,?)''' % tb,  tmp) # 3+3 = 6
			count2 += 1
	#print()
	conn.commit()	
	return count1, count2
#end string 2 database	
#---------------------------------		
def startSpider(conn, url_info):	
	raw_csv_file = util.midfilePath() + 'cffex_daily_raw.csv'
	raw_txt_file = util.midfilePath() + 'cffex_daily_raw.txt'
	
	exception_url_file = util.midfilePath() + 'cffex_daily_exception_url.txt'
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
				with open(raw_csv_file,'wb') as f:
			 		f.write(r.content)
			 	
			 	s = csv2txt(raw_csv_file, raw_txt_file)
			 	if s[0][:3] != '<!D':	
			 		remark = 'tradingday'
			 		string2database_cffex_daily(conn, s, d)
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

def getURL_list(conn, end_date=''):
	cffex_daily_info_url = 'http://www.cffex.com.cn/fzjy/mrhq/'
	#end_date = ('2016', '07', '09')	
	start_date = ('2010', '04', '16')
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
		#url = 'http://www.cffex.com.cn/fzjy/mrhq/201004/16/20100416_1.csv'
		date_str1 = d[0]+d[1]+'/'+d[2]+'/'
		date_str2 = ''.join(d)+'_1'
		url = cffex_daily_info_url + date_str1 + date_str2 + '.csv'
		
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

def test_cffex_daily(conn,):	
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()

	with open(util.midfilePath() + 'test_cffex_daily.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
				

def main_func(end_date = '', see_csv = False):
	ce_name = 'cffex/'
	db_name = 'cffex_daily_info.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  	

	table_name_list = ['TABLE_IF', 'TABLE_IC', 'TABLE_IH', 'TABLE_T', 'TABLE_TF']	
	create_table(conn, table_name_list)
	
	url_info = getURL_list( conn, end_date = end_date )
	startSpider(conn, url_info)

	if see_csv:
		test_cffex_daily(conn)
	conn.close()

if __name__ == '__main__':
	main_func(see_csv = True)