# -*-coding:utf-8-*-
import os, sys, pdb
import requests, sqlite3, csv
import util, copy, time, codecs

#http://www.cffex.com.cn//fzjy/ccpm/201607/07/IF.xml
# http://www.cffex.com.cn/fzjy/ccpm/201607/07/IF_1.csv
def create_table(conn, table_name_list,):
	for table_name in table_name_list:		
		#------------------
		# # line style
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
			tradingday DATE, productid varchar(10), instrumentid varchar(10), rank INT, 
			company_name1 nchar, volume1 INT, variation1 INT,
			company_name2 nchar, volume2 INT, variation2 INT,
			company_name3 nchar, volume3 INT, variation3 INT) ''' % table_name
		#------------------
		conn.execute(sql)
	# end for
	sql = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
		tradingday DATE, productid varchar(10) , instrumentid varchar(10),
		volume1 INT, variation1 INT, volume2 INT, variation2 INT,volume3 INT, variation3 INT)'''
	conn.execute( sql ) 

	sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
			remark char)'''
	conn.execute( sql ) 	
	conn.commit()

#---------------------------------
# raw_csv to utf-8 txt
def csv2txt(raw_csv_file, txt_file):	
	fp = open(txt_file,'w')	
	with  open(raw_csv_file,'r') as f2:
		s = f2.read()
		#print type(s)
		u = s.decode('gbk')
		str1 = u.encode('utf-8')
		fp.write(str1)

	fp.close()
		
	fp = codecs.open(txt_file,'r','utf-8')    
	s = fp.readlines()	
	# return a list of line
	return s


def string2database_cffex_ccpm(conn, s,  d):	
	def get_item_month(s):			
		for i in range(0, len(s)):
			if s[i].isdigit():
				break			
		return s[0:i], s[i:]
	
	cs = conn.cursor()
	linenum = len(s)		
	#pdb.set_trace()
	total_table_data=[]	
	item_table_data=[]
	start_rank = 0
	for idx in range(0, linenum):
		if s[idx][0] == ',':
			start_rank = idx
	idx = 0
	for line in s:
		tmp = line.replace(' ','').split(',')
		a = (idx == 1)  or  (start_rank >=7 and idx == 3) 
		b =  (start_rank >=9 and idx == 5) or (start_rank >=11 and idx == 7)
		if a or b :
			pkey = tmp[0]+'-'+tmp[1]			
			productid, settlementMonth = get_item_month(tmp[1]) #tmp[1] is instrumentid
			newline = [pkey, '-'.join(d), productid, tmp[1]] + tmp[3:]
			total_table_data.append(newline)
		#print idx
		if idx > start_rank:			
			pkey = tmp[0]+'-'+tmp[1] + '-' + tmp[2] # tmp[2] is rank
			productid, settlementMonth = get_item_month(tmp[1])
			newline = [pkey, '-'.join(d), productid] + tmp[1:]
			item_table_data.append(newline)  			
		#pdb.set_trace()
		idx +=1	
	#print len(item_table_data), linenum

	tb = 'TABLE_' + productid.upper()
	cs.executemany('''INSERT OR REPLACE INTO TABLE_TOTAL values	(?,?,?,?,   ?,?, ?,?, ?,?)''' , total_table_data)  	 		
	cs.executemany('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,?,  ?,?,?, ?,?,?, ?,?,? )''' % tb,  item_table_data) 
	conn.commit()			
	return len(total_table_data), len(item_table_data)

#---------------------------------
def startSpider(conn, url_info):
	raw_csv_file = util.midfilePath() + 'cffex_ccpm_raw.csv'
	raw_txt_file = util.midfilePath() + 'cffex_ccpm_raw.txt'
	exception_url_file = util.midfilePath() + 'cffex_ccpm_exception_url.txt'

	cs = conn.cursor()
	f1 = open(exception_url_file, 'w')
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
			 		string2database_cffex_ccpm(conn, s, d)
			 	else:
			 		remark = 'holiday'
			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :									
			print "---%s seconds---" %(time.time() - t1)	
	f1.close()		
	# end for
#end function startSpider
def getURL_list(conn, end_date = ('2016', '07', '07')):
	ccpm_url = 'http://www.cffex.com.cn/fzjy/ccpm/'
	url_postfix = ['IF_1.csv', 'IC_1.csv', 'IH_1.csv', 'T_1.csv', 'TF_1.csv']
	url_info = []
	visited_url = util.getVisitedURL(conn)

	for item_idx in range(0,5):
		if item_idx == 0:
			start_date = ('2010', '04', '16') 
		if item_idx == 1 or item_idx == 2:
			start_date = ('2015', '04', '16') 
		if item_idx == 3 :
			start_date = ('2015', '03', '20') 
		if item_idx == 4:
			start_date = ('2013', '09', '06') 
		#end_date = ('2016', '07', '07')  # 20170704 is Monday

		d = start_date
		while d != end_date:
			date_str = d[0]+d[1]+'/'+d[2]+'/'
			url = ccpm_url + date_str + url_postfix[item_idx]

			if url in visited_url:			
				visited_url_status = util.get_URL_status(conn, url)
				#print visited_url_status
				if visited_url_status == 'error':
					url_info.append((url, d))			
			else:
				url_info.append((url, d))
			
			d = util.next_day(d)
	return url_info

def test_cffex_ccpm(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	#pdb.set_trace()
	with open(util.midfilePath() + 'test_cffex_ccpm.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			if table_name == 'TABLE_TOTAL' or table_name == 'TABLE_URL':
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
			else:
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [5,8,11])


def main_func(end_date = '', see_csv = False):
	ce_name = 'cffex/'
	db_name = 'cffex_ccpm.db' #'daily_info.db'
	full_db_name =util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  		

	prefix = 'TABLE_'
	table_name_list = ['TABLE_IF', 'TABLE_IC', 'TABLE_IH', 'TABLE_T', 'TABLE_TF']

	create_table(conn, table_name_list)
	util.createTABLE_URL(conn )
	url_info = getURL_list(conn, end_date = end_date )
	print 'unvisited url :', len(url_info)
	startSpider(conn, url_info)

	if see_csv:
		test_cffex_ccpm(conn)
	conn.close()

if __name__ == '__main__':
	d = util.get_today_tup()
	print d
	main_func(end_date = d, see_csv = True)		