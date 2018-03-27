# -*-coding:utf-8-*-

import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, time
from bs4 import BeautifulSoup

def create_table(conn, flag = '', table_name = ''):
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
				tradingday DATE, productid char, instrumentid char , 					
				long_margin_ratio real, short_margin_ratio real, trade_fee real, deliivery_fee real,
				close_today_fee INT )''' % table_name
		conn.execute( sql ) 
	if flag == 'init':		
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 	
	conn.commit()

def getURL_list(conn, init = False):		
	# no need to loop if update	
	def get_url_list_from_page(conn, url):
		url_info = []
		r = requests.get(url)
		#print r.encoding
		r.encoding = 'gbk'				
		s = r.text.encode('utf-8')

		soup = BeautifulSoup(s)	
		pattern = 'table'
		table =  soup.find_all(pattern)	
		for x in table:	
			#print len(x.attrs)
			if len(x.attrs) == 4 and x['width'] == '92%':								
				s = x.text
				pattern = u'[0-9]{8}'
				r = re.compile(pattern, re.UNICODE)
				m = r.search(s) # return the first one
				if m:
					day = m.group()
					tmp_url = 'http://www.cffex.com.cn/fzjy/jsywcs/%s/%s/%s.csv' % (day[:6], day[6:], day)
					tup = (day[:4], day[4:6], day[6:])
					url_info.append((tmp_url, tup))							
		return url_info

	if init:
		simple_xml_init(conn)
	url = 'http://www.cffex.com.cn/fzjy/jsywcs/'
	tmp =  get_url_list_from_page(conn, url)
	if init:
		for i in range(1,3):
			url = 'http://www.cffex.com.cn/fzjy/jsywcs/index_%d.html' % i
			tmp += get_url_list_from_page(conn, url)	
	return parse_visited_url(conn, tmp)
	
def parse_visited_url(conn, url_list):

	visited_url = util.getVisitedURL(conn)
	url_info = []
	
	for url,d in url_list:
		if url in visited_url:		
			visited_url_status = util.get_URL_status(conn, url)
			#print visited_url_status
			if visited_url_status == 'error':
				url_info.append((url, d))			
		else:
			url_info.append((url, d))
	print 'unvisited url :', len(url_info)
	return url_info

def insert_sql(conn, tb, x):			
	cs = conn.cursor()
	#-----check table exist---------
	sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
	cs.execute(sql)
	if len(cs.fetchall()) == 0:
		create_table(conn, flag = 'item', table_name = tb)	
	#-------------------------------
	cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,  ?,?, ?,?,?,?)''' % tb,  x) 		
	

def response2database_cffex_param(conn, s,  d, url):
	def get_item_month(s):			
		for i in range(0, len(s)):
			if s[i].isdigit():
				break			
		return s[0:i], s[i:]
	def parse_line(x):
		for i in range(0, len(x)):
			elem = x[i]
			if elem.find(u'万分之') >= 0:				
				x[i] = float( x[i].replace(u'万分之','') )/10000.0
				x[i] = str(x[i])
			if elem.find(u'%') >= 0:				
				x[i] = float(x[i].replace(u'%','') )/100.0
				x[i] = str(x[i])
			if elem.find(u'元/手') >= 0:				
				x[i] = x[i].replace(u'元/手','') # yuan / shou
				x[i] = str(x[i])
		return x	

	date_str = int(d[0]+d[1]+d[2])	
	raw_txt_file = util.midfilePath() + 'cffex_param_raw.txt' 
	r = requests.get(url)
	r.encoding = 'gbk'					
	text_stream = r.text.encode('utf-8')
		
	with open(raw_txt_file,'wb') as f2:
		f2.write(text_stream)
	with codecs.open(raw_txt_file,'r', 'utf-8') as f2:
		s = f2.readlines()
			
	s = s[2:]				
	for line in s:
		x = line.replace(' ','').split(',')
		x = parse_line(x)	
		if len(x) != 6 and len(x) != 5:
			return
		cs = conn.cursor()
		contractid = x[0]
		productid, month = get_item_month(contractid)
		pkey = ''.join(d) + '-' + contractid
		
		if len(x) == 6:
			tmp = [pkey, '-'.join(d), productid, ] + x
		if len(x) == 5:
			a = copy.deepcopy(x[1])
			new_x = x[:2] + [a,] + x[2:]
			tmp = [pkey, '-'.join(d), productid, ] + new_x

		tb = 'TABLE_' + productid.upper()		
		insert_sql(conn, tb, tmp)
	conn.commit()

def simple_xml_init(conn):
	url = 'http://www.cffex.com.cn/fzjy/jsywcs/index_5425.xml'
	r = requests.get(url)	
	s = r.content
	soup = BeautifulSoup(s,'xml')	
	pattern = 'data'
	data_array =  soup.find_all(pattern)	
	count = 0
	for x in data_array:	
		day = x.day.string
		tup = (day[:4], day[4:6], day[6:])
		contractid = x.id.string
		productid = x.productid.string
		contractid = contractid.replace(' ','')
		productid = productid.replace(' ','')
		pkey = ''.join(tup) + '-' + contractid
		lmr = x.lid.string
		smr = x.sid.string
		trade_fee = float(x.tr.string)
		deliivery_fee = float(x.jr.string)
		close_today_fee = x.jg.string
		tmp = [pkey, '-'.join(tup), productid, contractid, 
			lmr, smr, trade_fee, deliivery_fee, close_today_fee]
		tb = 'TABLE_' + productid.upper()
		insert_sql(conn, tb, tmp)
		count += 1
		if count%100 == 0:
			print count
		#print tup
	conn.commit()
	return

def startSpider(conn, url_info ):			
	cs = conn.cursor()	
	t1 = time.time()
	count = 0
	for (url,d) in url_info:
		count += 1			
		r = requests.get(url)
		response2database_cffex_param(conn, r, d, url)
		remark = 'tradingday'			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :
			print "---%s seconds---" %(time.time() - t1)		
	# end for
#end  startSpider

def main_func(init_flag = False, see_csv = False):
	ce_name = 'cffex/'
	db_name = 'cffex_param.db'
	full_db_name = util.rootPath() + ce_name+db_name
	conn = sqlite3.connect(full_db_name)  

	create_table(conn, flag = 'init')
	url_info = getURL_list(conn, init = init_flag)	
	startSpider(conn, url_info)
	if see_csv:
		test_cffex_param(conn)

	conn.close()		

def test_cffex_param(conn):		
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	
	with open(util.midfilePath() + 'test_cffex_param.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
					

if __name__ == '__main__':
	main_func(init_flag = True, see_csv = True)
		