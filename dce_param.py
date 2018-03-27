# -*-coding:utf-8-*-
import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, time
from bs4 import BeautifulSoup

def create_table(conn,flag = '', table_name ='' ):
	cs = conn.cursor()
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s (	pkey char primary key,			
				tradingday DATE, productid char,  instrumentid char,  settlementprice real,
				open_fee real, close_fee real, short_term_open_fee real, short_term_close_fee real,
				charge_style char, spec_long_margin_ratio real, spec_short_margin_ratio real,
				hedge_long_margin_ratio real, hedge_short_margin_ratio real )'''  %  table_name
		
		cs.execute(sql)
	if flag == 'init' :
		sql = '''CREATE TABLE IF NOT EXISTS TABLE_URL ( url char primary key,
				remark char)'''
		conn.execute( sql ) 
	conn.commit()

def string2database_dce_param(conn, s, d):
	date_str = ''.join(d)		
	cs = conn.cursor()

	soup = BeautifulSoup(s)	
	pattern = 'table'
	table =  soup.find_all(pattern)	

	fp = codecs.open(util.midfilePath() + 'dce_param_soup.txt','w','utf-8')	
	for x in table:	
		#print 'table attrs', len(x.attrs) 		
		if len(x.attrs) !=5:
			continue
		rows = x.find_all('tr')
		title_flag = True
		for row in rows:
			cols = row.find_all('td')	
			cols = [ele.text.strip() for ele in cols]	
			def parse_persent(x):
				if x.find('%')>=0:					
					s = float(x.replace('%','') )/100.0
					return str(s)
				else:
					return x
			cols = [parse_persent(ele) for ele in cols]				
			new_line = ' '.join(cols) 
			new_line = (new_line.replace('\r','').replace('\n','') )
			if len(new_line.replace(' ','')) != 0 :								
				if title_flag:
					title_flag = False					
				else:
					cols[0] = cols[0].upper()
					cols[1] = cols[1].upper()
					pkey = ''.join(d) + '-' + cols[1]		
					tmp = [pkey, '-'.join(d),] + cols
					tb = 'TABLE_' + cols[0]
					#-----check table exist---------
					sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % tb
					cs.execute(sql)
					if len(cs.fetchall()) == 0:
						create_table(conn, flag = 'item', table_name = tb)	
					#-------------------------------
					#print tmp
					#pdb.set_trace()
					cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?, ?,?,?, ?,?,?,?, ?, ?,?,?,?)''' % tb,  tmp) 				
					
					fp.write(new_line)
					fp.write('\n')	
	fp.close()
	conn.commit()
	# end for	

def getURL_list(conn, end_date = ''):	
	param = {}	
	param['action'] = 'Pu00121_result'
	param['Pu00121_Input.variety'] = 'all'
	
	site = 'http://www.dce.com.cn/PublicWeb/MainServlet'	

	start_date = ('2012','01','01')
	#start_date = ('2016','07','01')
	if end_date == '':
		end_date   = ('2016','07','19')
	#d = ('2016','07','13')
	d = start_date
	visited_url = util.getVisitedURL(conn)
	url_info = []
	while d!= end_date:
		
		url = ''.join(d)	
		param['Pu00121_Input.trade_date'] = url
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
				(u'豆粕',u'm'),(u'棕榈油',u'p'),(u'聚丙烯',u'pp'),(u'聚氯乙烯',u'v'),(u'豆油',u'y'),
				(u'比例值',u'ratio'),(u'绝对值',u'absolute')]	
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
				remark = 'error'
			else:														
				text_stream = r.text.encode('utf-8')
				raw_txt_file = util.midfilePath() + 'dce_param_raw.txt' 
				with  open(raw_txt_file, 'wb') as f2:
					f2.write(text_stream)
				with codecs.open(raw_txt_file, 'r', 'utf-8') as f2:
					s = f2.read()				
				for a,b in item_code:
					s = s.replace(a,b)
				with codecs.open(raw_txt_file, 'w', 'utf-8') as f2:
					f2.write(s)
				with codecs.open(raw_txt_file, 'r', 'utf-8') as f2:
					cout_line_s = f2.readlines()	
				if len(cout_line_s) < 40:
					remark = 'holiday'
				else:
					remark = 'tradingday'
					string2database_dce_param(conn, s, d)
		count += 1
		print d, remark
		if count % 10 == 0:		
			print "---%s seconds---" %(time.time() - t1)			
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (date_str, remark)) # 3+3 = 6
		conn.commit()			
	# end for	
#end startSpider

def test_dce_param(conn):			
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	with open(util.midfilePath() + 'test_dce_param.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])

def main_func( end_date = '', see_csv = False):
	ce_name = 'dce/'
	db_name = 'dce_param.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  

	create_table(conn, flag = 'init')
	url_info = getURL_list(conn, end_date = end_date)
	startSpider(conn, url_info)
	if see_csv:
		test_dce_param(conn)

	conn.close()		

if __name__ == '__main__':
	main_func(see_csv = True)

	
	


			