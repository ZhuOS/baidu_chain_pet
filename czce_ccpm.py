# -*-coding:utf-8-*-
#czce
import requests, sys, pdb, copy
import re, codecs, sqlite3
import csv, util, helper_czce, time


#------------------------
def getURL_list(conn, end_date = ''):		
	start_date = ('2012', '07', '16')
	#start_date = ('2016','07','07')
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
		#czce_url['dfs'] = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/2016/20160706/FutureDataHolding.txt'
		#czce_url['exc'] = 'http://www.czce.com.cn/portal/exchange/2015/datatradeholding/20150309.txt'
		date_str = int(d[0]+d[1]+d[2])
		if date_str >= 20150921:
			url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%d/FutureDataHolding.txt' % (d[0], date_str)
		else:		
			url = 'http://www.czce.com.cn/portal/exchange/%s/datatradeholding/%d.txt' % (d[0], date_str)
		
		if url in visited_url:
			visited_url_status = util.get_URL_status(conn, url)
			#print visited_url_status
			if visited_url_status == 'error':
				url_info.append((url, d))			
		else:
			url_info.append((url, d))
		d = util.next_day(d)
	
	return url_info
#--------------------------
def startSpider(conn, url_info ):
	exception_url_file = util.midfilePath() + 'czce_daily_exception_url.txt'
	f1 = open(exception_url_file, 'w')
	cs = conn.cursor()
	t1 = time.time()
	count = 0
	for (url,d) in url_info:
		count += 1
		date_str = int(d[0]+d[1]+d[2])
		if util.weekday(d) >= 5:
			remark = 'weekend'		
		else:		
			try:
				r = requests.get(url)
			except requests.ConnectionError:
				f1.write(url)
				f1.write('\n')
				remark = 'error'
			else:
				r.encoding = 'gbk'	
				text_stream = r.text.encode('utf-8')
				raw_txt_file = util.midfilePath() + 'czce_ccpm.txt'
				with open(raw_txt_file,'wb') as ccpm_file:
					ccpm_file.write(text_stream)

				f2 = codecs.open(raw_txt_file,'r', 'utf-8')
				s = f2.readlines()
				f2.close()

			 	if s[:3] != '<ht':
			 		remark = 'tradingday'
			 		# insert to database
			 		if date_str >= 20150921:
			 			helper_czce.insert_text_to_sql(conn, s, d)
			 		else:
			 			html = 'http://www.czce.com.cn/portal/exchange/%s/datatradeholding/%d.htm' % (d[0], date_str)
			 			helper_czce.insert_text_to_sql(conn, s, d, html)
			 	else:
			 		remark = 'holiday'
			 #end try
		#end if week day >=5	
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (url, remark)) # 3+3 = 6
		conn.commit()
		print d, remark
		if count % 10 == 0 :		
			print "---%s seconds---" %(time.time() - t1)					
		#pdb.set_trace()
	#end for
	f1.close()	
#end startSpider


def test_czce_ccpm(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	#pdb.set_trace()
	with open(util.midfilePath() + 'test_czce_ccpm.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			if table_name == 'TABLE_TOTAL' or table_name == 'TABLE_URL':
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
			else:
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [5,8,11])

def main_func(end_date = '', see_csv = False):
	ce_name = 'czce/'
	db_name = 'czce_ccpm.db' #'daily_info.db'
	full_db_name = util.rootPath()+ce_name+db_name
	conn = sqlite3.connect(full_db_name)  

	helper_czce.create_table(conn, '', total_flag = True)
	util.createTABLE_URL(conn )

	url_info = getURL_list(conn)
	print 'unvisited url :', len(url_info)
	startSpider(conn, url_info)

	if see_csv :
		test_czce_ccpm(conn)
	conn.close()

if __name__ == '__main__':
	main_func(see_csv = True)		