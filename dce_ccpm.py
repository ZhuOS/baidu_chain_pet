# -*- coding: utf-8 -*- 
import requests, pdb, sys, codecs, copy
import sqlite3, csv, util, time

from os import  sys, path
father_folder = path.dirname( path.dirname(path.abspath(__file__)) ) #print father_folder
sys.path.append(father_folder)
import ExchangeDataReader
#pdb.set_trace()

def create_table(conn, flag , table_name= '', ):
	if flag == 'item':
		sql = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
		tradingday DATE, productid char, instrumentid char, rank INT, 
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

def string2database_dce_ccpm(conn, s ,d, param):	
	table = [[] for i in range (0,3) ] # we have three tables, volume, hold buy, hold sell
	table_idx = 0
	total_info = []
	write_table = False
	for line in s:
		line = line.replace('\n','').replace('\r','')
		if not write_table and line.find(u'名次') >= 0:			
			write_table = True
			continue
		
		if write_table and line.find(u'总计') >= 0:			
			write_table = False			
			table_idx += 1
			x = line.split()
			if len(x) < 3:
				x = [0, 0, 0]				
			#pdb.set_trace()
			#print x[1], x[2]
			total_info.append(x[1])
			total_info.append(x[2])

		if write_table:
			x = line.split()			
			table[table_idx].append( ' '.join(x[1:]) )# erase the rank	
	
	length_list = [len(x) for x in table ]
	max_len = max(length_list)
	for i in range (0,3):
		while len (table[i]) < max_len:		
			table[i].append( u' - 0 0 ') # make 3 table same height
			if len(table[i]) == max_len:
				break	
	
	list_of_str = [ str(i+1) + ' ' + ' '.join([table[0][i], table[1][i], table[2][i]]) for i in range(0, max_len)]
	#----------------------------------------	
	productid = param['Pu00021_Input.variety'].upper()
	cid_key = 'Pu00021_Input.contract_id'
	if param[cid_key] != '' :
		instrumentid = param[cid_key].upper()
	else:
		instrumentid = productid + '-all'

	table_name = 'TABLE_' + productid.upper()
	sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % table_name
	cs = conn.cursor()
	cs.execute(sql)	
	if len(cs.fetchall()) == 0:
		create_table(conn, 'item', table_name)
	#----------------------------------------
	for line in list_of_str:
		x = line.split()
		pkey = ''.join(d) +'-'+ instrumentid + '-' + x[0]  # date + contractid + rank is pkey
		tmp = [pkey, '-'.join(d), productid, instrumentid,] + x
		cs.execute('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,  ?,  ?,?,?, ?,?,?, ?,?,?)''' % table_name,  tmp) 	

	total_info = [''.join(d)+'-'+instrumentid, '-'.join(d), productid, instrumentid] + total_info
	cs.execute('''INSERT OR REPLACE INTO TABLE_TOTAL values	(?,?,?,?,   ?,?, ?,?, ?,?)''' , total_info) 
	conn.commit()

#-----------------------------

def getURL_list(conn, end_date = ''	):	
	# a  c m  y  l  p  v  start on 2010
	# j start on   2011
	# jd    jm  i  2013
	# cs pp        2015			
	reader = ExchangeDataReader.ExchangeDataReader()
	pid_list = ['i', 'a', 'c', 'm', 'y', 'p', 'v', 'l',
					'j', 'jd', 'jm', 'cs', 'pp',]	
	
	param = {}
	#param['action'] = 'Pu00021_result'
	param['action'] = 'Pu00022_download_1'	
	param['Pu00021_Input.content'] = 0
	param['Pu00021_Input.trade_type'] = 0
	site = 'http://www.dce.com.cn/PublicWeb/MainServlet'

	start_date = ('2015','01','01')
	#start_date = ('2016','07','07')	
	if end_date == '':
		end_date = ('2016','07','08')
	url_info = []
	for pid in pid_list:
		if pid !='cs' and pid != 'pp':
			d = start_date
		else:
			d =  ('2015','01','01')
		while d!= end_date:			
			param['Pu00021_Input.variety'] = pid
			param['Pu00021_Input.trade_date'] = ''.join(d)
			param['Pu00021_Input.prefix'] = '%s_%s' % (''.join(d), pid)
			param['Pu00021_Input.contract_id'] = ''  
			# sum contract ccpm
			url = ''.join(d) + '-' + pid + '-all'
			p = copy.deepcopy(param)
			tup = (url, d, site, p)
			url_info.append(tup)
			# every single contract ccpm
			date_str = '-'.join(d)
			tmp = reader.selectDailyData(columnName = ['instrumentid','openinterest'] , 
										contractid = pid.upper(),  startDate = date_str,
										 endDate = date_str, dataType = 'list')
			cid_list = []
			for tup in tmp:
				if tup[1] != 0:
					cid_list.append(tup[0])
			for cid in cid_list:				
				param['Pu00021_Input.contract_id'] = cid.lower()  
				url =  ''.join(d)+'-'+param['Pu00021_Input.contract_id']
				p = copy.deepcopy(param)
				tup = (url, d, site, p)
				url_info.append(tup)				
			
			d = util.next_day(d)
		#pdb.set_trace()
	visited_url = util.getVisitedURL(conn)
	res = []
	for tup in url_info:
		url = tup[0]
		if url not in visited_url:
			res.append(tup)
		else:
			visited_url_status = util.get_URL_status(conn, url)			
			if visited_url_status == 'error':
				res.append(tup)							
	return res


def startSpider(conn, url_info):
	exception_url_file = util.midfilePath() + 'dce_ccpm_exception_url.txt' 
	f1 = open(exception_url_file, 'w')
	t1 = time.time()
	count = 0
	for pkey, d, site, param in url_info:	
		count += 1	
		if util.weekday(d) >= 5:		
			remark = 'weekend'			
		else:	
			try:
				#print param
				r = requests.get(site, params = param, timeout = 5)					
			except Exception, e:
				print e
				f1.write(pkey)
				f1.write('\n')
				remark = 'error'

			else:														
				if r.encoding == 'GBK':
					remark = 'holiday'					
				else:										
					r.encoding = 'gbk'		
					text_stream = r.text.encode('utf-8')  # for txt
					filename = util.midfilePath() + 'dce_ccpm_raw.txt'
					with open(filename, 'w') as f2:
						f2.write(text_stream)	
					with codecs.open(filename,'r','utf-8') as f2:
						s = f2.readlines()
					if s[0][:3] == '<ht':
						remark = 'error'	
						f1.write(pkey)
						f1.write('\n')
					else:
						string2database_dce_ccpm(conn, s, d, param)
						remark = 'tradingday'												
		# end if weekday	
		print d, param['Pu00021_Input.contract_id'], remark
		if  count % 10 == 0 :		
			print "---%s seconds---" %(time.time() - t1)
		cs = conn.cursor()
		cs.execute('''INSERT OR REPLACE INTO TABLE_URL values 	(?,?)''' , (pkey, remark)) # 3+3 = 6
		conn.commit()			
	# end for	
	f1.close()
# end Spider


def test_dce_ccpm(conn):
	cs = conn.cursor()
	sql = 'select name FROM sqlite_master WHERE type = "table" '
	cs.execute(sql)
	table_name_list = cs.fetchall()
	#print table_name_list	
	#pdb.set_trace()
	with open(util.midfilePath() + 'test_dce_ccpm.csv','wb') as csv_file:
		csv_writer = csv.writer(csv_file)
		for x in table_name_list:
			table_name = x[0]  # x is a tuple
			if table_name == 'TABLE_TOTAL' or table_name == 'TABLE_URL':
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [])
			else:
				util.sql2csv_chinese(csv_writer, table_name, conn, col_of_chn = [5,8,11])

def main_func(end_date = '', see_csv = False):
	ce_name = 'dce/'
	db_name = 'dce_ccpm.db' #'daily_info.db'
	full_db_name = util.rootPath() + ce_name + db_name
	conn = sqlite3.connect(full_db_name)  
	create_table(conn, flag = 'init')
	url_info = getURL_list(conn ,end_date = end_date)
	print 'unvisited url :', len(url_info)
	startSpider(conn, url_info)

	if see_csv:
		test_dce_ccpm(conn)
	conn.close()

if __name__ == '__main__':
	d = util.get_today_tup()
	#print d
	main_func(end_date = d, see_csv = True)		