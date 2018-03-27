import sqlite3, pdb, re, copy
import requests

#-------------------------
def create_table(conn, table_name, item_flag = False, total_flag = False):
	
	sql1 = '''CREATE TABLE IF NOT EXISTS %s ( pkey char primary key,
		tradingday DATE, productid varchar(10), instrumentid varchar(10), rank INT, 
		company_name1 nchar, volume1 INT, variation1 INT,
		company_name2 nchar, volume2 INT, variation2 INT,
		company_name3 nchar, volume3 INT, variation3 INT) ''' % table_name
	if item_flag:
		print 'create_table', table_name
		conn.execute(sql1)

	sql2 = '''CREATE TABLE IF NOT EXISTS TABLE_TOTAL ( pkey char primary key,
		tradingday DATE, productid varchar(10) , instrumentid varchar(10),
		volume1 INT, variation1 INT, volume2 INT, variation2 INT,volume3 INT, variation3 INT)'''
	if total_flag:
		conn.execute(sql2) 
	conn.commit()

#-------------------------
def get_item_month(line):
	# input a unicode object	
	pattern1 = u'[A-Z]{1,2}[0-9]{3,4}'	#contract id
	pattern2 = u'[A-Z]{2,3}' # product id

	r1 = re.compile(pattern1, re.UNICODE)
	r2 = re.compile(pattern2, re.UNICODE)
	match1 = r1.search(line)
	
	if match1:
		s = match1.group()
		for i in range(0, len(s)):
			if s[i].isdigit():
				break
		
		# print 'match1', s[0:i], '1'+s[i:]		
		return s[0:i], s[i:]
	else:
		match2 = r2.search(line)
		if match2:
			s = match2.group()
			if s == 'PTA':
				s = 'TA'
			# print 'match2', s
			return s, 'all'
		else:
			# print 'not match'
			return None, None
	#end if

def get_info2split(s, d):
	# d is date tuple
	# s is list of line of the txt
	date_str = '-'.join(d)
	start_idx_list = []
	productid_list = [] # list of str
	contractid_list = []
	#----------------------
	for i in range(0,len(s)):	
		line = s[i]
		date_position = line.find(date_str)
		if date_position != -1 and i != 0:
			start_idx_list.append(i + 2)							
			a,b = get_item_month(line)
			productid_list.append(a)
			if b!= 'all':
				contractid_list.append(a+b)
			else:
				contractid_list.append(a+'-'+b)			
	#----------------------			
	line_num =  len(s)
	# print line_num
	while not s[line_num-1].strip():
		# print line_num,'is empty'
		line_num -=1
	# print line_num

	table_num = len(start_idx_list)
	end_idx_list = [0 for x in start_idx_list]
	for i in range(0, table_num):
		if i != table_num - 1:
			end_idx_list[i] = start_idx_list[i+1] - 5
		else:
			end_idx_list[table_num - 1] = line_num - 2
		# print start_idx_list[i], end_idx_list[i], end_idx_list[i] - start_idx_list[i] + 1

	return start_idx_list, end_idx_list, productid_list, contractid_list
	# return 4 list
def get_info2split_before(s,d, url):
	
	r = requests.get(url)
	#print r.encoding
	r.encoding = 'gbk'	
	text_stream = r.text.encode('utf-8')

	date_str = '-'.join(d)
	pattern = ur'<b>.{0,80}%s</b>' % date_str # find the line which has date pattern
	r = re.compile(pattern, re.UNICODE)
	match = r.findall(text_stream)		

	productid_list = [] # list of str
	contractid_list = []
	for item in match:
		a,b =  get_item_month(item)
		productid_list.append(a)
		if b!= 'all':
			contractid_list.append(a+b)
		else:
			contractid_list.append(a+'-'+b)	
	
	start_idx_list = []
	#----------------------
	for i in range(0,len(s)):	
		line = s[i]
		date_position = line.find(date_str)
		if date_position != -1 and i != 0:
			start_idx_list.append(i + 1)
	#----------------------	
	line_num =  len(s)
	# print line_num
	while not s[line_num-1].strip():
		# print line_num,'is empty'
		line_num -=1
	# print line_num

	table_num = len(start_idx_list)
	end_idx_list = [0 for x in start_idx_list]
	for i in range(0, table_num):
		if i != table_num - 1:
			end_idx_list[i] = start_idx_list[i+1] - 3
		else:
			end_idx_list[table_num - 1] = line_num - 2
		# print start_idx_list[i], end_idx_list[i], end_idx_list[i] - start_idx_list[i] + 1

	return start_idx_list, end_idx_list, productid_list, contractid_list
#------------------------------------------------	
def insert_text_to_sql(conn, s, d, html = ''):
	cs = conn.cursor()
	#-------------------------------
	if html == '':
		start_idx_list, end_idx_list, productid_list, contractid_list = get_info2split(s,d)
	else:
		start_idx_list, end_idx_list, productid_list, contractid_list = get_info2split_before(s, d, html)
	# process different condition	
	#-------------------------------
	d1 = '-'.join(d)#date format 20xx-0x-0x
	d2 = ''.join(d) #date format 20xx0x0x

	for i in range(0, len(start_idx_list)):
		productid = productid_list[i]
		table_name = 'TABLE_' + productid
		instrumentid = contractid_list[i]	
		#-----check table exist---------
		sql = 'select name FROM sqlite_master WHERE type = "table" AND name = "%s" ' % table_name
		cs.execute(sql)
		if len(cs.fetchall()) == 0:
			create_table(conn, table_name, item_flag = True)	
		#-------------------------------
		tmp1 = s[start_idx_list[i]: end_idx_list[i] + 1]		
		def parse_str(x):
			return x.replace(',','').replace(' ','')
		#-------------------------------	
		if html == '':
			tmp2 = [parse_str(x).split('|') for x in tmp1]	# process different condition	
		else:
			tmp2 = [x.split(',') for x in tmp1]
		#-------------------------------
		dataTable = []
		for line in tmp2:
			tmp3 = copy.deepcopy(line)			
			for j in range(1,4):
				jj = (j-1) *3 + 1
				#pdb.set_trace()
				if tmp3[jj] == '-' or tmp3[jj] == '':# process different condition	
					tmp3[jj]  = '-'
					tmp3[jj+1] = 0
					tmp3[jj+2] = 0
			pkey1 = '-'.join([d2, instrumentid, tmp3[0]]) #tmp3[0] is rank			
			new_line = [pkey1, d1, productid, instrumentid] + tmp3		
			dataTable.append(new_line)
		 	 		
		cs.executemany('''INSERT OR REPLACE INTO %s values 	(?,?,?,?,?,  ?,?,?, ?,?,?, ?,?,?  )''' % table_name,  dataTable) 

		pkey2 = '-'.join([d2, instrumentid,])
		#-------------------------------
		if html == '':
			tmp2 =  s[end_idx_list[i]+1].replace('|','').split()  # process different condition	
		else:
			tmp2 =  s[end_idx_list[i]+1].replace(',',' ').split()
		#-------------------------------		
		#tmp2[0] = 
		x = [pkey2, d1,productid, instrumentid] + tmp2[1:] 
		#pdb.set_trace()
		cs.execute('''INSERT OR REPLACE INTO TABLE_TOTAL values	(?,?,?,?,   ?,?, ?,?, ?,?   )''' , x) 
		
		#pdb.set_trace()
	conn.commit()