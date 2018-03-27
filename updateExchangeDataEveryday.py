# -*-coding:utf-8-*-
import cffex_param,  cffex_daily, cffex_ccpm
import czce_param,   czce_daily,  czce_ccpm
import dce_param,    dce_daily,   dce_ccpm
import shfe_param,   shfe_daily,  shfe_ccpm

import datetime, util


today = util.get_today_tup()
flag = False
 
cffex_param.main_func( see_csv = flag)
czce_param.main_func(end_date = today, see_csv = flag)
dce_param.main_func(end_date = today, see_csv = flag)
shfe_param.main_func(end_date = today, see_csv = flag)

cffex_daily.main_func(end_date = today, see_csv = flag)
czce_daily.main_func(end_date = today, see_csv = flag)
dce_daily.main_func(end_date = today, see_csv = flag)
shfe_daily.main_func(end_date = today, see_csv = flag)

cffex_ccpm.main_func(end_date = today, see_csv = flag)
czce_ccpm.main_func(end_date = today, see_csv = flag)
dce_ccpm.main_func(end_date = today, see_csv = flag)
shfe_ccpm.main_func(end_date = today, see_csv = flag)