# -*- coding: utf-8 -*-
# AlgoPlus量化投资开源框架范例
# 微信公众号：AlgoPlus
# 项目码云地址：http://gitee.com/AlgoPlus/AlgoPlus
# 项目Github地址：http://github.com/CTPPlus/AlgoPlus
# 项目网址：http://www.algo.plus
# 项目网址：http://www.ctp.plus
# 项目网址：http://www.7jia.com
# 项目QQ交流群：866469866

# 通过Tushare获取最新合约
import os
import pandas as pd
import tushare as ts
import datetime

token = '085032d38736787cc341e837f8eb2e26f52d8b9aca76cd39bebaf757'
ts.set_token(token)

pro = ts.pro_api(token)

exchanges = ['CFFEX','DCE','CZCE','SHFE','INE']

inst_id_list = list()

today_dt = datetime.datetime.today().date()

today_int = today_dt.year*10000 + today_dt.month*100 + today_dt.day
upper_case_names = ['SR','SF','SM']
for exg in exchanges:
    df = pro.fut_basic(exchange=exg, fut_type='1', 
                   fields='ts_code,symbol,exchange,\
                   name,fut_code,multiplier,trade_unit,per_unit,quote_unit,\
                   list_date,delist_date,d_month,last_ddate')
    
    df['_id'] = df['ts_code']
    df['list_date'] = df['list_date'].astype(int) 
    df['delist_date'] = df['delist_date'].astype(int)
    df['last_ddate'] = df['delist_date'].astype(int)
    namelist = df[df['delist_date']>today_int]
    simnow_names = [rows['symbol'].lower()  if exg != 'CZCE' else rows['symbol'].upper() for index,rows in namelist.iterrows()]
    inst_id_list = inst_id_list + simnow_names
    
BASE_LOCATION = "."
MD_LOCATION = BASE_LOCATION + os.path.sep + "MarketData"
TD_LOCATION = BASE_LOCATION + os.path.sep + "TradingData"
SD_LOCATION = BASE_LOCATION + os.path.sep + "StrategyData"

#inst_id_list = list(pd.read_csv('TargetListenContract.csv', encoding='gbk')['LegalContract'])
byte_inst_id_list = [instrument.encode('ascii') for instrument in inst_id_list]

class FutureAccountInfo:
    def __init__(self, broker_id, server_dict, reserve_server_dict, investor_id, password, app_id, auth_code, instrument_id_list, md_page_dir=MD_LOCATION, td_page_dir=TD_LOCATION):
        self.broker_id = broker_id  # 期货公司BrokerID
        self.server_dict = server_dict  # 登录的服务器地址
        self.reserve_server_dict = reserve_server_dict  # 备用服务器地址
        self.investor_id = investor_id  # 账户
        self.password = password  # 密码
        self.app_id = app_id  # 认证使用AppID
        self.auth_code = auth_code  # 认证使用授权码
        self.instrument_id_list = instrument_id_list  # 订阅合约列表[]
        self.md_page_dir = md_page_dir  # MdApi流文件存储地址，默认MD_LOCATION
        self.td_page_dir = td_page_dir  # TraderApi流文件存储地址，默认TD_LOCATION
        self.order_action_num_dict={}


my_future_account_info_dict = {
    # 交易时间-凌云账户
    'SimNow-LY': FutureAccountInfo(
        broker_id='9999',  # 期货公司BrokerID
        # TDServer为交易服务器，MDServer为行情服务器。服务器地址格式为"ip:port。"
        server_dict={'TDServer': "180.168.146.187:10101", 'MDServer': '180.168.146.187:10110'},
        # 备用服务器地址
        reserve_server_dict={'电信1': {'TDServer': "180.168.146.187:10100", 'MDServer': '180.168.146.187:10110'},
                             '电信2': {'TDServer': "180.168.146.187:10101", 'MDServer': '180.168.146.187:10111'},

                             '其他1': {'TDServer': "180.168.146.187:10130", 'MDServer': '180.168.146.187:10131'},  # 7*24
                             '其他2': {'TDServer': "218.202.237.33:10102", 'MDServer': '218.202.237.33:10112'},  # 移动
                             },
#        investor_id='044390',  # 测试账户 密码相同
##        investor_id='044392',  # 账户 密码相同
        investor_id='044393',  # 账户 密码相同

        password='123456',  # 测试账户密码
#        investor_id='153958',  # CTA策略账户
#        password='Lovdam8785!',  # CTA策略密码

        app_id='simnow_client_test',  # 认证使用AppID
        auth_code='0000000000000000',  # 认证使用授权码
        # 订阅合约列表
        instrument_id_list=byte_inst_id_list,
    ),
    # 交易时间-朝远账户
    'SimNow-CY': FutureAccountInfo(
        broker_id='9999',  # 期货公司BrokerID
        # TDServer为交易服务器，MDServer为行情服务器。服务器地址格式为"ip:port。"
        server_dict={'TDServer': "180.168.146.187:10101", 'MDServer': '180.168.146.187:10111'},
        # 备用服务器地址
        reserve_server_dict={'电信1': {'TDServer': "180.168.146.187:10100", 'MDServer': '180.168.146.187:10110'},
                             '电信2': {'TDServer': "180.168.146.187:10101", 'MDServer': '180.168.146.187:10111'},

                             '其他1': {'TDServer': "180.168.146.187:10130", 'MDServer': '180.168.146.187:10131'},  # 7*24
                             '其他2': {'TDServer': "218.202.237.33:10102", 'MDServer': '218.202.237.33:10112'},  # 移动
                             },
#        investor_id='044390',  # 测试账户 密码相同
##        investor_id='044392',  # 账户 密码相同
#        investor_id='044393',  # 账户 密码相同

#        password='123456',  # 测试账户密码
        investor_id='153958',  # CTA策略账户
        password='Lovdam8785!',  # CTA策略密码

        app_id='simnow_client_test',  # 认证使用AppID
        auth_code='0000000000000000',  # 认证使用授权码
        # 订阅合约列表
        instrument_id_list=byte_inst_id_list,
    ),

    # 非交易使用测试
    'SimNow24': FutureAccountInfo(
        broker_id='9999',  # 期货公司BrokerID
        # TDServer为交易服务器，MDServer为行情服务器。服务器地址格式为"ip:port。"
        server_dict={'TDServer': "180.168.146.187:10130", 'MDServer': '180.168.146.187:10131'},
        # 备用服务器地址
        reserve_server_dict={},
#        investor_id='044390',  # 测试账户 密码相同
#        investor_id='044392',  # 账户 密码相同
        investor_id='044393',  # 账户 密码相同
        password='123456',  # 密码
        app_id='simnow_client_test',  # 认证使用AppID
        auth_code='0000000000000000',  # 认证使用授权码
        # 订阅合约列表
        instrument_id_list=byte_inst_id_list,
    ),
}
