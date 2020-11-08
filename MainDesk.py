# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 19:50:50 2019

@author: lisd0
"""

# -*- coding: utf-8 -*-
# AlgoPlus量化投资开源框架范例
# 微信公众号：AlgoPlus
# 项目地址：http://gitee.com/AlgoPlus/AlgoPlus
# 项目网址：http://www.algo.plus
# 项目网址：http://www.ctp.plus
# 项目网址：http://www.7jia.com

import requests # 企业微信request 使用
from time import sleep
from multiprocessing import Process, Queue
from traderApi import TraderEngine
from tick_engine import TickEngine



            

class PortfolioManager(TraderEngine):
    def __init__(self, td_server, broker_id, investor_id, password, app_id, auth_code
                 , md_queue=None
                 , page_dir='', private_resume_type=2, public_resume_type=2):

        super(PortfolioManager, self).__init__(td_server, broker_id, investor_id, password, app_id, auth_code
                                               , md_queue
                                                  , page_dir, private_resume_type, public_resume_type)

        # 初始化参数
        self.init_parameter()

        # 等待子线程结束
        self.Join()




if __name__ == "__main__":
    from config.account_info import my_future_account_info_dict
    
    future_account = my_future_account_info_dict['SimNow24']
    
    # 监听队列
    instrument_id_list = future_account.instrument_info['InsList']
    
    #合约信息
    instrument_info = future_account.instrument_info['InsInfo']
    # 共享队列
    share_queue = Queue(maxsize=1000)
    share_queue.put(instrument_info)

    # 行情进程
    md_process = Process(target=TickEngine, args=(future_account.broker_id
                                                  ,future_account.server_dict['MDServer']
                                                  , future_account.investor_id
                                                  , future_account.password
                                                  , future_account.app_id
                                                  , future_account.auth_code
                                                  , instrument_id_list
                                                  , [share_queue]
                                                  , future_account.md_page_dir)
                          )


    # 交易进程
    trader_process = Process(target=PortfolioManager, args=( future_account.broker_id
                                                                , future_account.server_dict['TDServer']
                                                                , future_account.investor_id
                                                                , future_account.password
                                                                , future_account.app_id
                                                                , future_account.auth_code
                                                                , share_queue
                                                                , future_account.td_page_dir)
                               )

    md_process.start()
    trader_process.start()

    md_process.join()
    trader_process.join()