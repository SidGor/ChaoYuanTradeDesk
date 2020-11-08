# -*- coding: utf-8 -*-
"""
Created on Sat Dec 21 11:38:48 2019

@author: lisd0
"""
import numpy as np
import datetime
import pandas as pd
import os
import requests
from multiprocessing import Queue
from AlgoPlus.CTP.TraderApiBase import TraderApiBase
from AlgoPlus.CTP.ApiStruct import *
from AlgoPlus.utils.base_field import to_bytes, to_str
from time import sleep

class TraderEngine(TraderApiBase):
    def __init__(self,broker_id, td_server, investor_id, password, app_id, auth_code
                 ,md_queue=None,
                 page_dir='', private_resume_type=2, public_resume_type=2):
        
        self.in_trading_hour = False # 是否交易时间
        self.balance_init_save = False #是否保存了前结算balance
        self.md_dict = {}  # 行情字典
        self.instrument_info = md_queue.get() # 合约信息 初始化的时候，在MainDesk先把第一个info给put到Queue的第一顺位
        self.local_rtn_trade_list = []  # 成交通知列表
        self.last_rtn_trade_id = 0  # 已处理成交ID
        self.local_position_dict = {}  # {"InstrumentID": {"ActionNum": 0, "LongVolume": 0, "LongPositionList": [], "ShortVolume": 0, "ShortPositionList": []}}
        self.instrument_id_registered = []  # 所有持仓合约，包括已平
        self.position_qry_rst = []
        self.order_qry_rst = []
        self.action_num_dict = {}  # 撤单次数 # {"InstrumentID": 0}
        self.order_ref = 0
        self.pos_dict = {}
        self.account_balance = {'Balance':None,'Margin':None}
        self.withdraw_count = {}
        self.withdraw_limit = {'RegularOrder':249, 'BigOrder':49} #正常单是500次，单笔撤单量超过300手限制是50次
        self.withdraw_BotCommunicateLimit = 1
        self.MarginLimit = 0.75
        self.trade_vol_limit = 18
        
        self.Join()
################################## Logging 及信息 ####################################    
    def write_log(self, *args, sep=' || '):
        path = './Logs/acc%s_log_td_%s.txt' % (self.investor_id.decode(),datetime.datetime.today().strftime('%Y%m%d'))
        if os.path.exists(path):
            append_write = 'a'
        else:
            append_write = 'w'

        local_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = f'{local_time}{sep}{self.investor_id}.td{sep}{sep.join(map(str, args))}'
        print(log)

        log_doc = open(path,append_write)
        log_doc.write(log + ' \n')
        log_doc.close()


    def MsgWechatBotCY(self,content, mentioned_list = ['李思达'], mentioned_mobile_list = [],msgtype = 'text'):
        data = {
                 "msgtype": msgtype,
                 "text": {
                         "content": content,
                         "mentioned_list":mentioned_list,
                         "mentioned_mobile_list":[]
                         }
                 }
        r = requests.post(url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6976b971-43fd-4cd4-bda7-64d045d94a53', json=data)
        return

########################################################################################    

################################## 回报信息 ####################################    

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        rst = pTradingAccount
        self.write_log('OnRspQryTradingAccount', pTradingAccount)
        self.account_balance['Balance'] = rst['Balance']
        self.account_balance['Margin'] = rst['CurrMargin']
        if not self.balance_init_save:
            if ('%s_AccBalance.csv' % self.investor_id.decode()) not in os.listdir('./BalanceRcd'):
                o_file = open('./BalanceRcd/%s_AccBalance.csv' % self.investor_id.decode(), 'a+')
                o_file.write('TradingDay,AccountID,PreBalance\n')
                o_file.close()

            else:
                pass
                        
            o_file = open('./BalanceRcd/%s_AccBalance.csv' % self.investor_id.decode(), 'a')
            o_file.write(','.join([(rst['TradingDay'].decode()),
                        self.investor_id.decode(),
                        str(rst['PreBalance'])])+'\n')
            o_file.close()
            self.balance_init_save = True
        else:
            pass            
            
    def OnRtnTrade(self, pTrade):
        #成交回报
        # self.write_log('OnRtnTrade', pTrade)
        if ('%s_TradeRcd.csv' % self.investor_id.decode()) not in os.listdir('./TradeRcd'):
            o_file = open('./TradeRcd/%s_TradeRcd.csv' % self.investor_id.decode(), 'a+')
            o_file.write(','.join(pTrade.keys())+'\n')
            o_file.close()

        else:
            pass
                    
        o_file = open('./TradeRcd/%s_TradeRcd.csv' % self.investor_id.decode(), 'a')
        o_file.write(','.join([str(v) for v in pTrade.values()])+'\n')
        o_file.close()
        
        
        
    def OnFrontConnected(self):
        if self.status == -2:
          self.write_log('OnFrontConnected', '重连成功！')
          self.MsgWechatBotCY('td模块重连成功')
          
          
    # ///当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。   
    def OnFrontDisconnected(self, nReason):
        self.status = -2
        self.write_log('OnFrontDisconnected', nReason)
        self.MsgWechatBotCY('td模块检测到交易后台通信断开，请迅速检查交易模块')
        
    # ############################################################################# #
    # ///心跳超时警告。当长时间未收到报文时，该方法被调用。
    def OnHeartBeatWarning(self, nTimeLapse):
        self.write_log('OnHeartBeatWarning', nTimeLapse)
        self.MsgWechatBotCY('td模块心跳超时警告，请迅速检查交易模块')
    
    
    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
            """
            录入撤单回报。不适宜在回调函数里做比较耗时的操作。可参考OnRtnOrder的做法。
            :param pInputOrder: AlgoPlus.CTP.ApiStruct中InputOrderField的实例。
            :param pRspInfo: AlgoPlus.CTP.ApiStruct中RspInfoField的实例。包含错误代码ErrorID和错误信息ErrorMsg
            :param nRequestID:
            :param bIsLast:
            :return:
            """
            if self.is_my_order(pInputOrder.OrderRef):
                if pRspInfo.ErrorID != 0:
                    self.on_insert_fail(pInputOrder)
                self.write_log(f"{pRspInfo}=>{pInputOrder}")
                # # 延时计时开始
                # # 如果需要延时数据，请取消注释
                # # 不适宜在回调函数里做比较耗时的操作。
                # self.anchor_time = timer()
                # self.timer_dict["FunctionName"] = "OnRspOrderInsert"
                # self.timer_dict["OrderStatus"] = b""
                # self.timer_dict["AnchorTime"] = self.anchor_time
                # self.timer_dict["DeltaTime"] = self.anchor_time - self.start_time
                # self.csv_writer.writerow(self.timer_dict)
                # self.csv_file.flush()
                # # 延时计时结束    
    def on_insert_fail(self, rtn_order):
        pass 
    
    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        self.write_log('OnRspQryOrder',pOrder)
        self.order_qry_rst.append(pOrder)
        
    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):

        self.write_log(f"{pRspInfo}=>{pInputOrderAction}")        
        
########################################################################################    
       
#    def InqDetailPosition(self):
#        pQryInvestorPositionCombineDetail = QryInvestorPositionCombineDetailField(
#                                                        BrokerID = self.broker_id,
#                                                        InvestorID=self.investor_id)
#        
#        self.ReqQryInvestorPositionCombineDetail(pQryInvestorPositionCombineDetail)
#
#    def OnRspQryInvestorPositionCombineDetail(self, pInvestorPositionCombineDetail, pRspInfo, nRequestID, bIsLast):
#        
#        print(pInvestorPositionCombineDetail)
    def InqOrderRef(self):
        self.order_ref+=1
    def GetTradingPeriod(self, bypass = True):
        
        if bypass:
            return((True,'day')) #默认任何时候都是可交易时段(用于simnow24)
        else:
            check_point = datetime.datetime.now().time()
            morning_a =  datetime.time(9,00,5) < check_point < datetime.time(10,16,00)
            morning_b = datetime.time(10,30,0) < check_point < datetime.time(11,30,00)
            afternoon = datetime.time(13,30,0) < check_point < datetime.time(15,29,59)
            night_a = datetime.time(21,00,1) < check_point < datetime.time(23,59,59)
            night_b = datetime.time(0,0,0) < check_point < datetime.time(2,30,00)

            if morning_a or morning_b or afternoon:
                return((True,'day'))
            elif night_a or night_b:
                return((True,'night'))
            else:
                return((False,'out'))
                
    def req_order_insert(self, exchange_id, instrument_id, order_price, order_vol, order_ref, direction, offset_flag):
        """
        录入报单请求。将订单结构体参数传递给父类方法ReqOrderInsert执行。
        :param exchange_id:交易所ID。
        :param instrument_id:合约ID。
        :param order_price:报单价格。
        :param order_vol:报单手数。
        :param order_ref:报单引用，用来标识订单来源。
        :param direction:买卖方向。
        (‘买 : 0’,)
        (‘卖 : 1’,)
        :param offset_flag:开平标志，只有SHFE和INE区分平今、平昨。
        (‘开仓 : 0’,)
        (‘平仓 : 1’,)
        (‘强平 : 2’,)
        (‘平今 : 3’,)
        (‘平昨 : 4’,)
        (‘强减 : 5’,)
        (‘本地强平 : 6’,)
        :return:
        """
        input_order_field = InputOrderField(
            BrokerID=self.broker_id,
            InvestorID=self.investor_id,
            ExchangeID=exchange_id,
            InstrumentID=instrument_id,
            UserID=self.investor_id,
            OrderPriceType=b'2',
            Direction=direction,
            CombOffsetFlag=offset_flag,
            CombHedgeFlag=b'1',
            LimitPrice=order_price,
            VolumeTotalOriginal=int(order_vol),
            TimeCondition=b'3',
            VolumeCondition=b'1',
            MinVolume=1,
            ContingentCondition=b'1',
            StopPrice=0,
            ForceCloseReason=b'0',
            IsAutoSuspend=0,
            OrderRef=to_bytes(self.order_ref),
        )
        self.ReqOrderInsert(input_order_field)    
    def is_my_order(self, order_ref):
        """
        以OrderRef标识本策略订单。
        """
        return True
    

           
    def InqPosition(self):
        pQryInvestorPosition = QryInvestorPositionField(BrokerID = self.broker_id,
                                                        InvestorID=self.investor_id)
        self.ReqQryInvestorPosition(pQryInvestorPosition) # 查询持仓
        self.write_log(f"=>发出查询持仓请求！")
        
    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
#        self.position = pInvestorPosition
#        print(f"\n\n{pd.DataFrame(pInvestorPosition)}")
        self.position_qry_rst.append(pInvestorPosition)
#        print(rst)
#        self.write_log("OnRspQryInvestorPosition", pInvestorPosition)        
    
    def get_default_price(self, instrument_id, direction):
        """
        获取默认报单价格。
        :param instrument_id: 合约
        :param direction: 持仓方向
        :return: 报单价格
        """
        print(instrument_id)
        # max_price = md_dict[instrument_id]['UpperLimitPrice'] #挂涨跌停板
        # min_price = md_dict[instrument_id]['LowerLimitPrice'] #挂涨跌停板
        # AskPrice = self.md_dict[instrument_id]["AskPrice1"] if self.md_dict[instrument_id]["AskPrice1"]<max_price else max_price
        # BidPrice = self.md_dict[instrument_id]["BidPrice1"] if self.md_dict[instrument_id]["BidPrice1"]>min_price else min_price
        
        return self.md_dict[instrument_id]["AskPrice1"]+self.instrument_info[instrument_id]['PriceTick']*5 if direction == b"1" \
                else self.md_dict[instrument_id]["BidPrice1"]-self.instrument_info[instrument_id]['PriceTick']*5 # 上下5个tick主动成交
        # return self.md_dict[instrument_id]['UpperLimitPrice'] if direction == b"1" else self.md_dict[instrument_id]['LowerLimitPrice'] #市价成交


    def CheckOrders(self):
        pQryOrder = QryOrderField(BrokerID = self.broker_id, InvestorID = self.investor_id                                  
                                    )
        self.ReqQryOrder(pQryOrder)
        
    
    def WithdrawOrder(self, InstrumentID, amount):
        pass
    def SummaryOrder(self):
        today = datetime.datetime.now()
        while len(self.order_qry_rst) != 0:
            per_order = self.order_qry_rst.pop()
            if len(per_order.keys())>0:
                # print(per_order.keys())
                # print(len(per_order.keys()))
                # print(per_order['StatusMsg'].decode('gbk'))
                if  (per_order['StatusMsg'].decode('gbk') in ['未成交','部分成交']):                
                    open_date = per_order['InsertDate'].decode('gbk')
                    open_time = per_order['InsertTime'].decode('gbk').split(':')
                    open_datetime = datetime.datetime(year = int(open_date[0:4]),month = int(open_date[4:6]), day = int(open_date[6:]),
                                                      hour = int(open_time[0]), minute = int(open_time[1]), second = int(open_time[2]))
                    
                    if(datetime.datetime.now() - open_datetime)> datetime.timedelta(seconds=360): # 超时撤单
    #                print(open_datetime)
    #                print((datetime.datetime.now() - datetime.timedelta(seconds=10))> open_datetime)
    #                if(datetime.datetime.now() - datetime.timedelta(seconds=10))> open_datetime:
    #                    pass
                        self.req_order_action(exchange_id = per_order['ExchangeID'],
                                              instrument_id = per_order['InstrumentID'],
                                              order_ref = per_order['OrderRef'],
                                              order_sysid = per_order['OrderSysID'])        
    #        order_data.to_csv('D:/order_template.csv',index=False, encoding='gbk')
        
        self.order_qry_rst = []

    def req_order_action(self, exchange_id, instrument_id, order_ref, order_sysid=''):
            """
            撤单请求。将撤单结构体参数传递给父类方法ReqOrderAction执行。
            :param exchange_id:交易所ID
            :param instrument_id:合约ID
            :param order_ref:报单引用，用来标识订单来源。根据该标识撤单。
            :param order_sysid:系统ID，当录入成功时，可在回报/通知中获取该字段。
            :return:
            """
            
            # 撤单次数检查(包括在交易终端上开单也有可能导致撤单)：
            # 现在不需要分辨大小单，因为下单数量被限制在trade_vol_limit
            if instrument_id not in self.withdraw_count:
                self.withdraw_count[instrument_id] = {'RegularOrdWithdrawTimes':0,
                                                     'BigOrdWithdrawTimes':0,
                                                     'BotWarnings':0}
            else:
                pass                
            
            # 撤回次数超过限制不开仓
            RegularOrderRestricted = self.withdraw_count[instrument_id]['RegularOrdWithdrawTimes'] >= self.withdraw_limit['RegularOrder']
            BigOrderRestricted = self.withdraw_count[instrument_id]['BigOrdWithdrawTimes'] >= self.withdraw_limit['BigOrder']
            
            if (RegularOrderRestricted | BigOrderRestricted):
                self.write_log('req_order_action','Reach order withdraw limit',instrument_id)
                if self.withdraw_count[instrument_id]['BotWarnings'] < self.withdraw_BotCommunicateLimit:
                    
                    self.MsgWechatBotCY(('检测到：%s 撤单次数超出限制(%s 次)（来自于直接撤单函数），不予撤单，请考虑手动调整' % (instrument_id.decode(),self.withdraw_limit['RegularOrder'])))
                    self.withdraw_count[instrument_id]['BotWarnings'] += int(1)
                return()
            else:
                pass
            
            input_order_action_field = InputOrderActionField(
                BrokerID=self.broker_id,
                InvestorID=self.investor_id,
                UserID=self.investor_id,
                ExchangeID=exchange_id,
                ActionFlag=b'0',
                InstrumentID=instrument_id,
                FrontID=self.front_id,
                SessionID=self.session_id,
                OrderSysID=order_sysid,
                OrderRef=order_ref,
            )
            # print('\n\n\n', instrument_id,'\n','withdrawing','\n\n\n')
            
            self.withdraw_count[instrument_id]['RegularOrdWithdrawTimes'] += int(1) #增加计数器
            
            self.ReqOrderAction(input_order_action_field)
            
    
    def OpenContract(self,exchange_id,instrument_id, open_vol):
        # 拆单
        real_vol = self.trade_vol_limit if abs(open_vol)>=self.trade_vol_limit else abs(open_vol)
        open_vol = int(open_vol/abs(open_vol)) * real_vol
        
        # 无数据
        if instrument_id not in self.md_dict.keys():
            self.write_log("OpenContract||Instrument:%s not having md update" % instrument_id.decode())        

            return()
        
        else:
            pass
        # 当天首次开仓更新
        if instrument_id not in self.withdraw_count.keys():
            self.withdraw_count[instrument_id] = {'RegularOrdWithdrawTimes':0,
                                                  'BigOrdWithdrawTimes':0,
                                                  'BotWarnings':0}
        else:
            pass
        
        # 撤回次数超过限制不开仓
        RegularOrderRestricted = self.withdraw_count[instrument_id]['RegularOrdWithdrawTimes'] >= self.withdraw_limit['RegularOrder']
        BigOrderRestricted = self.withdraw_count[instrument_id]['BigOrdWithdrawTimes'] >= self.withdraw_limit['BigOrder']
        
        if (RegularOrderRestricted | BigOrderRestricted):
            self.write_log('OpenContract','Reach order withdraw limit',instrument_id)
            if self.withdraw_count[instrument_id]['BotWarnings'] < self.withdraw_BotCommunicateLimit:
                
                self.MsgWechatBotCY(('检测到：%s 撤单次数超出限制，不予开仓，请考虑手动调整' % instrument_id.decode()))
                self.withdraw_count[instrument_id]['BotWarnings'] += int(1)
            return()
        else:
            pass
        
        # 资金量检查
        # open_vol 为正负整数，正负为开仓方向，比如 int(1), -int(10)
        
        self.InqOrderRef()
        default_price = self.get_default_price(instrument_id, direction = b'1' if open_vol > 0 else b'0')
        
        if default_price == None:
            self.write_log('OpenContract','Default price is None',instrument_id)
            return()
        else:
            # 保证金检查
            MarRatio = self.instrument_info[instrument_id]['LongMarginRatio'] if open_vol >0 else self.instrument_info[instrument_id]['ShortMarginRatio']
            VM =  self.instrument_info[instrument_id]['VolumeMultiple'] 
            # print(self.account_balance['Margin'])
            MarValue = MarRatio*abs(int(open_vol))*VM*default_price #计算保证金占用
            ProjectedMargin = self.account_balance['Margin'] + MarValue
            
            if (ProjectedMargin/self.account_balance['Balance']) > self.MarginLimit:
                self.write_log('OpenContract','ReachMarginLimit',instrument_id)
                if self.withdraw_count[instrument_id]['BotWarnings'] < self.withdraw_BotCommunicateLimit:
                    self.MsgWechatBotCY('检测到：%s 开仓%s 手占用保证金 %s 元超出限制，不予开仓，请考虑手动调整' % (instrument_id.decode(),open_vol,MarValue))
                    self.withdraw_count[instrument_id]['BotWarnings'] += int(1)
                
                return()
            else:
                pass
    #        print(instrument_id,default_price)
                self.req_order_insert(exchange_id=exchange_id,
                              instrument_id=instrument_id,
                              order_price=self.get_default_price(instrument_id, direction = b'1' if open_vol > 0 else b'0'),
    #                          order_vol=str(abs(open_vol)).encode('ascii'),
                              order_vol=abs(open_vol),
                              order_ref = self.order_ref,
                              direction = b"0" if open_vol>0 else b"1", #开仓方向,
                              offset_flag = b"0"
                             )

    def CloseContract(self,exchange_id,instrument_id, close_vol, close_yd_vol,close_td_vol):
        
        # 平仓算法
        # close_vol 为正负整数，正负为开仓方向，比如 int(1), -int(10)

        
        # 无数据
        if instrument_id not in self.md_dict.keys():
            self.write_log("CloseContract||Instrument:%s not having md update" % instrument_id.decode())        

            return()
        
        else:
            pass
        # 当天首次平仓更新
        
        if instrument_id not in self.withdraw_count.keys():
            self.withdraw_count[instrument_id] = {'RegularOrdWithdrawTimes':0,
                                                  'BigOrdWithdrawTimes':0,
                                                  'BotWarnings':0}
        else:
            pass

        self.InqOrderRef()
        default_price = self.get_default_price(instrument_id, direction = b'1' if close_vol > 0 else b'0')
        if default_price != None:
            # print(close_td_vol)
            # print(close_yd_vol)
            # print(close_vol)

            if close_td_vol + close_yd_vol != close_vol:
                raise ValueError("%s total close vol not match with td_vol and yd_vol!" % instrument_id)
    
            if exchange_id == b'SHFE' or exchange_id == b'INE':
                #平昨
                # 拆平昨单
                actual_yd_vol = self.trade_vol_limit if abs(close_yd_vol) >= self.trade_vol_limit else abs(close_yd_vol)
                
                self.req_order_insert(exchange_id = exchange_id,
                                      instrument_id = instrument_id,
                                      order_price=self.get_default_price(instrument_id, direction = b'1' if close_vol > 0 else b'0'),
#                                      order_vol = str(abs(close_yd_vol)).encode('ascii'),
                                      order_vol=actual_yd_vol,
                                      order_ref = self.order_ref, 
                                      direction = b"0" if close_yd_vol>0 else b"1", #开仓方向,
                                      offset_flag = b'4')
                #优先平昨，然后平今
                # 拆平今单
                actual_td_vol = self.trade_vol_limit if abs(close_td_vol) >= self.trade_vol_limit else abs(close_td_vol)

                if abs(close_td_vol) > 0:
                    self.req_order_insert(exchange_id = exchange_id,
                                          instrument_id = instrument_id,
                                          order_price=self.get_default_price(instrument_id, direction = b'1' if close_vol > 0 else b'0'),
#                                          order_vol = str(abs(close_td_vol)).encode('ascii'),
                                          order_vol=actual_td_vol,
                                          order_ref = self.order_ref, 
                                          direction = b"0" if close_td_vol>0 else b"1", #开仓方向,
                                          offset_flag = b'3')
            else: # end of if exchange_id == b'SHFE'...
                # 拆全部单
                actual_vol = self.trade_vol_limit if abs(close_vol) >= self.trade_vol_limit else abs(close_vol)

                self.req_order_insert(exchange_id = exchange_id,
                                      instrument_id = instrument_id,
                                      order_price=self.get_default_price(instrument_id, direction = b'1' if close_vol > 0 else b'0'),
#                                      order_vol = str(abs(close_vol)).encode('ascii'),
                                      order_vol=actual_vol,
                                      order_ref = self.order_ref, 
                                      direction = b"0" if close_vol>0 else b"1", #开仓方向,
                                      offset_flag = b'1')
    
    def CovExchange(self,windcode):
        match_dict = {'INE':b'INE', 'SHF':b'SHFE', 'DCE':b'DCE','CZC':b'CZCE','FFEX':b'FFEX'}
        
        if windcode in match_dict.keys():
            return(match_dict[windcode])
        else:
            return(None)
    
    def SummaryPosition(self):
#        live_pos = pd.read_csv('D:/PositionTemplate.csv', encoding='gbk')
#        if len(self.position_qry_rst) == 0:
#            live_pos = pd.DataFrame(columns = ['InstrumentID','BrokerID','InvestorID','PosiDirection',
#                                               'HedgeFlag','PositionDate','YdPosition','Position',
#                                               'LongFrozen','ShortFrozen','LongFrozenAmount',
#                                               'ShortFrozenAmount','OpenVolume','CloseVolume','OpenAmount',
#                                               'CloseAmount','PositionCost','PreMargin','UseMargin','FrozenMargin',
#                                               'FrozenCash','FrozenCommission','CashIn	Commission',
#                                               'CloseProfit','PositionProfit','PreSettlementPrice',
#                                               'SettlementPrice','TradingDay','SettlementID',
#                                               'OpenCost','ExchangeMargin','CombPosition',
#                                               'CombLongFrozen','CombShortFrozen','CloseProfitByDate',
#                                               'CloseProfitByTrade','TodayPosition','MarginRateByMoney',
#                                               'MarginRateByVolume','StrikeFrozen','StrikeFrozenAmount',
#                                               'AbandonFrozen','ExchangeID','YdStrikeFrozen',
#                                               'InvestUnitID','PositionCostOffset','CurrentDir','CurrentNetPos'])
#        else:
#            live_pos = pd.DataFrame(self.position_qry_rst)
#        print(self.pos_dict)
        live_pos = pd.DataFrame(self.pos_dict).T.reset_index(drop=True)
#        live_pos.to_csv('D:/live_pos.csv',encoding='gbk')

        # live_pos.to_csv('C:/live_pos.csv',encoding='gbk')
        # print(live_pos)
        
        #提取路径中最新的持仓，以后需要替换成数据库读取记录
        latest_ts = max([int(pos.replace('.csv','').split('_')[1]) for pos in os.listdir('./TargetPos/044393') if pos.endswith('.csv')])
        live_target = pd.read_csv('./TargetPos/%s/TargetPos_%s.csv' % (self.investor_id.decode(),latest_ts), encoding='gbk')
        IsNight = self.GetTradingPeriod(bypass=False)[1] == 'night'
        if IsNight:
            live_target = pd.DataFrame(live_target[live_target['night']==True])
        live_target['WindExchange'] = [self.CovExchange(dc.split('.')[1]) for dc in live_target['dom_contract']]
        
        
        if len(live_pos)==0:
            return('Empty live_pos') #在live_pos还是空字典时返回Empty live_pos.
        else:
            pass
        live_pos['CurrentDir'] = [1 if PosD == b'2' else -1 if PosD== b'3' else None for PosD in live_pos['PosiDirection']]
        live_pos['CurrentNetPos'] = live_pos['CurrentDir']* abs(live_pos['Position'])
        # live_pos.to_csv('C:/liv_pos.csv',encoding='gbk')
        summary_live = live_pos[
                                ['InstrumentID','ExchangeID','Position',
                                 'CurrentNetPos','LongFrozen','ShortFrozen',
                                 'TodayPosition','YdPosition']
                                ].groupby(by = 'InstrumentID'
                                ).agg(
                                        {'ExchangeID':'first',
                                         'Position':'sum', #持仓量，不包含方向
                                         'CurrentNetPos':'sum', # 净持仓，包含了方向
                                         'LongFrozen':'sum',
                                         'ShortFrozen':'sum',
                                         'TodayPosition':'sum',
                                         'YdPosition':'sum'
                                         }
                                ).reset_index()
        summary_live['InstrumentID'] = [I_id.decode('gbk') for I_id in summary_live['InstrumentID']]
        if IsNight:
            compare_table = pd.merge(live_target[['InstrumentID','round_contract','WindExchange']],
                                 summary_live,how='left',on = 'InstrumentID')
        else:
            compare_table = pd.merge(live_target[['InstrumentID','round_contract','WindExchange']],
                                     summary_live,how='outer',on = 'InstrumentID')
        compare_table['ExchangeID'] = compare_table['ExchangeID'].fillna(compare_table['WindExchange'])
        compare_table = compare_table.fillna(0)
        # TradingPos必須在fillna之後計算，因爲多數倉位在一開始是空值
        compare_table['TradingPos'] = compare_table['LongFrozen'] - compare_table['ShortFrozen']
        
        compare_table['NeedToOpen'] = compare_table['round_contract'] - compare_table['CurrentNetPos'] - compare_table['TradingPos']
        # print(compare_table['round_contract'])
        # print(compare_table['TradingPos'])
        compare_table['InstrumentID'] = [I_id.encode('ascii') for I_id in compare_table['InstrumentID']]
        # compare_table.to_csv('C:/compare_table.csv',encoding='gbk')
        print(compare_table)
        live_target['PosiDirection'] = ['2' if direction > 0 else '3' if direction < 0 else None for direction in live_target['round_contract']]
        # live_pos.to_csv('C:/PositionTemplate.csv', encoding = 'gbk', index=False)   
        
        
        for index,rows in compare_table.iterrows():
            # print(type(rows['NeedToOpen']))
            if rows['NeedToOpen'] == 0:
                continue
            elif rows['Position'] == rows['round_contract']: #净持仓的扰动来自于冻结的交易失误导致的反向开仓，等待反向开仓成交后再解决。
                continue
            else:
                if (rows['CurrentNetPos']*rows['NeedToOpen']) >= -0.0001: #开仓
                    self.OpenContract(exchange_id=rows['ExchangeID'],
                                          instrument_id=rows['InstrumentID'],
                                          open_vol = int(rows['NeedToOpen'])
                                          )
#                    sleep(0.1)
                else:

                    net_open = int(rows['NeedToOpen'] + rows['CurrentNetPos'])
                    need_to_close = rows['NeedToOpen'] if abs(rows['NeedToOpen']) < abs(rows['CurrentNetPos']) else -rows['CurrentNetPos']
                    close_td_vol = 0 if abs(need_to_close)<=abs(rows['YdPosition']) else (abs(need_to_close) - abs(rows['YdPosition']))*(1 if rows['NeedToOpen'] > 0 else -1)
                    close_yd_vol = rows['NeedToOpen'] if abs(rows['NeedToOpen'])<=abs(rows['YdPosition']) else abs(rows['YdPosition'])*(1 if rows['NeedToOpen'] > 0 else -1)
                    self.CloseContract(exchange_id = rows['ExchangeID'],
                                        instrument_id=rows['InstrumentID'],
                                        close_vol = need_to_close,
                                        close_yd_vol = close_yd_vol,
                                        close_td_vol = close_td_vol)
                                       
#                    sleep(0.1)
                    if net_open*rows['NeedToOpen'] >0: #此时还需要反向开仓
                        self.OpenContract(exchange_id=rows['ExchangeID'],
                                              instrument_id=rows['InstrumentID'],
                                              open_vol = net_open
                                              )
                    sleep(0.1)   

        

    def Join(self):
        while True:
#            pass
            
            if self.status == 0:  #当status为0时才算服务完全启动
                trading_hour = self.GetTradingPeriod(bypass=True)
                if trading_hour[0]:
    #                self.process_rtn_trade()
                    self.req_qry_trading_account() #更新资金账户
    #                self.process_rtn_trade()

                    while not self.md_queue.empty(): # 更新价格
                        last_md = self.md_queue.get(block=False)
                        self.md_dict[last_md["InstrumentID"]] = last_md
                        # print(self.md_dict.keys())
                    sleep(5)
    #                try:                    
                    # 异步处理下，如果在position_qry_rst还未清零的情况下开始查询，容易导致查询内容被覆盖
                    if len(self.position_qry_rst)==0:
                        self.InqPosition()
                    sleep(3)
                    
                    while len(self.position_qry_rst) != 0:
                        last_pos_rcd = self.position_qry_rst.pop()
                        # 需要把受到仓位按照合约ID，持仓日期，持仓方向，对冲flag存储，否则净持仓数据就出错了
                        self.pos_dict["_".join([str(last_pos_rcd['InstrumentID']),
                                                str(last_pos_rcd['PositionDate']),
                                                str(last_pos_rcd['PosiDirection']),
                                                str(last_pos_rcd['HedgeFlag'])])] = last_pos_rcd
                    sleep(1)
                    self.SummaryPosition()
                    sleep(2)
                    
#                    except Exception as e:
#                        print(e)
    
                    self.CheckOrders()
                    sleep(1)
                    self.SummaryOrder()                    
                    print('\n\n 账户状态: \n\n |时间：%s |\n, |动态权益：%s |\n |保证金：%s |\n |保证金比例:%s |\n\n'
                          % (datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S'),
                             self.account_balance['Balance'],self.account_balance['Margin'],
                             str(100*(round(self.account_balance['Margin']/self.account_balance['Balance'],3)))+'%'))
                else:
                    print('\n\n ----------------------NOT TRADING HOUR, PLS WAIT---------------------\n\n')
                    sleep(1)
            else:
                sleep(1)

if __name__ == "__main__":
    import sys

    sys.path.append("..")
    from account_info import my_future_account_info_dict

    future_account = my_future_account_info_dict['SimNow24']
    
    ctp_trader = TraderEngine(future_account.broker_id
                              ,future_account.server_dict['TDServer']
                              , future_account.investor_id
                              , future_account.password
                              , future_account.app_id
                              , future_account.auth_code
                              , None
                              , future_account.td_page_dir)    