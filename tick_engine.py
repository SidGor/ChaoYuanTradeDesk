# -*- coding: utf-8 -*-
# AlgoPlus量化投资开源框架范例
# 微信公众号：AlgoPlus
# 项目码云地址：http://gitee.com/AlgoPlus/AlgoPlus
# 项目Github地址：http://github.com/CTPPlus/AlgoPlus
# 项目网址：http://www.algo.plus
# 项目网址：http://www.ctp.plus
# 项目网址：http://www.7jia.com
# 项目QQ交流群：866469866

import datetime
import os
import requests
from AlgoPlus.CTP.MdApiBase import MdApiBase


class TickEngine(MdApiBase):
    def __init__(self, broker_id, md_server, investor_id, password, app_id, auth_code
                 , instrument_id_list, md_queue_list=None
                 , page_dir='', using_udp=False, multicast=False):
        self.Join()

    # ///深度行情通知
    def OnRtnDepthMarketData(self, pDepthMarketData):
        # 将行情放入共享队列
        # print(pDepthMarketData.to_dict_raw()['UpdateTime'])
        # print(pDepthMarketData)
        for md_queue in self.md_queue_list:
            md_queue.put(pDepthMarketData)
    
    # Logging
    
    def write_log(self, *args, sep=' || '):
        path = './Logs/acc%s_log_md_%s.txt' % (self.investor_id.decode(),datetime.datetime.today().strftime('%Y%m%d'))
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

    def MsgWechatBotCY(content, mentioned_list = ['李思达'], mentioned_mobile_list = [],msgtype = 'text'):
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
    
    
    def OnFrontConnected(self):
        if self.status == -2:
          self.write_log('OnFrontConnected', '重连成功！')
          self.MsgWechatBotCY('md模块重连成功')
          
          
    # ///当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。   
    def OnFrontDisconnected(self, nReason):
        self.status = -2
        self.write_log('OnFrontDisconnected', nReason)
        self.MsgWechatBotCY('md模块检测到交易后台通信断开，请迅速检查交易模块')
        
    # ############################################################################# #
    # ///心跳超时警告。当长时间未收到报文时，该方法被调用。
    def OnHeartBeatWarning(self, nTimeLapse):
        self.write_log('OnHeartBeatWarning', nTimeLapse)
        self.MsgWechatBotCY('md模块心跳超时警告，请迅速检查交易模块')
