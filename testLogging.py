# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 23:35:52 2020

@author: lisd0
"""

import os
from datetime import datetime


account = '044392'

def write_log(self, *args, sep=' || '):
        path = './Logs/acc%s_log_on_%s.txt' % (account,datetime.today().strftime('%Y%m%d'))

        if os.path.exists(path):
            append_write = 'a'
        else:
            append_write = 'w'

        local_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # log = f'{local_time}{sep}{self.investor_id}.td{sep}{sep.join(map(str, args))}'
        log = f'{local_time}{sep}{sep.join(map(str, args))}'

        print(log)
        log_doc = open(path,append_write)
        log_doc.write(log + ' \n')
        log_doc.close()


if __name__ == "__main__":
    write_log('test', 'a new test')