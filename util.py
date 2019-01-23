# -*- coding:utf-8 -*-

import hashlib

def trans_md5(data):
    m2 = hashlib.md5()
    m2.update(data.encode())
    value = m2.hexdigest()
    if len(value) == 31:
        value = "0" + value
    return value