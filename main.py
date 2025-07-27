#!/usr/bin/env Python
# -*- coding: utf-8 -*-

"""
使用requests请求代理服务器
请求http和https网页均适用
"""

import httpx

# 隧道域名:端口号
tunnel = "z307.kdltpspro.com:15818"

# 用户名和密码方式
username = "t15351811402121"
password = "izsuxm6r"

proxies = httpx.Proxy(
    url="http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
)

with httpx.Client(proxy=proxies) as client:
    r = client.get('https://dev.kdlapi.com/testproxy')
    print(r.text)


