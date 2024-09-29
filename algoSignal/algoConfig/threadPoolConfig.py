# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 11:57
@file: threadPoolConfig.py
@author: Jerry
"""
from concurrent.futures import ThreadPoolExecutor

pool = ThreadPoolExecutor(max_workers=50)
