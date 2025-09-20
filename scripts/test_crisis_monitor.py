#!/usr/bin/env python3
import pandas as pd
import numpy as np

def to_monthly(s):
    return s.resample('ME').last()

def transform_series(s, method):
    return to_monthly(s)

print('测试文件创建成功')
