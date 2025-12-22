import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta

def analyze():
    # 1. 加载名单
    elite_pool = []
    if os.path.exists('backtest_results.csv'):
        elite_pool = pd.read_csv('backtest_results.csv', dtype={'代码': str})['代码'].head(10).tolist()
    
    # 2. 大盘风控
    bench_df = pd.read_csv('fund_data/510300.csv')
    bench_df.columns = [c.strip() for c in bench_df.columns]
    bench_df['日期'] = pd.to_datetime(bench_df['日期'])
    # 【核心修正】强制正序
    bench_df = bench_df.sort_values('日期').reset_index(drop=True)
    ma20 = bench_df['收盘'].rolling(20).mean().iloc[-1]
    curr_bench = bench_df['收盘'].iloc[-1]
    is_safe = curr_bench >= ma20

    results = []
    target_files = [os.path.join('fund_data', f"{c}.csv") for c in elite_pool] if elite_pool else glob.glob('fund_data/*.csv')

    for file in target_files:
        if not os.path.exists(file) or '510300' in file: continue
        try:
            df = pd.read_csv(file)
            df.columns = [c.strip() for c in df.columns]
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期').reset_index(drop=True)
            
            last = df.iloc[-1]
            ma5 = df['收盘'].rolling(5).mean().iloc[-1]
            hi40 = df['收盘'].rolling(40).max().iloc[-1]
            dd = (last['收盘'] - hi40) / hi40
            
            if last['收盘'] > ma5 and dd < -0.04:
                # 计算得分与止损 (同前逻辑)
                tr = np.maximum(df['最高'] - df['最低'], abs(df['最高'] - df['收盘'].shift(1)))
                atr = tr.rolling(14).mean().iloc[-1]
                stop_p = min(last['收盘'] - 3.0 * atr, last['收盘'] * 0.93)
                results.append({'code': os.path.basename(file)[:6], 'price': last['收盘'], 'stop': round(stop_p, 3), 'dd': f"{round(dd*100,2)}%"})
        except: continue

    # 输出 README (逻辑同前)
    print(f"✅ 分析完成，大盘状态: {is_safe}")

if __name__ == "__main__": analyze()
