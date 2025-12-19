import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz
import logging

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 5%回撤
MIN_TURNOVER_RATE = 1.0             # 换手率过滤阈值（例如：低于1%视为不活跃）
VOLUME_RATIO_THRESHOLD = 1.5        # 量比阈值（当日成交量/5日均量）
REPORT_BASE_NAME = 'Report_V5_1'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        # 自动识别编码并读取
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')

        # --- 核心：表头映射逻辑 ---
        # 这里的 key 是 CSV 里的原始列名，value 是程序统一用的列名
        column_map = {
            '日期': 'date', 'Date': 'date',
            '收盘': 'close', 'Close': 'close', '净值': 'close',
            '成交量': 'volume', 'Volume': 'volume',
            '换手率': 'turnover': '换手率'
        }
        # 针对你的 159001.csv 专门处理
        df = df.rename(columns={
            '日期': 'date', '收盘': 'close', 
            '成交量': 'volume', '换手率': 'turnover'
        })

        required = ['date', 'close', 'volume', 'turnover']
        if not all(col in df.columns for col in required):
            logging.error(f"文件 {filepath} 缺少必要列。现有列: {df.columns.tolist()}")
            return None

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        return df
    except Exception as e:
        logging.error(f"读取 {filepath} 失败: {e}")
        return None

def analyze_logic(df):
    # 计算技术指标
    df['ma5_vol'] = df['volume'].rolling(5).mean()
    df['vol_ratio'] = df['volume'] / df['ma5_vol']  # 量比
    
    # RSI计算 (简化)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # 计算回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    
    # 筛选逻辑：回撤 > 5% 且 换手率 > 1%
    if abs(last['drawdown']) >= MIN_MONTH_DRAWDOWN and last['turnover'] >= MIN_TURNOVER_RATE:
        vol_status = "放量" if last['vol_ratio'] >= VOLUME_RATIO_THRESHOLD else "平稳"
        return {
            'drawdown': last['drawdown'],
            'rsi': last['rsi'],
            'turnover': last['turnover'],
            'vol_status': vol_status,
            'close': last['close']
        }
    return None

def main():
    if not os.path.exists(FUND_DATA_DIR):
        os.makedirs(FUND_DATA_DIR)
        return

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.basename(f).replace('.csv', '')
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df)
            if res:
                res['code'] = code
                results.append(res)

    # 生成 Markdown 报告
    report_name = f"{REPORT_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金量价筛选报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
        if not results:
            f.write("今日无符合筛选条件的标的（回撤未达标或活跃度过低）。")
        else:
            f.write("| 基金代码 | 价格 | 20日回撤 | RSI(14) | 换手率 | 成交状态 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.2f} | {r['turnover']}% | {r['vol_status']} |\n")
    
    print(f"分析完成！报告已生成：{report_name}")

if __name__ == "__main__":
    main()
