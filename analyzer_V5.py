import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 5%回撤
MIN_TURNOVER_RATE = 1.0             # 换手率门槛 (1%)
VOLUME_RATIO_THRESHOLD = 1.5        # 量比阈值
REPORT_BASE_NAME = 'Report_V5_1'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        # 尝试不同编码读取
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')

        # --- 修复后的表头映射 ---
        column_map = {
            '日期': 'date',
            '收盘': 'close',
            '成交量': 'volume',
            '换手率': 'turnover'
        }
        df = df.rename(columns=column_map)

        # 检查必要列
        required = ['date', 'close', 'volume', 'turnover']
        missing = [col for col in required if col not in df.columns]
        if missing:
            logging.error(f"文件 {filepath} 缺失必要列: {missing}")
            return None

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 强制转换数值列，防止因字符串导致计算失败
        for col in ['close', 'volume', 'turnover']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df.dropna(subset=['close'])
    except Exception as e:
        logging.error(f"读取 {filepath} 失败: {e}")
        return None

def analyze_logic(df):
    if len(df) < 20: return None
    
    # 计算量比 (当日成交量 / 5日均量)
    df['ma5_vol'] = df['volume'].rolling(5).mean()
    df['vol_ratio'] = df['volume'] / df['ma5_vol'].replace(0, np.nan)
    
    # RSI(14) 计算
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, 0.001)
    df['rsi'] = 100 - (100 / (1 + rs))

    # 计算20日最大回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    
    # 筛选逻辑：回撤达标 且 活跃度达标
    # 注意：159001是货币ETF，回撤通常极小。如果是股票ETF，0.05(5%)比较合适
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
        logging.warning(f"目录 {FUND_DATA_DIR} 为空")
        # 即使为空也生成一个说明文件，避免 Action 报错
        with open(f"{REPORT_BASE_NAME}_no_data.md", 'w') as f: f.write("未发现数据目录")
        return

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df)
            if res:
                res['code'] = code
                results.append(res)

    # 生成 Markdown 报告
    report_name = f"{REPORT_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金量价分析报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
        if not results:
            f.write("> **提示**：今日无符合筛选条件的标的。\n")
            f.write(f"> 筛选标准：20日内回撤 >= {MIN_MONTH_DRAWDOWN*100}% 且 换手率 >= {MIN_TURNOVER_RATE}%\n")
        else:
            f.write("| 基金代码 | 价格 | 20日回撤 | RSI(14) | 换手率 | 成交状态 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.2f} | {r['turnover']:.2f}% | {r['vol_status']} |\n")
    
    logging.info(f"分析完成！报告：{report_name}")

if __name__ == "__main__":
    main()
