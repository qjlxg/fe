import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- V5.5 决策版配置 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 5%回撤基础
MIN_TURNOVER_RATE = 1.0             # 换手率门槛
REPORT_BASE_NAME = 'Trading_Decision_Report'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')
        column_map = {'日期': 'date', 'Date': 'date', '收盘': 'close', 'Close': 'close', 
                      '成交量': 'volume', 'Volume': 'volume', '换手率': 'turnover'}
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        for col in ['close', 'volume', 'turnover']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except:
        return None

def analyze_logic(df):
    if len(df) < 30: return None
    
    # 1. 指标计算
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['bias'] = (df['close'] - df['MA20']) / df['MA20'] * 100
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss.replace(0, 0.001))))
    
    # 回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 2. 决策逻辑 (核心修改)
    # 条件1：价格站上MA5 (右侧确认)
    is_right_side = last['close'] > last['MA5']
    # 条件2：超跌
    is_oversold = last['rsi'] < 40 or last['bias'] < -4.0
    
    decision = "🔴 继续观望 (未止跌)"
    if abs(last['drawdown']) >= MIN_MONTH_DRAWDOWN:
        if is_right_side and is_oversold:
            decision = "🟢 买入参考 (已站稳)"
        elif is_oversold:
            decision = "🟡 预警: 待站稳MA5"
            
        return {
            'code': "", 'close': last['close'], 'drawdown': last['drawdown'],
            'rsi': last['rsi'], 'bias': last['bias'], 'decision': decision
        }
    return None

def main():
    # 获取当前精确时间
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_time = datetime.now().strftime('%Y%m%d_%H%M')

    if not os.path.exists(FUND_DATA_DIR):
        print("错误：未找到数据目录")
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

    # 排序：决策级别高的排在前面
    results = sorted(results, key=lambda x: x['decision'], reverse=True)

    report_name = f"{REPORT_BASE_NAME}_{file_time}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金实战决策报告\n")
        f.write(f"**分析执行时间**: {run_time} (北京时间)\n\n")
        f.write("## 💡 决策建议说明\n")
        f.write("- **🟢 买入参考**: 满足回撤条件，且价格已站上 5 日线，短期跌势逆转。\n")
        f.write("- **🔴 继续观望**: 虽然跌得多，但仍被均线压制，此时买入容易被套。\n\n")
        
        if not results:
            f.write("### ❌ 今日市场无符合回撤 5% 以上的标的。")
        else:
            f.write("| 基金代码 | 最新价 | 20日回撤 | RSI | 乖离率 | 👈 最终动作决策 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.1f} | {r['bias']:.1f}% | **{r['decision']}** |\n")
    
    print(f"决策完成！报告生成时间：{run_time}")

if __name__ == "__main__":
    main()
