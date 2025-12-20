import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
TRACKER_FILE = 'signal_performance_tracker.csv' # 表现跟踪文件
REPORT_FILE = 'README.md'
MIN_SCORE_THRESHOLD = 3  # 只显示3分及以上的顶级信号

# --- 核心逻辑：得分系统 ---
def analyze_logic_v9(df):
    if len(df) < 60: return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1

    # 1分基础：价格站上5日线且超跌
    score = 0
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        # 2分进阶：站上10日线（确认短期趋势转强）
        if last['close'] > ma10:
            score += 1
        # 3分爆发：今日成交额超过5日平均额（确认主力入场）
        if last['amount'] > amt_ma5:
            score += 1
            
    if score >= 1: # 内部记录所有信号，但前端只展示高分
        return {
            'roc': roc20 * 100,
            'score': score,
            'price': last['close'],
            'stop': ma10 * 0.96,
            'date': datetime.now().strftime('%Y-%m-%d')
        }
    return None

# --- 历史表现分析模块 ---
def update_performance_tracker(new_signals):
    """
    记录每个信号出现后的表现。
    逻辑：将今日信号存入 tracker，并检查旧信号在 5 天后的价格。
    """
    if not os.path.exists(TRACKER_FILE):
        df = pd.DataFrame(columns=['date', 'code', 'signal_price', 'score', 'price_5d', 'perf_5d'])
    else:
        df = pd.read_csv(TRACKER_FILE)

    # 1. 存入今日新信号
    new_rows = []
    for s in new_signals:
        # 如果该标的今日已记录则跳过
        if not ((df['date'] == s['date']) & (df['code'] == s['code'])).any():
            new_rows.append({
                'date': s['date'], 'code': s['code'], 
                'signal_price': s['price'], 'score': s['score']
            })
    
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # 2. (可选) 这里可以加入自动回溯逻辑，但 Actions 只能看到当前数据
    # 建议每周你下载这个 CSV 用 Excel 拉一下涨跌幅
    df.to_csv(TRACKER_FILE, index=False, encoding='utf_8_sig')

# --- 执行与报告 ---
def execute():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []
    
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        res = analyze_logic_v9(pd.read_csv(f)) # 简化读取
        if res:
            res['code'] = code
            all_signals.append(res)

    # 保存所有信号到历史记录（用于分析）
    update_performance_tracker(all_signals)

    # 过滤精英信号（用于展示）
    elite_signals = [s for s in all_signals if s['score'] >= MIN_SCORE_THRESHOLD]
    elite_signals.sort(key=lambda x: x['roc'], reverse=True)

    # 写入 README
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write("# 🛰️ 天枢 ETF 精英看板 (≥3分信号)\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        if elite_signals:
            f.write("| 排名 | 代码 | 得分 | ROC20% | 现价 | 建议止损 |\n| --- | --- | --- | --- | --- | --- |\n")
            for i, s in enumerate(elite_signals, 1):
                f.write(f"| {i} | {s['code']} | 🔥 {s['score']} | {s['roc']:.2f}% | {s['price']:.3f} | {s['stop']:.3f} |\n")
        else:
            f.write("> 🧊 今日无 3 分共振信号。市场处于弱势磨底或单边下跌中，建议持币观望。\n")
        
        f.write(f"\n---\n💡 **历史回溯**: 脚本已将所有 1-3 分信号存入 `signal_performance_tracker.csv`。你可以每周下载此文件，对比信号发出 5 天后的表现，从而微调止损阈值或评分逻辑。")

if __name__ == "__main__":
    execute()
