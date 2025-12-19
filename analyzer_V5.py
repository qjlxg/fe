import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- 配置区 ---
TOTAL_ASSETS = 100000              # 建议按你的实际总资金修改
FUND_DATA_DIR = 'fund_data'
BENCHMARK_CODE = '510300'          # 沪深300作为天气风向标
REPORT_BASE_NAME = 'Trading_Decision_Report'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- 1. 保留你最稳健的数据加载函数 ---
def load_data(filepath):
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')
        
        # 兼容更多列名，加入 high/low 以计算 ATR
        column_map = {
            '日期': 'date', 'Date': 'date', 
            '收盘': 'close', 'Close': 'close', 
            '最高': 'high', 'High': 'high',
            '最低': 'low', 'Low': 'low',
            '成交量': 'volume', 'Volume': 'volume', 
            '换手率': 'turnover'
        }
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        for col in ['close', 'high', 'low', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.dropna(subset=['close'])
    except Exception as e:
        logging.error(f"加载 {filepath} 出错: {e}")
        return None

# --- 2. 注入天气预报逻辑 ---
def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "未知天气", 1.0
    
    df = load_data(path)
    if df is None or len(df) < 20: return 0, "数据不足", 1.0
    
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    
    if bias < -4: return bias, "❄️ 深冬 (极寒，需极度超跌)", 0.6
    if bias < -2: return bias, "🌨️ 初冬 (微寒，严格过滤)", 0.8
    if bias < 1:  return bias, "🌤️ 早春 (蓄势，正常执行)", 1.0
    return bias, "☀️ 盛夏 (亢奋，警惕追高)", 0.5

# --- 3. 升级决策逻辑 (核心算法) ---
def analyze_logic(df, bias_val, weather_multiplier):
    if len(df) < 30: return None
    
    # 动态门槛：天气越冷，RSI要求越低
    dynamic_rsi_limit = 35 + (bias_val * 1.5)
    
    # 指标计算
    df['MA5'] = df['close'].rolling(5).mean()
    
    # 计算 ATR (用于动态止损和仓位)
    if 'high' in df.columns and 'low' in df.columns:
        tr = pd.concat([
            (df['high'] - df['low']),
            (df['high'] - df['close'].shift()).abs(),
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
    else:
        df['atr'] = df['close'] * 0.02 # 缺少数据时的保底方案

    # RSI (Wilder平滑)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    
    # 回撤计算
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    
    # 判定：站上MA5 且 满足动态超跌门槛
    is_right_side = last['close'] > last['MA5'] and (last['MA5'] >= df['MA5'].iloc[-2])
    is_oversold = last['rsi'] < dynamic_rsi_limit

    if abs(last['drawdown']) >= 0.05: # 基础回撤5%门槛
        decision = "🔴 继续观望"
        pos_ratio = "0%"
        stop_price = 0.0
        
        if is_right_side and is_oversold:
            decision = "🟢 买入参考"
            # 动态仓位：单笔风险1% / (2*ATR距离) * 天气系数
            stop_price = last['close'] - (2 * last['atr'])
            risk_dist = last['close'] - stop_price
            raw_pos = (TOTAL_ASSETS * 0.01) / (risk_dist / last['close'])
            pos_ratio = f"{(raw_pos * weather_multiplier / TOTAL_ASSETS):.1%}"
        elif is_oversold:
            decision = "🟡 预警:等待止跌"
            
        return {
            'code': "", 'close': last['close'], 'drawdown': last['drawdown'],
            'rsi': last['rsi'], 'decision': decision, 
            'pos': pos_ratio, 'stop': stop_price
        }
    return None

def main():
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    file_time = datetime.now().strftime('%Y%m%d_%H%M')
    
    bias_val, weather_desc, weather_multiplier = get_market_weather()
    
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df, bias_val, weather_multiplier)
            if res:
                res['code'] = code
                results.append(res)

    # 生成报告
    report_name = f"{REPORT_BASE_NAME}_{file_time}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# ETF实战决策报告 (V5.8 天气感应版)\n")
        f.write(f"**分析时间**: {run_time} | **当前天气**: {weather_desc}\n\n")
        f.write(f"> **天气策略**: 动态RSI门槛已调整为 {35 + (bias_val * 1.5):.1f}，建议仓位已根据环境风险缩放。\n\n")
        
        f.write("| 代码 | 现价 | RSI | 回撤 | 建议仓位 | 止损价 | 最终决策 |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        
        for r in results:
            f.write(f"| {r['code']} | {r['close']:.3f} | {r['rsi']:.1f} | {r['drawdown']:.1%} | {r['pos']} | {r['stop']:.3f} | **{r['decision']}** |\n")
            
            # 同时写入复盘日志
            with open('history_signals.csv', 'a', encoding='utf-8') as log:
                if log.tell() == 0: log.write("日期,代码,价格,决策,仓位,天气\n")
                log.write(f"{run_time},{r['code']},{r['close']},{r['decision']},{r['pos']},{weather_desc}\n")

if __name__ == "__main__":
    main()
