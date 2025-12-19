import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- 核心配置区 ---
TOTAL_ASSETS = 100000              
FUND_DATA_DIR = 'fund_data'
BENCHMARK_CODE = '510300'          
REPORT_BASE_NAME = 'Trading_Decision_Report'

# 过滤阀值：胜率低于此值或换手率不足，不给买入信号
WIN_RATE_THRESHOLD = 0.40  
TURNOVER_CONFIRM = 1.0     

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')
        
        column_map = {
            '日期': 'date', '开盘': 'open', '收盘': 'close', 
            '最高': 'high', '最低': 'low', '换手率': 'turnover'
        }
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        for col in ['close', 'high', 'low', 'turnover']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except:
        return None

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "未知天气", 1.0
    df = load_data(path)
    if df is None or len(df) < 25: return 0, "数据收集中", 1.0
    
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    
    if bias < -4: return bias, "❄️ 深冬 (极寒，需严格缩减仓位)", 0.6
    if bias < -2: return bias, "🌨️ 初冬 (转冷，需放量确认)", 0.8
    if bias < 1:  return bias, "🌤️ 早春 (蓄势，正常执行)", 1.0
    return bias, "☀️ 盛夏 (亢奋，警惕追高)", 0.5

def check_history_win_rate(df):
    """回测该基金在过去250天内，满足 RSI<35且破5日线 后 T+5 的胜率"""
    if len(df) < 60: return 0.0, 0
    temp = df.tail(250).copy()
    temp['MA5'] = temp['close'].rolling(5).mean()
    delta = temp['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    temp['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    
    success, total = 0, 0
    for i in range(20, len(temp)-6):
        if temp['rsi'].iloc[i] < 35 and temp['close'].iloc[i] > temp['MA5'].iloc[i]:
            total += 1
            future_max = temp['close'].iloc[i+1:i+6].max()
            if (future_max - temp['close'].iloc[i]) / temp['close'].iloc[i] >= 0.02:
                success += 1
    return (success/total if total > 0 else 0.0), total

def analyze_logic(df, bias_val, weather_multiplier):
    if len(df) < 30: return None
    
    # 1. 动态指标
    dynamic_rsi_limit = 35 + (bias_val * 1.2)
    df['MA5'] = df['close'].rolling(5).mean()
    df['TO_MA10'] = df['turnover'].rolling(10).mean()
    
    # 2. 核心数值
    last = df.iloc[-1]
    turnover_ratio = last['turnover'] / df['TO_MA10'].iloc[-1] if df['TO_MA10'].iloc[-1] > 0 else 1.0
    
    tr = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    df['drawdown'] = (df['close'] - df['close'].rolling(20).max()) / df['close'].rolling(20).max()

    # 3. 信号判定
    win_rate, win_count = check_history_win_rate(df)
    is_right_side = last['close'] > last['MA5'] and (last['MA5'] >= df['MA5'].iloc[-2] * 0.999)
    is_oversold = last['rsi'] < dynamic_rsi_limit
    is_active = turnover_ratio >= TURNOVER_CONFIRM # 换手率必须不低于平均水平

    sort_weight = 1
    decision = "🔴 观望 (未见止跌)"
    pos_ratio = "0%"
    stop_price = 0.0

    if abs(last['drawdown']) >= 0.045:
        # 满足基础超跌条件
        if is_right_side and is_oversold:
            # 进入深度审核：必须 换手活跃 且 历史胜率达标
            if is_active and win_rate >= WIN_RATE_THRESHOLD:
                decision = "🟢 买入 (双重确认)"
                sort_weight = 4 # 最高等级
                stop_price = last['close'] - (2 * last['atr'])
                risk_unit = last['close'] - stop_price
                if risk_unit > 0:
                    raw_pos = (TOTAL_ASSETS * 0.01) / (risk_unit / last['close'])
                    pos_ratio = f"{(raw_pos * weather_multiplier / TOTAL_ASSETS):.1%}"
            else:
                # 即使站上5日线，如果胜率或量能不达标，也降级为预警
                decision = "🟡 预警 (量能/胜率不足)"
                sort_weight = 2
        elif is_oversold:
            decision = "🟡 预警 (待破5日线)"
            sort_weight = 2
            
        return {
            'code': "", 'close': last['close'], 'rsi': last['rsi'], 'drawdown': last['drawdown'],
            'pos': pos_ratio, 'stop': stop_price, 'decision': decision, 
            'weight': sort_weight, 'win': f"{win_rate:.0%}", 'to_ratio': turnover_ratio
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

    results = sorted(results, key=lambda x: x['weight'], reverse=True)

    report_name = f"{REPORT_BASE_NAME}_{file_time}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金实战决策报告 (V5.8 终极强化版)\n")
        f.write(f"**分析时间**: {run_time} | **环境**: {weather_desc}\n\n")
        f.write(f"> **策略逻辑**: 启用 [换手率确认 > {TURNOVER_CONFIRM}] 与 [历史信号胜率 > {WIN_RATE_THRESHOLD*100}%] 双重过滤。\n\n")
        f.write("| 代码 | 现价 | RSI | 回撤 | 换手倍率 | 历史胜率 | 建议仓位 | 止损参考 | 最终决策 |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        for r in results:
            stop_str = f"{r['stop']:.3f}" if r['stop'] > 0 else "0.000"
            to_str = f"**{r['to_ratio']:.2f}**" if r['to_ratio'] > 1.2 else f"{r['to_ratio']:.2f}"
            f.write(f"| {r['code']} | {r['close']:.3f} | {r['rsi']:.1f} | {r['drawdown']:.1%} | {to_str} | {r['win']} | {r['pos']} | {stop_str} | **{r['decision']}** |\n")

if __name__ == "__main__":
    main()
