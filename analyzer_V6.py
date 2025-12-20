import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, time as dt_time
import warnings
import csv

warnings.filterwarnings('ignore')

# --- 豹哥实战配置 ---
TOTAL_ASSETS = 100000              # 总本金
FUND_DATA_DIR = 'fund_data'        # 数据文件夹
BENCHMARK_CODE = '510300'          # 大盘风向标
TRADE_LOG_FILE = "豹哥实战日志.csv"

# 策略参数
WIN_RATE_THRESHOLD = 0.40          
TURNOVER_CONFIRM = 1.0             
MIN_DRAWDOWN = -0.045              
ATR_STOP_MULTIPLIER = 2            
MAX_SINGLE_POSITION = 0.3          

def validate_data_freshness():
    """检查数据是否是最新的"""
    print("🔍 正在检查数据新鲜度...")
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    if not files: return False
    
    latest_file = max(files, key=os.path.getmtime)
    file_time = os.path.getmtime(latest_file)
    days_diff = (datetime.now() - datetime.fromtimestamp(file_time)).days
    if days_diff > 1:
        print(f"⚠️ 警告：数据已过期 {days_diff} 天，请先运行更新脚本！")
        return False
    print("✅ 数据状态：新鲜")
    return True

def load_data(filepath):
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
    except:
        df = pd.read_csv(filepath, encoding='gbk')
    df.columns = [c.strip() for c in df.columns]
    column_map = {'日期': 'date', '收盘': 'close', '最高': 'high', '最低': 'low', '换手率': 'turnover'}
    df = df.rename(columns=column_map)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    for col in ['close', 'high', 'low', 'turnover']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['close'])

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "🌤️ 未知", 1.0
    df = load_data(path)
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    if bias < -4: return bias, "❄️ 深冬 (严控仓位)", 0.5
    if bias < -2: return bias, "🌨️ 初冬 (谨慎出击)", 0.8
    return bias, "🌤️ 早春 (正常执行)", 1.0

def calculate_shares(last_close, stop_price, multiplier):
    """计算具体买入股数（取整到百位，即1手）"""
    risk_per_share = last_close - stop_price
    if risk_per_share <= 0: return 0
    # 单笔风险不超过总本金的 1%
    max_risk_amount = TOTAL_ASSETS * 0.01
    max_shares = int(max_risk_amount / risk_per_share)
    # 环境调整并确保不超过单只上限
    adjusted_shares = int(max_shares * multiplier)
    limit_shares = int((TOTAL_ASSETS * MAX_SINGLE_POSITION) / last_close)
    final_shares = min(adjusted_shares, limit_shares)
    return (final_shares // 100) * 100  # A股买入必须是100的整数倍

def log_trade_signal(signal, weather):
    """记录交易信号到CSV"""
    file_exists = os.path.exists(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['日期', '时间', '代码', '动作', '价格', '建议股数', '止损价', '环境'])
        now = datetime.now()
        writer.writerow([
            now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'),
            signal['code'], signal['action'], signal['price'],
            signal['shares'], signal['stop'], weather
        ])

def analyze():
    # 1. 环境与时间检查
    trade_time = datetime.now().time()
    if not (dt_time(9, 15) <= trade_time <= dt_time(15, 5)):
        print("⚠️ 提示：当前非交易时段，分析结果仅供复盘")
    
    if not validate_data_freshness(): return

    bias_val, weather, multiplier = get_market_weather()
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if df is None or len(df) < 30: continue
        
        df['MA5'] = df['close'].rolling(5).mean()
        df['TO_MA10'] = df['turnover'].rolling(10).mean()
        tr = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        
        last = df.iloc[-1]
        drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
        
        action = "🔴 别看"
        stop_val = 0
        shares = 0
        
        if drawdown < MIN_DRAWDOWN:
            if last['close'] > last['MA5']:
                # 简化逻辑：实战中重点看站稳5日线和回撤
                action = "🟢 搞它"
                stop_val = last['close'] - (ATR_STOP_MULTIPLIER * last['atr'])
                shares = calculate_shares(last['close'], stop_val, multiplier)
            else:
                action = "🟡 等破5线"

        if action != "🔴 别看":
            results.append({
                'code': code, 'action': action, 'price': last['close'], 
                'shares': shares, 'stop': round(stop_val, 3), 
                'weight': 2 if action == "🟢 搞它" else 1
            })

    results.sort(key=lambda x: (x['weight'], x['shares']), reverse=True)

    # --- 输出报告 ---
    print("\n" + "="*75)
    print(f"🐆 豹哥实战操作手册 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"当前大盘环境: {weather} (仓位系数: {multiplier})")
    print("="*75)
    print(f"{'代码':<8} | {'动作':<10} | {'买入参考':<8} | {'建议股数':<10} | {'止损价':<8}")
    print("-" * 75)

    for r in results:
        print(f"{r['code']:<8} | {r['action']:<10} | {r['price']:<10.3f} | {r['shares']:<12} | {r['stop']:<8.3f}")
        if r['action'] == "🟢 搞它":
            log_trade_signal(r, weather)

    print("-" * 75)
    print("📌 豹哥实战纪律：1.不绿不买 2.按量下单 3.破位必卖")
    print("✅ 交易信号已记录至 [豹哥实战日志.csv]")

if __name__ == "__main__":
    analyze()
