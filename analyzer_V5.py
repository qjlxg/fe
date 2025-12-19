import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, time as dt_time
import warnings
import csv

# 屏蔽无关警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心实战配置
# ==========================================
TOTAL_ASSETS = 100000              # 总本金（建议根据实际资金调整）
FUND_DATA_DIR = 'fund_data'        # 数据存放目录
BENCHMARK_CODE = '510300'          # 市场风向标 (沪深300ETF)
TRADE_LOG_FILE = "豹哥实战日志.csv"   # 自动生成的交易日志
REPORT_FILE = "豹哥操作手册.txt"     # 每日操作指南

# 策略参数
WIN_RATE_THRESHOLD = 0.40          # 历史胜率门槛
TURNOVER_CONFIRM = 1.0             # 换手率倍数要求
MIN_DRAWDOWN = -0.045              # 触发预警的最小回撤
ATR_STOP_MULTIPLIER = 2            # 止损宽度（倍数越大，止损越宽）
MAX_SINGLE_POSITION = 0.3          # 单只ETF最大占用本金比例 (30%)

# ==========================================
# 2. 功能模块
# ==========================================

def validate_data():
    """验证数据新鲜度"""
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    if not files: return False, "文件夹为空"
    latest_file = max(files, key=os.path.getmtime)
    file_time = os.path.getmtime(latest_file)
    diff = (datetime.now() - datetime.fromtimestamp(file_time)).days
    if diff > 1: return False, f"数据过期 {diff} 天"
    return True, "数据新鲜"

def load_data(filepath):
    """读取并清洗数据"""
    try:
        try: df = pd.read_csv(filepath, encoding='utf-8')
        except: df = pd.read_csv(filepath, encoding='gbk')
        df.columns = [c.strip() for c in df.columns]
        column_map = {'日期': 'date', '收盘': 'close', '最高': 'high', '最低': 'low', '换手率': 'turnover'}
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        for col in ['close', 'high', 'low', 'turnover']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except: return None

def get_market_weather():
    """判断市场季节"""
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "🌤️ 未知", 1.0
    df = load_data(path)
    if df is None or len(df) < 20: return 0, "🌤️ 未知", 1.0
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    if bias < -4: return bias, "❄️ 深冬 (严控仓位)", 0.5
    if bias < -2: return bias, "🌨️ 初冬 (谨慎出击)", 0.8
    return bias, "🌤️ 早春 (正常执行)", 1.0

def calculate_shares(last_close, stop_price, multiplier):
    """计算A股买入股数"""
    risk_per_share = last_close - stop_price
    if risk_per_share <= 0: return 0
    max_risk_amount = TOTAL_ASSETS * 0.01 # 单笔风险1%
    raw_shares = (max_risk_amount / risk_per_share) * multiplier
    limit_shares = (TOTAL_ASSETS * MAX_SINGLE_POSITION) / last_close
    final_shares = min(raw_shares, limit_shares)
    return int(final_shares // 100) * 100

def log_signal(signal, weather):
    """记录信号到CSV日志"""
    exists = os.path.exists(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(['日期', '时间', '代码', '动作', '价格', '建议股数', '止损价', '环境'])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%H:%M:%S'),
            signal['code'], signal['action'], f"{signal['price']:.3f}",
            signal['shares'], f"{signal['stop']:.3f}", weather
        ])

# ==========================================
# 3. 主分析逻辑
# ==========================================

def run_leopard_system():
    # A. 检查时间与数据
    is_fresh, msg = validate_data()
    now_time = datetime.now().time()
    is_trading = dt_time(9, 15) <= now_time <= dt_time(15, 5)

    bias, weather, multiplier = get_market_weather()
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if df is None or len(df) < 30: continue
        
        # 指标计算
        df['MA5'] = df['close'].rolling(5).mean()
        df['TO_MA10'] = df['turnover'].rolling(10).mean()
        tr = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        
        last = df.iloc[-1]
        drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
        
        # 判定逻辑
        action = "🔴 别看"
        stop_val = 0.0
        shares = 0
        
        if drawdown < MIN_DRAWDOWN:
            if last['close'] > last['MA5']:
                action = "🟢 搞它"
                stop_val = last['close'] - (ATR_STOP_MULTIPLIER * last['atr'])
                shares = calculate_shares(last['close'], stop_val, multiplier)
            else:
                action = "🟡 等破5线"
        
        if action != "🔴 别看":
            results.append({
                'code': code, 'action': action, 'price': last['close'],
                'shares': shares, 'stop': stop_val, 
                'weight': 2 if action == "🟢 搞它" else 1
            })

    # 排序：动作优先，金额优先
    results.sort(key=lambda x: (x['weight'], x['shares']), reverse=True)

    # B. 输出与保存报告
    report_lines = []
    report_lines.append("="*75)
    report_lines.append(f"🐆 豹哥精英实战手册 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append(f"数据状态: {msg} | 市场环境: {weather}")
    report_lines.append(f"交易时间: {'✅ 在线' if is_trading else '🛑 已收盘'}")
    report_lines.append("="*75)
    report_lines.append(f"{'代码':<8} | {'动作':<10} | {'参考买价':<8} | {'建议股数':<10} | {'离场止损价':<8}")
    report_lines.append("-" * 75)

    for r in results:
        line = f"{r['code']:<8} | {r['action']:<10} | {r['price']:<12.3f} | {r['shares']:<12} | {r['stop']:<8.3f}"
        report_lines.append(line)
        if r['action'] == "🟢 搞它":
            log_signal(r, weather)

    report_lines.append("-" * 75)
    report_lines.append("📌 豹哥实战纪律：【1.不绿不买】 【2.按量下单】 【3.破位必卖】")
    
    final_output = "\n".join(report_lines)
    print(final_output)
    
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(final_output)

if __name__ == "__main__":
    run_leopard_system()
