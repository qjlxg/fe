import pandas as pd
import glob
import os
import subprocess
from datetime import datetime
import warnings
import csv

warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心实战配置
# ==========================================
TOTAL_ASSETS = 100000              
FUND_DATA_DIR = 'fund_data'        
BENCHMARK_CODE = '510300'          
TRADE_LOG_FILE = "豹哥实战日志.csv"
REPORT_FILE = "豹哥操作手册.txt"

# 策略精算参数
WIN_RATE_THRESHOLD = 0.40          
TURNOVER_CONFIRM = 1.0             
MIN_DRAWDOWN = -0.045              
ATR_STOP_MULTIPLIER = 2.0          
MAX_SINGLE_POSITION = 0.3          
MAX_TOTAL_EXPOSURE = 0.7           

# ==========================================
# 2. 核心功能函数
# ==========================================

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "🌤️ 未知", 1.0
    try:
        df = pd.read_csv(path, encoding='gbk')
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={'收盘':'close','日期':'date'})
        df['MA20'] = df['close'].rolling(20).mean()
        bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
        if bias < -4: return bias, "❄️ 深冬", 0.5
        if bias < -2: return bias, "🌨️ 初冬", 0.8
        return bias, "🌤️ 早春", 1.0
    except: return 0, "🌤️ 未知", 1.0

def calculate_history_win_rate(df):
    if len(df) < 60: return 0.0
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
            if (temp['close'].iloc[i+1:i+6].max() - temp['close'].iloc[i]) / temp['close'].iloc[i] >= 0.02:
                success += 1
    return success/total if total > 5 else 0.0

def git_push():
    """自动推送结果到仓库"""
    try:
        print("🚀 正在同步至远程仓库...")
        subprocess.run(["git", "add", REPORT_FILE, TRADE_LOG_FILE], check=True)
        commit_msg = f"Update trading report: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ 仓库更新成功！")
    except Exception as e:
        print(f"❌ Git推送失败: {e} (请确保已配置SSH或凭据)")

# ==========================================
# 3. 主分析流程
# ==========================================

def run_sync_analysis():
    bias, weather, multiplier = get_market_weather()
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        try:
            df = pd.read_csv(f, encoding='gbk')
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns={'日期':'date','收盘':'close','最高':'high','最低':'low','换手率':'turnover'})
            df['MA5'] = df['close'].rolling(5).mean()
            df['TO_MA10'] = df['turnover'].rolling(10).mean()
            tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
            df['atr'] = tr.rolling(14).mean()
            
            last = df.iloc[-1]
            drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
            to_ratio = last['turnover'] / last['TO_MA10'] if last['TO_MA10'] > 0 else 0
            win_rate = calculate_history_win_rate(df)
            
            action = "🔴 别看"
            stop_val, shares = 0.0, 0
            
            if drawdown < MIN_DRAWDOWN:
                if last['close'] > last['MA5']:
                    if to_ratio >= TURNOVER_CONFIRM and win_rate >= WIN_RATE_THRESHOLD:
                        action = "🟢 搞它"
                        stop_val = last['close'] - (ATR_STOP_MULTIPLIER * last['atr'])
                        risk_per_share = last['close'] - stop_val
                        raw_shares = (TOTAL_ASSETS * 0.01 / risk_per_share) * multiplier
                        shares = int(min(raw_shares, TOTAL_ASSETS * MAX_SINGLE_POSITION / last['close']) // 100 * 100)
                    else: action = "🟡 过滤未过"
                else: action = "🟡 等破5线"
            
            if action != "🔴 别看":
                results.append({
                    'code': code, 'action': action, 'price': last['close'],
                    'shares': shares, 'stop': stop_val, 'value': shares * last['close'],
                    'win_rate': win_rate, 'to_ratio': to_ratio, 'drawdown': drawdown
                })
        except: continue

    results.sort(key=lambda x: (x['action']=="🟢 搞它", x['value']), reverse=True)
    
    # 构造完整版报告
    report = [
        "="*95,
        f"🐆 豹哥实战手册 | {datetime.now().strftime('%Y-%m-%d %H:%M')} | 环境: {weather}",
        "="*95,
        f"{'代码':<8} | {'动作':<10} | {'价格':<6} | {'胜率':<6} | {'换手倍':<6} | {'回撤':<6} | {'建议股数':<8} | {'止损价':<8}",
        "-" * 95
    ]
    
    for r in results:
        line = f"{r['code']:<8} | {r['action']:<10} | {r['price']:<8.3f} | {r['win_rate']:<7.1%} | {r['to_ratio']:<8.2f} | {r['drawdown']:<7.1%} | {r['shares']:<10} | {r['stop']:<8.3f}"
        report.append(line)
        # 终端实时查看
        print(line)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
    
    # 执行同步
    git_push()

if __name__ == "__main__":
    run_sync_analysis()
