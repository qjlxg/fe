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
TOTAL_ASSETS = 100000              # 初始本金
FUND_DATA_DIR = 'fund_data'        # 数据文件夹
BENCHMARK_CODE = '510300'          # 市场风向标
TRADE_LOG_FILE = "豹哥实战日志.csv"
REPORT_FILE = "豹哥操作手册.txt"

# 策略精算参数
WIN_RATE_THRESHOLD = 0.40          
TURNOVER_CONFIRM = 1.0             
MIN_DRAWDOWN = -0.045              
ATR_STOP_MULTIPLIER = 2.0          
MAX_SINGLE_POSITION = 0.3          
MAX_TOTAL_EXPOSURE = 0.7           # 总仓位风险警戒线 70%

# ==========================================
# 2. 增强型功能模块
# ==========================================

def get_color_action(action):
    """终端输出颜色标记"""
    if "🟢" in action: return f"\033[92m{action}\033[0m"
    if "🟡" in action: return f"\033[93m{action}\033[0m"
    return f"\033[91m{action}\033[0m"

def validate_data():
    """数据新鲜度深度验证"""
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    if not files: return False, "文件夹为空"
    latest_file = max(files, key=os.path.getmtime)
    file_time = os.path.getmtime(latest_file)
    diff_days = (datetime.now() - datetime.fromtimestamp(file_time)).days
    if diff_days > 1: return False, f"数据过期 {diff_days} 天"
    return True, "数据状态: 🟢新鲜"

def calculate_performance_stats():
    """计算历史战绩统计"""
    if not os.path.exists(TRADE_LOG_FILE): return "暂无历史记录"
    try:
        log = pd.read_csv(TRADE_LOG_FILE)
        signals = log[log['动作'].str.contains('搞它')]
        if len(signals) == 0: return "尚无成交信号"
        count = len(signals)
        spring_pct = len(signals[signals['环境'].str.contains('早春')]) / count
        return f"历史累计信号: {count} | 早春占比: {spring_pct:.1%}"
    except: return "统计读取失败"

def calculate_shares(last_close, stop_price, multiplier):
    """A股合规股数精算"""
    risk_per_share = last_close - stop_price
    if risk_per_share <= 0: return 0
    # 核心：1% 风险暴露原则 (单笔最大损失限制在总资产1%)
    max_risk_amount = TOTAL_ASSETS * 0.01
    suggested_shares = (max_risk_amount / risk_per_share) * multiplier
    # 限制单只仓位上限
    limit_shares = (TOTAL_ASSETS * MAX_SINGLE_POSITION) / last_close
    final = min(suggested_shares, limit_shares)
    return int(final // 100) * 100

def log_signal(signal, weather):
    """写入实战日志(含UTF-8 BOM以兼容Excel)"""
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
# 3. 核心策略引擎
# ==========================================

def run_pro_system():
    # A. 启动自检
    is_fresh, data_msg = validate_data()
    perf_stats = calculate_performance_stats()
    
    # B. 获取大盘季节
    # (内部沿用 V9.0 的 BIAS/MA20 判定逻辑)
    bias, weather, multiplier = -2.5, "🌨️ 初冬 (谨慎出击)", 0.8 # 示例数据

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    raw_results = []
    
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        
        # 核心算法模拟 (实际运行时需包含 MA5/RSI/ATR 逻辑)
        # 这里展示数据结构...
        # ... logic ...
        action = "🟢 搞它" # 示例动作
        price = 1.410
        stop = 1.355
        sh = calculate_shares(price, stop, multiplier)
        
        raw_results.append({
            'code': code, 'action': action, 'price': price,
            'shares': sh, 'stop': stop, 'value': sh * price,
            'weight': 2 if "🟢" in action else 1
        })

    # C. 风险暴露过滤
    raw_results.sort(key=lambda x: (x['weight'], x['value']), reverse=True)
    current_exposure = 0
    final_results = []
    
    for r in raw_results:
        if "🟢" in r['action']:
            if (current_exposure + r['value']) / TOTAL_ASSETS <= MAX_TOTAL_EXPOSURE:
                current_exposure += r['value']
                final_results.append(r)
            else:
                r['action'] = "🟡 仓位预警(略过)"
                final_results.append(r)
        else:
            final_results.append(r)

    # D. 风险等级评定
    exposure_ratio = current_exposure / TOTAL_ASSETS
    risk_level = "🟢 保守" if exposure_ratio < 0.3 else "🟡 适中" if exposure_ratio < 0.6 else "🔴 激进"

    # E. 生成报告
    report = []
    report.append("="*75)
    report.append(f"🐆 豹哥旗舰交易系统 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"系统状态: {data_msg} | 绩效统计: {perf_stats}")
    report.append(f"风险暴露: {exposure_ratio:.1%} | 风险评级: {risk_level} | 仓位系数: {multiplier}")
    report.append("="*75)
    report.append(f"{'代码':<8} | {'动作':<10} | {'参考价':<8} | {'建议股数':<10} | {'止损价':<8}")
    report.append("-" * 75)

    for r in final_results:
        display_action = get_color_action(r['action'])
        line = f"{r['code']:<8} | {r['action']:<10} | {r['price']:<9.3f} | {r['shares']:<12} | {r['stop']:<8.3f}"
        report.append(line)
        # 控制台打印带颜色的版本
        print(f"{r['code']:<8} | {display_action:<20} | {r['price']:<9.3f} | {r['shares']:<12} | {r['stop']:<8.3f}")
        if "🟢" in r['action']: log_signal(r, weather)

    report.append("-" * 75)
    report.append("📌 实战纪律: 1.不绿不买 2.按量下单 3.破位必卖 | 脚本运行完毕")
    
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

if __name__ == "__main__":
    run_pro_system()
