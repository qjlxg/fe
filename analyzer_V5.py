import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings
import csv

warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心实战配置
# ==========================================
TOTAL_ASSETS = 100000              # 总本金
FUND_DATA_DIR = 'fund_data'        # 数据目录
BENCHMARK_CODE = '510300'          # 市场风向标
TRADE_LOG_FILE = "豹哥实战日志.csv"  # 自动记录所有信号
REPORT_FILE = "豹哥操作手册.txt"    # 每日操作指南

# 策略参数
WIN_RATE_THRESHOLD = 0.40          # 历史胜率门槛
TURNOVER_CONFIRM = 1.0             # 换手率倍数
MIN_DRAWDOWN = -0.045              # 最小回撤
ATR_STOP_MULTIPLIER = 2.0          # ATR止损倍数
MAX_SINGLE_POSITION = 0.3          # 单只最大仓位
MAX_TOTAL_EXPOSURE = 0.7           # 总仓位警戒线

# ==========================================
# 2. 核心功能函数
# ==========================================

def get_market_weather():
    """判断市场季节"""
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): 
        return 0, "🌤️ 未知", 1.0
    
    try:
        df = pd.read_csv(path, encoding='gbk')
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={'收盘': 'close', '日期': 'date'})
        df['MA20'] = df['close'].rolling(20).mean()
        bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
        
        if bias < -4: return bias, "❄️ 深冬", 0.5
        if bias < -2: return bias, "🌨️ 初冬", 0.8
        return bias, "🌤️ 早春", 1.0
    except:
        return 0, "🌤️ 未知", 1.0

def calculate_history_win_rate(df):
    """计算历史胜率"""
    if len(df) < 60: return 0.0
    
    temp = df.tail(250).copy()
    temp['MA5'] = temp['close'].rolling(5).mean()
    delta = temp['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    temp['rsi'] = 100 - (100 / (1 + gain / loss.replace(0, 0.001)))
    
    success, total = 0, 0
    for i in range(20, len(temp) - 6):
        if temp['rsi'].iloc[i] < 35 and temp['close'].iloc[i] > temp['MA5'].iloc[i]:
            total += 1
            max_gain = (temp['close'].iloc[i+1:i+6].max() - temp['close'].iloc[i]) / temp['close'].iloc[i]
            if max_gain >= 0.02:
                success += 1
    
    return success / total if total > 5 else 0.0

def calculate_shares(last_close, stop_price, multiplier):
    """计算买入股数（A股合规）"""
    risk_per_share = last_close - stop_price
    if risk_per_share <= 0: return 0
    
    # 单笔最大风险：总资金1%
    max_risk_amount = TOTAL_ASSETS * 0.01
    raw_shares = (max_risk_amount / risk_per_share) * multiplier
    
    # 单只ETF最大仓位限制
    limit_shares = (TOTAL_ASSETS * MAX_SINGLE_POSITION) / last_close
    
    final_shares = min(raw_shares, limit_shares)
    return int(final_shares // 100) * 100  # A股必须是100的整数倍

def log_signal(signal, weather, win_rate, to_ratio, drawdown):
    """记录交易信号"""
    exists = os.path.exists(TRADE_LOG_FILE)
    
    with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(['日期', '时间', '代码', '动作', '价格', '建议股数', 
                           '止损价', '环境', '胜率', '换手倍率', '20日回撤'])
        
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d'),
            datetime.now().strftime('%H:%M:%S'),
            signal['code'],
            signal['action'],
            f"{signal['price']:.3f}",
            signal['shares'],
            f"{signal['stop']:.3f}",
            weather,
            f"{win_rate:.1%}",
            f"{to_ratio:.2f}",
            f"{drawdown:.1%}"
        ])

# ==========================================
# 3. 主分析流程
# ==========================================

def run_analysis():
    """运行完整分析"""
    print(f"\n🐆 豹哥实战系统启动... {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # 1. 检查数据文件
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    if not files:
        print("❌ 未找到数据文件，请先运行数据更新脚本")
        return
    
    # 2. 判断市场环境
    bias, weather, multiplier = get_market_weather()
    print(f"📊 市场环境: {weather} (仓位系数: {multiplier})")
    
    # 3. 分析所有ETF
    results = []
    total_exposure = 0
    
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: 
            continue
        
        try:
            # 读取数据
            df = pd.read_csv(f, encoding='gbk')
            df.columns = [c.strip() for c in df.columns]
            
            # 重命名列
            rename_map = {
                '日期': 'date', '收盘': 'close', '最高': 'high', 
                '最低': 'low', '换手率': 'turnover'
            }
            df = df.rename(columns=rename_map)
            
            # 计算技术指标
            df['MA5'] = df['close'].rolling(5).mean()
            df['TO_MA10'] = df['turnover'].rolling(10).mean()
            
            # 计算ATR
            tr = pd.concat([
                (df['high'] - df['low']),
                (df['high'] - df['close'].shift()).abs(),
                (df['low'] - df['close'].shift()).abs()
            ], axis=1).max(axis=1)
            df['atr'] = tr.rolling(14).mean()
            
            last = df.iloc[-1]
            
            # 计算关键参数
            drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
            to_ratio = last['turnover'] / last['TO_MA10'] if last['TO_MA10'] > 0 else 0
            win_rate = calculate_history_win_rate(df)
            
            # 决策逻辑
            action = "🔴 别看"
            stop_val, shares = 0.0, 0
            
            if drawdown < MIN_DRAWDOWN:
                if last['close'] > last['MA5']:
                    if to_ratio >= TURNOVER_CONFIRM and win_rate >= WIN_RATE_THRESHOLD:
                        action = "🟢 搞它"
                        stop_val = last['close'] - (ATR_STOP_MULTIPLIER * last['atr'])
                        shares = calculate_shares(last['close'], stop_val, multiplier)
                        
                        # 检查总仓位限制
                        position_value = last['close'] * shares
                        if (total_exposure + position_value) / TOTAL_ASSETS <= MAX_TOTAL_EXPOSURE:
                            total_exposure += position_value
                        else:
                            action = "🟡 仓位已满"
                            shares = 0
                    else:
                        action = "🟡 过滤未过"
                else:
                    action = "🟡 等破5线"
            
            # 记录结果
            if action != "🔴 别看":
                results.append({
                    'code': code,
                    'action': action,
                    'price': last['close'],
                    'shares': shares,
                    'stop': stop_val,
                    'win_rate': win_rate,
                    'to_ratio': to_ratio,
                    'drawdown': drawdown,
                    'value': shares * last['close']
                })
                
        except Exception as e:
            print(f"⚠️ 分析 {code} 时出错: {str(e)[:50]}...")
            continue
    
    # 4. 排序：买入信号优先，金额大的优先
    results.sort(key=lambda x: (x['action'] == "🟢 搞它", x['value']), reverse=True)
    
    # 5. 生成报告
    exposure_ratio = total_exposure / TOTAL_ASSETS
    risk_level = "🟢 保守" if exposure_ratio < 0.3 else "🟡 适中" if exposure_ratio < 0.6 else "🔴 激进"
    
    report_lines = []
    report_lines.append("=" * 95)
    report_lines.append(f"🐆 豹哥实战操作手册 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append(f"市场环境: {weather} | 仓位系数: {multiplier}")
    report_lines.append(f"风险暴露: {exposure_ratio:.1%} | 风险等级: {risk_level}")
    report_lines.append("=" * 95)
    report_lines.append(f"{'代码':<8} | {'动作':<10} | {'价格':<6} | {'胜率':<6} | {'换手':<6} | {'回撤':<6} | {'建议股数':<8} | {'止损价':<8}")
    report_lines.append("-" * 95)
    
    print("\n" + "=" * 95)
    print(f"{'代码':<8} | {'动作':<10} | {'价格':<6} | {'胜率':<6} | {'换手':<6} | {'回撤':<6} | {'建议股数':<8} | {'止损价':<8}")
    print("-" * 95)
    
    buy_signals = 0
    for r in results:
        # 格式化行
        line = f"{r['code']:<8} | {r['action']:<10} | {r['price']:<8.3f} | " \
               f"{r['win_rate']:<7.1%} | {r['to_ratio']:<8.2f} | {r['drawdown']:<7.1%} | " \
               f"{r['shares']:<10} | {r['stop']:<8.3f}"
        
        report_lines.append(line)
        print(line)
        
        # 记录买入信号
        if r['action'] == "🟢 搞它":
            buy_signals += 1
            log_signal(r, weather, r['win_rate'], r['to_ratio'], r['drawdown'])
    
    report_lines.append("-" * 95)
    
    # 6. 添加操作说明
    report_lines.append("\n📋 今日操作建议:")
    if buy_signals > 0:
        report_lines.append(f"1. 今日有 {buy_signals} 个买入信号")
        report_lines.append("2. 建议选择前1-3个信号执行")
        report_lines.append("3. 买入后立即设置止损单")
    else:
        report_lines.append("1. 今日无符合条件的买入信号")
        report_lines.append("2. 建议空仓观望")
    
    report_lines.append("\n📌 豹哥实战纪律:")
    report_lines.append("1. 不绿不买（只买下跌的）")
    report_lines.append("2. 按量下单（严格仓位控制）")
    report_lines.append("3. 破位必卖（纪律大于一切）")
    report_lines.append("\n✅ 交易信号已自动记录到 [豹哥实战日志.csv]")
    
    # 7. 保存报告
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print("-" * 95)
    print(f"✅ 分析完成！")
    print(f"📝 详细报告: {REPORT_FILE}")
    print(f"📈 交易日志: {TRADE_LOG_FILE}")
    
    if buy_signals > 0:
        print(f"🎯 今日建议关注前 {min(3, buy_signals)} 个 🟢 信号")

# ==========================================
# 4. 主程序入口
# ==========================================

if __name__ == "__main__":
    run_analysis()