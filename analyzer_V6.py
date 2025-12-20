import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 系统配置 ---
TOTAL_ASSETS = 100000        # 初始总资产
DATA_DIR = 'fund_data'       # 数据文件夹
MARKET_INDEX = '510300'      # 大盘基准（沪深300ETF）
MAX_HOLD_COUNT = 5           # 最大持仓数量：精选前5只
MIN_DAILY_AMOUNT = 50000000  # 流动性门槛：日成交额 > 5000万
RISK_PER_TRADE = 0.015       # 单笔风险：总资金的 1.5%
ETF_DD_THRESHOLD = -0.06     # ETF超跌阈值：-6%

# --- 1. 核心计算模块 ---
def load_data(file_path):
    """加载并清洗数据"""
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        return df
    except Exception as e:
        return pd.DataFrame()

def calculate_advanced_indicators(df):
    """计算量化指标库"""
    if len(df) < 30: return df
    
    # 均线系统
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # ATR 波动率 (用于智能止损)
    tr = pd.concat([
        (df['high'] - df['low']), 
        (df['high'] - df['close'].shift()).abs(), 
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    
    # ROC20: 20日相对强度 (轮动核心)
    df['ROC20'] = df['close'].pct_change(20)
    
    # RSI (14日)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    
    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    # 成交量与成交额均线
    df['TO_MA5'] = df['turnover'].rolling(5).mean()
    df['AMT_MA5'] = df['amount'].rolling(5).mean()
    
    return df

# --- 2. 逻辑判断模块 ---
def get_market_sentiment():
    """大盘情绪滤网 (基于510300)"""
    mkt_path = os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")
    if not os.path.exists(mkt_path):
        return 0, "未知", 1.0
    
    mkt_df = load_data(mkt_path)
    if len(mkt_df) < 20: return 0, "数据不足", 1.0
    
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    current = mkt_df['close'].iloc[-1]
    bias = (current - ma20) / ma20
    
    if bias > 0.02: return bias, "🔥 强劲", 1.2
    if bias < -0.02: return bias, "❄️ 冰点", 0.6
    return bias, "⚖️ 平衡", 1.0

def analyze_etf_logic(df):
    """趋势共振买入逻辑"""
    if len(df) < 30 or 'MA5' not in df.columns: return "⚪ 观望", 0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    
    # 条件1：价格站上5日线（趋势初步反转）
    cond_price = last['close'] > last['MA5']
    # 条件2：超跌空间达标
    cond_dd = drawdown < ETF_DD_THRESHOLD
    
    if cond_price and cond_dd:
        # 辅助共振评分
        score = sum([
            last['RSI'] > 40,                   # 拒绝极弱势
            last['MACD_Hist'] > prev['MACD_Hist'], # 动能柱上行
            last['turnover'] > last['TO_MA5'] * 1.1 # 适度放量
        ])
        return ("🟢 介入" if score >= 2 else "🟡 观察"), score
    
    return "⚪ 观望", 0

# --- 3. 执行模块 ---
def execute_analysis():
    # 环境检查
    if not os.path.exists(DATA_DIR):
        print(f"❌ 错误: 目录 '{DATA_DIR}' 不存在。")
        return

    # 获取大盘背景
    bias, sentiment, mkt_weight = get_market_sentiment()
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    findings = []

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        
        # --- A. 早期成交额过滤 (性能优化) ---
        # 预读取最后一行检查成交额，不符合直接跳过指标计算
        raw_tail = pd.read_csv(f).tail(5)
        if raw_tail['amount'].mean() < MIN_DAILY_AMOUNT:
            continue
            
        # --- B. 计算全套指标 ---
        df = load_data(f)
        df = calculate_advanced_indicators(df)
        
        # --- C. 策略决策 ---
        decision, score = analyze_etf_logic(df)
        
        if decision != "⚪ 观望":
            last = df.iloc[-1]
            
            # --- D. 智能双重止损 ---
            # 取 (价格 - 2*ATR) 和 (10日均线*0.95) 的较小值
            atr_stop = last['close'] - (2 * last['atr'])
            ma10_stop = last['MA10'] * 0.95
            stop_price = min(atr_stop, ma10_stop)
            
            # --- E. 仓位计算 ---
            risk_cash = TOTAL_ASSETS * RISK_PER_TRADE
            # 风险间距：当前价 - 止损价 (设定最小间距为1.5%防止极值)
            risk_gap = max(last['close'] - stop_price, last['close'] * 0.015)
            shares = int(((risk_cash / risk_gap) * mkt_weight) // 100) * 100
            
            findings.append({
                'code': code,
                'decision': decision,
                'score': score,
                'price': last['close'],
                'roc20': round(last['ROC20'] * 100, 2),
                'rsi': round(last['RSI'], 1),
                'shares': shares if decision == "🟢 介入" else 0,
                'stop': round(stop_price, 3)
            })

    # --- F. 强度轮动排序 ---
    # 优先看 ROC20 (相对强度)，其次看共振得分
    findings.sort(key=lambda x: (x['roc20'], x['score']), reverse=True)
    
    # 截取最强的前 N 只进入最终备选池
    final_targets = findings[:MAX_HOLD_COUNT]

    # --- 4. 报告输出 ---
    print("\n" + "="*95)
    print(f"🚀 天枢 ETF 进阶轮动系统 V8 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"大盘情绪: {sentiment} (Bias: {bias:.2%}) | 流动性门槛: {MIN_DAILY_AMOUNT/10000:.0f}万")
    print(f"风控配置: 每笔风险 {RISK_PER_TRADE*100}% | 最大持仓: {MAX_HOLD_COUNT}只")
    print("="*95)
    print(f"{'排名':<4} | {'代码':<8} | {'决策':<8} | {'ROC20%':<8} | {'得分':<4} | {'现价':<8} | {'建议股数':<10} | {'智能止损':<8}")
    print("-" * 95)

    for i, r in enumerate(final_targets, 1):
        # 突出显示介入信号
        dec_str = f"★ {r['decision']}" if r['decision'] == "🟢 介入" else r['decision']
        print(f"{i:<4} | {r['code']:<8} | {dec_str:<8} | {r['roc20']:>7}% | {r['score']:<4} | {r['price']:<8.3f} | {r['shares']:<12} | {r['stop']:<8.3f}")
    
    if not final_targets:
        print("💡 当前无符合筛选条件的 ETF，请保持空仓观望。")
    print("-" * 95)
    print("操作建议：若出现“★ 介入”信号且ROC20排名靠前，可于次日早盘分仓买入；若跌破“智能止损”线，果断离场。")

if __name__ == "__main__":
    execute_analysis()
