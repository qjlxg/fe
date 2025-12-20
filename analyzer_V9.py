import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime, timedelta

# --- 1. 实盘与组合配置 ---
CONFIG = {
    'CAPITAL': 100000,        # 初始资金
    'RISK_PER_TRADE': 0.01,   # 单笔交易风险系数 (1%)
    'MAX_HOLDINGS': 2,        # 【新增】组合最大持仓数量：不超过2只
    'TOTAL_POS_LIMIT': 0.6,   # 【新增】总仓位上限：总投入不超过资金的60%
    'FEE_SLIPPAGE': 0.0005,   # 综合佣金与滑点预留 (万五)
    'DATA_DIR': 'fund_data',
    'MIN_SHARPE': 0.5,        # 历史性价比门槛
    'MAX_DD_LIMIT': -20.0     # 历史回撤容忍度 (%)
}

# --- 2. 核心指标引擎 ---
def calculate_metrics(df):
    """计算精准的指标：ATR 与 历史风控"""
    if len(df) < 30: return None
    
    # 精准 TR 计算 (当前高低、当前高昨收、当前低昨收的极大值)
    df['h_l'] = df['high'] - df['low']
    df['h_pc'] = (df['high'] - df['close'].shift(1)).abs()
    df['l_pc'] = (df['low'] - df['close'].shift(1)).abs()
    df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)
    df['atr'] = df['tr'].rolling(14).mean()
    
    # 历史风险指标 (基于过去252个交易日)
    hist = df.tail(252).copy()
    returns = hist['close'].pct_change().dropna()
    if len(returns) < 120: return None
    
    ann_return = returns.mean() * 252
    ann_vol = returns.std() * np.sqrt(252)
    sharpe = (ann_return - 0.02) / ann_vol if ann_vol != 0 else 0
    
    cum_ret = (1 + returns).cumprod()
    mdd = ((cum_ret - cum_ret.cummax()) / cum_ret.cummax()).min()
    
    return {
        'atr': df['atr'].iloc[-1],
        'sharpe': round(sharpe, 2),
        'mdd_pct': round(mdd * 100, 2)
    }

# --- 3. 策略与仓位模块 ---
def analyze_signal(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.lower().strip() for c in df.columns]
        
        metrics = calculate_metrics(df)
        if not metrics or metrics['sharpe'] < CONFIG['MIN_SHARPE'] or metrics['mdd_pct'] < CONFIG['MAX_DD_LIMIT']:
            return None
        
        last = df.iloc[-1]
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        
        # 信号逻辑：价格回踩后站上5日线
        peak_20 = df['close'].tail(20).max()
        dd_from_peak = (last['close'] - peak_20) / peak_20
        
        score = 0
        if last['close'] > ma5 and dd_from_peak < -0.05:
            score += 2  # 基础信号
            if last['amount'] > df['amount'].tail(5).mean(): score += 1 # 量能加分
            
        if score < 2: return None

        # --- 精准实盘配仓 ---
        # 考虑滑点后的拟成交价
        est_price = last['close'] * (1 + CONFIG['FEE_SLIPPAGE'])
        # 动态止损：2倍ATR
        stop_price = est_price - (2 * metrics['atr'])
        risk_per_share = est_price - stop_price
        
        # 股数 = (总资金 * 风险系数) / 每股风险
        raw_shares = (CONFIG['CAPITAL'] * CONFIG['RISK_PER_TRADE']) / risk_per_share
        # 限制单只最大金额 (总资金 / 最大持仓数)
        max_money_per_etf = CONFIG['CAPITAL'] * (CONFIG['TOTAL_POS_LIMIT'] / CONFIG['MAX_HOLDINGS'])
        limited_shares = min(raw_shares, max_money_per_etf / est_price)
        
        final_shares = int(limited_shares // 100 * 100)

        return {
            'score': score,
            'price': round(est_price, 3),
            'stop': round(stop_price, 3),
            'shares': final_shares,
            'sharpe': metrics['sharpe'],
            'mdd': metrics['mdd_pct'],
            'pos_value': round(final_shares * est_price, 0)
        }
    except:
        return None

# --- 4. 组合决策执行 ---
def run_portfolio_strategy():
    all_candidates = []
    files = glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv"))
    
    for f in files:
        code = "".join(filter(str.isdigit, os.path.basename(f))).zfill(6)
        res = analyze_signal(f)
        if res and res['shares'] > 0:
            res['code'] = code
            all_candidates.append(res)
    
    # 【核心逻辑】按 夏普比率 * 信号分 排序，选取最优的 N 只
    all_candidates.sort(key=lambda x: (x['score'] * x['sharpe']), reverse=True)
    final_selection = all_candidates[:CONFIG['MAX_HOLDINGS']]
    
    # 生成指令
    print(f"📅 执行时间: {datetime.now().strftime('%H:%M')} (建议收盘前5分钟运行)")
    print(f"🛡️ 组合限制: 最多持有 {CONFIG['MAX_HOLDINGS']} 只 | 单笔风险额: {CONFIG['CAPITAL']*CONFIG['RISK_PER_TRADE']}元")
    print("-" * 50)
    
    if not final_selection:
        print("今日无符合风控要求的交易指令。")
    else:
        for r in final_selection:
            print(f"【交易指令】代码: {r['code']} | 评分: {r['score']} | 夏普: {r['sharpe']}")
            print(f"👉 操作: 买入 {r['shares']} 股 | 预估成交价: {r['price']}")
            print(f"🛑 止损: 价格跌破 {r['stop']} 立即离场")
            print(f"💰 占用资金: {r['pos_value']}元")
            print("-" * 30)

if __name__ == "__main__":
    run_portfolio_strategy()
