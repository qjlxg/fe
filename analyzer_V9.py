import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# --- 核心实盘配置 ---
CONFIG = {
    'CAPITAL': 100000,        # 初始本金
    'MAX_HOLDINGS': 3,        # 提高分散度：最多持仓3只
    'RISK_PER_TRADE': 0.008,   # 严格风控：单笔风险控制在0.8%
    'TOTAL_POS_LIMIT': 0.7,   # 总仓位上限70%
    'DATA_DIR': 'fund_data',
    'REPORT_FILE': 'README.md',
    'FEE_SLIPPAGE': 0.001     # 预留千一的滑点+佣金成本
}

class QuantEngine:
    @staticmethod
    def calculate_metrics(df):
        """精准计算：增加趋势斜率与波动稳定性"""
        # 1. 精准 TR & ATR 计算
        df['h_l'] = df['high'] - df['low']
        df['h_pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l_pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        
        # 2. 趋势斜率 (防假突破：要求MA20走平或向上)
        df['ma20'] = df['close'].rolling(20).mean()
        df['ma5'] = df['close'].rolling(5).mean()
        df['slope_20'] = (df['ma20'] - df['ma20'].shift(5)) / 5
        
        # 3. 历史风控指标
        returns = df['close'].pct_change().tail(252)
        sharpe = (returns.mean() * 252 - 0.02) / (returns.std() * np.sqrt(252)) if returns.std() != 0 else 0
        mdd = ((df['close'] / df['close'].cummax()) - 1).min()
        
        return df, round(sharpe, 2), round(mdd * 100, 2)

    @staticmethod
    def analyze_signal(file_path):
        try:
            df = pd.read_csv(file_path)
            df.columns = [c.lower().strip() for c in df.columns]
            if len(df) < 60: return None
            
            df, sharpe, mdd = QuantEngine.calculate_metrics(df)
            last = df.iloc[-1]
            
            # --- 核心信号逻辑：趋势过滤 + 回撤修复 ---
            peak_20 = df['close'].tail(20).max()
            dd_20 = (last['close'] - peak_20) / peak_20
            avg_amt = df['amount'].tail(5).mean() / 1e6
            
            score = 0
            # A. 趋势保护：MA20斜率不能明显向下
            if last['slope_20'] > -0.001:
                # B. 均线金叉与回撤空间
                if last['close'] > last['ma5'] and dd_20 < -0.05:
                    score += 2
                    if last['close'] > last['ma20']: score += 1
                    if last['amount'] > df['amount'].tail(10).mean() * 1.1: score += 1
            
            if score >= 3 and sharpe > 0.5 and avg_amt > 50:
                # --- 实盘执行计算 ---
                # 考虑滑点的拟买入价
                est_entry = last['close'] * (1 + CONFIG['FEE_SLIPPAGE'])
                # 动态止损：2.2倍ATR (稍微放宽以防早盘诱空)
                stop_price = est_entry - (2.2 * last['atr'])
                # 动态止盈：3.5倍ATR
                target_price = est_entry + (3.5 * last['atr'])
                
                # 风险平摊仓位计算
                risk_amt = CONFIG['CAPITAL'] * CONFIG['RISK_PER_TRADE']
                shares = risk_amt / (est_entry - stop_price)
                # 结合单只持仓上限限制
                max_val = CONFIG['CAPITAL'] * (CONFIG['TOTAL_POS_LIMIT'] / CONFIG['MAX_HOLDINGS'])
                final_shares = int(min(shares, max_val / est_entry) // 100 * 100)
                
                return {
                    'code': os.path.basename(file_path)[:6],
                    'score': score, 'price': round(est_entry, 3),
                    'stop': round(stop_price, 3), 'target': round(target_price, 3),
                    'shares': final_shares, 'sharpe': sharpe, 'mdd': mdd,
                    'amt': round(avg_amt, 1)
                }
        except: return None

def execute():
    results = []
    files = glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv"))
    for f in files:
        res = QuantEngine.analyze_signal(f)
        if res and res['shares'] > 0: results.append(res)
    
    # 组合优选：评分 > 夏普 > 换手活性
    results.sort(key=lambda x: (x['score'], x['sharpe'], x['amt']), reverse=True)
    selection = results[:CONFIG['MAX_HOLDINGS']]
    
    # 生成 Markdown 报表
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘组合看板 V9.1\n\n")
        f.write(f"📅 更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}` (UTC+8)\n")
        f.write(f"🛡️ 风控：单笔风险 {CONFIG['RISK_PER_TRADE']*100}% | 最大持仓 {CONFIG['MAX_HOLDINGS']} 只\n\n")
        
        if not selection:
            f.write("> 😴 今日暂无高胜率信号，建议空仓观察。")
        else:
            f.write("| 代码 | 评分 | 建议股数 | 预估买入价 | 止损参考 | 目标止盈 | 夏普比 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- |\n")
            for s in selection:
                f.write(f"| {s['code']} | {'🔥'*s['score']} | **{s['shares']}** | {s['price']} | {s['stop']} | {s['target']} | {s['sharpe']} |\n")

if __name__ == "__main__":
    execute()
