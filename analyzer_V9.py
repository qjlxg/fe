import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# --- 核心实盘配置 ---
CONFIG = {
    'CAPITAL': 100000,        # 初始资金
    'MAX_HOLDINGS': 2,        # 组合最大持仓数
    'TOTAL_POS_LIMIT': 0.6,   # 总仓位上限 (60%)
    'RISK_PER_TRADE': 0.01,   # 单笔风险暴露 (1%)
    'DATA_DIR': 'fund_data',  # 数据存放目录
    'REPORT_FILE': 'README.md',
    'TRACKER_FILE': 'signal_performance_tracker.csv'
}

class QuantEngine:
    @staticmethod
    def calculate_indicators(df):
        """计算 ATR 止损及风险指标"""
        # 标准 ATR 计算
        df['h_l'] = df['high'] - df['low']
        df['h_pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l_pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        
        # 20日趋势与回撤
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['peak_20'] = df['close'].rolling(20).max()
        
        # 夏普比率 (简易版：年化收益/年化波动)
        returns = df['close'].pct_change().tail(252)
        sharpe = (returns.mean() * 252 - 0.02) / (returns.std() * np.sqrt(252)) if returns.std() != 0 else 0
        return df, round(sharpe, 2)

    @staticmethod
    def analyze_signal(file_path):
        """核心选股逻辑"""
        try:
            df = pd.read_csv(file_path)
            df.columns = [c.lower().strip() for c in df.columns]
            if len(df) < 30: return None
            
            df, sharpe = QuantEngine.calculate_indicators(df)
            last = df.iloc[-1]
            
            # 信号：价格站上5日线 & 20日回撤 > 5% & 5日均额 > 5000万
            dd = (last['close'] - last['peak_20']) / last['peak_20']
            avg_amt = df['amount'].tail(5).mean() / 1e6
            
            score = 0
            if last['close'] > last['ma5'] and dd < -0.05:
                score += 2
                if last['close'] > last['ma20']: score += 1
                if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
            
            if score >= 3 and sharpe > 0.5 and avg_amt > 50:
                # 止损价 = 现价 - 2*ATR
                stop_price = last['close'] - (2 * last['atr'])
                risk_dist = last['close'] - stop_price
                
                # 仓位计算
                shares = (CONFIG['CAPITAL'] * CONFIG['RISK_PER_TRADE']) / max(risk_dist, last['close'] * 0.01)
                final_shares = int(min(shares, (CONFIG['CAPITAL']*0.3)/last['close']) // 100 * 100)
                
                return {
                    'code': os.path.basename(file_path)[:6],
                    'score': score, 'price': round(last['close'], 3),
                    'stop': round(stop_price, 3), 'shares': final_shares,
                    'sharpe': sharpe, 'dd': round(dd*100, 2), 'amt': round(avg_amt, 1)
                }
        except: return None

def execute():
    results = []
    files = glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv"))
    for f in files:
        res = QuantEngine.analyze_signal(f)
        if res and res['shares'] > 0: results.append(res)
    
    # 组合筛选：评分 > 夏普 排序
    results.sort(key=lambda x: (x['score'], x['sharpe']), reverse=True)
    final_selection = results[:CONFIG['MAX_HOLDINGS']]
    
    # 更新 README
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘组合看板 V9\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}` | 运行环境: `GitHub Actions`\n\n")
        f.write("| 代码 | 评分 | 建议股数 | 现价 | 止损参考 | 夏普比 | 20日回撤 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- |\n")
        for s in final_selection:
            f.write(f"| {s['code']} | {'🔥'*s['score']} | **{s['shares']}** | {s['price']} | {s['stop']} | {s['sharpe']} | {s['dd']}% |\n")
    
    print(f"✨ 分析完成，生成信号 {len(final_selection)} 个")

if __name__ == "__main__":
    execute()
