import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# --- 核心配置 ---
CONFIG = {
    'TOTAL_CAPITAL': 100000,
    'MAX_HOLDINGS': 3,
    'RISK_PER_TRADE': 0.01,
    'DATA_DIR': 'fund_data',
    'EXCEL_DB': 'ETF列表.xlsx',
    'REPORT_FILE': 'README.md',
    'MIN_SHARPE': 0.2
}

class AdvancedStrategy:
    @staticmethod
    def calculate_indicators(df):
        """计算 ETF 专用的多维技术指标"""
        df.columns = [str(c).strip().lower() for c in df.columns]
        mapping = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交额': 'amount'}
        df.rename(columns=mapping, inplace=True)
        
        # 1. RSI (14日): 监测板块是否超买/超卖
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / loss)))
        
        # 2. KDJ (9,3,3): 监测短线情绪拐点
        low_9 = df['low'].rolling(9).min()
        high_9 = df['high'].rolling(9).max()
        rsv = (df['close'] - low_9) / (high_9 - low_9) * 100
        df['k'] = rsv.ewm(com=2).mean()
        df['d'] = df['k'].ewm(com=2).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']
        
        # 3. Bollinger Bands (20, 2): 监测反弹空间
        df['ma20'] = df['close'].rolling(20).mean()
        df['std'] = df['close'].rolling(20).std()
        df['upper'] = df['ma20'] + 2 * df['std']
        df['lower'] = df['ma20'] - 2 * df['std']
        
        # 4. ATR & 均线: 动态风控
        df['tr'] = np.maximum((df['high'] - df['low']), 
                             np.maximum((df['high'] - df['close'].shift(1)).abs(), 
                                        (df['low'] - df['close'].shift(1)).abs()))
        df['atr'] = df['tr'].rolling(14).mean()
        df['ma5'] = df['close'].rolling(5).mean()
        
        return df

    @staticmethod
    def analyze(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) < 30: return None
            df = AdvancedStrategy.calculate_indicators(df)
            last = df.iloc[-1]
            prev = df.iloc[-2]
            
            score = 0
            # 维度A：均线动量 - 站上5日线且5日线不向下
            if last['close'] > last['ma5']: score += 1
            
            # 维度B：KDJ金叉 - J线上穿D线，且处于非超买区 (J < 85)
            if last['j'] > last['d'] and prev['j'] <= prev['d'] and last['j'] < 85:
                score += 1
                
            # 维度C：RSI强弱 - 拒绝极度弱势(RSI>35)，且未过度透支(RSI<70)
            if 35 < last['rsi'] < 70: score += 1
                
            # 维度D：布林空间 - 价格在下轨上方反弹，且距离上轨有盈利空间
            if last['close'] > last['lower'] and last['close'] < last['upper'] * 0.98:
                score += 1

            # 绩效过滤 (夏普)
            rets = df['close'].pct_change().tail(252)
            sharpe = (rets.mean() * 252 - 0.02) / (rets.std() * np.sqrt(252)) if rets.std() != 0 else 0

            if score >= 3 and sharpe > CONFIG['MIN_SHARPE']:
                stop_p = last['close'] - (2.1 * last['atr']) # 动态止损
                risk_amt = CONFIG['TOTAL_CAPITAL'] * CONFIG['RISK_PER_TRADE']
                shares = int(risk_amt / max(last['close'] - stop_p, 0.01) // 100 * 100)
                
                return {
                    'code': "".join(filter(str.isdigit, os.path.basename(file_path))).zfill(6),
                    'score': score, 'price': round(last['close'], 3),
                    'stop': round(stop_p, 3), 'shares': shares,
                    'rsi': round(last['rsi'], 1), 'j': round(last['j'], 1),
                    'sharpe': round(sharpe, 2)
                }
        except: return None

# --- 执行逻辑 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(CONFIG['EXCEL_DB']): return fund_db
    df = pd.read_excel(CONFIG['EXCEL_DB'], dtype=str, engine='openpyxl')
    df.columns = [str(c).strip() for c in df.columns]
    c_code = next((c for c in df.columns if '代码' in c), "证券代码")
    c_name = next((c for c in df.columns if '简称' in c or '名称' in c), "证券简称")
    for _, row in df.iterrows():
        code = "".join(filter(str.isdigit, str(row[c_code]))).zfill(6)
        fund_db[code] = {'name': str(row[c_name])}
    return fund_db

def main():
    db = load_fund_db()
    results = []
    for f in glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv")):
        res = AdvancedStrategy.analyze(f)
        if res:
            res.update(db.get(res['code'], {'name': '未匹配'}))
            results.append(res)
    
    results.sort(key=lambda x: (x['score'], x['sharpe']), reverse=True)
    
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 多维指标实盘看板 V11\n\n")
        f.write(f"📅 更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n")
        f.write("> **策略：均线 + KDJ金叉 + RSI强弱 + 布林空间**\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 指标得分 | 现价 | 建议买入 | 止损参考 | RSI | J值 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results[:CONFIG['MAX_HOLDINGS'] * 2]:
                f.write(f"| {s['code']} | **{s['name']}** | {'🔥'*s['score']} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} | {s['rsi']} | {s['j']} |\n")
        else:
            f.write("😴 暂无满足多维指标交叉验证的标的。")

if __name__ == "__main__":
    main()
