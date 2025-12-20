import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# --- 核心配置 ---
CONFIG = {
    'TOTAL_CAPITAL': 100000,    # 模拟实盘资金
    'MAX_HOLDINGS': 3,          # 最大持仓数
    'RISK_PER_TRADE': 0.01,     # 单笔风险 1%
    'DATA_DIR': 'fund_data',
    'EXCEL_DB': 'ETF列表.xlsx',
    'REPORT_FILE': 'README.md',
    'MIN_SHARPE': 0.2,          # 中等强度：允许性价比一般的标的进入
    'MIN_DD': -0.03,            # 中等强度：回撤3%即进入监控
}

# --- 1. 深度匹配引擎 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(CONFIG['EXCEL_DB']):
        print(f"❌ 找不到数据库: {CONFIG['EXCEL_DB']}")
        return fund_db
    try:
        df = pd.read_excel(CONFIG['EXCEL_DB'], dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '标的', '追踪', '行业'])), None)

        for _, row in df.iterrows():
            raw_code = str(row[c_code]).strip()
            clean_code = "".join(filter(str.isdigit, raw_code)).zfill(6)
            if clean_code and len(clean_code) == 6:
                fund_db[clean_code] = {
                    'name': str(row[c_name]).strip() if not pd.isna(row[c_name]) else "未知基金",
                    'index': str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "行业/指数"
                }
        return fund_db
    except Exception as e:
        print(f"❌ 解析 Excel 失败: {e}")
        return fund_db

# --- 2. 策略引擎 ---
class StrategyV10:
    @staticmethod
    def get_metrics(df):
        # 字段兼容处理
        df.columns = [str(c).strip().lower() for c in df.columns]
        mapping = {'收盘': 'close', '成交额': 'amount', '成交量': 'volume', '最高': 'high', '最低': 'low'}
        df.rename(columns=mapping, inplace=True)
        
        # 指标计算
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        
        # ATR 计算
        df['h_l'] = df['high'] - df['low']
        df['h_pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l_pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        
        # 夏普比率 (过去252天)
        returns = df['close'].pct_change().tail(252)
        sharpe = (returns.mean() * 252 - 0.02) / (returns.std() * np.sqrt(252)) if returns.std() != 0 else 0
        return df, round(sharpe, 2)

    @staticmethod
    def analyze(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) < 30: return None
            df, sharpe = StrategyV10.get_metrics(df)
            last = df.iloc[-1]
            
            # --- 信号评估逻辑 ---
            peak_20 = df['close'].tail(20).max()
            dd = (last['close'] - peak_20) / peak_20
            score = 0
            
            # 1. 入场门槛：站上5日线 + 满足最小回撤
            if last['close'] > last['ma5'] and dd <= CONFIG['MIN_DD']:
                score = 1
                if last['close'] > last['ma10']: score += 1
                # 趋势斜率放宽：只要20日均线不处于极速下跌状态(斜率>-0.003)
                slope_20 = (last['ma20'] - df['ma20'].iloc[-5]) / 5
                if slope_20 > -0.003: score += 1
                # 量能：比过去5日均量稍大
                if last['amount'] > df['amount'].tail(5).mean(): score += 1
            
            # 2. 过滤：得分>=3 且 夏普>0.2
            if score >= 3 and sharpe >= CONFIG['MIN_SHARPE']:
                # 动态风控
                stop_p = last['close'] - (2 * last['atr'])
                risk_amt = CONFIG['TOTAL_CAPITAL'] * CONFIG['RISK_PER_TRADE']
                shares = int(risk_amt / max(last['close'] - stop_p, 0.01) // 100 * 100)
                
                return {
                    'code': "".join(filter(str.isdigit, os.path.basename(file_path))).zfill(6),
                    'score': score, 'price': round(last['close'], 3),
                    'stop': round(stop_p, 3), 'shares': shares,
                    'sharpe': sharpe, 'dd': round(dd * 100, 1)
                }
        except: return None

# --- 3. 执行模块 ---
def main():
    db = load_fund_db()
    results = []
    files = glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv"))
    
    for f in files:
        res = StrategyV10.analyze(f)
        if res:
            info = db.get(res['code'], {'name': '未匹配', 'index': '未知'})
            res.update(info)
            results.append(res)
    
    # 排序：高分 > 高夏普
    results.sort(key=lambda x: (x['score'], x['sharpe']), reverse=True)
    
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 中等强度实盘看板 V10\n\n")
        f.write(f"最后更新: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n")
        f.write(f"🛡️ 风控配置: 单笔风险 {CONFIG['RISK_PER_TRADE']*100}% | 准入夏普 > {CONFIG['MIN_SHARPE']}\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 得分 | 现价 | 建议买入 | 止损参考 | 20日回撤 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results[:CONFIG['MAX_HOLDINGS'] * 2]:
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {'🔥'*s['score']} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} | {s['dd']}% |\n")
        else:
            f.write("> 😴 当前市场信号强度一般，建议继续观察。")

if __name__ == "__main__":
    main()
