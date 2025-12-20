import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# --- 1. 核心配置 ---
CONFIG = {
    'TOTAL_CAPITAL': 10000,    # 模拟实盘初始金
    'RISK_PER_TRADE': 0.01,     # 单笔亏损控制在总资金 1%
    'DATA_DIR': 'fund_data',
    'EXCEL_DB': 'ETF列表.xlsx',  # 根目录下必须有此文件
    'REPORT_FILE': 'README.md',
    'HISTORY_FILE': 'signal_history.csv',
    'MIN_SHARPE': 0.2           # 基础性价比门槛
}

# --- 2. 数据库与技术指标引擎 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(CONFIG['EXCEL_DB']):
        print("⚠️ 未找到 ETF列表.xlsx")
        return fund_db
    try:
        df = pd.read_excel(CONFIG['EXCEL_DB'], dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)
        if c_code and c_name:
            for _, row in df.iterrows():
                code = "".join(filter(str.isdigit, str(row[c_code]))).zfill(6)
                fund_db[code] = str(row[c_name]).strip()
        return fund_db
    except Exception as e:
        print(f"❌ Excel加载错误: {e}")
        return fund_db

class QuantEngine:
    @staticmethod
    def calculate_indicators(df):
        df.columns = [str(c).strip().lower() for c in df.columns]
        mapping = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交额': 'amount'}
        df.rename(columns=mapping, inplace=True)
        
        # RSI (14日)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['rsi'] = round(100 - (100 / (1 + (gain / (loss + 1e-9)))), 1)
        
        # KDJ (9,3,3)
        l9, h9 = df['low'].rolling(9).min(), df['high'].rolling(9).max()
        rsv = (df['close'] - l9) / (h9 - l9 + 1e-9) * 100
        df['k'] = rsv.ewm(com=2).mean()
        df['d'] = df['k'].ewm(com=2).mean()
        df['j'] = round(3 * df['k'] - 2 * df['d'], 1)
        
        # MA & ATR
        df['ma5'] = df['close'].rolling(5).mean()
        df['tr'] = np.maximum((df['high'] - df['low']), (df['high'] - df['close'].shift(1)).abs())
        df['atr'] = df['tr'].rolling(14).mean()
        
        return df

    @staticmethod
    def analyze(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) < 30: return None
            df = QuantEngine.calculate_indicators(df)
            last, prev = df.iloc[-1], df.iloc[-2]
            
            # 多维评分逻辑
            score = 0
            if last['close'] > last['ma5']: score += 1      # 动能
            if last['j'] > last['d'] and prev['j'] <= prev['d']: score += 1 # 拐点
            if 35 < last['rsi'] < 75: score += 1           # 强弱
            if last['amount'] > df['amount'].tail(5).mean(): score += 1 # 量能

            if score >= 3:
                rets = df['close'].pct_change().tail(252)
                sharpe = (rets.mean() * 252 - 0.02) / (rets.std() * np.sqrt(252)) if rets.std() != 0 else 0
                if sharpe < CONFIG['MIN_SHARPE']: return None
                
                # 计算建议仓位 (2倍ATR止损)
                stop_loss = 2.1 * last['atr']
                shares = int((CONFIG['TOTAL_CAPITAL'] * CONFIG['RISK_PER_TRADE']) / max(stop_loss, 0.01) // 100 * 100)
                
                return {
                    'code': "".join(filter(str.isdigit, os.path.basename(file_path))).zfill(6),
                    'score': score, 'price': round(last['close'], 3),
                    'rsi': last['rsi'], 'j': last['j'], 'shares': shares,
                    'sharpe': round(sharpe, 2)
                }
        except: return None

# --- 3. 绩效回溯与报告生成 ---
def process_performance(current_signals, db):
    history_file = CONFIG['HISTORY_FILE']
    if not os.path.exists(history_file):
        pd.DataFrame(columns=['date', 'code', 'name', 'entry_price']).to_csv(history_file, index=False)
    
    history_df = pd.read_csv(history_file, dtype={'code': str})
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 记录今日新信号
    new_entries = []
    for s in current_signals:
        if not ((history_df['date'] == today_str) & (history_df['code'] == s['code'])).any():
            new_entries.append({'date': today_str, 'code': s['code'], 'name': s['name'], 'entry_price': s['price']})
    
    if new_entries:
        history_df = pd.concat([history_df, pd.DataFrame(new_entries)], ignore_index=True)
    
    # 计算历史胜率 (T+1起算)
    win_count, total_tracked, total_ret = 0, 0, 0
    past_signals = history_df[history_df['date'] < today_str]
    
    for _, row in past_signals.iterrows():
        csv_path = os.path.join(CONFIG['DATA_DIR'], f"{str(row['code']).zfill(6)}.csv")
        if os.path.exists(csv_path):
            df_now = pd.read_csv(csv_path)
            now_price = df_now.iloc[-1]['收盘']
            ret = (now_price - row['entry_price']) / row['entry_price']
            total_tracked += 1
            total_ret += ret
            if ret > 0: win_count += 1
            
    history_df.tail(100).to_csv(history_file, index=False, encoding='utf_8_sig')
    
    wr = round(win_count / total_tracked * 100, 1) if total_tracked > 0 else 0
    ar = round(total_ret / total_tracked * 100, 2) if total_tracked > 0 else 0
    return wr, ar

def main():
    db = load_fund_db()
    current_results = []
    for f in glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv")):
        res = QuantEngine.analyze(f)
        if res:
            res['name'] = db.get(res['code'], f"未匹配({res['code']})")
            current_results.append(res)
    
    current_results.sort(key=lambda x: (x['score'], x['sharpe']), reverse=True)
    win_rate, avg_ret = process_performance(current_results, db)
    
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘绩效看板 V12.2\n\n")
        f.write(f"### 📊 策略回溯统计 (实盘对齐)\n")
        f.write(f"- **近期信号胜率**: `{win_rate}%` | **平均涨幅**: `{avg_ret}%` \n")
        f.write(f"- **核心逻辑**: 多指标交叉验证 (KDJ金叉 + RSI强弱 + MA5动能)\n\n")
        f.write(f"📅 更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        if current_results:
            f.write("| 代码 | 简称 | 强度 | 现价 | RSI | J值 | 建议买入 | 信号状态 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in current_results[:10]:
                f.write(f"| {s['code']} | **{s['name']}** | {'🔥'*s['score']} | {s['price']:.3f} | `{s['rsi']}` | `{s['j']}` | {s['shares']}股 | 🚩新入场 |\n")
        else:
            f.write("> 😴 今日暂无强信号标的。")

if __name__ == "__main__":
    main()
