import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# --- 核心配置 ---
CONFIG = {
    'TOTAL_CAPITAL': 100000,
    'DATA_DIR': 'fund_data',
    'EXCEL_DB': 'ETF列表.xlsx',     # 确保根目录下有这个文件
    'REPORT_FILE': 'README.md',
    'HISTORY_FILE': 'signal_history.csv',
    'MIN_SHARPE': 0.2
}

# --- 1. 修复后的名称匹配引擎 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(CONFIG['EXCEL_DB']):
        print(f"❌ 找不到数据库: {CONFIG['EXCEL_DB']}")
        return fund_db
    try:
        # 强制以字符串读取代码列
        df = pd.read_excel(CONFIG['EXCEL_DB'], dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 智能匹配列名
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)

        if c_code and c_name:
            for _, row in df.iterrows():
                raw_code = str(row[c_code]).strip()
                # 提取数字并补足6位
                clean_code = "".join(filter(str.isdigit, raw_code)).zfill(6)
                fund_db[clean_code] = str(row[c_name]).strip()
        print(f"✅ 成功加载 {len(fund_db)} 条ETF名称记录")
        return fund_db
    except Exception as e:
        print(f"❌ Excel匹配失败: {e}")
        return fund_db

# --- 2. 策略引擎 (保持多维指标) ---
class AdvancedStrategy:
    @staticmethod
    def calculate_indicators(df):
        df.columns = [str(c).strip().lower() for c in df.columns]
        mapping = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交额': 'amount'}
        df.rename(columns=mapping, inplace=True)
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        
        # KDJ
        l9, h9 = df['low'].rolling(9).min(), df['high'].rolling(9).max()
        rsv = (df['close'] - l9) / (h9 - l9 + 1e-9) * 100
        df['k'] = rsv.ewm(com=2).mean()
        df['d'] = df['k'].ewm(com=2).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']
        
        # ATR & MA
        df['tr'] = np.maximum((df['high'] - df['low']), (df['high'] - df['close'].shift(1)).abs())
        df['atr'] = df['tr'].rolling(14).mean()
        df['ma5'] = df['close'].rolling(5).mean()
        return df

    @staticmethod
    def analyze(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) < 30: return None
            df = AdvancedStrategy.calculate_indicators(df)
            last, prev = df.iloc[-1], df.iloc[-2]
            
            score = 0
            if last['close'] > last['ma5']: score += 1
            if last['j'] > last['d'] and prev['j'] <= prev['d']: score += 1
            if 35 < last['rsi'] < 75: score += 1
            if last['amount'] > df['amount'].tail(5).mean(): score += 1
            
            if score >= 3:
                # 计算夏普比率
                rets = df['close'].pct_change().tail(252)
                sharpe = (rets.mean() * 252 - 0.02) / (rets.std() * np.sqrt(252)) if rets.std() != 0 else 0
                if sharpe < CONFIG['MIN_SHARPE']: return None
                
                return {
                    'code': "".join(filter(str.isdigit, os.path.basename(file_path))).zfill(6),
                    'score': score, 
                    'price': round(last['close'], 3),
                    'sharpe': round(sharpe, 2)
                }
        except: return None

# --- 3. 绩效回溯模块 ---
def track_performance(current_signals, db):
    history_file = CONFIG['HISTORY_FILE']
    if os.path.exists(history_file):
        history_df = pd.read_csv(history_file, dtype={'code': str})
    else:
        history_df = pd.DataFrame(columns=['date', 'code', 'name', 'entry_price'])

    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 记录新信号
    new_entries = []
    for s in current_signals:
        # 如果历史里今天已经记录过，就不重复记
        if not ((history_df['date'] == today_str) & (history_df['code'] == s['code'])).any():
            new_entries.append({
                'date': today_str,
                'code': s['code'],
                'name': s.get('name', '未知'),
                'entry_price': s['price']
            })
    
    if new_entries:
        history_df = pd.concat([history_df, pd.DataFrame(new_entries)], ignore_index=True)

    # 统计胜率 (回溯 2 天前到 30 天前的信号)
    win_count = 0
    total_tracked = 0
    total_return = 0
    
    for idx, row in history_df.iterrows():
        code = row['code']
        csv_path = os.path.join(CONFIG['DATA_DIR'], f"{code}.csv")
        if os.path.exists(csv_path):
            df_now = pd.read_csv(csv_path)
            now_price = df_now.iloc[-1]['收盘']
            ret = (now_price - row['entry_price']) / row['entry_price']
            
            # 统计 T+1 之后的信号
            if row['date'] < today_str:
                total_tracked += 1
                total_return += ret
                if ret > 0: win_count += 1
    
    history_df.tail(100).to_csv(history_file, index=False)
    
    wr = (win_count / total_tracked * 100) if total_tracked > 0 else 0
    ar = (total_return / total_tracked * 100) if total_tracked > 0 else 0
    return round(wr, 1), round(ar, 2)

# --- 4. 执行逻辑 ---
def main():
    db = load_fund_db()
    current_results = []
    
    # 扫描所有CSV
    files = glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv"))
    for f in files:
        res = AdvancedStrategy.analyze(f)
        if res:
            res['name'] = db.get(res['code'], f"未匹配({res['code']})")
            current_results.append(res)
    
    # 排序
    current_results.sort(key=lambda x: (x['score'], x['sharpe']), reverse=True)
    
    # 绩效跟踪
    win_rate, avg_ret = track_performance(current_results, db)
    
    # 生成看板
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘绩效看板 V12.1\n\n")
        f.write(f"### 📊 策略回溯统计 (历史信号复盘)\n")
        f.write(f"- **近期胜率**: `{win_rate}%` (基于已发出的信号)\n")
        f.write(f"- **平均信号涨幅**: `{avg_ret}%` (T+N 跟踪)\n\n")
        f.write(f"📅 更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        if current_results:
            f.write("| 代码 | 简称 | 强度 | 现价 | 信号状态 |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            for s in current_results[:8]:
                f.write(f"| {s['code']} | **{s['name']}** | {'🔥'*s['score']} | {s['price']:.3f} | 🚩 新入场 |\n")
        else:
            f.write("> 😴 今日暂无强信号标的。")

if __name__ == "__main__":
    main()
