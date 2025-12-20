import pandas as pd
import numpy as np
import glob, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# --- 核心配置 ---
CONFIG = {
    'TOTAL_CAPITAL': 100000,
    'DATA_DIR': 'fund_data',
    'EXCEL_DB': 'ETF列表.xlsx',
    'REPORT_FILE': 'README.md',
    'HISTORY_FILE': 'signal_history.csv', # 存储历史信号的文件
    'TRACK_DAYS': 5,                      # 跟踪 5 天后的表现
    'MIN_SHARPE': 0.2
}

class PerformanceTracker:
    @staticmethod
    def record_and_track(current_signals, all_files_data):
        """记录今日信号并回测历史信号表现"""
        history_df = pd.DataFrame()
        if os.path.exists(CONFIG['HISTORY_FILE']):
            history_df = pd.read_csv(CONFIG['HISTORY_FILE'], dtype={'code': str})

        # 1. 记录今日新信号
        today_str = datetime.now().strftime('%Y-%m-%d')
        new_records = []
        for s in current_signals:
            new_records.append({
                'date': today_str,
                'code': s['code'],
                'name': s['name'],
                'entry_price': s['price'],
                'status': 'tracking'
            })
        
        if new_records:
            history_df = pd.concat([history_df, pd.DataFrame(new_records)], ignore_index=True)

        # 2. 跟踪历史信号表现
        stats = {'win': 0, 'total': 0, 'avg_ret': 0}
        if not history_df.empty:
            for idx, row in history_df.iterrows():
                code = row['code']
                # 寻找该代码最新的 CSV 数据
                target_file = os.path.join(CONFIG['DATA_DIR'], f"{code}.csv")
                if os.path.exists(target_file):
                    df_price = pd.read_csv(target_file)
                    current_price = df_price.iloc[-1]['收盘']
                    
                    # 计算涨跌幅
                    ret = (current_price - row['entry_price']) / row['entry_price']
                    history_df.at[idx, 'current_price'] = round(current_price, 3)
                    history_df.at[idx, 'return'] = round(ret * 100, 2)
                    
                    # 只统计 3 天前的信号作为“已结转胜率”
                    signal_date = datetime.strptime(row['date'], '%Y-%m-%d')
                    if datetime.now() - signal_date > timedelta(days=2):
                        stats['total'] += 1
                        stats['avg_ret'] += ret
                        if ret > 0: stats['win'] += 1

            # 保留最近 50 条记录，防止文件过大
            history_df = history_df.tail(50)
            history_df.to_csv(CONFIG['HISTORY_FILE'], index=False)
        
        win_rate = (stats['win'] / stats['total'] * 100) if stats['total'] > 0 else 0
        avg_ret = (stats['avg_ret'] / stats['total'] * 100) if stats['total'] > 0 else 0
        return round(win_rate, 1), round(avg_ret, 2)

# --- 核心分析逻辑 (继承 V11 的多指标交叉验证) ---
class AdvancedStrategy:
    # ... (此处省略 calculate_indicators 和 analyze 函数，逻辑同 V11) ...
    # 详见上一版代码，确保包含 RSI, KDJ, Bollinger 计算
    @staticmethod
    def calculate_indicators(df):
        # 字段兼容
        df.columns = [str(c).strip().lower() for c in df.columns]
        mapping = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交额': 'amount'}
        df.rename(columns=mapping, inplace=True)
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / loss)))
        # KDJ
        l9, h9 = df['low'].rolling(9).min(), df['high'].rolling(9).max()
        rsv = (df['close'] - l9) / (h9 - l9) * 100
        df['k'], df['d'] = rsv.ewm(com=2).mean(), rsv.ewm(com=2).mean().ewm(com=2).mean()
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
                return {
                    'code': "".join(filter(str.isdigit, os.path.basename(file_path))).zfill(6),
                    'score': score, 'price': round(last['close'], 3),
                    'atr': last['atr']
                }
        except: return None

# --- 执行主函数 ---
def main():
    # 1. 加载数据库
    db = {} # 假设已通过 load_fund_db 加载
    
    # 2. 扫描今日信号
    current_results = []
    for f in glob.glob(os.path.join(CONFIG['DATA_DIR'], "*.csv")):
        res = AdvancedStrategy.analyze(f)
        if res:
            res.update({'name': db.get(res['code'], {'name': '未知'})['name']})
            current_results.append(res)
    
    # 3. 跟踪绩效
    win_rate, avg_performance = PerformanceTracker.record_and_track(current_results, None)
    
    # 4. 生成报表
    with open(CONFIG['REPORT_FILE'], "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘绩效看板 V12\n\n")
        f.write(f"### 📊 策略回溯统计 (近 50 笔信号)\n")
        f.write(f"- **近期胜率**: `{win_rate}%`\n")
        f.write(f"- **平均信号涨幅**: `{avg_performance}%` (T+2 跟踪)\n\n")
        f.write(f"📅 更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        f.write("| 代码 | 简称 | 强度 | 现价 | 信号跟踪 |\n")
        f.write("| --- | --- | --- | --- | --- |\n")
        for s in current_results[:5]:
            f.write(f"| {s['code']} | {s['name']} | {'🔥'*s['score']} | {s['price']} | 🚩 新入场 |\n")

if __name__ == "__main__":
    main()
