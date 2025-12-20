import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings
import time

warnings.filterwarnings('ignore')

# --- 系统配置 ---
TOTAL_ASSETS = 100000
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
MARKET_INDEX = '510300'  # 沪深300ETF，用于判断大盘情绪
MAX_HOLD_COUNT = 5
MIN_DAILY_AMOUNT = 50000000  # 成交额过滤：5000万
RISK_PER_TRADE = 0.015
ETF_DD_THRESHOLD = -0.06

# --- 1. 数据读取模块 ---
def load_data(file_path):
    """读取并标准化 CSV 列名"""
    try:
        # 兼容你上传的 CSV 格式：日期,开盘,收盘,最高,最低,成交量,成交额...
        df = pd.read_csv(file_path)
        # 映射中文列名到英文，确保后续逻辑通用
        column_map = {
            '日期': 'date', '开盘': 'open', '收盘': 'close', 
            '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount', '换手率': 'turnover'
        }
        df.rename(columns=column_map, inplace=True)
        # 强制小写处理，防止列名大小写不一致
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except Exception as e:
        print(f"❌ 读取文件 {file_path} 失败: {e}")
        return pd.DataFrame()

# --- 2. 指标与逻辑 ---
def get_market_sentiment():
    """判断大盘背景"""
    mkt_path = os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")
    if not os.path.exists(mkt_path):
        return 0, "未知", 1.0
    
    mkt_df = load_data(mkt_path)
    if len(mkt_df) < 20: return 0, "数据不足", 1.0
    
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    bias = (mkt_df['close'].iloc[-1] - ma20) / ma20
    
    if bias > 0.02: return bias, "🔥 强劲", 1.2
    if bias < -0.02: return bias, "❄️ 冰点", 0.6
    return bias, "⚖️ 平衡", 1.0

def calculate_indicators(df):
    """计算核心技术指标"""
    if len(df) < 30: return df
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    # ATR
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    # ROC20 动量
    df['ROC20'] = df['close'].pct_change(20)
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    # MACD Hist
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_Hist'] = (exp1 - exp2 - (exp1 - exp2).ewm(span=9, adjust=False).mean()) * 2
    # 5日平均成交额
    df['AMT_MA5'] = df['amount'].rolling(5).mean()
    return df

# --- 3. 执行主逻辑 ---
def execute_system():
    # 自动定位目录下所有 CSV 文件
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not csv_files:
        print(f"❌ 在 {DATA_DIR} 目录下未找到任何数据文件，请检查数据更新脚本。")
        return

    # 初始化持仓账本
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'shares', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    current_holds = portfolio['code'].astype(str).tolist()

    bias, sentiment, mkt_weight = get_market_sentiment()
    new_signals, hold_monitor = [], []

    print(f"🔍 正在读取本地目录，扫描 {len(csv_files)} 个 ETF 品种...")

    for f_path in csv_files:
        code = os.path.splitext(os.path.basename(f_path))[0]
        if code == MARKET_INDEX: continue
        
        df = load_data(f_path)
        if len(df) < 30: continue
        df = calculate_indicators(df)
        last = df.iloc[-1]

        # 1. 持仓监控逻辑
        if code in current_holds:
            p_row = portfolio[portfolio['code'].astype(str) == code].iloc[0]
            status = "✅ 正常"
            if last['close'] <= p_row['stop_price']: status = "💥 触发止损"
            elif last['close'] < last['MA10']: status = "📉 破10日线"
            
            hold_monitor.append({
                'code': code, 'profit': (last['close']-p_row['buy_price'])/p_row['buy_price']*100,
                'price': last['close'], 'status': status
            })
            continue

        # 2. 新信号筛选逻辑 (成交额过滤 + 动量超跌共振)
        if last['amount'] < MIN_DAILY_AMOUNT: continue
        
        peak_20 = df['close'].rolling(20).max().iloc[-1]
        drawdown = (last['close'] - peak_20) / peak_20
        
        if last['close'] > last['MA5'] and drawdown < ETF_DD_THRESHOLD:
            score = sum([last['RSI'] > 40, last['MACD_Hist'] > df.iloc[-2]['MACD_Hist']])
            if score >= 1:
                stop_p = min(last['close'] - 2*last['atr'], last['MA10']*0.95)
                new_signals.append({
                    'code': code, 'roc': last['ROC20']*100, 'price': last['close'], 'stop': stop_p, 'score': score
                })

    # --- 输出可视化报告 ---
    print("\n" + "="*85)
    print(f"🚀 天枢 ETF 监控报告 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"大盘情绪: {sentiment} (Bias: {bias:.2%}) | 建议权重: {mkt_weight}")
    print("="*85)

    if hold_monitor:
        print("\n【持仓健康度】")
        print(f"{'代码':<8} | {'现价':<8} | {'盈亏%':<8} | {'状态'}")
        for h in hold_monitor:
            print(f"{h['code']:<8} | {h['price']:<8.3f} | {h['profit']:>7.2f}% | {h['status']}")

    if new_signals:
        print("\n【入场扫描信号】(按 ROC20 动量排序)")
        new_signals.sort(key=lambda x: x['roc'], reverse=True)
        print(f"{'代码':<8} | {'ROC20%':<8} | {'得分':<4} | {'现价':<8} | {'建议止损'}")
        for s in new_signals[:MAX_HOLD_COUNT]:
            print(f"{s['code']:<8} | {s['roc']:>7.2f}% | {s['score']:<4} | {s['price']:<8.3f} | {s['stop']:<8.3f}")
    else:
        print("\n💡 扫描完成：当前池内无满足共振条件的入场信号。")
    print("="*85)

if __name__ == "__main__":
    execute_system()
