import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- V5.2 进阶配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 20日内回撤门槛 (5%)
MIN_TURNOVER_RATE = 1.0             # 换手率门槛 (1%)，低于此值视为不活跃
BIAS_THRESHOLD = -5.0               # 乖离率阈值 (价格偏离MA20过远)
VOL_RATIO_HIGH = 1.5                # 放量定义 (1.5倍)
VOL_RATIO_LOW = 0.6                 # 缩量定义 (0.6倍)
REPORT_BASE_NAME = 'Report_V5_2'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    """
    自适应表头加载函数，适配中文/英文/各类导出格式
    """
    try:
        # 编码探测
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')

        # 统一表头映射
        column_map = {
            '日期': 'date', 'Date': 'date',
            '收盘': 'close', 'Close': 'close', '净值': 'close',
            '成交量': 'volume', 'Volume': 'volume',
            '换手率': 'turnover', 'Turnover': 'turnover'
        }
        df = df.rename(columns=column_map)

        # 检查并保留核心列
        required = ['date', 'close', 'volume', 'turnover']
        for col in required:
            if col not in df.columns:
                # 如果缺少成交量或换手率，填充为0以免报错，但会影响筛选
                df[col] = 0
                
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 数值转换
        for col in ['close', 'volume', 'turnover']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df.dropna(subset=['close'])
    except Exception as e:
        logging.error(f"加载 {filepath} 失败: {e}")
        return None

def analyze_logic(df):
    """
    量化选股逻辑核心：回撤 + RSI + BIAS + 量价背离
    """
    if len(df) < 30: return None
    
    # 1. 计算均线与乖离率 (BIAS)
    df['MA20'] = df['close'].rolling(20).mean()
    df['bias'] = (df['close'] - df['MA20']) / df['MA20'] * 100
    
    # 2. 计算量比 (当日量 / 5日均量)
    df['ma5_vol'] = df['volume'].rolling(5).mean()
    df['vol_ratio'] = df['volume'] / df['ma5_vol'].replace(0, np.nan)
    
    # 3. 计算 RSI(14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, 0.001)
    df['rsi'] = 100 - (100 / (1 + rs))

    # 4. 计算20日最大回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # --- 信号识别 ---
    signals = []
    
    # 信号1: 极度乖离 (反弹引力)
    if last['bias'] <= BIAS_THRESHOLD:
        signals.append("超跌乖离")
    
    # 信号2: RSI 超卖
    if last['rsi'] < 30:
        signals.append("RSI超卖")
        
    # 信号3: RSI底背离 (价格创新低但RSI未创新低)
    if last['close'] < prev['close'] and last['rsi'] > prev['rsi'] and last['rsi'] < 35:
        signals.append("RSI底背离")

    # 信号4: 量价配合状态
    vol_status = "平稳"
    if last['vol_ratio'] >= VOL_RATIO_HIGH:
        vol_status = "放量"
    elif last['vol_ratio'] <= VOL_RATIO_LOW:
        vol_status = "缩量"

    # --- 最终筛选条件 ---
    # 条件：20日回撤达标 且 (满足任意技术信号) 且 活跃度达标
    if abs(last['drawdown']) >= MIN_MONTH_DRAWDOWN and last['turnover'] >= MIN_TURNOVER_RATE:
        if signals or last['rsi'] < 35:
            return {
                'code': "", 
                'close': last['close'],
                'drawdown': last['drawdown'],
                'rsi': last['rsi'],
                'bias': last['bias'],
                'vol_status': vol_status,
                'turnover': last['turnover'],
                'action': " | ".join(signals) if signals else "超卖观察"
            }
    return None

def main():
    if not os.path.exists(FUND_DATA_DIR):
        os.makedirs(FUND_DATA_DIR)
        return

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df)
            if res:
                res['code'] = code
                results.append(res)

    # 按回撤和信号强度排序 (回撤越大越靠前)
    results = sorted(results, key=lambda x: x['drawdown'])

    # 生成 Markdown 报告
    report_name = f"{REPORT_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金量价进阶分析报告 (V5.2)\n")
        f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write("> **策略逻辑**：回撤 > 5% 且 (RSI < 35 或 满足乖离率/背离信号) 且 换手率 > 1%\n\n")
        
        if not results:
            f.write("今日市场未满足筛选条件。")
        else:
            f.write("| 基金代码 | 价格 | 20日回撤 | RSI | 乖离率 | 换手 | 量价状态 | 信号提示 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.1f} | {r['bias']:.1f}% | {r['turnover']:.1f}% | {r['vol_status']} | **{r['action']}** |\n")
    
    print(f"✅ 分析完成！成功捕获 {len(results)} 个潜在标的。报告：{report_name}")

if __name__ == "__main__":
    main()
