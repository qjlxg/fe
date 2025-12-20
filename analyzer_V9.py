import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       # 模拟总本金
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
TRACKER_FILE = 'signal_performance_tracker.csv'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3

# --- 1. 增强型数据读取：匹配名称与行业 ---
def get_fund_info():
    """从网络或静态数据获取基金名称及行业信息"""
    try:
        import akshare as ak
        # 获取全量ETF基础信息
        fund_info = ak.fund_etf_category_sina("ETF基金")
        # 建立 基金代码 -> (名称) 映射
        name_map = dict(zip(fund_info['代码'], fund_info['名称']))
        return name_map
    except:
        # 备用映射：如果网络失败，常用代码手动映射
        return {"513060": "恒生医疗ETF", "513780": "港股互联网ETF", "159102": "中证1000ETF"}

def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
        df.rename(columns=mapping, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

# --- 2. 评分与分析引擎 ---
def analyze_signal(df):
    if len(df) < 30: return None
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1

    score = 0
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > amt_ma5: score += 1
            
    if score >= 1:
        # 智能仓位：每笔交易风险控制在总资金的 2%
        risk_per_trade = TOTAL_CAPITAL * 0.02
        stop_gap = last['close'] - (ma10 * 0.97)
        suggest_shares = int(risk_per_trade / max(stop_gap, 0.01) // 100 * 100)
        
        return {
            'roc': roc20 * 100,
            'score': score,
            'price': last['close'],
            'stop': ma10 * 0.97,
            'shares': suggest_shares,
            'date': datetime.now().strftime('%Y-%m-%d')
        }
    return None

# --- 3. 执行并输出 ---
def execute():
    # 获取名称字典
    name_map = get_fund_info()
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []
    
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        res = analyze_signal(load_data(f))
        if res:
            res['code'] = code
            # 匹配名称和行业标签（根据名称关键字简单分类）
            name = name_map.get(code, "未知ETF")
            res['name'] = name
            res['tag'] = "医疗/医药" if "医" in name else "互联网/科技" if "网" in name or "科技" in name else "宽基/其他"
            all_signals.append(res)

    # 更新历史记录
    pd.DataFrame(all_signals).to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE))

    # 筛选 ≥3 分精英信号
    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite.sort(key=lambda x: x['roc'], reverse=True)

    # 写入 README
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write("# 🛰️ 天枢 ETF 精英监控看板 (V10.0)\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        f.write("## 🎯 顶级入场信号 (得分=3, 价格+趋势+资金共振)\n")
        if elite:
            f.write("| 排名 | 代码 | 基金简称 | 主题标签 | ROC20% | 现价 | 建议买入 | 建议止损 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for i, s in enumerate(elite, 1):
                f.write(f"| {i} | {s['code']} | **{s['name']}** | `{s['tag']}` | {s['roc']:.2f}% | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 🧊 今日暂无 3 分信号。请关注 `tracker` 文件中的 1-2 分备选品种。\n")

        f.write("\n## 📊 板块异动统计\n")
        if all_signals:
            tag_counts = pd.DataFrame(all_signals)['tag'].value_counts()
            for tag, count in tag_counts.items():
                f.write(f"- `{tag}` 板块今日触发信号数量: **{count}**\n")

    print(f"✨ V10.0 运行完成，看板已同步。")

if __name__ == "__main__":
    execute()
