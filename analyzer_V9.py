import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 暴力列名感应与数据清洗 ---
def load_fund_db():
    fund_db = {}
    # 定义沪深两个本地文件名
    files = ['ETF列表沪.xls - 基金列表.csv', 'ETF列表深.xlsx - ETF列表.csv']
    
    for f_name in files:
        if not os.path.exists(f_name):
            continue
        try:
            # 1. 尝试多种编码读取
            df = pd.read_csv(f_name, encoding='utf-8-sig', dtype=str)
            
            # 2. 清洗所有列名：去掉空格、换行、制表符
            df.columns = [str(c).strip() for c in df.columns]
            
            # 3. 动态寻找列名（不写死，只匹配关键字）
            c_code = next((c for c in df.columns if '代码' in c), None)
            c_name = next((c for c in df.columns if '简称' in c), None)
            c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '拟合', '标的'])), None)
            c_size = next((c for c in df.columns if '规模' in c), None)

            if c_code and c_name:
                for _, row in df.iterrows():
                    # 强力清洗代码：转数字去掉.0再补零
                    raw_code = str(row[c_code]).strip().split('.')[0].zfill(6)
                    if len(raw_code) != 6: continue
                    
                    name = str(row[c_name]).strip()
                    idx = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "指数/宽基"
                    size = str(row[c_size]).replace('"', '').replace(',', '').strip() if c_size else "0"
                    
                    fund_db[raw_code] = {
                        'name': name,
                        'index': idx if idx != '-' else "策略指数",
                        'size': size
                    }
        except Exception as e:
            print(f"解析 {f_name} 失败: {e}")
    return fund_db

# --- 2. 增强型策略引擎 (带波动率过滤) ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    # 强制对齐 fund_data 中的 CSV 列名
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','换手率':'turnover','振幅':'vol'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    for col in ['close','amount','turnover','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    # 评分逻辑：超跌反转 + 量价共振
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        if last['close'] > ma10: score += 1
        # 成交额放量
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        # 波动率收敛（代表磨底成功）
        if 'vol' in df.columns and last['vol'] < df['vol'].rolling(10).mean().iloc[-1]:
            score += 1

    if score >= 3:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.965 # 3.5% 固定止损
        shares = int(risk / (last['close'] - stop_p) // 100 * 100)
        return {
            'score': score, 'price': last['close'], 'stop': stop_p,
            'shares': shares, 'dd': dd * 100, 'turnover': last.get('turnover', 0)
        }
    return None

# --- 3. 执行主程序 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0].zfill(6)
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                info = db.get(code, {'name': '未知标的', 'index': '-', 'size': '0'})
                res.update({'code': code, 'name': info['name'], 'index': info['index'], 'size': info['size']})
                results.append(res)
        except: continue

    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V13.5\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 数据库: `沪深全适配版`\n\n")
        f.write("### 🎯 高胜率信号 (量价收敛 + 底部放量)\n")
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损位 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                score_icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {score_icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 当前市场波动平淡，暂无精英级别信号。")

if __name__ == "__main__":
    execute()
