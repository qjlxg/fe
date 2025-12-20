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

# --- 1. 强力本地数据库解析引擎 ---
def load_fund_db():
    fund_db = {}
    
    def get_col(df, keywords):
        """模糊匹配列名，防止空格或不可见字符干扰"""
        for k in keywords:
            for c in df.columns:
                if k in str(c): return c
        return None

    # 处理沪市/深市文件
    for info_file in ['ETF列表沪.xls - 基金列表.csv', 'ETF列表深.xlsx - ETF列表.csv']:
        if not os.path.exists(info_file): continue
        try:
            # 使用 utf-8-sig 自动处理 BOM 头
            df = pd.read_csv(info_file, encoding='utf-8-sig', dtype=str)
            
            # 定位关键列
            c_code = get_col(df, ['代码', '证券代码', '基金代码'])
            c_name = get_col(df, ['简称', '证券简称', '基金简称'])
            c_idx  = get_col(df, ['指数', '拟合', '标的'])
            
            if c_code and c_name:
                for _, row in df.iterrows():
                    raw_code = str(row[c_code]).strip().split('.')[0].zfill(6)
                    if len(raw_code) != 6: continue
                    
                    name = str(row[c_name]).strip()
                    idx = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "-"
                    if idx == "-": idx = "宽基/策略指数"
                    
                    fund_db[raw_code] = {'name': name, 'index': idx}
        except Exception as e:
            print(f"解析 {info_file} 出错: {e}")
            
    return fund_db

# --- 2. 深度数据挖掘算法 (利用换手率、振幅、成交额) ---
def analyze_enhanced(df):
    if len(df) < 30: return None
    
    # 统一列名清洗
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','换手率':'turnover','振幅':'vol','最高':'high','最低':'low'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    # 转换数值
    for col in ['close','amount','turnover','vol','high','low']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    # 1分：基本面企稳（超跌 + 站上5日线）
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        # 2分：趋势转强（站上10日线）
        if last['close'] > ma10: score += 1
        # 3分：主力确认（换手率较昨日温和放大 或 成交额大于5日均值）
        avg_amt5 = df['amount'].rolling(5).mean().iloc[-1]
        if last['amount'] > avg_amt5: score += 1
        # 4分额外奖励：波动收敛（缩量磨底后的小阳线）
        if 'vol' in df.columns:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    if score >= 3:
        risk_money = TOTAL_CAPITAL * 0.02
        stop_p = ma10 * 0.97
        shares = int(risk_money / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {
            'score': score, 'price': last['close'], 'stop': stop_p, 
            'shares': shares, 'dd': dd * 100, 'turnover': last.get('turnover', 0)
        }
    return None

# --- 3. 执行主流程 ---
def execute():
    bj_now = get_beijing_time()
    fund_db = load_fund_db()
    all_signals = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0].zfill(6)
        if code == MARKET_INDEX: continue
        try:
            res = analyze_enhanced(pd.read_csv(f))
            if res:
                info = fund_db.get(code, {'name': '未知标的', 'index': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                all_signals.append(res)
        except: continue

    # 排序：得分 > 回撤深度
    all_signals.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V13.0\n\n")
        f.write(f"更新时间: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 数据库: `沪深全量本地化适配`\n\n")
        f.write("### 🎯 顶级共振信号 (量价收敛+超跌反弹)\n")
        if all_signals:
            f.write("| 代码 | 基金简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损位 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in all_signals:
                score_str = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {score_str} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 暂无高分共振信号，请耐心等待底部确认。")

if __name__ == "__main__":
    execute()
