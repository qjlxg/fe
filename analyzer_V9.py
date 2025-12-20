import pandas as pd
import glob
import os
import io
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 2  # 调低门槛，让 2 分（潜力）和 3/4 分（精英）都能看到
EXCEL_DB = 'ETF列表.xlsx' # 指定匹配用的 Excel 文件

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 工业级数据清洗引擎 (已适配 Excel 匹配) ---
def load_fund_db():
    fund_db = {}
    
    if not os.path.exists(EXCEL_DB):
        print(f"警告: 未找到 {EXCEL_DB}，将无法匹配基金名称和指数。")
        return fund_db

    try:
        # 直接读取 Excel 文件
        df = pd.read_excel(EXCEL_DB, dtype=str)
        
        # 清洗列名
        df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]
        
        # 动态定位关键列：代码、简称、指数名称
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '拟合', '标的'])), None)

        if c_code and c_name:
            for _, row in df.iterrows():
                # 处理代码格式，补齐 6 位，移除可能存在的 .SH 或 .SZ
                raw_code = str(row[c_code]).strip()
                code = raw_code.split('.')[0].zfill(6)
                
                name = str(row[c_name]).strip()
                
                # 处理指数列
                idx = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "-"
                if idx in ["-", "nan", "None"]: 
                    idx = "宽基/策略指数"
                
                fund_db[code] = {'name': name, 'index': idx}
        
        print(f"数据库加载成功: 已匹配 {len(fund_db)} 条基金信息。")
    except Exception as e:
        print(f"解析 {EXCEL_DB} 失败: {e}")
        
    return fund_db

# --- 2. 增强策略 (区分潜力与精英) ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    # 转换数值并填充空值
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    # 基础分：超跌且站上5日线 (1分)
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        # 2分：确认站上10日线 (趋势初步扭转)
        if last['close'] > ma10: score += 1
        # 3分：成交量有效放大 (主力资金入场)
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        # 4分：波动率收敛 (底部的极致信号)
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    if score >= MIN_SCORE_SHOW:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.96 # 固定4%止损
        shares = int(risk / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {'score': score, 'price': last['close'], 'stop': stop_p, 'shares': shares, 'dd': dd * 100}
    return None

# --- 3. 执行流程 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    # 扫描 fund_data 文件夹下的所有数据文件
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"错误: {DATA_DIR} 文件夹下未发现数据 CSV 文件。")
        return

    for f in files:
        # 获取文件名作为代码
        code = os.path.splitext(os.path.basename(f))[0].replace('SH', '').replace('SZ', '').zfill(6)
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                # 从 Excel 加载的信息库中匹配
                info = db.get(code, {'name': '未知标的', 'index': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                results.append(res)
        except Exception as e:
            continue

    # 排序：得分越高越靠前，同分则看回撤（回撤大的可能反弹力强）
    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 看板 V14.0\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 数据源: `Excel 精准对齐`\n\n")
        f.write("### 🎯 实时信号追踪 (2分潜力 / 3分及以上精英)\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score'] if s['score'] >= 3 else "⭐"
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 市场处于横盘震荡，暂无符合要求的超跌信号。")
    
    print(f"分析完成！报告已生成至 {REPORT_FILE}")

if __name__ == "__main__":
    execute()
