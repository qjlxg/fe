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
MIN_SCORE_SHOW = 3  # 强制过滤：只保留 3 分及以上精英信号
EXCEL_DB = 'ETF列表.xlsx' 

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 针对“证券代码”优化的数据匹配引擎 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB):
        print(f"❌ 错误: 未找到数据库文件 {EXCEL_DB}")
        return fund_db

    try:
        # 显式使用 openpyxl，并强制读取为字符串
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        
        # 清洗列名
        df.columns = [str(c).strip() for c in df.columns]
        
        # 精准匹配：寻找“证券代码”和“证券简称”
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c), None)
        # 指数列如果不存在，则默认为空
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '拟合', '标的'])), None)

        if c_code and c_name:
            for _, row in df.iterrows():
                # 提取纯数字代码，确保 159139 变成 "159139"
                val = str(row[c_code]).strip()
                clean_code = "".join(filter(str.isdigit, val))[:6].zfill(6)
                
                if not clean_code or len(clean_code) < 6: continue
                
                name = str(row[c_name]).strip()
                # 提取指数信息
                idx_val = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "宽基/行业指数"
                if idx_val in ["-", "nan", "None", ""]: idx_val = "宽基/行业指数"
                
                fund_db[clean_code] = {'name': name, 'index': idx_val}
            
            print(f"✅ 匹配引擎就绪: 已从 Excel 加载 {len(fund_db)} 条标的信息")
        else:
            print(f"❌ 匹配失败: Excel 必须包含 '证券代码' 和 '证券简称' 列。当前列名: {list(df.columns)}")
    except Exception as e:
        print(f"❌ 解析 {EXCEL_DB} 失败: {e}")
        
    return fund_db

# --- 2. 增强策略引擎 ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    # 评分体系
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    # 只保留 3 分及以上
    if score >= MIN_SCORE_SHOW:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.96 
        shares = int(risk / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {'score': score, 'price': last['close'], 'stop': stop_p, 'shares': shares, 'dd': dd * 100}
    return None

# --- 3. 执行流程 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"❌ 文件夹 {DATA_DIR} 为空，请放入数据文件。")
        return

    for f in files:
        # 清洗文件名：提取数字部分，如 "159139.csv" -> "159139"
        filename = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, filename))[:6].zfill(6)
        
        try:
            res = analyze_signal(pd.read_csv(f))
            # 过滤 3 分及以上
            if res:
                info = db.get(code)
                if info:
                    res.update({'code': code, 'name': info['name'], 'index': info['index']})
                    results.append(res)
                else:
                    # 如果匹配不到，打印出来调试，方便你看是哪个代码漏了
                    print(f"⚠️ 无法匹配 Excel 信息: {code} (已排除)")
        except:
            continue

    # 排序：分值优先，回撤次之
    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V15.1\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 状态: `仅展示 3-4 分精英标的`\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 当前市场暂无 3 分及以上的高价值信号标的。")
    
    print(f"✨ 分析完成！结果已同步至 {REPORT_FILE}，已自动排除 3 分以下标的。")

if __name__ == "__main__":
    execute()
