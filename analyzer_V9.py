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
MIN_SCORE_SHOW = 2  

def get_beijing_time():
    # 修正：使用 timezone 处理，避免旧版 datetime.utcnow() 警告
    return datetime.now(timedelta(hours=8))

# --- 1. 修复后的数据清洗引擎 ---
def load_fund_db():
    fund_db = {}
    # 你的目标文件名（根据你上传的文件名修改）
    target_file = 'ETF列表.xlsx - Sheet1.csv'
    
    if not os.path.exists(target_file):
        print(f"警告：未找到匹配文件 {target_file}，请检查文件名！")
        return fund_db

    try:
        # 彻底解决 BOM 和 特殊字符
        with open(target_file, 'r', encoding='utf-8-sig') as f:
            content = f.read().replace('\r\n', '\n').replace('\r', '\n')
        
        df = pd.read_csv(io.StringIO(content), dtype=str)
        # 清理列名中的空格和不可见字符
        df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]
        
        # 核心匹配逻辑：对应你提供的“证券代码”和“证券简称”
        c_code = '证券代码'
        c_name = '证券简称'
        
        # 指数列在你的新 CSV 中没有，这里做个兼容处理
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '标的'])), None)

        if c_code in df.columns and c_name in df.columns:
            for _, row in df.iterrows():
                # 提取代码并补全 6 位
                raw_code = str(row[c_code]).strip().split('.')[0]
                if not raw_code.isdigit(): continue # 过滤非数字行
                
                code = raw_code.zfill(6)
                name = str(row[c_name]).strip()
                
                # 如果有指数列则读取，没有则标记为 "-"
                idx = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "-"
                
                fund_db[code] = {'name': name, 'index': idx}
            print(f"✅ 成功加载 {len(fund_db)} 条基金数据")
        else:
            print(f"❌ 错误：CSV 文件缺失关键列 '证券代码' 或 '证券简称'")
            
    except Exception as e:
        print(f"解析 {target_file} 失败: {e}")
    
    return fund_db

# --- 2. 增强策略 (保持不变) ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    if 'close' not in df.columns or len(df) < 20: return None
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / (peak_20 if peak_20 != 0 else 1)
    
    score = 0
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    if score >= MIN_SCORE_SHOW:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.96 
        shares = int(risk / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {'score': score, 'price': last['close'], 'stop': stop_p, 'shares': shares, 'dd': dd * 100}
    return None

# --- 3. 执行流程 (保持不变) ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    # 获取 data 目录下所有数据
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        # 获取文件名作为代码
        code = os.path.splitext(os.path.basename(f))[0].zfill(6)
        try:
            # 读取个股数据并分析
            df_data = pd.read_csv(f)
            res = analyze_signal(df_data)
            if res:
                # 关键：从 db 中根据代码获取对应的简称
                info = db.get(code, {'name': '未知标的', 'index': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                results.append(res)
        except Exception as e:
            continue

    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V14.0\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 适配状态: `CSV名称库对齐`\n\n")
        f.write("### 🎯 实时信号追踪 (2分潜力 / 3分及以上精英)\n")
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score'] if s['score'] >= 3 else "⭐"
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 市场处于横盘震荡，无符合逻辑的超跌信号。")
    print(f"看板生成完毕：{REPORT_FILE}")

if __name__ == "__main__":
    execute()
