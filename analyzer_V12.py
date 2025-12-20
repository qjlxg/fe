import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
# --- 核心配置 ---
TOTAL_CAPITAL = 100000       # 总资金
DATA_DIR = 'fund_data'       # 数据目录
REPORT_FILE = 'README.md'    # 输出报告
EXCEL_DB = 'ETF列表.xlsx'    # ETF数据库
# 策略参数（针对ETF优化）
MIN_SCORE_SHOW = 2           # 最低显示分数（原为3，适当降低以捕捉机会）
MA_SHORT = 5                 # 短期均线
MA_LONG = 10                 # 长期均线
VOL_MA = 5                   # 成交量均线
# --- 1. 辅助函数 ---
def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)
# --- 2. ETF数据库加载 ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB):
        print(f"❌ 找不到数据库: {EXCEL_DB}")
        return fund_db
    try:
        # 强制以字符串读取，避免代码变成浮点数
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 智能匹配列名（支持多种变体）
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '标的', '跟踪', '追踪', '行业'])), None)
        if not c_code or not c_name:
            print(f"❌ Excel 列名无法识别，当前列: {list(df.columns)}")
            return fund_db
        for _, row in df.iterrows():
            raw_code = str(row[c_code]).strip()
            clean_code = "".join(filter(str.isdigit, raw_code)).zfill(6)
            
            if clean_code and len(clean_code) == 6:
                fund_db[clean_code] = {
                    'name': str(row[c_name]).strip() if not pd.isna(row[c_name]) else "未知基金",
                    'index': str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "需手动补充指数"
                }
        print(f"✅ 匹配库加载完成，共 {len(fund_db)} 条记录")
    except Exception as e:
        print(f"❌ 解析 Excel 失败: {e}")
    return fund_db
# --- 3. ETF策略引擎（重构版） ---
def analyze_etf_signal(df):
    """
    针对ETF数据优化的策略：
    1. 趋势：收盘价 > MA5 且 MA5 > MA10 (多头排列)
    2. 量能：今日成交量 > 5日均量 (放量)
    3. 波动：振幅不过大（过滤异常）
    """
    if len(df) < 30: return None
    
    # 确保列名存在（直接使用CSV原始列名）
    required_cols = ['日期', '收盘', '成交量', '振幅']
    if not all(col in df.columns for col in required_cols):
        # 尝试兼容常见变体
        col_map = {}
        if '收盘' not in df.columns and '收盘价' in df.columns: col_map['收盘价'] = '收盘'
        if '成交量' not in df.columns and '成交额' in df.columns: col_map['成交额'] = '成交量' # 注意：这里假设CSV里的成交量是股数，如果是成交额需调整逻辑
        df.rename(columns=col_map, inplace=True)
        if not all(col in df.columns for col in required_cols):
            return None
    # 数据清洗
    df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
    df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
    df['振幅'] = pd.to_numeric(df['振幅'], errors='coerce')
    df.dropna(subset=['收盘', '成交量'], inplace=True)
    
    if len(df) < 30: return None
    # 计算指标
    last = df.iloc[-1]
    ma5 = df['收盘'].rolling(MA_SHORT).mean().iloc[-1]
    ma10 = df['收盘'].rolling(MA_LONG).mean().iloc[-1]
    vol_ma5 = df['成交量'].rolling(VOL_MA).mean().iloc[-1]
    
    # 评分逻辑
    score = 0
    
    # 1. 趋势分 (1分)
    if last['收盘'] > ma5 and ma5 > ma10:
        score += 1
        
    # 2. 量能分 (1分) - 放量上涨
    if last['成交量'] > vol_ma5:
        score += 1
        
    # 3. 强势分 (1分) - 创近期新高或接近新高 (替代原逻辑的回撤条件)
    # 这里改为：20日最高点回撤小于 2% (即非常接近20日高点)
    peak_20 = df['收盘'].rolling(20).max().iloc[-1]
    dd = (last['收盘'] - peak_20) / peak_20
    if dd > -0.02: # 距离20日高点回撤不超过2%
        score += 1
    # 如果满足最低分数
    if score >= MIN_SCORE_SHOW:
        # 资金管理与风控（针对ETF优化）
        # 假设止损为当前价的 1% (ETF波动小，止损设窄一点)
        # 或者固定金额止损
        risk_per_share = last['收盘'] * 0.01  # 每股风险1%
        
        # 单次最大风险资金 (总资金的 2%)
        max_risk_capital = TOTAL_CAPITAL * 0.02
        
        # 计算可买股数 (必须是100的倍数)
        if risk_per_share > 0:
            shares = int(max_risk_capital / risk_per_share)
            # 向下取整到100的倍数
            shares = (shares // 100) * 100
        else:
            shares = 0
            
        if shares < 100: shares = 100 # 最少买100股
        
        stop_price = last['收盘'] - risk_per_share
        
        return {
            'score': score, 
            'price': last['收盘'], 
            'stop': stop_price, 
            'shares': shares, 
            'dd': dd * 100, # 记录距离20日高点的幅度
            'vol_ratio': last['成交量'] / vol_ma5 if vol_ma5 > 0 else 1 # 量比
        }
    return None
# --- 4. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    # 检查数据目录
    if not os.path.exists(DATA_DIR):
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        return
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"❌ {DATA_DIR} 目录下没有找到CSV文件")
        return
    print(f"🔍 开始扫描 {len(files)} 个ETF数据文件...")
    
    for f in files:
        fname = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, fname)).zfill(6)
        
        try:
            # 读取CSV，指定分隔符为制表符或空格（根据您提供的数据格式）
            # 您的数据看起来是用Tab分隔的，pandas默认会自动处理
            df = pd.read_csv(f, sep='\s+') # \s+ 匹配空格或Tab
            
            # 检查列名并提示（仅第一次）
            # print(f"列名: {list(df.columns)}")
            
            res = analyze_etf_signal(df)
            if res:
                info = db.get(code)
                if info:
                    name_display = info['name']
                    index_display = info['index']
                else:
                    name_display = f"未匹配({code})"
                    index_display = "需检查Excel"
                res.update({
                    'code': code,
                    'name': name_display,
                    'index': index_display
                })
                results.append(res)
        except Exception as e:
            print(f"⚠️ 处理 {code} 失败: {e}")
            continue
    # 排序：得分高 -> 量比大 优先
    results.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)
    # 生成报告
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ ETF智能筛选看板 (V9-Fix)\\n\\n")
        f.write(f"**更新时间**: `{bj_now.strftime('%Y-%m-%d %H:%M')}`\\n")
        f.write(f"**筛选逻辑**: 趋势(MA5>MA10) + 放量 + 接近20日高点\\n")
        f.write(f"**资金策略**: 总资金 {TOTAL_CAPITAL/10000}w, 单票风控2%\\n\\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数 | 趋势得分 | 现价 | 建议买入 | 止损参考 | 备注 |\\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\\n")
            for s in results:
                # 生成评分图标
                icon = "🔥" * s['score']
                # 备注信息
                note = ""
                if s['dd'] > -1: note += "📈 接近高点 "
                if s['vol_ratio'] > 1.5: note += "⚡ 放量明显 "
                
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | {s['price']:.3f} | {s['shares']}份 | {s['stop']:.3f} | {note} |\\n")
        else:
            f.write("> 😴 当前市场暂无满足条件的ETF标的。\\n")
            f.write("> 提示：请检查CSV数据是否完整（至少30行），或调整策略参数。\\n")
    
    print(f"✨ 执行完毕！共筛选出 {len(results)} 个符合条件的标的。")
    print(f"📄 报告已生成至: {os.path.abspath(REPORT_FILE)}")
if __name__ == "__main__":
    execute()
