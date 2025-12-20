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
REPORT_FILE = 'README.md'    # 正式报告
DEBUG_FILE = 'DEBUG_REPORT.md' # 调试报告
EXCEL_DB = 'ETF列表.xlsx'    # ETF数据库
# 策略参数
MIN_SCORE_SHOW = 2           # 最低显示分数
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
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
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
# --- 3. ETF策略引擎（带调试版 - 修正版） ---
def analyze_etf_signal_debug(df, code, fund_db):
    """
    带调试信息的策略分析
    返回: (result_dict, debug_dict)
    """
    # 基础信息获取
    info = fund_db.get(code)
    name = info['name'] if info else f"未匹配({code})"
    
    # 错误检查
    if len(df) < 30: 
        return None, {
            'code': code, 'name': name, 'score': 0, 'price': 0,
            'reasons': [], 'fail_reasons': [f"数据不足(仅{len(df)}行)"],
            'raw_data': {}
        }
    
    # 确保列名存在
    required_cols = ['日期', '收盘', '成交量', '振幅']
    if not all(col in df.columns for col in required_cols):
        return None, {
            'code': code, 'name': name, 'score': 0, 'price': 0,
            'reasons': [], 'fail_reasons': [f"列名缺失: 需要{required_cols}"],
            'raw_data': {}
        }
    
    # 数据清洗
    df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
    df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
    df['振幅'] = pd.to_numeric(df['振幅'], errors='coerce')
    df.dropna(subset=['收盘', '成交量'], inplace=True)
    
    if len(df) < 30: 
        return None, {
            'code': code, 'name': name, 'score': 0, 'price': 0,
            'reasons': [], 'fail_reasons': ["清洗后数据不足30行"],
            'raw_data': {}
        }
        
    # 计算指标
    last = df.iloc[-1]
    ma5 = df['收盘'].rolling(MA_SHORT).mean().iloc[-1]
    ma10 = df['收盘'].rolling(MA_LONG).mean().iloc[-1]
    vol_ma5 = df['成交量'].rolling(VOL_MA).mean().iloc[-1]
    peak_20 = df['收盘'].rolling(20).max().iloc[-1]
    
    # 详细指标
    price = last['收盘']
    vol = last['成交量']
    dd = (price - peak_20) / peak_20 if peak_20 != 0 else 0
    
    # 评分逻辑分解
    score = 0
    reasons = []
    fail_reasons = []
    
    # 条件1: 趋势 (1分)
    cond1 = (price > ma5) and (ma5 > ma10)
    if cond1:
        score += 1
        reasons.append(f"✅ 趋势多头: 价格{price:.3f} > MA5{ma5:.3f} > MA10{ma10:.3f}")
    else:
        fail_reasons.append(f"❌ 趋势不符: 价格{price:.3f}, MA5{ma5:.3f}, MA10{ma10:.3f}")
        
    # 条件2: 量能 (1分)
    cond2 = vol > vol_ma5
    if cond2:
        score += 1
        reasons.append(f"✅ 放量: 成交量{vol:.0f} > 均量{vol_ma5:.0f}")
    else:
        fail_reasons.append(f"❌ 缩量: 成交量{vol:.0f} <= 均量{vol_ma5:.0f}")
        
    # 条件3: 强势 (1分)
    cond3 = dd > -0.02
    if cond3:
        score += 1
        reasons.append(f"✅ 接近高点: 回撤{dd*100:.2f}% > -2%")
    else:
        fail_reasons.append(f"❌ 回撤过大: 回撤{dd*100:.2f}% <= -2%")
    
    # 组装调试信息
    debug_info = {
        'code': code,
        'name': name,
        'score': score,
        'price': price,
        'reasons': reasons,
        'fail_reasons': fail_reasons,
        'raw_data': {
            'price': price, 'ma5': ma5, 'ma10': ma10, 
            'vol': vol, 'vol_ma5': vol_ma5, 'dd': dd*100
        }
    }
    
    if score >= MIN_SCORE_SHOW:
        # 计算买入股数
        risk_per_share = price * 0.01
        max_risk_capital = TOTAL_CAPITAL * 0.02
        shares = int(max_risk_capital / risk_per_share)
        shares = (shares // 100) * 100
        if shares < 100: shares = 100
        stop_price = price - risk_per_share
        
        return {
            'code': code,
            'name': name,
            'index': info['index'] if info else "未知",
            'score': score,
            'price': price,
            'shares': shares,
            'stop': stop_price,
            'dd': dd * 100,
            'vol_ratio': vol / vol_ma5 if vol_ma5 > 0 else 1
        }, debug_info
    else:
        return None, debug_info
# --- 4. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    debug_logs = []
    
    if not os.path.exists(DATA_DIR):
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        return
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"❌ {DATA_DIR} 目录下没有找到CSV文件")
        return
    
    print(f"🔍 开始扫描 {len(files)} 个ETF数据文件 (调试模式)...")
    
    for f in files:
        fname = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, fname)).zfill(6)
        
        try:
            df = pd.read_csv(f, sep='\s+')
            # 调用带调试的分析函数
            res, debug_info = analyze_etf_signal_debug(df, code, db)
            
            if res:
                results.append(res)
            
            # 记录所有标的的调试信息（只记录前20个，避免日志太长）
            if len(debug_logs) < 20:
                debug_logs.append(debug_info)
                
        except Exception as e:
            print(f"⚠️ 处理 {code} 失败: {e}")
            continue
            
    # 排序
    results.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)
    
    # 1. 生成正式报告
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ ETF智能筛选看板 (V9-Debug)\\n\\n")
        f.write(f"**更新时间**: `{bj_now.strftime('%Y-%m-%d %H:%M')}`\\n")
        f.write(f"**筛选结果**: 共 {len(results)} 个标的\\n\\n")
        if results:
            f.write("| 代码 | 简称 | 趋势得分 | 现价 | 建议买入 | 止损参考 |\\n")
            f.write("| --- | --- | --- | --- | --- | --- |\\n")
            for s in results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | {icon} | {s['price']:.3f} | {s['shares']}份 | {s['stop']:.3f} |\\n")
        else:
            f.write("> 😴 暂无符合条件的标的。\\n")
            
    # 2. 生成调试报告
    with open(DEBUG_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🐛 调试分析报告\\n\\n")
        f.write(f"**生成时间**: `{bj_now.strftime('%Y-%m-%d %H:%M')}`\\n")
        f.write(f"**样本数量**: 前 {len(debug_logs)} 个标的详情\\n\\n")
        
        for item in debug_logs:
            f.write(f"## {item['code']} - {item['name']}\\n")
            f.write(f"**最终得分**: {item['score']}/{MIN_SCORE_SHOW} \\n")
            
            if 'price' in item and item['price'] > 0:
                f.write(f"**当前价格**: {item['price']:.3f}\\n\\n")
            else:
                f.write(f"**当前价格**: 无数据\\n\\n")
                
            if item['reasons']:
                f.write("**✅ 通过条件:**  \\n")
                for r in item['reasons']:
                    f.write(f"- {r}  \\n")
            else:
                f.write("**✅ 通过条件:** 无\\n")
                
            if item['fail_reasons']:
                f.write("**❌ 未通过条件:**  \\n")
                for r in item['fail_reasons']:
                    f.write(f"- {r}  \\n")
            else:
                f.write("**❌ 未通过条件:** 无\\n")
                
            if 'raw_data' in item and item['raw_data']:
                rd = item['raw_data']
                f.write(f"**📊 原始数据:** MA5={rd.get('ma5', 0):.3f}, MA10={rd.get('ma10', 0):.3f}, Vol={rd.get('vol', 0):.0f}, VolMA={rd.get('vol_ma5', 0):.0f}, DD={rd.get('dd', 0):.2f}%\\n")
            f.write("---\\n\\n")
            
    print(f"✨ 执行完毕！")
    print(f"📄 正式报告: {os.path.abspath(REPORT_FILE)}")
    print(f"🐛 调试报告: {os.path.abspath(DEBUG_FILE)}")
    print(f"💡 请查看 DEBUG_REPORT.md 分析评分细节！")
if __name__ == "__main__":
    execute()
