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
# 策略参数
MIN_SCORE_SHOW = 2           # 最低显示分数
MA_SHORT = 5                 # 短期均线
MA_LONG = 10                 # 长期均线
VOL_MA = 5                   # 成交量均线
# --- 1. 辅助函数 ---
def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)
def normalize_column_name(col):
    """统一列名格式：去除空格、转英文、小写"""
    col = str(col).strip().lower()
    # 中文到英文的映射
    col_map = {
        '日期': 'date', 'date': 'date',
        '收盘': 'close', '收盘价': 'close', 'close': 'close',
        '成交量': 'volume', '成交额': 'volume', 'volume': 'volume',
        '振幅': 'amplitude', 'amplitude': 'amplitude',
        '涨跌幅': 'change_pct', '涨跌额': 'change_amount', '换手率': 'turnover'
    }
    return col_map.get(col, col)
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
# --- 3. 智能列名匹配 ---
def match_columns(df):
    """智能匹配CSV文件的列名"""
    # 标准化所有列名
    normalized_cols = [normalize_column_name(col) for col in df.columns]
    
    # 创建映射字典
    col_mapping = {}
    required = ['date', 'close', 'volume', 'amplitude']
    
    for req in required:
        # 寻找匹配的列
        matched_idx = None
        for i, col in enumerate(normalized_cols):
            if req in col:  # 模糊匹配
                matched_idx = i
                break
        
        if matched_idx is not None:
            col_mapping[df.columns[matched_idx]] = req
        else:
            print(f"⚠️ 未找到匹配列: {req}")
            return None
            
    return col_mapping
# --- 4. ETF策略引擎（智能列名版） ---
def analyze_etf_signal(df, code, fund_db):
    """带智能列名匹配的策略分析"""
    info = fund_db.get(code)
    name = info['name'] if info else f"未匹配({code})"
    
    # 1. 智能匹配列名
    col_mapping = match_columns(df)
    if col_mapping is None:
        return {
            'code': code,
            'name': name,
            'success': False,
            'reason': '列名匹配失败',
            'original_cols': list(df.columns)
        }
    
    try:
        # 2. 重命名列
        df_clean = df.rename(columns=col_mapping)
        
        # 3. 数据检查
        if len(df_clean) < 30: 
            return {
                'code': code, 'name': name, 'success': False,
                'reason': f'数据不足(仅{len(df_clean)}行)'
            }
            
        # 4. 数据清洗
        df_clean['close'] = pd.to_numeric(df_clean['close'], errors='coerce')
        df_clean['volume'] = pd.to_numeric(df_clean['volume'], errors='coerce')
        df_clean['amplitude'] = pd.to_numeric(df_clean['amplitude'], errors='coerce')
        df_clean.dropna(subset=['close', 'volume'], inplace=True)
        
        if len(df_clean) < 30: 
            return {
                'code': code, 'name': name, 'success': False,
                'reason': '清洗后数据不足30行'
            }
            
        # 5. 计算指标
        last = df_clean.iloc[-1]
        ma5 = df_clean['close'].rolling(MA_SHORT).mean().iloc[-1]
        ma10 = df_clean['close'].rolling(MA_LONG).mean().iloc[-1]
        vol_ma5 = df_clean['volume'].rolling(VOL_MA).mean().iloc[-1]
        peak_20 = df_clean['close'].rolling(20).max().iloc[-1]
        
        price = last['close']
        vol = last['volume']
        dd = (price - peak_20) / peak_20 if peak_20 != 0 else 0
        
        # 6. 评分逻辑
        score = 0
        reasons = []
        fail_reasons = []
        
        # 条件1: 趋势 (1分)
        cond1 = (price > ma5) and (ma5 > ma10)
        if cond1:
            score += 1
            reasons.append(f"趋势多头")
        else:
            fail_reasons.append(f"趋势不符")
            
        # 条件2: 量能 (1分)
        cond2 = vol > vol_ma5
        if cond2:
            score += 1
            reasons.append(f"放量上涨")
        else:
            fail_reasons.append(f"缩量")
            
        # 条件3: 强势 (1分)
        cond3 = dd > -0.02
        if cond3:
            score += 1
            reasons.append(f"接近高点")
        else:
            fail_reasons.append(f"回撤过大")
        
        # 7. 返回结果
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
                'vol_ratio': vol / vol_ma5 if vol_ma5 > 0 else 1,
                'success': True,
                'reasons': reasons
            }
        else:
            return {
                'code': code,
                'name': name,
                'score': score,
                'price': price,
                'success': False,
                'reason': '评分不足',
                'reasons': reasons,
                'fail_reasons': fail_reasons
            }
            
    except Exception as e:
        return {
            'code': code,
            'name': name,
            'success': False,
            'reason': f'分析异常: {str(e)}'
        }
# --- 5. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    errors = []
    
    if not os.path.exists(DATA_DIR):
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        return
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"❌ {DATA_DIR} 目录下没有找到CSV文件")
        return
    
    print(f"🔍 开始扫描 {len(files)} 个ETF数据文件 (智能列名匹配)...")
    
    # 先检查第一个文件的列名作为示例
    if files:
        try:
            sample_df = pd.read_csv(files[0], sep='\s+', nrows=1)
            print(f"📄 示例文件列名: {list(sample_df.columns)}")
        except:
            pass
    
    for i, f in enumerate(files):
        fname = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, fname)).zfill(6)
        
        try:
            # 尝试多种分隔符
            try:
                df = pd.read_csv(f, sep='\s+')  # 空格或Tab
            except:
                try:
                    df = pd.read_csv(f, sep=',')  # 逗号
                except:
                    df = pd.read_csv(f, sep=';')  # 分号
            
            res = analyze_etf_signal(df, code, db)
            
            if res['success']:
                results.append(res)
            else:
                # 只记录前10个错误
                if len(errors) < 10:
                    errors.append(res)
                
        except Exception as e:
            errors.append({
                'code': code,
                'name': f"未匹配({code})",
                'success': False,
                'reason': f'读取失败: {str(e)}'
            })
            continue
            
        # 进度提示
        if (i + 1) % 100 == 0:
            print(f"⏳ 已处理 {i + 1}/{len(files)} 个文件...")
    
    # 排序
    results.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)
    
    # 生成报告
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ ETF智能筛选看板 (V9-Smart)\\n\\n")
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
            
        if errors:
            f.write("\\n\\n## ⚠️ 部分文件处理失败示例\\n")
            for e in errors[:5]:
                f.write(f"- **{e['code']}**: {e['reason']}\\n")
    
    print(f"✨ 执行完毕！共筛选出 {len(results)} 个标的。")
    if len(errors) > 0:
        print(f"⚠️ 有 {len(errors)} 个文件处理失败，请查看报告了解详情。")
    print(f"📄 报告已生成至: {os.path.abspath(REPORT_FILE)}")
if __name__ == "__main__":
    execute()
