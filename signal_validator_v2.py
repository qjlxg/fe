import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- 配置文件路径 (根据你的要求已修改) ---
HISTORY_FILE = 'signal_history.csv'        
DATA_DIR = 'fund_data'                    
REPORT_FILE = 'VALIDATION_REPORT.md'       
BACKTEST_REPORT = 'backtest_results.csv'   # 已按要求修改
NAME_LIST_FILE = 'ETF列表.xlsx'           # 已按要求修改为直接读取 Excel

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    print(f"🔍 正在启动信号效能校验系统... {get_beijing_time()}")

    # 1. 基础文件检查
    if not os.path.exists(HISTORY_FILE):
        print(f"⚠️ 找不到账本文件 {HISTORY_FILE}")
        return

    # 2. 加载名称映射表 (从 Excel 直接读取，规避 CSV 乱码问题)
    name_map = {}
    if os.path.exists(NAME_LIST_FILE):
        try:
            # 注意：如果运行环境报错缺少 openpyxl，请在终端执行 pip install openpyxl
            df_names = pd.read_excel(NAME_LIST_FILE, dtype={'证券代码': str})
            name_map = dict(zip(df_names['证券代码'].str.strip(), df_names['证券简称'].str.strip()))
            print(f"✅ 成功从 Excel 加载名称映射: {len(name_map)} 条记录")
        except Exception as e:
            print(f"⚠️ 加载 Excel 名称表失败 (尝试读取 CSV 备用): {e}")
            # 备用逻辑：如果 Excel 读不了，尝试读你之前的 CSV
            alt_csv = NAME_LIST_FILE + ".csv"
            if os.path.exists(alt_csv):
                for enc in ['gbk', 'utf-8-sig']:
                    try:
                        df_names = pd.read_csv(alt_csv, dtype={'证券代码': str}, encoding=enc)
                        name_map = dict(zip(df_names['证券代码'].str.strip(), df_names['证券简称'].str.strip()))
                        break
                    except: continue

    # 3. 加载精英池 (回测前10)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        for enc in ['utf-8', 'gbk', 'utf-8-sig']:
            try:
                df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str}, encoding=enc)
                elite_pool = df_bt['代码'].head(10).tolist()
                print(f"✅ 成功加载精英池 (编码:{enc}): {elite_pool}")
                break
            except: continue

    # 4. 读取账本
    try:
        # low_memory=False 用于处理列数不一致的情况
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str}, low_memory=False)
    except Exception as e:
        print(f"❌ 读取账本失败: {e}")
        return
        
    results = []
    print(f"📈 正在分析 {len(df_h)} 条信号的盈亏表现...")

    for _, row in df_h.iterrows():
        try:
            code = str(row['code']).strip().zfill(6)
            signal_date = str(row['date']).strip()
            
            # 名称翻译：优先用 Excel 里的中文，没有则用账本里的
            real_name = name_map.get(code, row.get('name', f"ETF_{code}"))
            is_elite = code in elite_pool
            identity_tag = "🏆精英" if is_elite else "⚪普通"
            display_name = f"🏆{real_name}" if is_elite else real_name

            # 获取 K 线数据计算
            file_path = os.path.join(DATA_DIR, f"{code}.csv")
            if not os.path.exists(file_path): continue
            
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            df_d['日期_dt'] = pd.to_datetime(df_d['日期'])
            df_d = df_d.sort_values('日期_dt').reset_index(drop=True)
            
            # 价格提取 (适配新账本 13 列)
            entry_p = float(row.get('entry_price', row.get('price', 0)))
            stop_p = float(row.get('stop', 0))
            if stop_p == 0: stop_p = entry_p * 0.93 # 容错止损

            # 计算信号日之后的表现
            df_after = df_d[df_d['日期_dt'] > pd.to_datetime(signal_date)]
            
            if df_after.empty:
                status, last_p, curr_ret = "⏳ 观察中", entry_p, 0.0
            else:
                last_row = df_after.iloc[-1]
                last_p = last_row['收盘']
                lowest_since = df_after['最低'].min()
                
                if lowest_since <= stop_p:
                    status, last_p = "❌ 已止损", stop_p
                elif last_p > entry_p:
                    status = "✅ 盈利中"
                else:
                    status = "📉 被套中"
                
                curr_ret = (last_p - entry_p) / entry_p * 100 if entry_p != 0 else 0

            results.append({
                '身份': identity_tag, '信号日期': signal_date, '代码': code, '名称': display_name,
                '入场价': round(entry_p, 3), '止损价': round(stop_p, 3),
                '现价/结算': round(last_p, 3), '收益%': round(curr_ret, 2), '状态': status
            })
        except: continue

    # 5. 生成报告
    if not results: return
    df_res = pd.DataFrame(results)
    df_sorted = df_res.sort_values(['身份', '信号日期'], ascending=[False, False])

    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告 (Elite-V12)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        # 核心数据统计
        total = len(df_res)
        wins = len(df_res[df_res['状态'] == '✅ 盈利中'])
        f.write(f"### 📊 总体战绩统计\n- 累计信号: `{total}` | 盈利中: `{wins}` | 胜率: `{(wins/total*100):.2f}%` (含观察)\n\n")
        
        f.write("### 📝 详细信号列表\n")
        f.write("| 身份 | 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价/结算 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for _, r in df_sorted.iterrows():
            f.write(f"| {r['身份']} | {r['信号日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价/结算']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 报告生成成功: {REPORT_FILE}")

if __name__ == "__main__":
    validate()
