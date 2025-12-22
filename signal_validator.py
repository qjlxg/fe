import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- 配置文件路径 (请确保这些文件在同一目录下) ---
HISTORY_FILE = 'signal_history.csv'        
DATA_DIR = 'fund_data'                    
REPORT_FILE = 'VALIDATION_REPORT.md'       
BACKTEST_REPORT = 'backtest_results.csv' # 匹配你最新的回测报告
NAME_LIST_FILE = 'ETF列表.xlsx' # 匹配你的映射表

def get_beijing_time():
    """获取北京时间"""
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    print(f"🔍 开始信号效能校验... {get_beijing_time()}")

    # 1. 基础文件检查
    if not os.path.exists(HISTORY_FILE):
        print(f"⚠️ 找不到账本文件 {HISTORY_FILE}，请先运行分析脚本。")
        return

    # 2. 加载名称映射表 (核心逻辑：强制将 ETF_代码 替换为中文)
    name_map = {}
    if os.path.exists(NAME_LIST_FILE):
        try:
            df_names = pd.read_csv(NAME_LIST_FILE, dtype={'证券代码': str})
            # 去除可能存在的空格并建立映射字典
            name_map = dict(zip(df_names['证券代码'].str.strip(), df_names['证券简称'].str.strip()))
            print(f"✅ 成功加载名称映射，共 {len(name_map)} 条记录")
        except Exception as e:
            print(f"⚠️ 加载名称映射表失败: {e}")

    # 3. 加载精英池 (识别身份)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
            print(f"✅ 成功加载精英池: {elite_pool}")
        except:
            print("⚠️ 读取回测报告失败。")

    # 4. 读取账本并处理
    try:
        # 使用 low_memory=False 兼容新旧不同列数的 CSV 格式
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str}, low_memory=False)
    except Exception as e:
        print(f"❌ 读取账本失败: {e}")
        return
        
    if df_h.empty:
        print("⚠️ 账本为空，暂无信号需要校验。")
        return

    results = []
    print(f"📈 正在分析 {len(df_h)} 条信号的盈亏表现...")

    for _, row in df_h.iterrows():
        try:
            # 基础信息清洗
            code = str(row['code']).strip().zfill(6)
            signal_date = str(row['date']).strip()
            
            # 强制更名逻辑：如果映射表里有，就绝对不用账本里的 ETF_xxxx
            real_name = name_map.get(code, row.get('name', f"代码_{code}"))
            is_elite = code in elite_pool
            identity_tag = "🏆精英" if is_elite else "⚪普通"
            display_name = f"🏆{real_name}" if is_elite else real_name

            # 匹配本地 K 线数据
            file_path = os.path.join(DATA_DIR, f"{code}.csv")
            if not os.path.exists(file_path):
                continue
            
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            df_d['日期_dt'] = pd.to_datetime(df_d['日期'])
            df_d = df_d.sort_values('日期_dt').reset_index(drop=True)
            
            # 确定入场价与止损价 (新账本格式中这两项已填入数值)
            entry_p = float(row.get('entry_price', row.get('price', 0)))
            stop_p = float(row.get('stop', 0))
            
            # 健壮性：如果止损价缺失，默认给个 -7%
            if stop_p == 0:
                stop_p = entry_p * 0.93

            # 筛选信号发出后的数据
            df_after = df_d[df_d['日期_dt'] > pd.to_datetime(signal_date)]
            
            if df_after.empty:
                status, last_p, curr_ret = "⏳ 观察中", entry_p, 0.0
            else:
                last_row = df_after.iloc[-1]
                last_p = last_row['收盘']
                
                # 穿透性测试：期间最低价是否跌破过止损位
                lowest_after = df_after['最低'].min()
                
                if lowest_after <= stop_p:
                    status = "❌ 已止损"
                    last_p = stop_p  # 以止损价作为结算价
                elif last_p > entry_p:
                    status = "✅ 盈利中"
                else:
                    status = "📉 被套中"
                
                curr_ret = (last_p - entry_p) / entry_p * 100 if entry_p != 0 else 0

            results.append({
                '身份': identity_tag,
                '信号日期': signal_date,
                '代码': code,
                '名称': display_name,
                '入场价': round(entry_p, 3),
                '止损价': round(stop_p, 3),
                '现价/结算': round(last_p, 3),
                '收益%': round(curr_ret, 2),
                '状态': status
            })
        except Exception as e:
            continue

    if not results: return

    # 5. 统计与报告生成
    df_res = pd.DataFrame(results)
    total_cnt = len(df_res)
    win_cnt = len(df_res[df_res['状态'] == '✅ 盈利中'])
    stop_cnt = len(df_res[df_res['状态'] == '❌ 已止损'])
    win_rate = (win_cnt / total_cnt * 100) if total_cnt > 0 else 0

    # 按照身份(精英优先)和日期(倒序)排序
    df_sorted = df_res.sort_values(['身份', '信号日期'], ascending=[False, False])

    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告 (Elite-V12)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        f.write(f"### 📊 总体战绩统计\n")
        f.write(f"| 累计信号 | 盈利中 | 已止损 | 胜率 (含观察) |\n")
        f.write(f"| --- | --- | --- | --- |\n")
        f.write(f"| {total_cnt} | {win_cnt} | {stop_cnt} | **{win_rate:.2f}%** |\n\n")
        
        f.write("### 📝 详细信号追踪\n")
        f.write("| 身份 | 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价/结算 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        for _, r in df_sorted.iterrows():
            f.write(f"| {r['身份']} | {r['信号日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价/结算']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验报告已更新至: {REPORT_FILE}")

if __name__ == "__main__":
    validate()
