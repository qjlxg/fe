import pandas as pd
import os
from datetime import datetime, timedelta

# --- 路径配置 ---
HISTORY_FILE = 'signal_history.csv'        # 历史账本
DATA_DIR = 'fund_data'                    # 本地数据文件夹
REPORT_FILE = 'VALIDATION_REPORT.md'       # 生成的报告名称
BACKTEST_REPORT = 'backtest_results.csv'   # 回测精英名单
NAME_LIST_FILE = 'ETF列表.xlsx - Sheet1.csv' # 名称映射表

def get_beijing_time():
    """获取北京时间"""
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    # 1. 基础文件检查
    if not os.path.exists(HISTORY_FILE):
        print(f"⚠️ 找不到账本文件 {HISTORY_FILE}，请先运行分析脚本。")
        return

    # 2. 加载名称映射表 (从你的 CSV 映射)
    name_map = {}
    if os.path.exists(NAME_LIST_FILE):
        try:
            # 证券代码, 证券简称
            df_names = pd.read_csv(NAME_LIST_FILE, dtype={'证券代码': str})
            name_map = dict(zip(df_names['证券代码'], df_names['证券简称']))
            print(f"✅ 成功加载名称映射，共 {len(name_map)} 条记录。")
        except Exception as e:
            print(f"⚠️ 加载名称映射表失败: {e}")

    # 3. 加载精英池 (回测前10)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
            print(f"✅ 成功加载精英池: {elite_pool}")
        except:
            print("⚠️ 读取回测报告失败。")

    # 4. 读取账本并开始校验
    try:
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    except Exception as e:
        print(f"❌ 读取账本失败: {e}")
        return
        
    if df_h.empty:
        print("⚠️ 账本为空，暂无信号需要校验。")
        return

    results = []
    for _, row in df_h.iterrows():
        # 获取基础信息
        code = str(row['code']).zfill(6)
        signal_date = str(row['date'])
        # 优先从映射表获取名称
        real_name = name_map.get(code, row.get('name', '未知ETF'))
        
        # 判定身份：如果是前10名，打上精英标
        is_elite = code in elite_pool
        identity_tag = "🏆精英" if is_elite else "⚪普通"
        # 展示名称：精英标的在名字前加 🏆
        display_name = f"🏆{real_name}" if is_elite else real_name

        # 读取本地数据文件进行盈亏计算
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path):
            continue
        
        try:
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            df_d['日期'] = pd.to_datetime(df_d['日期'])
            df_d = df_d.sort_values('日期').reset_index(drop=True)
            
            # 筛选信号发出后的数据
            df_after = df_d[df_d['日期'] > signal_date]
            
            # 入场价与止损价
            entry_p = float(row.get('entry_price', row.get('price')))
            stop_p = float(row.get('stop'))

            if df_after.empty:
                status = "⏳ 观察中"
                last_p = entry_p
                curr_ret = 0.0
            else:
                last_row = df_after.iloc[-1]
                last_p = last_row['收盘']
                lowest_after = df_after['最低'].min()
                
                if lowest_after <= stop_p:
                    status = "❌ 已止损"
                    last_p = stop_p  # 止损价作为结算价
                    curr_ret = (stop_p - entry_p) / entry_p * 100
                elif last_p > entry_p:
                    status = "✅ 盈利中"
                    curr_ret = (last_p - entry_p) / entry_p * 100
                else:
                    status = "📉 被套中"
                    curr_ret = (last_p - entry_p) / entry_p * 100

            results.append({
                '身份': identity_tag,
                '信号日期': signal_date,
                '代码': code,
                '名称': display_name,
                '入场价': round(entry_p, 3),
                '止损价': round(stop_p, 3),
                '现价/止损价': round(last_p, 3),
                '收益%': round(curr_ret, 2),
                '状态': status
            })
        except Exception as e:
            print(f"⚠️ 校验 {code} 时出错: {e}")
            continue

    if not results: return

    # 5. 生成报告统计项
    df_res = pd.DataFrame(results)
    total_count = len(df_res)
    win_count = len(df_res[df_res['状态'] == '✅ 盈利中'])
    stop_count = len(df_res[df_res['状态'] == '❌ 已止损'])
    
    # 精英池子集统计
    elite_df = df_res[df_res['身份'] == "🏆精英"]
    elite_total = len(elite_df)
    elite_wins = len(elite_df[elite_df['状态'] == '✅ 盈利中']) if elite_total > 0 else 0

    # 6. 写入 MD 报告
    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告 (满血映射版)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        f.write(f"### 📊 总体战绩统计\n")
        f.write(f"- 累计发出信号: `{total_count}`\n")
        f.write(f"- 当前盈利中: `{win_count}`\n")
        f.write(f"- 触发止损: `{stop_count}`\n")
        f.write(f"- **综合胜率**: `{(win_count/total_count*100):.2f}%` (含观察中)\n\n")
        
        if elite_total > 0:
            f.write(f"### 🏆 精英池表现追踪\n")
            f.write(f"- 精英池信号数: `{elite_total}`\n")
            f.write(f"- 精英池胜率: `{(elite_wins/elite_total*100):.2f}%`\n\n")
        
        f.write("### 📝 详细信号列表\n")
        f.write("| 身份 | 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价/结算 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        # 排序展示：精英优先，日期倒序
        df_sorted = df_res.sort_values(['身份', '信号日期'], ascending=[False, False])
        for _, r in df_sorted.iterrows():
            f.write(f"| {r['身份']} | {r['信号日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价/止损价']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验报告已更新至 {REPORT_FILE}")

if __name__ == "__main__":
    validate()
