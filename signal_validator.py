import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
HISTORY_FILE = 'signal_history.csv'
DATA_DIR = 'fund_data'
REPORT_FILE = 'VALIDATION_REPORT.md'
BACKTEST_REPORT = 'backtest_results.csv'

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    if not os.path.exists(HISTORY_FILE):
        print(f"⚠️ 找不到账本文件 {HISTORY_FILE}，跳过校验。")
        return

    # 1. 加载精英池名单 (用于对比统计)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
        except:
            print("⚠️ 读取回测报告失败，将无法标记精英标的。")

    # 2. 读取历史账本
    try:
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    except Exception as e:
        print(f"❌ 读取账本失败: {e}")
        return
        
    if df_h.empty:
        print("⚠️ 账本为空，暂无信号需要校验。")
        return

    results = []
    # 3. 逐条校验信号表现
    for _, row in df_h.iterrows():
        code = str(row['code']).zfill(6)
        signal_date = str(row['date'])
        
        # 统一取值逻辑 (处理不同版本列名差异)
        entry_p = float(row.get('entry_price') if pd.notna(row.get('entry_price')) else row.get('price'))
        stop_p = float(row.get('stop'))
        
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path):
            continue
        
        try:
            # 读取个股数据并纠正排序
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            df_d['日期'] = pd.to_datetime(df_d['日期'])
            df_d = df_d.sort_values('日期').reset_index(drop=True)
            
            # 筛选出信号发出日期之后的数据
            df_after = df_d[df_d['日期'] > signal_date]
            
            if df_after.empty:
                status = "⏳ 观察中"
                last_p = entry_p
                curr_ret = 0.0
            else:
                last_row = df_after.iloc[-1]
                last_p = last_row['收盘']
                lowest_after = df_after['最低'].min()
                
                # 状态判定
                if lowest_after <= stop_p:
                    status = "❌ 已止损"
                    # 收益率计算以止损价为准，模拟真实亏损
                    curr_ret = (stop_p - entry_p) / entry_p * 100
                elif last_p > entry_p:
                    status = "✅ 盈利中"
                    curr_ret = (last_p - entry_p) / entry_p * 100
                else:
                    status = "📉 被套中"
                    curr_ret = (last_p - entry_p) / entry_p * 100

            # 身份标记
            is_elite = "🏆精英" if code in elite_pool else "⚪普通"

            results.append({
                '身份': is_elite,
                '信号日期': signal_date,
                '代码': code,
                '名称': row.get('name', 'ETF标的'),
                '入场价': round(entry_p, 3),
                '止损价': round(stop_p, 3),
                '现价/止损价': round(last_p if status != "❌ 已止损" else stop_p, 3),
                '收益%': round(curr_ret, 2),
                '状态': status
            })
        except Exception as e:
            print(f"⚠️ 校验 {code} 出错: {e}")
            continue

    if not results: return

    df_res = pd.DataFrame(results)
    
    # 4. 统计模块 (完整计算)
    total_count = len(df_res)
    win_count = len(df_res[df_res['状态'] == '✅ 盈利中'])
    stop_count = len(df_res[df_res['状态'] == '❌ 已止损'])
    
    # 精英池专项统计
    elite_df = df_res[df_res['身份'] == "🏆精英"]
    elite_total = len(elite_df)
    elite_wins = len(elite_df[elite_df['状态'] == '✅ 盈利中']) if elite_total > 0 else 0

    # 5. 生成报告文件
    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告 (Elite完整版)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        f.write(f"### 📊 总体战绩统计\n")
        f.write(f"- 累计发出信号: `{total_count}`\n")
        f.write(f"- 当前盈利中: `{win_count}`\n")
        f.write(f"- 触发止损: `{stop_count}`\n")
        f.write(f"- **综合胜率**: `{(win_count/total_count*100):.2f}%` (含观察中)\n\n")
        
        if elite_total > 0:
            f.write(f"### 🏆 精英池表现 (回测前10)\n")
            f.write(f"- 精英池信号数: `{elite_total}`\n")
            f.write(f"- 精英池胜率: `{(elite_wins/elite_total*100):.2f}%`\n\n")
        
        f.write("### 📝 详细信号追踪\n")
        f.write("| 身份 | 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价/止损 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        # 排序展示：精英优先，日期倒序
        df_sorted = df_res.sort_values(['身份', '信号日期'], ascending=[False, False])
        for _, r in df_sorted.iterrows():
            f.write(f"| {r['身份']} | {r['信号日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价/止损价']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验报告已更新至 {REPORT_FILE}")

if __name__ == "__main__":
    validate()
