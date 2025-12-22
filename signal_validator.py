import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
HISTORY_FILE = 'signal_history.csv'
DATA_DIR = 'fund_data'
REPORT_FILE = 'VALIDATION_REPORT.md'
BACKTEST_REPORT = 'backtest_results.csv' # 用于识别精英标的

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    if not os.path.exists(HISTORY_FILE):
        print("❌ 找不到历史信号文件")
        return

    # 加载精选池名单（前10名）
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
        except: pass

    try:
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    except: return
        
    if df_h.empty:
        print("⚠️ 信号历史为空")
        return

    results = []
    for _, row in df_h.iterrows():
        code = str(row['code']).zfill(6)
        signal_date = str(row['date'])
        
        # 兼容性取值
        entry_p = row.get('entry_price') if pd.notna(row.get('entry_price')) else row.get('price')
        stop_p = row.get('stop')
        
        if pd.isna(entry_p) or pd.isna(stop_p): continue
        
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path): continue
        
        try:
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            df_after = df_d[df_d['日期'] > signal_date].sort_values('日期')
            
            if df_after.empty:
                status, curr_ret, last_p = "⏳ 观察中", 0.0, entry_p
            else:
                last_p = df_after.iloc[-1]['收盘']
                low_after = df_after['最低'].min()
                
                if low_after <= float(stop_p):
                    status = "❌ 已止损"
                elif last_p > float(entry_p):
                    status = "✅ 盈利中"
                else:
                    status = "📉 被套中"
                curr_ret = (last_p - float(entry_p)) / float(entry_p) * 100

            # 身份识别：是否属于精选池
            is_elite = "🏆" if code in elite_pool else "⚪"

            results.append({
                '精英': is_elite,
                '日期': signal_date, '代码': code, '名称': row.get('name', '未知'),
                '入场价': entry_p, '止损价': stop_p, '现价': last_p,
                '收益%': round(curr_ret, 2), '状态': status
            })
        except: continue

    if not results: return

    df_res = pd.DataFrame(results)
    
    # 统计：精选池 vs 全量
    total = len(df_res)
    wins = len(df_res[df_res['状态'] == '✅ 盈利中'])
    elite_signals = df_res[df_res['精英'] == "🏆"]
    elite_wins = len(elite_signals[elite_signals['状态'] == '✅ 盈利中']) if not elite_signals.empty else 0

    # 写入报告
    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告 (Elite版)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        f.write(f"### 📊 战绩统计\n")
        f.write(f"- **所有信号胜率**: `{(wins/total*100):.1f}%` ({wins}/{total})\n")
        if not elite_signals.empty:
            f.write(f"- **🏆 精选池胜率**: `{(elite_wins/len(elite_signals)*100):.1f}%` ({elite_wins}/{len(elite_signals)})\n\n")
        
        f.write("| 身份 | 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        for _, r in df_res.sort_values(['精英', '日期'], ascending=[False, False]).iterrows():
            f.write(f"| {r['精英']} | {r['日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验报告已更新，包含精选池对比。")

if __name__ == "__main__":
    validate()
