import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
HISTORY_FILE = 'signal_history.csv'
DATA_DIR = 'fund_data'
REPORT_FILE = 'VALIDATION_REPORT.md'

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def validate():
    if not os.path.exists(HISTORY_FILE):
        print("❌ 找不到历史信号文件")
        return

    # 读取历史记录，确保代码是6位字符串
    try:
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    except Exception as e:
        print(f"读取失败: {e}")
        return
        
    if df_h.empty: return

    results = []
    for _, row in df_h.iterrows():
        code = row['code'].zfill(6)
        signal_date = row['date']
        entry_price = float(row['price'])
        stop_price = float(row['stop'])
        
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path): continue
        
        df_d = pd.read_csv(file_path)
        df_d.columns = [c.strip() for c in df_d.columns]
        
        # 筛选信号之后的数据
        df_after = df_d[df_d['日期'] > signal_date].sort_values('日期')
        
        if df_after.empty:
            status, curr_ret, last_p = "⏳ 观察中", 0.0, entry_price
        else:
            last_p = df_after.iloc[-1]['收盘']
            low_after = df_after['最低'].min()
            
            if low_after <= stop_price:
                status = "❌ 已止损"
            elif last_p > entry_price:
                status = "✅ 盈利中"
            else:
                status = "📉 被套中"
            curr_ret = (last_p - entry_price) / entry_price * 100

        results.append({
            '日期': signal_date, '代码': code, '名称': row['name'],
            '入场价': entry_price, '止损价': stop_price, '现价': last_p,
            '收益%': round(curr_ret, 2), '状态': status
        })

    df_res = pd.DataFrame(results)
    # 统计胜率
    total = len(df_res)
    wins = len(df_res[df_res['状态'] == '✅ 盈利中'])
    win_rate = (wins / total * 100) if total > 0 else 0

    # 写入 Markdown 报告
    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告\n\n")
        f.write(f"更新时间 (北京): `{get_beijing_time()}`\n\n")
        f.write(f"### 📊 统计概览\n")
        f.write(f"- **总计信号**: `{total}`\n")
        f.write(f"- **盈利标的**: `{wins}`\n")
        f.write(f"- **当前胜率**: `{win_rate:.1f}%`\n\n")
        f.write("| 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for _, r in df_res.sort_values('日期', ascending=False).iterrows():
            f.write(f"| {r['日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价']} | {r['收益%']}% | {r['状态']} |\n")

if __name__ == "__main__":
    validate()
