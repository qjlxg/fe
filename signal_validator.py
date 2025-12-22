import pandas as pd
import os
import glob

# --- 配置 ---
HISTORY_FILE = 'signal_history.csv'
DATA_DIR = 'fund_data'
VALIDATION_REPORT = 'VALIDATION_REPORT.md'

def validate():
    if not os.path.exists(HISTORY_FILE):
        print("❌ 未发现历史信号文件")
        return

    # 加载历史信号
    df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    if df_h.empty: return

    results = []
    
    # 遍历每一个历史信号进行验证
    for _, row in df_h.iterrows():
        code = row['code']
        signal_date = row['date']
        entry_price = row['price']
        stop_price = row['stop']
        
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path): continue
        
        # 读取该标的完整行情
        df_d = pd.read_csv(file_path)
        df_d.columns = [c.strip() for c in df_d.columns]
        
        # 筛选信号日期之后的行情
        df_after = df_d[df_d['日期'] > signal_date].sort_values('日期')
        
        if df_after.empty:
            status = "⏳ 持仓观察"
            curr_ret = 0.0
        else:
            last_price = df_after.iloc[-1]['收盘']
            low_after = df_after['最低'].min()
            
            # 逻辑判断
            if low_after <= stop_price:
                status = "❌ 已止损"
            elif last_price > entry_price:
                status = "✅ 盈利中"
            else:
                status = "📉 被套中"
            
            curr_ret = (last_price - entry_price) / entry_price * 100

        results.append({
            '日期': signal_date,
            '代码': code,
            '名称': row['name'],
            '入场价': entry_price,
            '止损价': stop_price,
            '现价/终价': last_price if not df_after.empty else entry_price,
            '当前收益%': round(curr_ret, 2),
            '状态': status
        })

    # 生成报告
    df_res = pd.DataFrame(results)
    win_rate = len(df_res[df_res['状态'] == '✅ 盈利中']) / len(df_res) * 100 if not df_res.empty else 0
    
    with open(VALIDATION_REPORT, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告\n\n")
        f.write(f"**历史信号胜率**: `{win_rate:.1f}%` (注：盈利中标的占比)\n\n")
        f.write("| 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 当前收益 | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- |\n")
        for _, r in df_res.sort_values('日期', ascending=False).iterrows():
            f.write(f"| {r['日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['当前收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验完成，胜率：{win_rate:.1f}%")

if __name__ == "__main__":
    validate()
