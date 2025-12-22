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

    try:
        # 读取时强制指定 code 为字符串，防止前导0丢失
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
    except Exception as e:
        print(f"读取失败: {e}")
        return
        
    if df_h.empty:
        print("⚠️ 信号历史文件为空")
        return

    results = []
    
    for _, row in df_h.iterrows():
        # 1. 获取基础数据并清洗
        code = str(row['code']).zfill(6)
        signal_date = str(row['date'])
        
        # 2. 核心修复：多字段兼容 + 空值跳过
        # 你的CSV里有 'entry_price' 也有 'price'，这里按优先级取第一个非空值
        entry_p = row.get('entry_price')
        if pd.isna(entry_p): entry_p = row.get('price')
        
        stop_p = row.get('stop')
        
        # 如果入场价或止损价是空的，说明这条信号无效，直接跳过
        if pd.isna(entry_p) or pd.isna(stop_p):
            continue
            
        try:
            entry_p = float(entry_p)
            stop_p = float(stop_p)
        except:
            continue

        # 3. 寻找对应的历史行情文件
        file_path = os.path.join(DATA_DIR, f"{code}.csv")
        if not os.path.exists(file_path):
            continue
        
        try:
            df_d = pd.read_csv(file_path)
            df_d.columns = [c.strip() for c in df_d.columns]
            
            # 筛选信号产生日期之后的行情
            df_after = df_d[df_d['日期'] > signal_date].sort_values('日期')
            
            if df_after.empty:
                status, curr_ret, last_p = "⏳ 观察中", 0.0, entry_p
            else:
                last_p = df_after.iloc[-1]['收盘']
                low_after = df_after['最低'].min()
                
                if low_after <= stop_p:
                    status = "❌ 已止损"
                elif last_p > entry_p:
                    status = "✅ 盈利中"
                else:
                    status = "📉 被套中"
                curr_ret = (last_p - entry_p) / entry_p * 100

            results.append({
                '日期': signal_date, '代码': code, '名称': row.get('name', '未知'),
                '入场价': entry_p, '止损价': stop_p, '现价': last_p,
                '收益%': round(curr_ret, 2), '状态': status
            })
        except:
            continue

    # 4. 生成报告 (增加防御逻辑)
    if not results:
        print("⚠️ 经过清洗后，无可验证的有效信号（入场价缺失）。")
        return

    df_res = pd.DataFrame(results)
    
    # 只要 results 不为空，'状态' 列一定存在
    total = len(df_res)
    wins = len(df_res[df_res['状态'] == '✅ 盈利中'])
    win_rate = (wins / total * 100) if total > 0 else 0

    with open(REPORT_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🔍 信号实战校验报告\n\n")
        f.write(f"更新时间 (北京): `{get_beijing_time()}`\n\n")
        f.write(f"### 📊 统计概览\n")
        f.write(f"- **有效信号总数**: `{total}` (已过滤价格缺失行)\n")
        f.write(f"- **盈利标的**: `{wins}`\n")
        f.write(f"- **当前胜率**: `{win_rate:.1f}%`\n\n")
        f.write("| 信号日期 | 代码 | 名称 | 入场价 | 止损价 | 现价 | 收益% | 状态 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        # 按日期降序排列
        df_res = df_res.sort_values('日期', ascending=False)
        for _, r in df_res.iterrows():
            f.write(f"| {r['日期']} | {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} | {r['现价']} | {r['收益%']}% | {r['状态']} |\n")

    print(f"✅ 校验完成，报告已更新至 {REPORT_FILE}")

if __name__ == "__main__":
    validate()
