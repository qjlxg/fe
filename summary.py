import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
VALIDATION_FILE = 'VALIDATION_REPORT.md'
SUMMARY_FILE = 'DAILY_ACTION_PLAN.md'

def generate_summary():
    if not os.path.exists(VALIDATION_FILE):
        print("❌ 找不到校验报告，请先运行校验脚本。")
        return

    # 从 Markdown 提取数据 (简单处理)
    try:
        df = pd.read_html(VALIDATION_FILE, encoding='utf_8_sig')[1] # 读取详细信号表
    except Exception as e:
        print(f"❌ 解析报告失败: {e}")
        return

    today = datetime.now().strftime('%Y-%m-%d')
    
    # 1. 筛选买入：今日新增的精英信号
    buy_list = df[(df['身份'].str.contains('精英')) & (df['信号日期'] == today) & (df['状态'] == '⏳ 观察中')]
    
    # 2. 筛选卖出：触发止损的信号
    sell_list = df[df['状态'] == '❌ 已止损']

    # 3. 生成行动清单
    with open(SUMMARY_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🚀 今日实战操作指南\n\n")
        f.write(f"生成时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        f.write("## 🛒 今日建议买入 (精英超跌标的)\n")
        if not buy_list.empty:
            f.write("| 代码 | 名称 | 入场参考 | 止损位 |\n| --- | --- | --- | --- |\n")
            for _, r in buy_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} |\n")
        else:
            f.write("> *今日暂无符合条件的精英信号，建议空仓观望。*\n")

        f.write("\n## 🚩 今日必须卖出 (止损清仓)\n")
        if not sell_list.empty:
            f.write("| 代码 | 名称 | 信号日期 | 最终收益 |\n| --- | --- | --- | --- |\n")
            for _, r in sell_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['信号日期']} | {r['收益%']} |\n")
        else:
            f.write("> *目前无触发止损的标的，持仓继续观察。*\n")

    print(f"✅ 今日行动计划已生成: {SUMMARY_FILE}")

if __name__ == "__main__":
    generate_summary()
