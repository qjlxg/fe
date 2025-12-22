import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
VALIDATION_FILE = 'VALIDATION_REPORT.md'
SUMMARY_FILE = 'DAILY_ACTION_PLAN.md'

def generate_summary():
    print("🚀 正在从校验报告中提取决策信息...")
    if not os.path.exists(VALIDATION_FILE):
        print(f"❌ 错误: 找不到文件 {VALIDATION_FILE}")
        return

    # 1. 稳健的 Markdown 表格读取逻辑
    try:
        with open(VALIDATION_FILE, 'r', encoding='utf_8_sig') as f:
            lines = f.readlines()
        
        # 寻找表格数据 (从包含列名的行开始)
        table_data = []
        start_collecting = False
        for line in lines:
            if '| 身份 | 信号日期 |' in line:
                start_collecting = True
                header = [c.strip() for c in line.split('|') if c.strip()]
                continue
            if start_collecting and '| --- |' in line:
                continue
            if start_collecting and line.startswith('|'):
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if len(cells) >= 9:
                    table_data.append(cells)
            elif start_collecting and not line.strip():
                break # 表格结束
        
        df = pd.DataFrame(table_data, columns=['身份', '信号日期', '代码', '名称', '入场价', '止损价', '现价/结算', '收益%', '状态'])
    except Exception as e:
        print(f"❌ 解析 Markdown 失败: {e}")
        return

    # 获取上海时间 (北京时间)
    bj_today = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d')
    
    # 2. 筛选逻辑
    # 买入：今日新增的精英
    buy_list = df[(df['身份'].str.contains('精英')) & (df['信号日期'] == bj_today)]
    # 卖出：已止损
    stop_list = df[df['状态'].str.contains('止损')]
    # 止盈：收益 >= 10%
    df['profit_val'] = df['收益%'].str.replace('%', '').astype(float)
    profit_list = df[df['profit_val'] >= 10.0]

    # 3. 生成行动清单
    with open(SUMMARY_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏹 今日实战操作建议 ({bj_today})\n\n")
        
        f.write("## 🟢 买入指令 (新晋精英信号)\n")
        if not buy_list.empty:
            f.write("| 代码 | 名称 | 入场参考 | 止损位 |\n| --- | --- | --- | --- |\n")
            for _, r in buy_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['入场价']} | {r['止损价']} |\n")
        else:
            f.write("*今日暂无新出现的精英买入信号。*\n")

        f.write("\n## 🔴 卖出指令 (触发止损)\n")
        if not stop_list.empty:
            f.write("| 代码 | 名称 | 信号日期 | 最终盈亏 |\n| --- | --- | --- | --- |\n")
            for _, r in stop_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['信号日期']} | {r['收益%']} |\n")
        else:
            f.write("*当前持仓表现正常，无触发止损标的。*\n")

        f.write("\n## 🟡 止盈提醒 (收益 > 10%)\n")
        if not profit_list.empty:
            f.write("| 代码 | 名称 | 累计收益 | 状态 |\n| --- | --- | --- | --- |\n")
            for _, r in profit_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | **{r['收益%']}** | 可考虑减仓 |\n")
        else:
            f.write("*尚无收益超过 10% 的标的。*\n")

    print(f"✅ 指挥清单已生成: {SUMMARY_FILE}")

if __name__ == "__main__":
    generate_summary()
