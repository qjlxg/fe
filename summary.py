import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
VALIDATION_FILE = 'VALIDATION_REPORT.md'
SUMMARY_FILE = 'DAILY_ACTION_PLAN.md'

def get_main_name(full_name):
    """
    智能提取核心名称：去掉杯子图标、基金公司名、ETF、基金等字样
    例如：'🏆创业板50ETF华夏' -> '创业板50'
    """
    # 1. 去掉图标和特殊字符
    name = full_name.replace('🏆', '').replace('⚪', '').strip()
    # 2. 定义需要剔除的关键词（常见基金公司名及后缀）
    cleanup_list = [
        'ETF', '基金', '指数', '华夏', '嘉实', '工银', '华泰柏瑞', '国泰', 
        '易方达', '广发', '富国', '南方', '招商', '汇添富', '天弘', '鹏华'
    ]
    for word in cleanup_list:
        name = name.replace(word, '')
    
    # 3. 取前4-5个字符作为核心标识，防止不同板块误伤
    return name[:5]

def generate_summary():
    print("🚀 正在生成智能去重版行动指南...")
    if not os.path.exists(VALIDATION_FILE):
        print(f"❌ 错误: 找不到文件 {VALIDATION_FILE}")
        return

    # 1. 读取并解析 Markdown 表格
    try:
        with open(VALIDATION_FILE, 'r', encoding='utf_8_sig') as f:
            lines = f.readlines()
        
        table_data = []
        start_collecting = False
        for line in lines:
            if '| 身份 | 信号日期 |' in line:
                start_collecting = True
                continue
            if start_collecting and '| --- |' in line:
                continue
            if start_collecting and line.startswith('|'):
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if len(cells) >= 9:
                    table_data.append(cells)
            elif start_collecting and not line.strip():
                break
        
        df = pd.DataFrame(table_data, columns=['身份', '信号日期', '代码', '名称', '入场价', '止损价', '现价/结算', '收益%', '状态'])
    except Exception as e:
        print(f"❌ 解析报告失败: {e}")
        return

    # 获取北京时间
    bj_today = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d')
    
    # 2. 核心去重逻辑
    # 提取今日精英信号
    today_elites = df[(df['身份'].str.contains('精英')) & (df['信号日期'] == bj_today)].copy()
    
    if not today_elites.empty:
        # 给每只基金打上“核心标识”标签
        today_elites['核心标识'] = today_elites['名称'].apply(get_main_name)
        # 按核心标识去重，每个标识只留第一只
        buy_list = today_elites.drop_duplicates(subset=['核心标识'], keep='first')
    else:
        buy_list = pd.DataFrame()

    # 3. 筛选止损和止盈
    stop_list = df[df['状态'].str.contains('止损')]
    df['profit_val'] = df['收益%'].str.replace('%', '').astype(float)
    profit_list = df[df['profit_val'] >= 10.0]

    # 4. 写入文件
    with open(SUMMARY_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏹 今日实战操作建议 ({bj_today})\n\n")
        f.write(f"> **注意**：系统已自动识别并剔除了重复的同类指数基金，每类仅保留一只最推荐标的。\n\n")
        
        f.write("## 🟢 核心买入建议 (精英池 + 异动去重)\n")
        if not buy_list.empty:
            f.write("| 代码 | 核心标的 | 原始名称 | 入场参考 | 止损位 |\n| --- | --- | --- | --- | --- |\n")
            for _, r in buy_list.iterrows():
                f.write(f"| {r['代码']} | **{r['核心标识']}** | {r['名称']} | {r['入场价']} | {r['止损价']} |\n")
        else:
            f.write("*今日暂无新晋精英信号，或市场共振过强，建议观望。*\n")

        f.write("\n## 🔴 强制卖出提醒 (止损清仓)\n")
        if not stop_list.empty:
            f.write("| 代码 | 名称 | 信号日期 | 盈亏 | 状态 |\n| --- | --- | --- | --- | --- |\n")
            for _, r in stop_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['信号日期']} | {r['收益%']} | 立即卖出 |\n")
        else:
            f.write("*当前持仓健康，无触发止损信号。*\n")

        f.write("\n## 🟡 止盈参考 (盈利 > 10%)\n")
        if not profit_list.empty:
            f.write("| 代码 | 名称 | 累计收益 | 操作建议 |\n| --- | --- | --- | --- |\n")
            for _, r in profit_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | **{r['收益%']}** | 分批止盈 |\n")
        else:
            f.write("*暂无收益超 10% 标的，请继续耐心持股。*\n")

    print(f"✅ 智能计划已生成至: {SUMMARY_FILE}")

if __name__ == "__main__":
    generate_summary()
