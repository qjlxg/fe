import pandas as pd
import os
from datetime import datetime, timedelta

# --- 配置 ---
VALIDATION_FILE = 'VALIDATION_REPORT.md'
SUMMARY_FILE = 'DAILY_ACTION_PLAN.md'

def get_smart_fingerprint(full_name):
    """
    指纹识别算法：提取基金的核心灵魂，过滤公司名和语义重叠。
    """
    # 1. 基础清洗
    name = str(full_name).replace('🏆', '').replace('⚪', '').strip()
    
    # 2. 核心黑名单：剔除干扰词
    blacklist = [
        # 基金公司
        '华夏', '嘉实', '工银', '华泰柏瑞', '国泰', '易方达', '广发', '富国', '南方', 
        '招商', '汇添富', '天弘', '鹏华', '华安', '大成', '万家', '博时', '银华', 
        '中欧', '兴业', '泰康', '建信', '摩根', '景顺', '永赢', '交银',
        # 产品后缀
        'ETF', '联接', 'A', 'C', '基金', '指数', '增强', 'LOF', '发起式', '权重', '100', '50'
    ]
    for word in blacklist:
        name = name.replace(word, '')

    # 3. 语义映射：将“长得不同但本质一样”的板块合并
    # 如果名字里包含 Key，则统一返回 Value
    semantic_map = {
        '创业板': '创业板系列',
        '科创': '科创板系列',
        '芯片': '半导体芯片',
        '半导体': '半导体芯片',
        '人工智能': 'AI人工智通',
        'AI': 'AI人工智通',
        '软件': '计算机软件',
        '互联网': '港股互联网',
        '恒生科技': '港股互联网',
        '纳斯达克': '纳指',
        '纳指': '纳指',
        '沪深300': '沪深300',
        '中证500': '中证500',
        '红利': '红利低波',
        '光伏': '新能源光伏',
        '新能源': '新能源光伏',
        '证券': '大金融券商',
        '券商': '大金融券商',
        '银行': '大金融银行'
    }
    
    for key, val in semantic_map.items():
        if key in name:
            return val
            
    # 4. 兜底逻辑：取剩下的前 4 个字符
    return name[:4] if name else "其他"

def generate_summary():
    print("🚀 启动指纹级去重决策系统...")
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
            if start_collecting and ('| --- |' in line or not line.startswith('|')):
                if not line.startswith('|') and table_data: break
                continue
            if start_collecting:
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if len(cells) >= 9:
                    table_data.append(cells)
        
        df = pd.DataFrame(table_data, columns=['身份', '信号日期', '代码', '名称', '入场价', '止损价', '现价/结算', '收益%', '状态'])
    except Exception as e:
        print(f"❌ 解析报告失败: {e}")
        return

    # 获取上海时间
    bj_today = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d')
    
    # 2. 精英信号去重筛选
    today_elites = df[(df['身份'].str.contains('精英')) & (df['信号日期'] == bj_today)].copy()
    
    if not today_elites.empty:
        # 生成指纹
        today_elites['指纹'] = today_elites['名称'].apply(get_smart_fingerprint)
        # 按指纹去重，只保留每类第一只
        buy_list = today_elites.drop_duplicates(subset=['指纹'], keep='first')
    else:
        buy_list = pd.DataFrame()

    # 3. 止损与止盈分析
    stop_list = df[df['状态'].str.contains('止损')]
    df['profit_val'] = df['收益%'].str.replace('%', '').replace('nan', '0').astype(float)
    profit_list = df[df['profit_val'] >= 10.0]

    # 4. 输出最终行动指南
    with open(SUMMARY_FILE, 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏹 每日实战指挥手册 ({bj_today})\n\n")
        f.write(f"> **防重策略**：系统已从 1000+ 标的中自动识别语义重叠，合并了同类板块，确保持仓分散。\n\n")
        
        f.write("## 🟢 今日买入指令 (精选唯一标的)\n")
        if not buy_list.empty:
            f.write("| 代码 | 指纹分类 | 推荐标的 | 入场参考 | 止损位 |\n| --- | --- | --- | --- | --- |\n")
            for _, r in buy_list.iterrows():
                f.write(f"| {r['代码']} | **{r['指纹']}** | {r['名称']} | {r['入场价']} | {r['止损价']} |\n")
        else:
            f.write("*今日暂无新信号，或信号已被语义合并。*\n")

        f.write("\n## 🔴 强制平仓清单 (止损避险)\n")
        if not stop_list.empty:
            f.write("| 代码 | 名称 | 信号日期 | 盈亏 | 动作 |\n| --- | --- | --- | --- | --- |\n")
            for _, r in stop_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | {r['信号日期']} | {r['收益%']} | **坚决卖出** |\n")
        else:
            f.write("*持仓安全，无触发止损标的。*\n")

        f.write("\n## 🟡 减仓获利建议 (收益 > 10%)\n")
        if not profit_list.empty:
            f.write("| 代码 | 名称 | 累计收益 | 操作建议 |\n| --- | --- | --- | --- |\n")
            for _, r in profit_list.iterrows():
                f.write(f"| {r['代码']} | {r['名称']} | **{r['收益%']}** | 分批获利了结 |\n")
        else:
            f.write("*暂无收益达标标的，让利润再飞一会儿。*\n")

    print(f"✅ 终极行动清单已生成: {SUMMARY_FILE}")

if __name__ == "__main__":
    generate_summary()
