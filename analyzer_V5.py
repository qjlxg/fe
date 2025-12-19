import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz
import logging
import math

# --- V5.1 策略所需配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.06           # V5.0 震荡市核心触发 (回撤 >= 6%)
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.15 # 高弹性策略的基础回撤要求 (15%)
MIN_DAILY_DROP_PERCENT = 0.03       # 当日大跌的定义 (3%)
REPORT_BASE_NAME = 'fund_warning_report_v5_1_volume'

# --- 核心阈值调整 ---
EXTREME_RSI_THRESHOLD_P1 = 29.0     # 网格级：RSI(14) 极值超卖
STRONG_RSI_THRESHOLD_P2 = 35.0      # 强力超卖观察池
SHORT_TERM_RSI_EXTREME = 20.0       # RSI(6)的极值超卖阈值
TREND_HEALTH_THRESHOLD = 0.9        # MA50/MA250 健康度阈值
MIN_BUY_SIGNAL_SCORE = 3.7          # 最低信号分数
TREND_SLOPE_THRESHOLD = 0.005       # 趋势拟合斜率阈值

# --- 新增：成交量与活跃度阈值 ---
MIN_TURNOVER_RATE = 0.005           # 换手率门槛 (0.5%)，低于此值视为活跃度不足
VOLUME_STRETCH_RATIO = 1.5          # 放量定义 (当日成交量 > 5日均量 * 1.5)

# --- 设置日志 ---
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('fund_analysis.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# --- 数据预处理 (更新：支持成交量和换手率) ---
def load_and_preprocess_data(filepath, fund_code):
    try:
        try:
            df = pd.read_csv(filepath)
        except UnicodeDecodeError:
            df = pd.read_csv(filepath, encoding='gbk')
        
        # 统一映射表头 (请确保您的 CSV 包含以下字段或其变体)
        column_map = {
            'Date': 'date', '日期': 'date',
            'NetValue': 'net_value', '净值': 'net_value', 'Close': 'net_value',
            'Volume': 'volume', '成交量': 'volume',
            'Turnover': 'turnover_rate', '换手率': 'turnover_rate'
        }
        df = df.rename(columns=column_map)
        
        if 'date' not in df.columns or 'net_value' not in df.columns:
            return None, "缺少关键列 (date/net_value)"
            
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        df = df.rename(columns={'net_value': 'value'})
        
        # 缺失值填充 (成交量若缺失填0)
        if 'volume' not in df.columns: df['volume'] = 0
        if 'turnover_rate' not in df.columns: df['turnover_rate'] = 0
        
        if df.empty or len(df) < 60: return None, "数据量不足"
        
        return df, "数据有效"
    except Exception as e:
        return None, f"加载错误: {e}"

# --- 布林带计算 ---
def calculate_bollinger_bands(series, window=20):
    df_temp = pd.DataFrame({'value': series.values})
    df_temp['MA20'] = df_temp['value'].rolling(window=window).mean()
    df_temp['STD20'] = df_temp['value'].rolling(window=window).std()
    
    if df_temp['STD20'].iloc[-1] == 0: return "波动极小"
        
    df_temp['Lower'] = df_temp['MA20'] - (df_temp['STD20'] * 2)
    df_temp['Upper'] = df_temp['MA20'] + (df_temp['STD20'] * 2)
    
    val = df_temp['value'].iloc[-1]
    low, up = df_temp['Lower'].iloc[-1], df_temp['Upper'].iloc[-1]
    
    if val <= low: return "**下轨下方**" 
    elif val >= up: return "**上轨上方**" 
    return "轨道中间"

# --- 技术指标计算 (更新：包含成交量分析) ---
def calculate_technical_indicators(df):
    try:
        df_asc = df.copy()
        # RSI 逻辑
        delta = df_asc['value'].diff()
        for window in [14, 6]:
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.ewm(span=window, adjust=False).mean()
            avg_loss = loss.ewm(span=window, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, 1e-10)
            df_asc[f'RSI_{window}'] = 100 - (100 / (1 + rs))

        # MACD
        ema12 = df_asc['value'].ewm(span=12, adjust=False).mean()
        ema26 = df_asc['value'].ewm(span=26, adjust=False).mean()
        df_asc['MACD'] = ema12 - ema26
        df_asc['Signal'] = df_asc['MACD'].ewm(span=9, adjust=False).mean()
        
        # 成交量分析
        df_asc['Vol_MA5'] = df_asc['volume'].rolling(window=5).mean()
        latest_vol = df_asc['volume'].iloc[-1]
        avg_vol_5 = df_asc['Vol_MA5'].iloc[-1]
        
        volume_status = "平稳"
        if avg_vol_5 > 0:
            if latest_vol > avg_vol_5 * VOLUME_STRETCH_RATIO: volume_status = "放量"
            elif latest_vol < avg_vol_5 * 0.6: volume_status = "缩量"

        # 趋势
        df_asc['MA50'] = df_asc['value'].rolling(window=50).mean()
        df_asc['MA250'] = df_asc['value'].rolling(window=250).mean()
        
        ma50_l = df_asc['MA50'].iloc[-1]
        ma250_l = df_asc['MA250'].iloc[-1]
        ma_ratio = ma50_l / ma250_l if ma250_l else np.nan
        
        return {
            'RSI(14)': round(df_asc['RSI_14'].iloc[-1], 2),
            'RSI(6)': round(df_asc['RSI_6'].iloc[-1], 2),
            'MACD信号': '金叉' if df_asc['MACD'].iloc[-1] > df_asc['Signal'].iloc[-1] else '死叉',
            'MA50/MA250': round(ma_ratio, 3),
            '布林带位置': calculate_bollinger_bands(df_asc['value']),
            '最新净值': round(df_asc['value'].iloc[-1], 4),
            '当日跌幅': round((df_asc['value'].iloc[-1] / df_asc['value'].iloc[-2] - 1), 4) if len(df_asc)>1 else 0,
            '换手率': df_asc['turnover_rate'].iloc[-1],
            '量比状态': volume_status
        }
    except Exception as e:
        logging.error(f"技术指标计算失败: {e}")
        return None

# --- 行动信号 (更新：加入成交量权重) ---
def generate_v5_action_signal(row):
    signals = []
    rsi14 = row.get('RSI(14)', 50)
    vol_status = row.get('量比状态', '平稳')
    drop = row.get('当日跌幅', 0)
    
    # 极值信号 + 放量 = 恐慌盘出尽
    if rsi14 <= EXTREME_RSI_THRESHOLD_P1:
        prefix = "💥【网格级】"
        if vol_status == "放量" and drop < -0.02:
            signals.append(f"{prefix}放量恐慌出尽")
        else:
            signals.append(f"{prefix}RSI极值")

    # 换手率过滤逻辑可在 generate_report 处理，此处仅生成描述
    if row.get('换手率', 0) < MIN_TURNOVER_RATE and row.get('换手率', 0) > 0:
        signals.append("⚠️活跃度极低")

    if not signals: return '等待信号'
    return ' | '.join(signals)

# --- 报告生成核心 ---
def generate_report(results, timestamp_str):
    if not results: return "无有效数据"
    
    df = pd.DataFrame(results)
    # 1. 基础筛选
    df_filtered = df[df['最大回撤'] >= MIN_MONTH_DRAWDOWN].copy()
    
    # 2. 评分逻辑 (量能加分)
    def score_logic(r):
        score = 0
        if "网格级" in r['行动提示']: score += 4.0
        if r['量比状态'] == "放量" and r['当日跌幅'] < 0: score += 0.5 # 放量下跌往往是底部
        if r['换手率'] < MIN_TURNOVER_RATE: score -= 2.0 # 流动性惩罚
        return score

    df_filtered['signal_score'] = df_filtered.apply(score_logic, axis=1)
    
    # 3. 分组
    # I.1 必须：趋势健康、分数达标、换手率合格
    mask_i1 = (df_filtered['MA50/MA250'] >= TREND_HEALTH_THRESHOLD) & \
              (df_filtered['signal_score'] >= 3.0) & \
              (df_filtered['换手率'] >= MIN_TURNOVER_RATE)
              
    df_i1 = df_filtered[mask_i1].sort_values(by='signal_score', ascending=False)
    df_others = df_filtered[~mask_i1].sort_values(by='最大回撤', ascending=False)

    # 4. 构建 Markdown (表格列增加成交量/换手率)
    report = [f"# 基金 V5.1 量价选股报告 ({timestamp_str})\n\n", "## 🥇 I.1 最高优先级试仓 (量价配合)\n\n"]
    
    header = "| 基金代码 | 最大回撤 | 跌幅 | RSI14 | 量比 | 换手 | 行动提示 | 趋势 |\n"
    sep = "| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n"
    
    report.append(header + sep)
    for _, row in df_i1.iterrows():
        report.append(f"| `{row['基金代码']}` | {row['最大回撤']:.2%} | {row['当日跌幅']:.2%} | {row['RSI(14)']} | {row['量比状态']} | {row['换手率']:.2%} | **{row['行动提示']}** | {row['MA50/MA250']} |\n")
    
    report.append("\n## ⚠️ 其他观察标的 (活跃度不足或趋势走弱)\n\n" + header + sep)
    for _, row in df_others.iterrows():
        report.append(f"| `{row['基金代码']}` | {row['最大回撤']:.2%} | {row['当日跌幅']:.2%} | {row['RSI(14)']} | {row['量比状态']} | {row['换手率']:.2%} | {row['行动提示']} | {row['MA50/MA250']} |\n")

    return "".join(report)

# --- 主循环逻辑 ---
def analyze_single_fund(filepath):
    fund_code = os.path.splitext(os.path.basename(filepath))[0]
    df, msg = load_and_preprocess_data(filepath, fund_code)
    if df is None: return None
    
    # 计算近一月回撤
    mdd = (df['value'].cummax() - df['value']) / df['value'].cummax()
    latest_mdd = mdd.tail(20).max()
    
    tech = calculate_technical_indicators(df)
    if not tech: return None
    
    action = generate_v5_action_signal(tech)
    
    return {
        '基金代码': fund_code,
        '最大回撤': latest_mdd,
        **tech,
        '行动提示': action
    }

def main():
    setup_logging()
    if not os.path.exists(FUND_DATA_DIR):
        os.makedirs(FUND_DATA_DIR)
        print(f"请在 {FUND_DATA_DIR} 文件夹中放入数据文件。")
        return

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = [analyze_single_fund(f) for f in files if analyze_single_fund(f)]
    
    report = generate_report(results, datetime.now().strftime('%Y-%m-%d %H:%M'))
    
    save_path = f"Report_V5_1_{datetime.now().strftime('%Y%m%d')}.md"
    with open(save_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"分析完成！报告已生成：{save_path}")

if __name__ == "__main__":
    main()