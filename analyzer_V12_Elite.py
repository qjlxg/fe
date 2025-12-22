import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta

# --- 配置 ---
DATA_DIR = 'fund_data'
HISTORY_FILE = 'signal_history.csv'
BACKTEST_REPORT = 'backtest_results.csv' # 回测报告来源
BENCHMARK_CODE = '510300'               # 大盘风控基准

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

# 1. 获取回测前10名作为“精选池”
def get_elite_pool():
    if not os.path.exists(BACKTEST_REPORT):
        print("⚠️ 未发现回测报告，将扫描全量数据...")
        return None
    try:
        df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
        # 取前10名
        elite_list = df_bt['代码'].head(10).tolist()
        print(f"✅ 已锁定回测精选池: {elite_list}")
        return elite_list
    except:
        return None

# 2. 大盘风控检查
def check_market_safety():
    bench_file = os.path.join(DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(bench_file):
        print("⚠️ 缺少大盘数据，默认安全")
        return True, 0
    
    df = pd.read_csv(bench_file)
    df.columns = [c.strip() for c in df.columns]
    df = df.sort_values('日期')
    
    ma20 = df['收盘'].rolling(20).mean().iloc[-1]
    current_price = df['收盘'].iloc[-1]
    
    is_safe = current_price >= ma20
    return is_safe, round(current_price, 3)

def analyze():
    elite_pool = get_elite_pool()
    is_safe, bench_p = check_market_safety()
    
    results = []
    
    # 获取待分析文件
    if elite_pool:
        target_files = [os.path.join(DATA_DIR, f"{c}.csv") for c in elite_pool]
    else:
        target_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    for file in target_files:
        if not os.path.exists(file) or BENCHMARK_CODE in file: continue
        
        try:
            df = pd.read_csv(file)
            df.columns = [c.strip() for c in df.columns]
            df = df.sort_values('日期')
            if len(df) < 40: continue
            
            code = os.path.basename(file).replace('.csv','')
            last_row = df.iloc[-1]
            curr_p = last_row['收盘']
            
            # 指标计算
            ma5 = df['收盘'].rolling(5).mean().iloc[-1]
            hi40 = df['收盘'].rolling(40).max().iloc[-1]
            dd = (curr_p - hi40) / hi40
            
            # 基础门槛：站上MA5 且 回撤超过4%
            if curr_p > ma5 and dd < -0.04:
                # 进一步计算辅助得分
                score = 1
                # RSI 因子
                delta = df['收盘'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs.iloc[-1]))
                if rsi < 40: score += 1
                
                # 成交量因子
                v_ma14 = df['成交量'].rolling(14).mean().iloc[-1]
                if last_row['成交量'] > v_ma14: score += 1
                
                # ATR止损计算
                tr = np.maximum(df['最高'] - df['最低'], 
                                np.maximum(abs(df['最高'] - df['收盘'].shift(1)), 
                                           abs(df['最低'] - df['收盘'].shift(1))))
                atr = tr.rolling(14).mean().iloc[-1]
                stop_p = min(curr_p - 3.0 * atr, curr_p * 0.93)
                
                results.append({
                    'date': last_row['日期'],
                    'code': code,
                    'name': '精选标的', # 实际环境可对接名称表
                    'price': curr_p,
                    'stop': round(stop_p, 3),
                    'score': score,
                    'rsi': round(rsi, 1),
                    'dd': f"{round(dd*100, 2)}%"
                })
        except:
            continue

    # --- 输出看板 ---
    with open('README.md', 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏆 精选池实战看板 (V12-Elite)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        status_icon = "✅ 运行中" if is_safe else "🛑 休息中 (大盘风险)"
        f.write(f"### 🚦 市场环境: {status_icon}\n")
        f.write(f"- 沪深300指数: `{bench_p}` (MA20线下强行空仓)\n\n")
        
        if not is_safe:
            f.write("> 🚩 **当前系统处于避险模式**：大盘趋势走弱，已屏蔽所有买入信号。\n")
        elif not results:
            f.write("> 🔍 **扫描完毕**：精选池中暂无符合“超跌反弹”逻辑的标的。\n")
        else:
            f.write("| 代码 | 现价 | 止损位 | 得分 | RSI | 40D回撤 |\n")
            f.write("| --- | --- | --- | --- | --- | --- |\n")
            for r in sorted(results, key=lambda x: x['score'], reverse=True):
                f.write(f"| {r['code']} | {r['price']} | {r['stop']} | {r['score']} | {r['rsi']} | {r['dd']} |\n")
                
                # 同时写入历史记录
                with open(HISTORY_FILE, 'a') as hf:
                    hf.write(f"{r['date']},{r['code']},{r['name']},{r['price']},index,{r['price']},{r['stop']},{r['rsi']},{r['dd']},{r['score']}\n")

    print(f"分析完成，精选池信号数: {len(results)}")

if __name__ == "__main__":
    analyze()
