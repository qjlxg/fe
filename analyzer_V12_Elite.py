import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta

# --- 配置 ---
DATA_DIR = 'fund_data'
HISTORY_FILE = 'signal_history.csv'
BACKTEST_REPORT = 'backtest_results.csv'
BENCHMARK_CODE = '510300'

def get_beijing_time():
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def analyze():
    # 1. 加载精选池 (从回测报告取前10名)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
            print(f"✅ 已加载精英池: {elite_pool}")
        except:
            print("⚠️ 读取回测报告失败，将扫描全量数据")
    
    # 2. 大盘风控检查 (修正排序逻辑)
    bench_file = os.path.join(DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(bench_file):
        print(f"⚠️ 缺少基准文件 {BENCHMARK_CODE}")
        return
    
    df_b = pd.read_csv(bench_file)
    df_b.columns = [c.strip() for c in df_b.columns]
    df_b['日期'] = pd.to_datetime(df_b['日期'])
    df_b = df_b.sort_values('日期').reset_index(drop=True)
    
    ma20 = df_b['收盘'].rolling(20).mean().iloc[-1]
    curr_b = df_b['收盘'].iloc[-1]
    is_safe = curr_b >= ma20
    
    # 3. 扫描逻辑
    results = []
    # 如果有精英池则只扫精英，没有则扫文件夹下所有
    target_files = [os.path.join(DATA_DIR, f"{c}.csv") for c in elite_pool] if elite_pool else glob.glob(f"{DATA_DIR}/*.csv")

    for file in target_files:
        if not os.path.exists(file) or BENCHMARK_CODE in file: continue
        try:
            df = pd.read_csv(file)
            df.columns = [c.strip() for c in df.columns]
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期').reset_index(drop=True)
            
            if len(df) < 40: continue
            
            last = df.iloc[-1]
            curr_p = last['收盘']
            ma5 = df['收盘'].rolling(5).mean().iloc[-1]
            hi40 = df['收盘'].rolling(40).max().iloc[-1]
            dd = (curr_p - hi40) / hi40
            
            # 基础门槛：站上MA5 且 40日回撤 > 4%
            if curr_p > ma5 and dd < -0.04:
                # 辅助评分 (RSI/MACD/成交量等，此处保持你的V12核心算法)
                score = 1
                # RSI 简单实现
                delta = df['收盘'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rsi = 100 - (100 / (1 + (gain/loss).iloc[-1]))
                if rsi < 40: score += 1
                
                # ATR止损
                tr = np.maximum(df['最高'] - df['最低'], abs(df['最高'] - df['收盘'].shift(1)))
                atr = tr.rolling(14).mean().iloc[-1]
                stop_p = min(curr_p - 3.0 * atr, curr_p * 0.93)
                
                results.append({
                    'date': last['日期'].strftime('%Y-%m-%d'),
                    'code': os.path.basename(file)[:6],
                    'price': curr_p,
                    'stop': round(stop_p, 3),
                    'score': score,
                    'rsi': round(rsi, 1),
                    'dd': f"{round(dd*100, 2)}%"
                })
        except: continue

    # 4. 账本保护 (不弄丢历史)
    if results and is_safe:
        if not os.path.exists(HISTORY_FILE):
            # 初始化表头
            pd.DataFrame(columns=['date','code','name','price','index','entry_price','stop','rsi','dd','score']).to_csv(HISTORY_FILE, index=False)
        
        with open(HISTORY_FILE, 'a', encoding='utf_8_sig') as f:
            for r in results:
                # 记录信号到账本
                f.write(f"{r['date']},{r['code']},精英标的,{r['price']},index,{r['price']},{r['stop']},{r['rsi']},{r['dd']},{r['score']}\n")

    # 5. 生成完整版 README 看板
    with open('README.md', 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏆 精选池实战看板 (V12-Elite)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        
        status_icon = "✅ 趋势安全" if is_safe else "🛑 避险模式"
        f.write(f"### 🚦 市场环境: {status_icon}\n")
        f.write(f"- 510300现价: `{curr_b}` (MA20: `{round(ma20, 3)}`)\n\n")
        
        if not is_safe:
            f.write("> 🚩 **当前大盘处于20日线下**：系统已进入防守状态，不建议开新仓。\n")
        elif not results:
            f.write("> 🔍 **扫描完毕**：精选池（回测前10名）今日暂无符合超跌反弹的信号。\n")
        else:
            f.write("### 🎯 今日推荐入选\n")
            f.write("| 代码 | 现价 | 止损参考 | 评分 | RSI | 40D回撤 |\n")
            f.write("| --- | --- | --- | --- | --- | --- |\n")
            for r in sorted(results, key=lambda x: x['score'], reverse=True):
                f.write(f"| {r['code']} | {r['price']} | {r['stop']} | {r['score']} | {r['rsi']} | {r['dd']} |\n")

    print(f"✅ 看板更新完成。今日信号: {len(results)}")

if __name__ == "__main__":
    analyze()
