import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta

# --- 核心配置 (请确保文件名与你仓库一致) ---
DATA_DIR = 'fund_data'
HISTORY_FILE = 'signal_history.csv'     
BACKTEST_REPORT = 'backtest_results.csv'
NAME_LIST_FILE = 'ETF列表.xlsx'
BENCHMARK_CODE = '510300'

def get_beijing_time():
    """获取北京时间用于看板展示"""
    return (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

def analyze():
    print(f"🚀 启动 V12-Elite 分析系统... {get_beijing_time()}")

    # 1. 加载名称映射表 (从源头解决名称显示问题)
    name_map = {}
    if os.path.exists(NAME_LIST_FILE):
        try:
            df_n = pd.read_csv(NAME_LIST_FILE, dtype={'证券代码': str})
            # 去除代码和简称的空格
            df_n['证券代码'] = df_n['证券代码'].str.strip()
            df_n['证券简称'] = df_n['证券简称'].str.strip()
            name_map = dict(zip(df_n['证券代码'], df_n['证券简称']))
            print(f"✅ 成功映射 {len(name_map)} 个基金名称")
        except Exception as e:
            print(f"⚠️ 名称映射表加载失败: {e}")

    # 2. 加载精英池 (回测前10名)
    elite_pool = []
    if os.path.exists(BACKTEST_REPORT):
        try:
            df_bt = pd.read_csv(BACKTEST_REPORT, dtype={'代码': str})
            elite_pool = df_bt['代码'].head(10).tolist()
            print(f"✅ 精英池已锁定: {elite_pool}")
        except:
            print("⚠️ 未能加载回测报告，所有信号将标为普通")
    
    # 3. 大盘风控逻辑 (基于 510300 MA20)
    bench_file = os.path.join(DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(bench_file): 
        print(f"❌ 关键错误: 缺少大盘数据 {bench_file}")
        return
    
    df_b = pd.read_csv(bench_file)
    df_b.columns = [c.strip() for c in df_b.columns]
    df_b['日期'] = pd.to_datetime(df_b['日期'])
    df_b = df_b.sort_values('日期').reset_index(drop=True)
    
    curr_b = df_b['收盘'].iloc[-1]
    ma20 = df_b['收盘'].rolling(20).mean().iloc[-1]
    is_safe = curr_b >= ma20
    print(f"🚦 大盘状态: {'安全' if is_safe else '风险'} (现价:{curr_b:.3f} / MA20:{ma20:.3f})")

    # 4. 扫描所有标的产生信号
    results = []
    target_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    for file in target_files:
        code = os.path.basename(file)[:6]
        if code == BENCHMARK_CODE: continue # 跳过大盘标的本身
        
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
            
            # 策略核心：站上MA5 且 40日高位回撤超过4%
            if curr_p > ma5 and dd < -0.04:
                # 计算ATR止损 (3倍ATR 或 强制7%)
                tr = np.maximum(df['最高'] - df['最低'], 
                                np.maximum(abs(df['最高'] - df['收盘'].shift(1)), 
                                           abs(df['最低'] - df['收盘'].shift(1))))
                atr = tr.rolling(14).mean().iloc[-1]
                stop_p = min(curr_p - 3.0 * atr, curr_p * 0.93)
                
                # 从映射表获取名称，获取不到则用代码
                real_name = name_map.get(code, f"ETF_{code}")
                
                results.append({
                    'date': last['日期'].strftime('%Y-%m-%d'),
                    'code': code,
                    'name': real_name,
                    'price': round(curr_p, 3),
                    'stop': round(stop_p, 3),
                    'dd': f"{round(dd*100, 2)}%"
                })
        except:
            continue

    # 5. 精准对齐写入 13 列账本
    # 账本表头定义
    header = "date,code,name,entry_price,index,price,stop,rsi,dd,score,lots,pos_pct,turnover\n"
    
    if results and is_safe:
        file_exists = os.path.exists(HISTORY_FILE)
        with open(HISTORY_FILE, 'a', encoding='utf_8_sig') as f:
            if not file_exists:
                f.write(header)
            for r in results:
                # 按照 entry_price(第4列) 和 price(第6列) 均填入当前价的逻辑
                # 后面 3 个空逗号补齐 lots, pos_pct, turnover
                line = f"{r['date']},{r['code']},{r['name']},{r['price']},index,{r['price']},{r['stop']},0,{r['dd']},4,,,\n"
                f.write(line)
        print(f"💾 账本已更新，新增 {len(results)} 条记录")

    # 6. 更新 README.md 实时看板
    with open('README.md', 'w', encoding='utf_8_sig') as f:
        f.write(f"# 🏆 精选池实战看板 (V12-Elite)\n\n")
        f.write(f"更新时间: `{get_beijing_time()}`\n\n")
        f.write(f"### 🚦 市场环境: {'✅ 趋势安全' if is_safe else '🛑 风险避险'}\n")
        f.write(f"- 510300 现价: `{curr_b:.3f}` (MA20: `{ma20:.3f}`)\n\n")
        
        if not is_safe:
            f.write("> ⚠️ 当前处于风险区域，策略已暂停新信号触发，请关注存量标的止损。\n\n")
        
        f.write("### 🎯 今日推荐入选\n")
        if results:
            f.write("| 代码 | 名称 | 现价 | 止损参考 | 40D回撤 | 身份 |\n")
            f.write("| --- | --- | --- | --- | --- | --- |\n")
            # 排序：精英在前
            results_sorted = sorted(results, key=lambda x: x['code'] in elite_pool, reverse=True)
            for r in results_sorted:
                tag = "🏆精英" if r['code'] in elite_pool else "⚪普通"
                f.write(f"| {r['code']} | {r['name']} | {r['price']} | {r['stop']} | {r['dd']} | {tag} |\n")
        else:
            f.write("*今日暂无满足筛选条件的标的。*\n")

    print(f"🏁 分析流程结束。")

if __name__ == "__main__":
    analyze()
