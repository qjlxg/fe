import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime

def backtest_rotation(data_dir, top_n=3, cost_rate=0.001, benchmark_code='510300'):
    """
    简易ETF动量轮动回测（ROC20排名）
    参数:
        data_dir: 数据文件夹
        top_n: 每日持仓数量
        cost_rate: 单次完全换仓成本（双边，如0.001 = 0.1%）
        benchmark_code: 基准ETF代码
    """
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    # 读取所有ETF数据
    dfs = []
    for f in all_files:
        code = os.path.basename(f).split('.')[0]
        try:
            df = pd.read_csv(f, usecols=['date', 'close'], parse_dates=['date'])
            df['code'] = code
            df = df.sort_values('date').reset_index(drop=True)
            df['roc20'] = df['close'].pct_change(20)
            df['daily_ret'] = df['close'].pct_change()
            dfs.append(df)
        except:
            continue
    
    if not dfs:
        print("❌ 无有效数据")
        return None
    
    big_df = pd.concat(dfs, ignore_index=True)
    big_df = big_df.dropna(subset=['roc20', 'daily_ret'])
    
    # 重塑为宽表：日期 x 代码
    pivot_ret = big_df.pivot(index='date', columns='code', values='daily_ret')
    pivot_roc = big_df.pivot(index='date', columns='code', values='roc20')
    
    # 每日选出ROC20排名前top_n的ETF（要求当日有ROC数据）
    valid_dates = pivot_roc.dropna(how='all').index  # 有至少一只ETF数据的日子
    
    strategy_rets = []
    benchmark_rets = []
    
    for date in valid_dates:
        roc_today = pivot_roc.loc[date].dropna()
        if len(roc_today) < top_n:
            continue  # 数据不足，空仓
        
        # 选出最强的top_n
        top_codes = roc_today.nlargest(top_n).index
        
        # 次日收益（使用shift(-1)避免前视）
        next_day = pivot_ret.index[pivot_ret.index > date]
        if len(next_day) == 0:
            continue
        next_date = next_day[0]
        
        next_rets = pivot_ret.loc[next_date, top_codes]
        if next_rets.isna().all():
            continue
        
        avg_ret = next_rets.mean()
        # 扣除换仓成本（假设每天完全换仓）
        strategy_ret = avg_ret - cost_rate
        strategy_rets.append({'date': next_date, 'ret': strategy_ret})
        
        # 基准收益
        if benchmark_code in pivot_ret.columns:
            bench_ret = pivot_ret.loc[next_date, benchmark_code]
            if not np.isnan(bench_ret):
                benchmark_rets.append({'date': next_date, 'ret': bench_ret})
    
    if not strategy_rets:
        print("❌ 无有效回测交易日")
        return None
    
    strat_df = pd.DataFrame(strategy_rets).set_index('date')
    strat_cum = (1 + strat_df['ret']).cumprod()
    
    # 指标计算
    total_ret = strat_cum.iloc[-1] - 1
    max_dd = (strat_cum / strat_cum.cummax() - 1).min()
    trading_days = len(strat_df)
    annualized = (1 + total_ret) ** (252 / trading_days) - 1 if trading_days > 0 else 0
    
    # 基准表现
    if benchmark_rets:
        bench_df = pd.DataFrame(benchmark_rets).set_index('date')
        bench_cum = (1 + bench_df['ret']).cumprod()
        bench_total = bench_cum.iloc[-1] - 1
        bench_annual = (1 + bench_total) ** (252 / len(bench_df)) - 1
        bench_mdd = (bench_cum / bench_cum.cummax() - 1).min()
    else:
        bench_total, bench_annual, bench_mdd = 0, 0, 0
    
    # 输出报告
    print("\n" + "="*60)
    print(f"📊 ETF动量轮动回测报告 (ROC20排名 | 持仓{top_n}只)")
    print(f"回测期间: {strat_cum.index[0].date()} 至 {strat_cum.index[-1].date()}")
    print(f"交易天数: {trading_days}")
    print("-"*60)
    print(f"【策略表现】")
    print(f"累计收益: {total_ret:+.2%}")
    print(f"年化收益: {annualized:+.2%}")
    print(f"最大回撤: {max_dd:.2%}")
    print(f"\n【基准表现 - {benchmark_code}】")
    print(f"累计收益: {bench_total:+.2%}")
    print(f"年化收益: {bench_annual:+.2%}")
    print(f"最大回撤: {bench_mdd:.2%}")
    print("-"*60)
    print(f"超额年化: {(annualized - bench_annual):+.2%}")
    print("="*60)
    
    return strat_cum
