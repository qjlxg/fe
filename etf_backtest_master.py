import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 数据适配器 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
        ('openinterest', -1),
    )

# --- 核心回测策略 ---
class MultiFactorStrategy(bt.Strategy):
    params = (
        ('atr_period', 14), 
        ('atr_dist', 3.0), 
        ('risk_pct', 0.02), 
        ('min_score', 4)
    )

    def __init__(self):
        self.target = self.datas[0]
        self.benchmark = self.datas[1]
        
        # 指标计算
        self.ma5 = bt.indicators.SMA(self.target.close, period=5)
        self.ma20_bench = bt.indicators.SMA(self.benchmark.close, period=20)
        self.rsi = bt.indicators.RSI(self.target.close, period=14)
        self.macd = bt.indicators.MACDHisto(self.target.close)
        self.bb = bt.indicators.BollingerBands(self.target.close, period=20)
        self.atr = bt.indicators.ATR(self.target, period=self.params.atr_period)
        self.hi40 = bt.indicators.Highest(self.target.close, period=40)
        
        self.stop_price = None

    def next(self):
        if self.position:
            if self.target.close[0] < self.stop_price:
                self.close()
            return

        # 大盘风控刹车：510300 必须在 20 日线上方
        if self.benchmark.close[0] < self.ma20_bench[0]:
            return

        # 评分逻辑
        dd = (self.target.close[0] - self.hi40[0]) / (self.hi40[0] + 0.00001)
        score = 0
        if self.target.close[0] > self.ma5[0] and dd < -0.04:
            score += 1
            if self.macd[0] > self.macd[-1]: score += 1
            if self.rsi[0] < 40: score += 1
            if self.target.close[0] < self.bb.lines.bot[0] * 1.05: score += 1
            if self.target.volume[0] > bt.indicators.SMA(self.target.volume, period=14)[0] * 1.1:
                score += 1

        if score >= self.params.min_score:
            atr_val = self.atr[0] if self.atr[0] > 0 else self.target.close[0] * 0.02
            self.stop_price = min(self.target.close[0] - self.params.atr_dist * atr_val, self.target.close[0] * 0.93)
            risk_amount = self.broker.get_cash() * self.params.risk_pct
            size = risk_amount / max((self.target.close[0] - self.stop_price), 0.001)
            # 权重控制
            max_size = (self.broker.get_cash() * 0.25) / self.target.close[0]
            self.buy(size=min(size, max_size))

# --- 执行引擎 ---
def run_backtest(file_info):
    target_file, benchmark_file = file_info
    code = os.path.basename(target_file).replace('.csv', '')
    if code == '510300': return None 

    try:
        cerebro = bt.Cerebro()
        cerebro.broker.set_coc(False) # 信号发出后次日开盘成交
        cerebro.broker.setcash(10000.0)
        cerebro.broker.set_slippage_perc(0.001) # 0.1% 滑点

        df_target = pd.read_csv(target_file, parse_dates=['日期'], index_col='日期')
        if len(df_target) < 60: return None
        cerebro.adddata(ETFDataFeed(dataname=df_target), name="target")

        df_bench = pd.read_csv(benchmark_file, parse_dates=['日期'], index_col='日期')
        cerebro.adddata(ETFDataFeed(dataname=df_bench), name="benchmark")

        cerebro.addstrategy(MultiFactorStrategy)
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)

        results = cerebro.run()
        res = results[0]
        ann_ret = res.analyzers.ret.get_analysis().get('rnorm100', 0)
        sharpe = res.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        max_dd = res.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0)

        # 剔除逻辑：异常跳空或数据污染
        if ann_ret > 120 or ann_ret < -60: return None

        return {
            '代码': code,
            '期末净值': round(cerebro.broker.getvalue(), 2),
            '年化收益%': round(ann_ret, 2),
            '最大回撤%': round(max_dd, 2),
            '夏普比率': round(sharpe if sharpe else 0, 2)
        }
    except: return None

def main():
    data_dir = 'fund_data'
    benchmark_file = os.path.join(data_dir, '510300.csv')
    if not os.path.exists(benchmark_file): return

    files = glob.glob(os.path.join(data_dir, "*.csv"))
    tasks = [(f, benchmark_file) for f in files]

    with Pool(cpu_count()) as pool:
        results = [r for r in pool.map(run_backtest, tasks) if r is not None]

    df = pd.DataFrame(results)
    
    # --- 核心修改：实战黄金区间排序逻辑 ---
    # 定义黄金区间权重：夏普比率 > 2 且 年化在 8-20% 之间的排在最前
    def priority_sort(row):
        is_gold = 1 if (row['年化收益%'] >= 8 and row['年化收益%'] <= 20 and row['夏普比率'] >= 2) else 0
        return (is_gold, row['夏普比率'], row['年化收益%'])

    if not df.empty:
        # 应用排序：先看是否黄金区间，再看夏普，最后看年化
        df['sort_key'] = df.apply(priority_sort, axis=1)
        df = df.sort_values(by='sort_key', ascending=False).drop(columns=['sort_key'])
        
        df.to_csv('backtest_results_pro.csv', index=False, encoding='utf_8_sig')
        print("✅ 回测完成！已优先排列 8%-20% 年化且夏普 > 2 的稳健标的。")
        print(df.head(20))

if __name__ == '__main__':
    main()
