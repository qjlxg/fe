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

# --- 实战策略 ---
class MultiFactorStrategy(bt.Strategy):
    params = (('atr_period', 14), ('atr_dist', 3.0), ('risk_pct', 0.02), ('min_score', 4))

    def __init__(self):
        self.target = self.datas[0]
        self.benchmark = self.datas[1] # 沪深300
        self.ma5 = bt.indicators.SMA(self.target.close, period=5)
        self.ma20_bench = bt.indicators.SMA(self.benchmark.close, period=20)
        self.atr = bt.indicators.ATR(self.target, period=self.params.atr_period)
        self.hi40 = bt.indicators.Highest(self.target.close, period=40)
        self.stop_price = None

    def next(self):
        if self.position:
            if self.target.close[0] < self.stop_price: self.close()
            return
        
        # 大盘风控刹车
        if self.benchmark.close[0] < self.ma20_bench[0]: return

        # 简化版评分逻辑
        dd = (self.target.close[0] - self.hi40[0]) / (self.hi40[0] + 0.00001)
        if self.target.close[0] > self.ma5[0] and dd < -0.04:
            atr_val = self.atr[0] if self.atr[0] > 0 else self.target.close[0] * 0.02
            self.stop_price = min(self.target.close[0] - self.params.atr_dist * atr_val, self.target.close[0] * 0.93)
            # 计算仓位
            risk_amt = self.broker.get_cash() * self.params.risk_pct
            size = risk_amt / max((self.target.close[0] - self.stop_price), 0.001)
            self.buy(size=min(size, (self.broker.get_cash() * 0.25) / self.target.close[0]))

def run_backtest(file_info):
    t_file, b_file = file_info
    code = os.path.basename(t_file).replace('.csv', '')
    if code == '510300': return None
    try:
        cerebro = bt.Cerebro()
        cerebro.broker.set_coc(False) # 次日开盘成交
        cerebro.broker.set_slippage_perc(0.001) # 滑点
        cerebro.adddata(ETFDataFeed(dataname=pd.read_csv(t_file, parse_dates=['日期'], index_col='日期')))
        cerebro.adddata(ETFDataFeed(dataname=pd.read_csv(b_file, parse_dates=['日期'], index_col='日期')))
        cerebro.addstrategy(MultiFactorStrategy)
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        res = cerebro.run()[0]
        ann_ret = res.analyzers.ret.get_analysis().get('rnorm100', 0)
        sharpe = res.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        if ann_ret > 120 or ann_ret < -60: return None # 剔除异常数据
        return {'代码': code, '年化收益%': round(ann_ret, 2), '夏普比率': round(sharpe or 0, 2)}
    except: return None

def main():
    data_dir = 'fund_data'
    bench_file = os.path.join(data_dir, '510300.csv')
    if not os.path.exists(bench_file):
        print("❌ 核心错误：缺少 fund_data/510300.csv，回测无法运行！")
        return

    files = [(f, bench_file) for f in glob.glob(os.path.join(data_dir, "*.csv"))]
    with Pool(cpu_count()) as pool:
        results = [r for r in pool.map(run_backtest, files) if r is not None]

    if results:
        df = pd.DataFrame(results)
        # 黄金区间排序权重：年化8-20% & 夏普>2 设为最高优先级
        df['priority'] = df.apply(lambda r: 1 if (8<=r['年化收益%']<=20 and r['夏普比率']>=2) else 0, axis=1)
        df = df.sort_values(by=['priority', '夏普比率'], ascending=False).drop(columns=['priority'])
        df.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
        print(f"✅ 成功生成回测结果，共 {len(df)} 条记录。")
    else:
        print("⚠️ 未发现符合条件的标的。")

if __name__ == '__main__': main()
