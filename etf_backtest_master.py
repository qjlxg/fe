import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

class ETFDataFeed(bt.feeds.PandasData):
    params = (('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
              ('low', '最低'), ('close', '收盘'), ('volume', '成交量'))

class SyncStrategy(bt.Strategy):
    params = (('atr_period', 14), ('atr_dist', 3.0), ('min_score', 4))
    def __init__(self):
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.stop_price = None
    def next(self):
        if self.position:
            if self.data.close[0] < self.stop_price: self.close()
            return
        dd = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 0.00001)
        if self.data.close[0] > self.ma5[0] and dd < -0.04:
            atr_val = self.atr[0] if self.atr[0] > 0 else self.data.close[0] * 0.02
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_val, self.data.close[0] * 0.93)
            self.buy(size=100)

def run_backtest(file):
    code = os.path.basename(file).replace('.csv', '')
    try:
        df = pd.read_csv(file)
        df.columns = [c.strip() for c in df.columns]
        df['日期'] = pd.to_datetime(df['日期'])
        # 【核心修正】强制正序排列，确保均线计算正确
        df = df.sort_values('日期', ascending=True).reset_index(drop=True)
        if len(df) < 50: return None
        cerebro = bt.Cerebro()
        cerebro.broker.set_coc(True)
        cerebro.adddata(ETFDataFeed(dataname=df))
        cerebro.addstrategy(SyncStrategy)
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')
        res = cerebro.run()[0]
        ann_ret = res.analyzers.ret.get_analysis().get('rnorm100', 0)
        sharpe = res.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        max_dd = res.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0)
        if ann_ret > 120 or ann_ret < -50: return None
        return {'代码': code, '年化收益%': round(ann_ret, 2), '夏普比率': round(sharpe or 0, 2), '最大回撤%': round(max_dd, 2)}
    except: return None

def main():
    files = glob.glob(os.path.join('fund_data', "*.csv"))
    with Pool(cpu_count()) as pool:
        results = [r for r in pool.map(run_backtest, files) if r is not None]
    if results:
        df = pd.DataFrame(results)
        # 排除年化为0的无效品种
        df = df[df['年化收益%'] != 0]
        df = df.sort_values(by=['夏普比率', '年化收益%'], ascending=False)
        df.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
        print(f"✅ 精英选拔完成")

if __name__ == '__main__': main()
