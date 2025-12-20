import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 数据源定义 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
        ('openinterest', -1),
    )

# --- 策略逻辑 (初始资金1W) ---
class MultiFactorStrategy(bt.Strategy):
    params = (('atr_period', 14), ('atr_dist', 3.0), ('risk_pct', 0.02), ('min_score', 4))

    def __init__(self):
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.macd = bt.indicators.MACDHisto(self.data.close)
        self.bb = bt.indicators.BollingerBands(self.data.close, period=20)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        self.avg_vol = bt.indicators.SMA(self.data.volume, period=5)
        self.stop_price = None

    def next(self):
        if self.position:
            if self.data.close[0] < self.stop_price:
                self.close()
            return

        dd_40 = (self.data.close[0] - self.hi40[0]) / self.hi40[0]
        score = 0
        if self.data.close[0] > self.ma5[0] and dd_40 < -0.04:
            score += 1
            if self.macd.histo[0] > self.macd.histo[-1]: score += 1
            if self.rsi[0] < 40: score += 1
            if self.data.close[0] < self.bb.lines.bot[0] * 1.05: score += 1
            if self.data.volume[0] > self.avg_vol[0] * 1.1: score += 1

        if score >= self.params.min_score:
            atr_v = self.atr[0] if self.atr[0] > 0 else self.data.close[0]*0.05
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_v, self.data.close[0]*0.93)
            risk_amt = self.broker.get_cash() * self.params.risk_pct
            size = int(risk_amt / max(self.data.close[0] - self.stop_price, 0.001))
            if size > 0: self.buy(size=size)

def run_single_backtest(file_path):
    code = os.path.basename(file_path).split('.')[0]
    if code == 'backtest_results': return None
    try:
        # 适配中文字段名
        df = pd.read_csv(file_path, parse_dates=['日期']).sort_values('日期')
        if len(df) < 60: return None
        
        cerebro = bt.Cerebro()
        cerebro.adddata(ETFDataFeed(dataname=df))
        cerebro.addstrategy(MultiFactorStrategy)
        cerebro.broker.setcash(10000.0) # 初始资金 1W
        cerebro.broker.setcommission(commission=0.0005)
        
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)

        strat = cerebro.run()[0]
        return {
            '代码': code,
            '期末净值': round(cerebro.broker.getvalue(), 2),
            '年化收益%': round(strat.analyzers.ret.get_analysis().get('rnorm100', 0), 2),
            '最大回撤%': round(strat.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0), 2),
            '夏普比率': round(strat.analyzers.sharpe.get_analysis().get('sharperatio', 0) or 0, 2)
        }
    except Exception:
        return None

def main():
    # 自动搜索 fund_data 目录或当前目录下的 csv
    data_dir = 'fund_data' if os.path.exists('fund_data') else './'
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    print(f"🚀 并行回测启动 | 核心数: {cpu_count()} | 目标: {len(files)} 只标的")

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(run_single_backtest, files)

    final_results = [r for r in results if r is not None]
    df_res = pd.DataFrame(final_results).sort_values('夏普比率', ascending=False)
    
    # 结果保存
    df_res.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
    print(f"✅ 完成！结果已存入 backtest_results.csv")

if __name__ == '__main__':
    main()
