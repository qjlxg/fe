import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 数据适配 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
        ('openinterest', -1),
    )

# --- 实战策略逻辑 ---
class MultiFactorStrategy(bt.Strategy):
    params = (('atr_period', 14), ('atr_dist', 3.0), ('risk_pct', 0.02), ('min_score', 4))

    def __init__(self):
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.macd = bt.indicators.MACDHisto(self.data.close)
        self.bb = bt.indicators.BollingerBands(self.data.close, period=20)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        self.stop_price = None

    def next(self):
        if self.position:
            # 止损检测
            if self.data.close[0] < self.stop_price:
                self.close()
            return

        # 评分逻辑
        dd_40 = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 0.0001)
        score = 0
        if self.data.close[0] > self.ma5[0] and dd_40 < -0.04:
            score += 1
            if self.macd.histo[0] > self.macd.histo[-1]: score += 1
            if self.rsi[0] < 40: score += 1
            if self.data.close[0] < self.bb.lines.bot[0] * 1.05: score += 1
            if self.data.volume[0] > self.data.volume[-1] * 1.1: score += 1

        if score >= self.params.min_score:
            atr_v = self.atr[0] if self.atr[0] > 0 else self.data.close[0]*0.05
            # 计算止损
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_v, self.data.close[0]*0.93)
            
            # 1W资金的风险头寸
            risk_amt = self.broker.get_cash() * self.params.risk_pct
            risk_per_share = max(self.data.close[0] - self.stop_price, 0.001)
            size = int(risk_amt / risk_per_share)
            
            if size > 0:
                # 触发买入指令，下个bar(明日)开盘成交
                self.buy(size=size)

def run_single_backtest(file_path):
    code = os.path.basename(file_path).split('.')[0]
    if code == 'backtest_results': return None
    try:
        df = pd.read_csv(file_path, parse_dates=['日期']).sort_values('日期')
        # 修正1：必须有1年以上数据，否则统计无意义
        if len(df) < 250: return None
        
        cerebro = bt.Cerebro()
        # 修正2：禁用当天收盘成交，模拟真实交易延迟
        cerebro.broker.set_coc(False) 
        
        cerebro.adddata(ETFDataFeed(dataname=df))
        cerebro.addstrategy(MultiFactorStrategy)
        cerebro.broker.setcash(10000.0) # 初始1W
        
        # 修正3：加入佣金(万5)和滑点(千1)
        # 滑点是回测的“灵魂”，不加滑点回测全是废纸
        cerebro.broker.setcommission(commission=0.0005)
        cerebro.broker.set_slippage_fixed(0.001) 
        
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)

        strat_results = cerebro.run()
        if not strat_results: return None
        res = strat_results[0]
        
        sharpe = res.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        ann_ret = res.analyzers.ret.get_analysis().get('rnorm100', 0)
        
        # 修正4：数据污染过滤
        # 如果由于分红除权导致价格减半，回测会误判收益翻倍，必须剔除这类异常标的
        if ann_ret > 150 or (sharpe and sharpe > 10): return None

        return {
            '代码': code,
            '期末净值': round(cerebro.broker.getvalue(), 2),
            '年化收益%': round(ann_ret, 2),
            '最大回撤%': round(res.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0), 2),
            '夏普比率': round(sharpe or 0, 2)
        }
    except:
        return None

def main():
    data_dir = 'fund_data' if os.path.exists('fund_data') else './'
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    print(f"🕵️ 正在对 1W 初始资金进行‘生存测试’... 核心数: {cpu_count()}")

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(run_single_backtest, files)

    final_results = [r for r in results if r is not None]
    df_res = pd.DataFrame(final_results).sort_values('夏普比率', ascending=False)
    
    df_res.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
    print(f"✅ 结果已重写。请查看 backtest_results.csv 观察真实损益。")

if __name__ == '__main__':
    main()
