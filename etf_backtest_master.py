import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 1. 数据适配器 (增加成交额映射) ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
        ('openinterest', -1),
    )

# --- 2. 策略逻辑 (修正交易撮合) ---
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
        # 已经在持仓中，仅维护止损
        if self.position:
            if self.data.close[0] < self.stop_price:
                self.close()
            return

        # 评分计算
        dd_40 = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 0.001)
        score = 0
        if self.data.close[0] > self.ma5[0] and dd_40 < -0.04:
            score += 1
            if self.macd.histo[0] > self.macd.histo[-1]: score += 1
            if self.rsi[0] < 40: score += 1
            if self.data.close[0] < self.bb.lines.bot[0] * 1.05: score += 1
            if self.data.volume[0] > self.data.volume[-1] * 1.1: score += 1

        if score >= self.params.min_score:
            # 基于当前bar计算止损，但买入指令将在下一个bar(明天)执行
            atr_v = self.atr[0] if self.atr[0] > 0 else self.data.close[0]*0.05
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_v, self.data.close[0]*0.93)
            
            risk_amt = self.broker.get_cash() * self.params.risk_pct
            risk_per_share = max(self.data.close[0] - self.stop_price, 0.001)
            size = int(risk_amt / risk_per_share)
            
            if size > 0:
                self.buy(size=size) # Backtrader默认在下个Bar以开盘价成交

def run_single_backtest(file_path):
    code = os.path.basename(file_path).split('.')[0]
    try:
        df = pd.read_csv(file_path, parse_dates=['日期']).sort_values('日期')
        # 过滤数据过短的标的 (至少1.5年数据才有参考意义)
        if len(df) < 300: return None
        
        cerebro = bt.Cerebro()
        # 重要：关闭“收盘价撮合”，启用“次日成交”
        cerebro.broker.set_coc(False) 
        
        cerebro.adddata(ETFDataFeed(dataname=df))
        cerebro.addstrategy(MultiFactorStrategy)
        
        cerebro.broker.setcash(10000.0)
        # 佣金万五 + 滑点千一
        cerebro.broker.setcommission(commission=0.0005)
        cerebro.broker.set_slippage_fixed(0.001) 
        
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)

        strat = cerebro.run()[0]
        
        sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        # 异常数据过滤：如果单年年化收益超过200%或夏普超过10，通常是复权问题，剔除
        ann_ret = strat.analyzers.ret.get_analysis().get('rnorm100', 0)
        if ann_ret > 200 or (sharpe and sharpe > 10): return None

        return {
            '代码': code,
            '回测天数': len(df),
            '最终价值': round(cerebro.broker.getvalue(), 2),
            '年化收益%': round(ann_ret, 2),
            '最大回撤%': round(strat.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0), 2),
            '夏普比率': round(sharpe or 0, 2)
        }
    except:
        return None

def main():
    data_dir = 'fund_data' if os.path.exists('fund_data') else './'
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    print(f"🕵️ 启动‘冷水版’深度回测... 核心数: {cpu_count()}")

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(run_single_backtest, files)

    final_results = [r for r in results if r is not None]
    df_res = pd.DataFrame(final_results).sort_values('夏普比率', ascending=False)
    
    df_res.to_csv('backtest_results_filtered.csv', index=False, encoding='utf_8_sig')
    print(f"📊 过滤后的真实排名已生成。夏普比率前5名：\n{df_res.head(5)}")

if __name__ == '__main__':
    main()
