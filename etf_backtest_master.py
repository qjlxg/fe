import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 数据适配：强制去除列名空格 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
    )

# --- 核心策略：完全对齐 analyzer_V12 ---
class SyncStrategy(bt.Strategy):
    params = (
        ('atr_period', 14), 
        ('atr_dist', 3.0),   # 对齐 3.0xATR 止损
        ('min_score', 4),    # 对齐 4 分门槛
    )

    def __init__(self):
        # 1. 指标对齐
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.macd = bt.indicators.MACDHisto(self.data.close)
        
        self.stop_price = None

    def next(self):
        # 2. 止损逻辑：如果已持仓，检测止损
        if self.position:
            if self.data.close[0] < self.stop_price:
                self.close(msg="触发止损")
            return

        # 3. 评分逻辑 (完全复刻分析脚本)
        dd = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 0.00001)
        
        score = 0
        if self.data.close[0] > self.ma5[0] and dd < -0.04:
            score += 1 # 基础分
            if self.macd[0] > self.macd[-1]: score += 1
            if self.rsi[0] < 40: score += 1
            # 回测中简化换手率逻辑，仅作为得分参考
            if self.data.volume[0] > bt.indicators.SMA(self.data.volume, period=14)[0]: score += 2

        # 4. 执行买入
        if score >= self.params.min_score:
            # 计算 ATR 止损位 (对齐分析脚本算法)
            atr_val = self.atr[0] if self.atr[0] > 0 else self.data.close[0] * 0.02
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_val, self.data.close[0] * 0.93)
            
            # 简单固定仓位模拟
            self.buy(size=100)

def run_backtest(file):
    code = os.path.basename(file).replace('.csv', '')
    try:
        df = pd.read_csv(file, parse_dates=['日期'])
        df.columns = [c.strip() for c in df.columns]
        if len(df) < 50: return None

        cerebro = bt.Cerebro()
        cerebro.broker.set_coc(True) # 允许信号当天成交，对齐分析看板
        cerebro.broker.setcash(10000.0)
        cerebro.broker.set_slippage_perc(0.001) # 模拟 0.1% 滑点摩擦

        cerebro.adddata(ETFDataFeed(dataname=df))
        cerebro.addstrategy(SyncStrategy)
        
        # 5. 加入高级分析器
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')

        results = cerebro.run()
        res = results[0]
        
        ann_ret = res.analyzers.ret.get_analysis().get('rnorm100', 0)
        sharpe = res.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        max_dd = res.analyzers.dd.get_analysis().get('max', {}).get('drawdown', 0)

        # 过滤数据异常值
        if ann_ret > 120 or ann_ret < -50: return None

        return {
            '代码': code,
            '年化收益%': round(ann_ret, 2),
            '夏普比率': round(sharpe or 0, 2),
            '最大回撤%': round(max_dd, 2)
        }
    except:
        return None

def main():
    data_dir = 'fund_data'
    target_files = glob.glob(os.path.join(data_dir, "*.csv"))
    print(f"🚀 正在按照 analyzer_V12 标准回测 {len(target_files)} 个标的...")
    
    with Pool(cpu_count()) as pool:
        results = [r for r in pool.map(run_backtest, target_files) if r is not None]

    if results:
        df = pd.DataFrame(results)
        # 排序逻辑：优先看夏普比率（稳定性），其次看年化
        df = df.sort_values(by=['夏普比率', '年化收益%'], ascending=False)
        df.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
        print(f"✅ 回测完成，报告已更新。")
    else:
        print("⚠️ 还是没有标的，请确认数据是否支持 min_score=4 的条件。")

if __name__ == '__main__':
    main()
