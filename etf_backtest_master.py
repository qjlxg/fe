import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 1. 定义数据加载格式 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'),
        ('open', '开盘'),
        ('high', '最高'),
        ('low', '最低'),
        ('close', '收盘'),
        ('volume', '成交量'),
        ('openinterest', -1),
    )

# --- 2. 策略核心逻辑 (同步 analyzer_V12) ---
class SyncStrategy(bt.Strategy):
    params = (('atr_period', 14), ('atr_dist', 3.0))

    def __init__(self):
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.stop_price = None

    def next(self):
        # 如果已持仓，检查止损
        if self.position:
            if self.data.close[0] < self.stop_price:
                self.close()
            return

        # 计算40日回撤
        dd = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 1e-6)
        
        # 买入逻辑：站上MA5且超跌 > 4%
        if self.data.close[0] > self.ma5[0] and dd < -0.04:
            atr_val = self.atr[0] if self.atr[0] > 0 else self.data.close[0] * 0.02
            # 计算止损位
            self.stop_price = min(self.data.close[0] - self.params.atr_dist * atr_val, 
                                  self.data.close[0] * 0.93)
            self.buy(size=100)

# --- 3. 单个标的回测执行函数 ---
def run_backtest(file_path):
    code = os.path.basename(file_path).replace('.csv', '')
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        df['日期'] = pd.to_datetime(df['日期'])
        # 【关键补丁】强制正序排列
        df = df.sort_values('日期', ascending=True).reset_index(drop=True)
        
        if len(df) < 50: return None

        cerebro = bt.Cerebro()
        cerebro.addstrategy(SyncStrategy)
        
        data = ETFDataFeed(dataname=df)
        cerebro.adddata(data)
        cerebro.broker.setcash(10000.0) # 模拟你投入的1W元
        cerebro.broker.set_coc(True)    # 以当日收盘价成交

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='dd')

        results = cerebro.run()
        strat = results[0]

        # 获取统计指标
        ret_info = strat.analyzers.ret.get_analysis()
        sharpe_info = strat.analyzers.sharpe.get_analysis()
        dd_info = strat.analyzers.dd.get_analysis()

        ann_ret = ret_info.get('rnorm100', 0)
        sharpe = sharpe_info.get('sharperatio', 0)
        max_dd = dd_info.get('max', {}).get('drawdown', 0)

        # 过滤掉极端异常值
        if ann_ret > 200 or ann_ret < -90: return None

        return {
            '代码': code,
            '年化收益%': round(ann_ret, 2),
            '夏普比率': round(sharpe if sharpe else 0, 2),
            '最大回撤%': round(max_dd, 2)
        }
    except:
        return None

# --- 4. 主程序：多线程扫描 ---
if __name__ == '__main__':
    data_dir = 'fund_data'
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    print(f"🚀 开始回测，标的总数: {len(files)}")

    with Pool(cpu_count()) as pool:
        results = pool.map(run_backtest, files)

    # 过滤无效结果并排序
    valid_results = [r for r in results if r is not None and r['年化收益%'] != 0]
    df_results = pd.DataFrame(valid_results)
    
    if not df_results.empty:
        # 按照夏普比率降序，年化收益降序
        df_results = df_results.sort_values(by=['夏普比率', '年化收益%'], ascending=False)
        df_results.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
        print(f"✅ 回测报告已生成，已选出 {len(df_results)} 个有效品种。")
    else:
        print("❌ 未选出任何有效品种，请检查数据质量。")
