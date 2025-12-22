import backtrader as bt
import pandas as pd
import os
import glob
from multiprocessing import Pool, cpu_count

# --- 强制对齐的数据解析器 ---
class ETFDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', '日期'), ('open', '开盘'), ('high', '最高'),
        ('low', '最低'), ('close', '收盘'), ('volume', '成交量'),
    )

# --- 逻辑完全同步策略 ---
class MultiFactorStrategy(bt.Strategy):
    params = (('min_score', 1),) # 哪怕设为1，也要确保逻辑通畅

    def __init__(self):
        # 这里的指标计算必须和分析脚本一模一样
        self.ma5 = bt.indicators.SMA(self.data.close, period=5)
        self.hi40 = bt.indicators.Highest(self.data.close, period=40)
        # 其他指标按需添加，目前先保通
        
    def next(self):
        if self.position: return

        # 复刻分析脚本最核心逻辑
        dd = (self.data.close[0] - self.hi40[0]) / (self.hi40[0] + 0.00001)
        
        # 只要站上MA5且有一定回撤
        if self.data.close[0] > self.ma5[0] and dd < -0.04:
            # 这里的成交逻辑：强制信号当天成交
            self.buy(size=100) 

def run_one(file):
    code = os.path.basename(file).replace('.csv', '')
    try:
        # 读取并清洗数据，确保列名没有空格
        df = pd.read_csv(file, parse_dates=['日期'])
        df.columns = [c.strip() for c in df.columns]
        if len(df) < 50: return None

        cerebro = bt.Cerebro()
        # 【关键修改】：允许信号当天收盘成交，强制对齐分析脚本
        cerebro.broker.set_coc(True) 
        cerebro.broker.setcash(10000.0)
        
        data = ETFDataFeed(dataname=df)
        cerebro.adddata(data)
        cerebro.addstrategy(MultiFactorStrategy)
        cerebro.addanalyzer(bt.analyzers.Returns, _name='ret')
        
        results = cerebro.run()
        ann_ret = results[0].analyzers.ret.get_analysis().get('rnorm100', 0)
        
        return {'代码': code, '年化收益%': round(ann_ret, 2)}
    except Exception as e:
        return None

def main():
    data_dir = 'fund_data'
    target_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    print(f"开始回测 {len(target_files)} 个文件...")
    
    with Pool(cpu_count()) as pool:
        results = [r for r in pool.map(run_one, target_files) if r is not None]

    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(by='年化收益%', ascending=False)
        df.to_csv('backtest_results.csv', index=False, encoding='utf_8_sig')
        print(f"✅ 生成结果: {len(df)} 条记录")
    else:
        # 如果还是没有，打印出第一个文件的列名，排查格式问题
        print("⚠️ 依然没有标的。")
        test_file = target_files[0]
        temp_df = pd.read_csv(test_file)
        print(f"数据列名排查: {list(temp_df.columns)}")

if __name__ == '__main__':
    main()
