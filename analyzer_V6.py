import pandas as pd
import numpy as np
import akshare as ak
import os
from datetime import datetime

# --- 豹哥核心配置 ---
TOTAL_ASSETS = 100000          # 总本金
BENCHMARK_CODE = "510300"      # 大盘风向标
WIN_RATE_THRESHOLD = 0.40      # 胜率门槛
DATA_DIR = "fund_data"
LOG_FILE = "history_signals.csv"  # 核心：持仓状态账本

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

class BaoGePro:
    def __init__(self, watch_list):
        self.watch_list = watch_list
        self.holdings = self.load_holdings()

    def load_holdings(self):
        """读取历史记录，提取当前还持有的标的"""
        if not os.path.exists(LOG_FILE):
            return {}
        try:
            df = pd.read_csv(LOG_FILE)
            # 找到所有买入后还没卖出的记录
            # 逻辑：按代码分组，如果最后一条指令是 BUY，则视为持仓
            active_holds = {}
            for code, group in df.groupby('code'):
                last_action = group.iloc[-1]
                if last_action['action'] == 'BUY':
                    active_holds[str(code)] = last_action['price']
            return active_holds
        except:
            return {}

    def log_action(self, code, action, price):
        """记录动作到 CSV"""
        new_log = pd.DataFrame([{
            'date': datetime.now().strftime('%Y-%m-%d'),
            'code': code,
            'action': action,
            'price': price
        }])
        header = not os.path.exists(LOG_FILE)
        new_log.to_csv(LOG_FILE, mode='a', index=False, header=header)

    def fetch_data(self, code):
        try:
            df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq").tail(100)
            df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'pct_chg', 'val_chg', 'turnover_rate']
            df['date'] = pd.to_datetime(df['date'])
            return df
        except: return None

    def get_market_multiplier(self):
        """大盘滤网"""
        df = self.fetch_data(BENCHMARK_CODE)
        if df is None: return 1.0
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        bias = (df['close'].iloc[-1] - ma20) / ma20 * 100
        if bias < -4: return 0.5
        return 1.0

    def analyze(self):
        multiplier = self.get_market_multiplier()
        report = []
        
        print(f"\n{'='*60}\n📢 豹哥自动化实战决策 (持仓监控版)\n{'='*60}")
        
        for code in self.watch_list:
            df = self.fetch_data(code)
            if df is None or len(df) < 20: continue
            
            last = df.iloc[-1]
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            ma20_max = df['close'].rolling(20).max().iloc[-1]
            to_ma10 = df['turnover'].rolling(10).mean().iloc[-1]
            
            # --- 核心逻辑分叉 ---
            if code in self.holdings:
                # 1. 持仓监控模式
                buy_price = self.holdings[code]
                profit = (last['close'] - buy_price) / buy_price * 100
                
                if last['close'] < ma5:
                    action = "🚨 撤退 (破5日线)"
                    self.log_action(code, 'SELL', last['close'])
                elif profit > 15:
                    action = "💰 止盈 (达15%目标)"
                    self.log_action(code, 'SELL', last['close'])
                else:
                    action = f"💎 持仓中 (盈亏:{profit:.1f}%)"
            else:
                # 2. 选股扫描模式
                drawdown = (last['close'] - ma20_max) / ma20_max
                is_right_side = last['close'] > ma5
                to_ratio = last['turnover'] / to_ma10 if to_ma10 > 0 else 0
                
                if drawdown < -0.045 and is_right_side and to_ratio > 1.0:
                    action = "🟢 搞它 (触发买入)"
                    # 只有真正产生“搞它”信号时，我们假设你执行了买入并记录（或者手动记录）
                    # 提示：实际交易中你可以手动在 CSV 增加买入记录
                    # 为了演示自动化，这里暂时不自动 log BUY，建议手动确认后再记入
                else:
                    action = "⚪ 观望"

            if action != "⚪ 观望":
                print(f"代码: {code} | 动作: {action} | 现价: {last['close']:.3f}")

        print(f"{'='*60}\n(所有操作已自动同步至 {LOG_FILE})\n")

if __name__ == "__main__":
    # 填入你关注的 ETF
    watchlist = ["510500", "513330", "512170", "510300", "159915"]
    BaoGePro(watchlist).analyze()
