import pandas as pd
import numpy as np
import akshare as ak
import os
from datetime import datetime, timedelta

# --- 豹哥核心配置 ---
TOTAL_ASSETS = 100000          # 总本金
RISK_LEVEL = 0.01              # 单笔交易风险系数 (1% 风险)
BENCHMARK_CODE = "510300"      # 沪深300 ETF 作为大盘风向标
WIN_RATE_THRESHOLD = 0.40      # 历史胜率准入门槛
TURNOVER_CONFIRM = 1.0         # 换手倍率阈值
DATA_DIR = "fund_data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

class BaoGeTrader:
    def __init__(self, codes):
        self.codes = codes
        self.results = []

    def fetch_data(self, code):
        """自动抓取最新行情 (接入AkShare)"""
        try:
            # 场内基金数据接口
            df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
            df = df.rename(columns={
                '日期': 'date', '开盘': 'open', '收盘': 'close', 
                '最高': 'high', '最低': 'low', '成交量': 'volume', '换手率': 'turnover'
            })
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            print(f"❌ 抓取 {code} 失败: {e}")
            return None

    def get_market_weather(self):
        """判断大盘环境：确定仓位乘数"""
        df = self.fetch_data(BENCHMARK_CODE)
        if df is None: return 1.0, "🌤️ 正常"
        
        df['MA20'] = df['close'].rolling(20).mean()
        last_close = df['close'].iloc[-1]
        last_ma20 = df['MA20'].iloc[-1]
        bias = (last_close - last_ma20) / last_ma20 * 100
        
        if bias < -4: return 0.5, "❄️ 深冬 (极轻仓)"
        if bias < -2: return 0.8, "🌨️ 初冬 (谨慎)"
        if bias > 5:  return 0.7, "🥵 盛夏 (防冲高回落)"
        return 1.0, "🌤️ 早春 (正常)"

    def fast_win_rate(self, df):
        """高性能向量化回测：计算该标的历史信号胜率"""
        if len(df) < 60: return 0.0
        
        df = df.copy()
        # 计算指标
        df['MA5'] = df['close'].rolling(5).mean()
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
        
        # 定义信号：RSI超卖后站上5日线
        df['signal'] = (df['rsi'].shift(1) < 35) & (df['close'] > df['MA5'])
        
        # 计算信号发出后5日内的最高涨幅是否超过2%
        df['future_max'] = df['close'].rolling(5).max().shift(-5)
        df['is_win'] = (df['future_max'] - df['close']) / df['close'] >= 0.02
        
        wins = df[df['signal']]['is_win'].sum()
        total = df['signal'].sum()
        
        return wins / total if total > 0 else 0.0

    def analyze(self):
        multiplier, weather = self.get_market_weather()
        print(f"\n{'='*60}\n🚀 豹哥实战报告 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"当前大盘环境: {weather} | 仓位乘数: {multiplier}")
        print(f"{'='*60}")
        print(f"{'代码':<8} | {'状态':<10} | {'参考价':<8} | {'建议仓位':<8} | {'止损价'}")
        print(f"{'-'*60}")

        for code in self.codes:
            df = self.fetch_data(code)
            if df is None or len(df) < 30: continue
            
            # 基础指标
            last = df.iloc[-1]
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            ma20_max = df['close'].rolling(20).max().iloc[-1]
            to_ma10 = df['turnover'].rolling(10).mean().iloc[-1]
            
            # ATR风控计算
            tr = pd.concat([(df['high'] - df['low']), 
                            (df['high'] - df['close'].shift()).abs(), 
                            (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
            atr = tr.rolling(14).mean().iloc[-1]
            
            # 核心判断逻辑
            drawdown = (last['close'] - ma20_max) / ma20_max
            is_right_side = last['close'] > ma5
            to_ratio = last['turnover'] / to_ma10 if to_ma10 > 0 else 0
            
            status = "⚪ 观望"
            pos_str = "---"
            stop_price = "---"

            # 1. 卖出逻辑 (假设你已持仓，这里判断是否该卖)
            if last['close'] < ma5:
                status = "🚨 撤退"
            
            # 2. 买入逻辑 (不绿不买，转强才买)
            elif drawdown < -0.045:
                if is_right_side:
                    win_rate = self.fast_win_rate(df)
                    if to_ratio >= TURNOVER_CONFIRM and win_rate >= WIN_RATE_THRESHOLD:
                        status = "🟢 搞它"
                        # 风险头寸计算
                        stop_val = last['close'] - (2 * atr)
                        stop_price = f"{stop_val:.3f}"
                        risk_per_share = last['close'] - stop_val
                        if risk_per_share > 0:
                            # 算出理论应买入金额
                            raw_pos = (TOTAL_ASSETS * RISK_LEVEL) / (risk_per_share / last['close'])
                            final_pos = min(raw_pos * multiplier, TOTAL_ASSETS * 0.3)
                            pos_str = f"{final_pos/10000:.1f}万"
                    else:
                        status = "🟡 信号弱"
                else:
                    status = "🟡 等突破"

            if status != "⚪ 观望":
                print(f"{code:<8} | {status:<10} | {last['close']:<10.3f} | {pos_str:<10} | {stop_price}")

        print(f"{'-'*60}")
        print("💡 豹哥嘱托：控制仓位是生存之本，止损线是生命线！")

# --- 使用示例 ---
if __name__ == "__main__":
    # 在这里输入你想监控的 ETF 或 股票代码
    my_watch_list = ["510500", "512170", "515050", "159915", "513330"]
    trader = BaoGeTrader(my_watch_list)
    trader.analyze()
