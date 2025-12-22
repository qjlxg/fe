import pandas as pd
import os
import glob

def check_data_health():
    data_dir = 'fund_data'
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    if not files:
        print(f"❌ 错误：在 {data_dir} 文件夹下没找到任何 CSV 文件！")
        return

    report = []
    print(f"🚀 开始体检，共发现 {len(files)} 个标的...\n")

    for file in files:
        code = os.path.basename(file).replace('.csv', '')
        issues = []
        try:
            # 1. 读取测试
            df = pd.read_csv(file)
            df.columns = [c.strip() for c in df.columns]
            
            # 2. 检查关键列是否存在
            required_cols = ['日期', '开盘', '收盘', '最高', '最低']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                issues.append(f"缺少列: {missing_cols}")
            
            # 3. 检查数据长度 (回测至少需要50行)
            if len(df) < 50:
                issues.append(f"数据太短: 仅 {len(df)} 行")
            
            # 4. 检查日期格式与排序
            try:
                df['日期'] = pd.to_datetime(df['日期'])
                # 检查是否有重复日期
                if df['日期'].duplicated().any():
                    issues.append("存在重复日期")
            except:
                issues.append("日期格式异常")

            # 5. 检查数值异常 (0值或空值)
            if df[['开盘', '收盘', '最高', '最低']].isnull().values.any():
                issues.append("包含空值(NaN)")
            if (df[['开盘', '收盘', '最高', '最低']] <= 0).values.any():
                issues.append("包含0或负数价格")

            # 汇总结果
            if issues:
                report.append({'代码': code, '问题描述': " | ".join(issues)})
        
        except Exception as e:
            report.append({'代码': code, '问题描述': f"文件损坏或无法读取: {str(e)}"})

    # --- 输出诊断报告 ---
    print("="*50)
    print("📋 数据体检报告")
    print("="*50)
    
    if not report:
        print("✅ 完美！所有数据格式正确，可以直接开始回测。")
    else:
        print(f"⚠️ 警告：共发现 {len(report)} 个标的数据存在隐患：\n")
        df_report = pd.DataFrame(report)
        print(df_report.to_string(index=False))
        
        # 自动生成清理建议
        print("\n💡 建议操作：")
        print("1. 对于‘缺少列’的文件：请重新下载，确保包含 OHLC 基础数据。")
        print("2. 对于‘数据太短’的文件：如果该 ETF 上市不足 3 个月，建议先从回测池删除。")
        print("3. 对于‘日期格式’或‘0值’：这通常是爬虫抓取失败导致的，建议手动检查该 CSV。")
    print("="*50)

if __name__ == "__main__":
    check_data_health()
