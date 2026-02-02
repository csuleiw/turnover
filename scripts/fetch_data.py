import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime
import pytz
import time

# 文件路径
DATA_FILE = 'data/market_data.json'

def get_market_data():
    print("正在获取A股实时行情(尝试分市场合并策略)...")
    
    df_all = pd.DataFrame()
    
    # 策略：分别获取沪A、深A、京A数据并合并，这比获取全市场数据更稳定
    # 1. 沪A
    try:
        print("  - 获取沪A数据...")
        df_sh = ak.stock_sh_a_spot_em()
        if df_sh is not None and not df_sh.empty:
            df_all = pd.concat([df_all, df_sh], ignore_index=True)
    except Exception as e:
        print(f"  - 沪A获取失败: {e}")

    # 2. 深A
    try:
        print("  - 获取深A数据...")
        df_sz = ak.stock_sz_a_spot_em()
        if df_sz is not None and not df_sz.empty:
            df_all = pd.concat([df_all, df_sz], ignore_index=True)
    except Exception as e:
        print(f"  - 深A获取失败: {e}")
        
    # 3. 京A (可选，防止接口报错影响整体)
    try:
        print("  - 获取京A数据...")
        df_bj = ak.stock_bj_a_spot_em()
        if df_bj is not None and not df_bj.empty:
            df_all = pd.concat([df_all, df_bj], ignore_index=True)
    except Exception as e:
        print(f"  - 京A获取失败(忽略): {e}")

    # 检查数据是否获取成功
    if df_all.empty:
        print("错误：未能获取任何市场数据")
        return None

    print(f"成功获取 {len(df_all)} 条数据")

    # 数据清洗
    # 确保关键列存在
    required_cols = ['换手率', '成交额', '涨跌幅']
    for col in required_cols:
        if col not in df_all.columns:
            print(f"错误：缺少关键列 '{col}'，当前列: {df_all.columns.tolist()}")
            return None
            
    # 类型转换 (处理可能的字符串/空值)
    for col in required_cols:
        df_all[col] = pd.to_numeric(df_all[col], errors='coerce')

    # 去除没有换手率的数据
    df = df_all[df_all['换手率'].notna()]
    
    # 核心指标计算
    # 1. 全市场平均换手率
    avg_turnover_rate = round(df['换手率'].mean(), 4)
    
    # 2. 换手率中位数
    median_turnover_rate = round(df['换手率'].median(), 4)
    
    # 3. 两市总成交额 (单位：亿元)
    total_amount = df['成交额'].sum()
    total_amount_yi = round(total_amount / 100000000, 2)
    
    # 4. 上涨家数占比
    up_count = len(df[df['涨跌幅'] > 0])
    total_count = len(df)
    up_ratio = round((up_count / total_count) * 100, 2) if total_count > 0 else 0

    # 获取当前日期 (北京时间)
    tz = pytz.timezone('Asia/Shanghai')
    current_date = datetime.now(tz).strftime('%Y-%m-%d')

    data_point = {
        "date": current_date,
        "avg_turnover": avg_turnover_rate,
        "median_turnover": median_turnover_rate,
        "total_amount": total_amount_yi,
        "up_ratio": up_ratio
    }
    
    return data_point

def save_data(new_data):
    if not new_data:
        return

    # 读取旧数据
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            try:
                history_data = json.load(f)
            except json.JSONDecodeError:
                history_data = []
    else:
        history_data = []

    # 去重与更新
    data_dict = {item['date']: item for item in history_data}
    data_dict[new_data['date']] = new_data
    
    sorted_data = sorted(data_dict.values(), key=lambda x: x['date'])

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    
    print(f"数据已更新: {new_data}")

if __name__ == "__main__":
    data = get_market_data()
    if data:
        save_data(data)
