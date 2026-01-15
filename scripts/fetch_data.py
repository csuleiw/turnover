import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime
import pytz

# 文件路径
DATA_FILE = 'data/market_data.json'

def get_market_data():
    print("正在获取A股实时行情...")
    # 获取所有A股的实时行情数据
    # 包含：代码、名称、最新价、涨跌幅、成交量、成交额、换手率等
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"API调用失败: {e}")
        return None

    # 数据清洗：去除没有换手率的数据（停牌等）
    df = df[df['换手率'].notna()]
    
    # 核心指标计算
    # 1. 全市场平均换手率 (简单算术平均，也可以根据市值加权)
    avg_turnover_rate = round(df['换手率'].mean(), 4)
    
    # 2. 换手率中位数 (更能代表大多数股票的状态)
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
        "avg_turnover": avg_turnover_rate,     # 平均换手率
        "median_turnover": median_turnover_rate, # 中位数换手率
        "total_amount": total_amount_yi,       # 总成交额(亿)
        "up_ratio": up_ratio                   # 上涨家数占比
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

    # 简单的去重逻辑：如果日期已存在，则覆盖，否则追加
    # 创建一个字典便于按日期索引
    data_dict = {item['date']: item for item in history_data}
    data_dict[new_data['date']] = new_data
    
    # 转换回列表并按日期排序
    sorted_data = sorted(data_dict.values(), key=lambda x: x['date'])

    # 写入文件
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    
    print(f"数据已更新: {new_data}")

if __name__ == "__main__":
    data = get_market_data()
    if data:
        save_data(data)