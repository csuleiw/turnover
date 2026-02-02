import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime
import pytz

# 文件路径
DATA_FILE = 'data/market_data.json'

def get_market_data():
    print("正在获取A股实时行情(源:新浪)...")
    
    try:
        # --- 修改点 START ---
        # 原接口: df = ak.stock_zh_a_spot_em()
        # 替换为新浪接口 (Sina API)
        df = ak.stock_zh_a_spot()
        
        # 新浪接口返回的是英文列名，需要映射回原代码使用的中文列名
        # turnoverratio -> 换手率
        # amount -> 成交额
        # changepercent -> 涨跌幅
        # trade -> 最新价
        rename_map = {
            'turnoverratio': '换手率',
            'amount': '成交额',
            'changepercent': '涨跌幅',
            'trade': '最新价'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # 确保数据是数值类型 (防止API返回字符串导致计算报错)
        numeric_cols = ['换手率', '成交额', '涨跌幅']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        # --- 修改点 END ---

    except Exception as e:
        print(f"API调用失败: {e}")
        return None

    # 数据清洗：去除没有换手率的数据（停牌等）
    # 注意：新浪数据中有时会有NaN，需要清洗
    df = df[df['换手率'].notna()]
    
    # 核心指标计算
    # 1. 全市场平均换手率
    avg_turnover_rate = round(df['换手率'].mean(), 4)
    
    # 2. 换手率中位数
    median_turnover_rate = round(df['换手率'].median(), 4)
    
    # 3. 两市总成交额 (单位：亿元)
    # 新浪返回的amount单位通常也是元
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
    # 建议先在命令行执行: pip install --upgrade akshare
    data = get_market_data()
    if data:
        save_data(data)
