import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime
import pytz

# 文件路径配置
DATA_FILE = 'data/market_data.json'

def get_market_data():
    print("正在获取A股实时行情数据(Akshare)...")
    
    try:
        # 使用东方财富接口获取全市场实时行情
        # 包含字段：代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"API调用失败，请检查网络或Akshare版本: {e}")
        return None

    if df is None or df.empty:
        print("未获取到数据")
        return None

    # --- 数据清洗与类型转换 ---
    # 强制将关键列转换为数值型，非数值转为NaN
    numeric_cols = ['换手率', '成交额', '涨跌幅', '最新价']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 过滤掉停牌或无数据的股票（换手率为NaN 或 成交额为0）
    df_clean = df.dropna(subset=['换手率', '涨跌幅'])
    df_clean = df_clean[df_clean['成交额'] > 0]

    print(f"有效股票数量: {len(df_clean)}")

    # --- 核心指标计算 ---
    
    # 1. 换手率 (Turnover Rate)
    # 平均换手率：反映市场整体热度
    avg_turnover_rate = round(df_clean['换手率'].mean(), 4)
    # 中位数换手率：排除极端值影响，更能代表大部分股票的状态
    median_turnover_rate = round(df_clean['换手率'].median(), 4)
    
    # 2. 两市总成交额 (Total Amount)
    # 原始单位为元，转换为亿元
    total_amount = df_clean['成交额'].sum()
    total_amount_yi = round(total_amount / 100000000, 2)
    
    # 3. 市场赚钱效应 (Up Ratio)
    # 上涨家数占比
    up_count = len(df_clean[df_clean['涨跌幅'] > 0])
    total_count = len(df_clean)
    up_ratio = round((up_count / total_count) * 100, 2) if total_count > 0 else 0

    # 获取当前北京时间
    tz = pytz.timezone('Asia/Shanghai')
    current_time = datetime.now(tz)
    current_date_str = current_time.strftime('%Y-%m-%d')
    # 可选：如果你需要记录具体的时间点（如收盘后复盘）
    # current_datetime_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

    # 构造数据结构
    data_point = {
        "date": current_date_str,
        "avg_turnover": avg_turnover_rate,       # 平均换手率(%)
        "median_turnover": median_turnover_rate, # 中位数换手率(%)
        "total_amount": total_amount_yi,         # 总成交额(亿元)
        "up_ratio": up_ratio,                    # 上涨家数占比(%)
        "stock_count": total_count               # 统计样本数
    }
    
    print(f"计算完成: {data_point}")
    return data_point

def save_data(new_data):
    if not new_data:
        return

    # 确保目录存在
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)

    history_data = []
    # 读取旧数据
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                if content:
                    history_data = json.loads(content)
        except json.JSONDecodeError:
            print("警告：JSON文件格式错误，将重新创建")
            history_data = []
        except Exception as e:
            print(f"读取文件失败: {e}")

    # 数据更新逻辑：按日期去重
    # 使用字典覆盖旧日期的记录
    data_dict = {item['date']: item for item in history_data}
    data_dict[new_data['date']] = new_data
    
    # 转换回列表并按日期排序
    sorted_data = sorted(data_dict.values(), key=lambda x: x['date'])

    # 写入文件
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
        print(f"数据已成功保存至 {DATA_FILE}")
    except Exception as e:
        print(f"写入文件失败: {e}")

if __name__ == "__main__":
    # 获取数据
    market_data = get_market_data()
    
    # 保存数据
    if market_data:
        save_data(market_data)
