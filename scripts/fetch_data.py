import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime
import pytz

# 文件路径
DATA_FILE = 'data/market_data.json'

def get_market_data():
    print("正在获取A股实时行情(源: 新浪财经)...")
    
    try:
        # 使用新浪接口，避开东方财富(EM)的IP封锁
        df = ak.stock_zh_a_spot()
        
        # 调试：打印前几行和列名，方便排查问题
        # print(f"API返回列名: {df.columns.tolist()}")
        
    except Exception as e:
        print(f"API调用直接失败: {e}")
        return None

    if df is None or df.empty:
        print("API返回数据为空")
        return None

    # --- 核心修复逻辑：列名映射与缺失值计算 ---
    
    # 1. 建立映射字典 (新浪英文名 -> 中文名)
    rename_map = {
        'turnoverratio': '换手率',
        'changepercent': '涨跌幅',
        'trade': '最新价',
        'amount': '成交额',
        'nmc': '流通市值', # nmc通常单位是万股或万元，新浪接口一般是“流通市值(万元)”
        'mktcap': '总市值'
    }
    df.rename(columns=rename_map, inplace=True)

    # 2. 强制转换数值类型，处理非数字字符
    numeric_cols = ['换手率', '成交额', '涨跌幅', '流通市值']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3. 【关键修复】如果“换手率”不存在，尝试手动计算
    # 公式：换手率(%) = (成交额 / 流通市值) * 100
    # 注意：新浪的 amount 单位通常是“元”，nmc 单位通常是“万元”
    if '换手率' not in df.columns or df['换手率'].isnull().all():
        print("警告：API未返回'换手率'字段，正在尝试通过成交额/流通市值计算...")
        if '成交额' in df.columns and '流通市值' in df.columns:
            # 成交额(元) / (流通市值(万元) * 10000) * 100
            # 加上 dropna 防止除以0
            df['换手率'] = (df['成交额'] / (df['流通市值'] * 10000)) * 100
            df['换手率'] = df['换手率'].round(4)
        else:
            print(f"错误：无法计算换手率，缺少必要字段。现有字段: {df.columns.tolist()}")
            return None

    # 4. 数据清洗：去除无效数据
    df = df[df['换手率'].notna()]
    df = df[df['最新价'] > 0] # 去除价格为0的异常数据

    print(f"有效数据条数: {len(df)}")

    # --- 核心指标计算 ---
    
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
