import akshare as ak
import pandas as pd
import time

def get_market_data_safe():
    df = None
    
    # --- 尝试 1: 东方财富接口 (首选) ---
    try:
        print("尝试连接东方财富接口...")
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"东方财富接口报错: {e}")
    
    # --- 尝试 2: 新浪财经接口 (备选) ---
    if df is None:
        print("东方财富不可用，切换至新浪财经接口...")
        try:
            # 新浪接口：stock_zh_a_spot
            # 注意：新浪返回的列名可能不同，需要做映射
            df_sina = ak.stock_zh_a_spot()
            
            # 简单的列名对齐 (根据实际返回调整)
            # 新浪通常返回: symbol, code, name, trade, pricechange, changepercent, buy, sell, settlement, open, high, low, volume, amount, ticktime, per, pb, mktcap, nmc, turnoverratio
            df = df_sina.rename(columns={
                'trade': '最新价',
                'changepercent': '涨跌幅',
                'amount': '成交额',
                'turnoverratio': '换手率'  # 新浪通常也有换手率
            })
            
            # 新浪的换手率可能是 0.5 这样的数值，也可能是 0.5% 字符串，需要清洗
            # 这里的清洗逻辑要根据实际数据加
        except Exception as e:
            print(f"新浪接口也报错了: {e}")

    if df is None:
        return None

    # ...后续的数据清洗逻辑保持不变...
    return df
