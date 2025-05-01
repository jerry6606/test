import requests
import json


def fetch_and_save_okx_spot_instruments():
    # 1. 发送API请求获取数据
    url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
    response = requests.get(url)

    # 检查请求是否成功
    if response.status_code != 200:
        print(f"请求失败，状态码: {response.status_code}")
        return

    data = response.json()

    # 2. 检查API返回的数据结构
    if 'data' not in data:
        print("API返回的数据格式不符合预期")
        return

    # 3. 准备要保存的数据结构
    output_data = []
    for item in data['data']:
        instrument_info = {
            'instrument': f"{item['baseCcy']}/{item['quoteCcy']}",
            'min_size': item['minSz'],
            'price_precision': item['tickSz'],
            'base_currency': item['baseCcy'],
            'quote_currency': item['quoteCcy'],
            'instId': item['instId']
        }
        output_data.append(instrument_info)

    # 4. 将数据保存到JSON文件
    with open('token_data_one.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)

    print("数据已成功保存到 token_data_one.json")


# 执行函数
fetch_and_save_okx_spot_instruments()
