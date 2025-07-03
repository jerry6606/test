import decimal
import requests
import json
import time
from decimal import Decimal
from typing import Dict, List, Tuple
from collections import defaultdict
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('exchange_data_fetcher.log'), logging.StreamHandler()]
)


class ExchangeDataFetcher:
    def __init__(self):
        self.exchange_apis = {
            'OKX': {
                'url': 'https://www.okx.com/api/v5/market/tickers?instType=SPOT',
                'parser': self._get_okx_data
            },
            'Binance': {
                'url': 'https://api.binance.com/api/v3/ticker/24hr',
                'parser': self._get_binance_data
            },
            'Bitget': {
                'url': 'https://api.bitget.com/api/spot/v1/market/tickers?limit=5000',
                'parser': self._get_bitget_data
            },
            'Gate': {
                'url': 'https://api.gateio.ws/api/v4/spot/tickers',
                'parser': self._get_gate_data
            },
            'MEXC': {
                'url': 'https://api.mexc.com/api/v3/ticker/24hr',
                'parser': self._get_mexc_data
            },
            'HTX': {
                'url': 'https://api.huobi.pro/market/tickers',
                'parser': self._get_htx_data
            }
        }
        self.ticker_data = defaultdict(dict)
        self.request_timeout = 10  # 请求超时时间(秒)
        self.last_fetch_time = 0
        self.cache_duration = 60  # 缓存时间(秒)

    def fetch_all_data(self) -> Dict[str, Dict]:
        """获取所有交易所数据，带缓存机制"""
        current_time = time.time()
        if current_time - self.last_fetch_time < self.cache_duration:
            logging.info("使用缓存数据")
            return self.ticker_data

        self.ticker_data.clear()  # 清除旧数据

        for exchange, config in self.exchange_apis.items():
            try:
                logging.info(f"正在获取 {exchange} 数据...")
                start_time = time.time()
                response = requests.get(config['url'], timeout=self.request_timeout)
                response.raise_for_status()
                raw_data = response.json()

                # 调用解析器并获取处理数量
                processed_count = config['parser'](exchange, raw_data)
                elapsed = time.time() - start_time
                logging.info(f"{exchange} 数据获取成功，共处理 {processed_count} 个交易对 ({elapsed:.2f}秒)")

            except requests.exceptions.RequestException as e:
                logging.error(f"获取 {exchange} 数据失败: 网络错误 - {str(e)}")
            except json.JSONDecodeError:
                logging.error(f"获取 {exchange} 数据失败: JSON解析错误")
            except Exception as e:
                logging.error(f"获取 {exchange} 数据失败: {str(e)}")

        self.last_fetch_time = time.time()
        return self.ticker_data

    def _get_okx_data(self, exchange: str, data: Dict) -> int:
        """获取OKX数据"""
        processed_count = 0
        if data.get('code') != '0':
            logging.error(f"OKX API返回错误: {data.get('msg', '未知错误')}")
            return 0

        tickers = data.get('data', [])
        if not isinstance(tickers, list):
            logging.error("OKX API返回数据格式错误: data字段不是列表")
            return 0

        for ticker in tickers:
            try:
                inst_id = ticker.get('instId', '')
                if not inst_id or '-' not in inst_id:
                    continue  # 跳过无效的交易对格式

                # 统一转换为 BTC/USDT 格式
                symbol = inst_id.replace('-', '/')

                # 检查必要字段是否存在
                bid_px = ticker.get('bidPx')  # 买一价（用户卖出价）
                ask_px = ticker.get('askPx')  # 卖一价（用户买入价）
                vol_24h = ticker.get('vol24h')

                if None in (ask_px, bid_px, vol_24h):
                    continue  # 跳过字段缺失的交易对

                self.ticker_data[symbol][exchange] = {
                    'buy': Decimal(str(bid_px)),  # 买一价 -> 用户卖出价
                    'sell': Decimal(str(ask_px)),  # 卖一价 -> 用户买入价
                    'volume': Decimal(str(vol_24h)),
                    'timestamp': int(time.time())
                }
                processed_count += 1

            except Exception as e:
                logging.warning(f"跳过OKX交易对 {ticker.get('instId')}（解析错误: {str(e)}）")
                continue

        return processed_count

    def _get_binance_data(self, exchange: str, data: List[Dict]) -> int:
        """获取Binance数据（买卖方向已修正）"""
        processed_count = 0
        for ticker in data:
            try:
                symbol = ticker['symbol']
                if symbol.endswith('USDT'):
                    base = symbol[:-4]
                    quote = 'USDT'
                elif symbol.endswith('BTC'):
                    base = symbol[:-3]
                    quote = 'BTC'
                else:
                    continue

                formatted_symbol = f"{base}/{quote}"

                #买卖方向
                self.ticker_data[formatted_symbol][exchange] = {
                    'buy': Decimal(ticker['bidPrice']),  # 买一价 -> 用户卖出价
                    'sell': Decimal(ticker['askPrice']),  # 卖一价 -> 用户买入价
                    'volume': Decimal(ticker['quoteVolume']),
                    'timestamp': int(time.time())
                }
                processed_count += 1

            except KeyError as e:
                logging.warning(f"Binance交易对 {symbol} 缺少必要字段: {str(e)}")
            except Exception as e:
                logging.warning(f"处理Binance交易对 {symbol} 时出错: {str(e)}")

        return processed_count

    def _get_bitget_data(self, exchange: str, data: Dict) -> int:
        """获取Bitget数据"""
        processed_count = 0
        try:
            if data.get('code') != '00000':
                logging.error(f"Bitget API返回错误: {data.get('msg', '未知错误')}")
                return 0

            tickers = data.get('data', [])
            if not isinstance(tickers, list):
                logging.error("Bitget API返回数据格式错误: data字段不是列表")
                return 0

            for ticker in tickers:
                try:
                    symbol = ticker.get('symbol', '')
                    if not symbol.endswith('USDT'):
                        continue

                    # 使用正确的字段名
                    buy_price = ticker.get('buyOne')  # 买一价（用户卖出价）
                    sell_price = ticker.get('sellOne')  # 卖一价（用户买入价）
                    volume = ticker.get('usdtVol') or ticker.get('quoteVol')  # USDT交易量

                    if None in (buy_price, sell_price, volume):
                        logging.warning(f"Bitget 交易对 {symbol} 缺少必要字段")
                        continue

                    try:
                        formatted_symbol = f"{symbol[:-4]}/USDT"
                        self.ticker_data[formatted_symbol][exchange] = {
                            'buy': Decimal(str(buy_price)),
                            'sell': Decimal(str(sell_price)),
                            'volume': Decimal(str(volume)),
                            'timestamp': int(time.time())
                        }
                        processed_count += 1
                    except decimal.InvalidOperation:
                        logging.warning(f"跳过Bitget交易对 {symbol}（数字格式无效）")
                        continue

                except Exception as e:
                    logging.warning(f"处理Bitget交易对 {symbol} 时出错: {str(e)}")
                    continue

            return processed_count

        except Exception as e:
            logging.error(f"解析Bitget数据时发生严重错误: {str(e)}")
            return 0

    def _get_gate_data(self, exchange: str, data: List[Dict]) -> int:
        """获取Gate数据"""
        processed_count = 0
        if not isinstance(data, list):
            logging.error("Gate API返回数据格式错误: 期望列表")
            return 0

        skipped_pairs = set()  # 记录跳过的交易对

        for ticker in data:
            symbol = ticker.get('currency_pair', '')
            try:
                if not symbol or not symbol.endswith('_USDT'):
                    continue

                # 跳过杠杆代币
                if any(x in symbol for x in ['3L', '3S', '5L', '5S', 'BEAR', 'BULL']):
                    continue

                #最高买价 -> 用户卖出价，最低卖价 -> 用户买入价
                buy_price = ticker.get('highest_bid')  # 买一价（用户卖出价）
                sell_price = ticker.get('lowest_ask')  # 卖一价（用户买入价）
                volume = ticker.get('quote_volume')

                # 检查字段是否有效
                if None in (buy_price, sell_price, volume):
                    skipped_pairs.add(symbol)
                    continue

                try:
                    formatted_symbol = f"{symbol[:-5]}/USDT"
                    self.ticker_data[formatted_symbol][exchange] = {
                        'buy': Decimal(str(buy_price)),
                        'sell': Decimal(str(sell_price)),
                        'volume': Decimal(str(volume)),
                        'timestamp': int(time.time())
                    }
                    processed_count += 1
                except decimal.InvalidOperation:
                    skipped_pairs.add(symbol)
                    continue

            except Exception as e:
                logging.warning(f"处理Gate交易对 {symbol} 时出错: {str(e)}")
                skipped_pairs.add(symbol)

        # 只在最后打印一次跳过的交易对
        if skipped_pairs:
            logging.info(f"Gate 跳过 {len(skipped_pairs)} 个无效交易对，例如: {', '.join(sorted(skipped_pairs)[:3])}等")

        return processed_count

    def _get_mexc_data(self, exchange: str, data: List[Dict]) -> int:
        """获取MEXC数据（买卖方向已修正）"""
        processed_count = 0
        if not isinstance(data, list):
            logging.error("MEXC API返回数据格式错误: 期望列表")
            return 0

        for ticker in data:
            try:
                symbol = ticker.get('symbol', '')
                if not symbol.endswith('USDT'):
                    continue

                # 跳过杠杆代币
                if any(x in symbol for x in ['3L', '3S', '5L', '5S']):
                    continue

                formatted_symbol = f"{symbol[:-4]}/USDT"

                #买卖方向
                self.ticker_data[formatted_symbol][exchange] = {
                    'buy': Decimal(str(ticker['bidPrice'])),  # 买一价 -> 用户卖出价
                    'sell': Decimal(str(ticker['askPrice'])),  # 卖一价 -> 用户买入价
                    'volume': Decimal(str(ticker['quoteVolume'])),
                    'timestamp': int(time.time())
                }
                processed_count += 1
            except Exception as e:
                logging.warning(f"处理MEXC交易对 {symbol} 时出错: {str(e)}")
        return processed_count

    def _get_htx_data(self, exchange: str, data: Dict) -> int:
        """获取HTX数据"""
        processed_count = 0
        if data.get('status') != 'ok':
            logging.error(f"HTX API返回错误: {data.get('err-msg', '未知错误')}")
            return 0

        tickers = data.get('data', [])
        if isinstance(tickers, dict):
            tickers = tickers.get('tickers', [])

        for ticker in tickers:
            try:
                symbol = ticker.get('symbol', '').upper()
                if not symbol.endswith('USDT'):
                    continue

                bid = Decimal(str(ticker['bid']))  # 买一价（用户卖出价）
                ask = Decimal(str(ticker['ask']))  # 卖一价（用户买入价）
                vol = Decimal(str(ticker.get('vol', 0)))

                # 过滤异常价格
                if ask < Decimal('0.000001') or bid < Decimal('0.000001'):
                    continue

                formatted_symbol = f"{symbol[:-4]}/USDT"

                # 买卖方向
                self.ticker_data[formatted_symbol][exchange] = {
                    'buy': bid,  # 买一价 -> 用户卖出价
                    'sell': ask,  # 卖一价 -> 用户买入价
                    'volume': vol,
                    'timestamp': int(time.time())
                }
                processed_count += 1
            except Exception as e:
                logging.warning(f"处理HTX交易对 {symbol} 时出错: {str(e)}")
        return processed_count

    def save_to_file(self, filename: str = 'exchange_data.json'):
        """将数据保存到JSON文件"""
        # 将Decimal转换为字符串以便JSON序列化
        serializable_data = {}
        for symbol, exchanges in self.ticker_data.items():
            serializable_data[symbol] = {}
            for exchange, values in exchanges.items():
                serializable_data[symbol][exchange] = {
                    'buy': str(values['buy']),
                    'sell': str(values['sell']),
                    'volume': str(values['volume']),
                    'timestamp': values['timestamp']
                }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, indent=2, ensure_ascii=False)

        logging.info(f"数据已保存到 {filename}")


if __name__ == "__main__":
    fetcher = ExchangeDataFetcher()

    ticker_data = fetcher.fetch_all_data()

    fetcher.save_to_file()
