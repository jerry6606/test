import asyncio
import json
import websockets
import gzip
from collections import defaultdict
from decimal import Decimal


class RealTimeDataFeed:
    def __init__(self):
        self.order_books = defaultdict(dict)
        self.tickers = defaultdict(dict)
        self.connections = {}

        # 交易所 WebSocket 配置
        self.ws_config = {
            'Binance': 'wss://stream.binance.com:9443/ws',
            'OKX': 'wss://ws.okx.com:8443/ws/v5/public',
            'HTX': 'wss://api.huobi.pro/ws'
        }

    async def connect(self, exchange):
        """优化后的连接方法"""
        url = self.ws_config[exchange]
        self.connections[exchange] = await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_queue=1024
        )

        # 初始化订阅
        if exchange == 'Binance':
            await self.connections[exchange].send(json.dumps({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@depth5@100ms", "ethusdt@depth5@100ms"],
                "id": 1
            }))
        elif exchange == 'OKX':
            await self.connections[exchange].send(json.dumps({
                "op": "subscribe",
                "args": [
                    {"channel": "books5", "instId": "BTC-USDT"},
                    {"channel": "books5", "instId": "ETH-USDT"}
                ]
            }))
        elif exchange == 'HTX':
            await self.connections[exchange].send(json.dumps({
                "sub": "market.btcusdt.depth.step0",
                "id": "depth_sub"
            }))
            await self.connections[exchange].send(json.dumps({
                "sub": "market.ethusdt.depth.step0",
                "id": "depth_sub_eth"
            }))

    async def handle_messages(self, exchange):
        """增强心跳处理的消息循环"""
        while True:
            try:
                async for message in self.connections[exchange]:
                    try:
                        # 解码和心跳处理
                        if isinstance(message, bytes):
                            try:
                                message = gzip.decompress(message).decode('utf-8')
                            except:
                                message = message.decode('utf-8')

                        data = json.loads(message)
                        if exchange == 'Binance' and 'ping' in data:
                            await self.connections[exchange].send(json.dumps({'pong': data['ping']}))
                        elif exchange == 'OKX' and data.get('op') == 'ping':
                            await self.connections[exchange].send(json.dumps({'op': 'pong'}))
                        elif exchange == 'HTX' and 'ping' in data:
                            await self.connections[exchange].send(json.dumps({'pong': data['ping']}))

                        # 处理业务数据...
                        print(f"{exchange} 收到数据: {data}")

                    except Exception as e:
                        print(f"{exchange} 消息解析失败: {e}")
                        continue

            except websockets.exceptions.ConnectionClosed as e:
                print(f"{exchange} 连接断开: {e}, 5秒后重连...")
                await asyncio.sleep(5)
                await self.connect(exchange)  # 自动重连

    async def run(self):
        """支持自动恢复的运行方法"""
        for exchange in self.ws_config:
            await self.connect(exchange)
        await asyncio.gather(*[
            self.handle_messages(exchange)
            for exchange in self.ws_config
        ])


async def main():
    feed = RealTimeDataFeed()
    await feed.run()


if __name__ == "__main__":
    asyncio.run(main())
