import json
from decimal import Decimal
from config import EXCHANGE_DEPOSIT_CONFIG, get_network_strategy


def calc_deposit_cost(target_ex, amount, currency="USDT"):
    """计算充币到目标交易所的成本（优化网络选择策略）"""
    try:
        strategy = get_network_strategy(amount, currency)
        min_cost = float('inf')
        best_network = None
        deposit_time = 0

        # 优先选择策略推荐的网络   
        for network in strategy:
            if target_ex in EXCHANGE_DEPOSIT_CONFIG and network in EXCHANGE_DEPOSIT_CONFIG[target_ex]:
                fee_data = EXCHANGE_DEPOSIT_CONFIG[target_ex][network]
                cost = fee_data.get("fee", float('inf'))
                if cost < min_cost:
                    min_cost = cost
                    best_network = network
                    deposit_time = fee_data.get("time", 0)

        return min_cost, best_network, deposit_time
    except Exception as e:
        print(f"计算充币成本时出错: {str(e)}")
        return float('inf'), None, 0


def load_market_data(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def find_arbitrage_opportunities(data, min_profit=0.5, min_volume=1000, max_spread_pct=5, available_capital=100):
    """
    修改后的套利机会发现逻辑，基于可用资金计算
    """
    opportunities = []

    for pair, exchanges in data.items():
        if not isinstance(exchanges, dict) or len(exchanges) < 2:
            continue

        # 跳过杠杆代币和非常规交易对
        if any(x in pair for x in ['3L', '3S', '5L', '5S', 'BEAR', 'BULL']):
            continue

        markets = []
        for exchange, values in exchanges.items():
            try:
                # 确保价格有效性
                buy_price = Decimal(values['buy'])
                sell_price = Decimal(values['sell'])
                volume = Decimal(values.get('volume', 0))

                # 过滤无效数据
                min_valid_price = Decimal('0.000001')
                if (volume < min_volume or
                        buy_price <= min_valid_price or
                        sell_price <= min_valid_price or
                        buy_price / sell_price > Decimal('1.5')):
                    continue

                markets.append({
                    'exchange': exchange,
                    'buy': buy_price,  # 交易所买价（用户卖出价）
                    'sell': sell_price,  # 交易所卖价（用户买入价）
                    'volume': volume
                })
            except Exception as e:
                continue

        if len(markets) < 2:
            continue

        # 寻找最佳买入交易所（用户买入成本最低）
        best_buy_market = min(markets, key=lambda x: x['sell'])
        # 寻找最佳卖出交易所（用户卖出收益最高）
        best_sell_market = max(markets, key=lambda x: x['buy'])

        if best_buy_market['exchange'] == best_sell_market['exchange']:
            continue

        # 计算价差（百分比）
        spread_pct = ((best_sell_market['buy'] - best_buy_market['sell']) / best_buy_market['sell']) * 100
        if spread_pct > max_spread_pct or spread_pct < min_profit:
            continue

        # ==== 基于可用资金计算交易量 ====
        capital = Decimal(str(available_capital))

        # 计算可用资金能购买的最大代币数量（考虑买入手续费0.2%）
        token_amount = capital / (best_buy_market['sell'] * Decimal('1.002'))

        # 应用滑价保护（保守估计）
        effective_token_amount = token_amount * Decimal('0.8')

        # ==== 利润计算 ====
        # 理论利润（考虑实际代币数量）
        buy_cost = best_buy_market['sell'] * effective_token_amount
        sell_revenue = best_sell_market['buy'] * effective_token_amount
        theoretical_profit = sell_revenue - buy_cost

        # 交易手续费（0.2%）
        trade_fee = (buy_cost + sell_revenue) * Decimal('0.002')

        # 充币成本（充到卖出交易所）
        deposit_cost, best_net, deposit_time = calc_deposit_cost(
            target_ex=best_sell_market['exchange'],
            amount=float(sell_revenue),
            currency=pair.split('/')[1]
        )

        # 净收益
        net_profit = theoretical_profit - trade_fee - Decimal(str(deposit_cost))

        if net_profit >= min_profit:
            opportunities.append({
                'pair': pair,
                'buy_at': best_buy_market['exchange'],
                'sell_at': best_sell_market['exchange'],
                'buy_price': float(best_buy_market['sell']),
                'sell_price': float(best_sell_market['buy']),
                'token_amount': float(effective_token_amount),
                'theoretical_profit': float(theoretical_profit),
                'net_profit': float(net_profit),
                'deposit_cost': deposit_cost,
                'best_net': best_net,
                'deposit_time': deposit_time,
                'spread_pct': float(spread_pct),
                'capital_used': float(buy_cost)
            })

    return sorted(opportunities, key=lambda x: (-x['net_profit'], -x['token_amount']))


if __name__ == "__main__":
    data = load_market_data("exchange_data.json")
    opportunities = find_arbitrage_opportunities(data, min_profit=2)

    print(f"共发现 {len(opportunities)} 个有效套利机会\n")
    for opp in opportunities:
        print(
            f"交易对: {opp['pair']} | 价差: {opp['spread_pct']:.2f}%\n"
            f"操作: 在 {opp['buy_at']} 以 {opp['buy_price']} 买入 | "
            f"在 {opp['sell_at']} 以 {opp['sell_price']} 卖出\n"
            f"数量: {opp['token_amount']:.4f} 代币 | "
            f"充币网络: {opp['best_net']} ({opp['deposit_time']}分钟)\n"
            f"理论利润: {opp['theoretical_profit']:.2f} → "
            f"实际利润: {opp['net_profit']:.2f} USDT (含手续费 {opp['deposit_cost']} USDT)\n"
        )
