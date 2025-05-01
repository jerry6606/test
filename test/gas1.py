import json
import math
from collections import defaultdict
from decimal import Decimal, getcontext
import decimal
import datetime

getcontext().prec = 8


def safe_decimal_convert(value, default=None):
    """安全转换为Decimal，处理各种异常情况"""
    if value is None:
        return default
    try:
        return Decimal(str(value).strip())
    except (decimal.InvalidOperation, ValueError, TypeError):
        try:
            return Decimal(float(value))
        except (ValueError, TypeError):
            print(f"警告：无法转换值为Decimal: {value} (类型: {type(value)})")
            return default


def load_data():
    """加载价格数据和交易对信息（增加price_precision校验）"""
    try:
        with open('okx_token_data.json', 'r') as f:
            price_data = json.load(f)
        with open('token_data_one.json', 'r') as f:
            instrument_data = json.load(f)

        # 校验必要字段
        required_instrument_fields = ['instId', 'base_currency', 'quote_currency', 'price_precision']
        for item in instrument_data:
            if not all(field in item for field in required_instrument_fields):
                raise ValueError("交易对数据缺少必要字段")

        return price_data, instrument_data
    except Exception as e:
        print(f"数据加载失败: {str(e)}")
        return None, None


def preprocess_data(price_data, instrument_data):
    """预处理数据（完整滑点计算）"""
    if not price_data or not instrument_data:
        return None

    # 构建交易对信息字典
    instruments = {}
    for item in instrument_data:
        try:
            inst_id = item['instId']
            instruments[inst_id] = {
                'base_currency': item['base_currency'],
                'quote_currency': item['quote_currency'],
                'price_precision': safe_decimal_convert(item['price_precision'], Decimal('0.0001'))
            }
        except KeyError as e:
            print(f"交易对信息缺少必要字段: {str(e)}")
            continue

    prices = {}
    valid_count = 0
    invalid_count = 0

    for item in price_data[0]['data']:
        try:
            inst_id = item['instId']
            if inst_id not in instruments:
                invalid_count += 1
                continue

            # 获取交易对精度信息
            instrument_info = instruments[inst_id]
            price_precision = instrument_info['price_precision']

            # 转换价格数据
            ask_px = safe_decimal_convert(item.get('askPx'))
            bid_px = safe_decimal_convert(item.get('bidPx'))
            ask_sz = safe_decimal_convert(item.get('askSz', '0'), Decimal('0'))
            bid_sz = safe_decimal_convert(item.get('bidSz', '0'), Decimal('0'))

            # 数据有效性检查
            if not all([ask_px, bid_px]) or ask_px <= 0 or bid_px <= 0:
                invalid_count += 1
                continue

            # ============== 滑点计算 ==============
            spread = ask_px - bid_px
            mid_price = (ask_px + bid_px) / Decimal('2')

            # 百分比滑点（处理除零异常）
            try:
                slippage_percent = (spread / mid_price) * Decimal('100')
            except decimal.DivisionByZero:
                slippage_percent = Decimal('0')
                print(f"警告：中间价为0的交易对 {inst_id}")

            # 最小价格单位倍数（处理除零异常）
            try:
                price_units = spread / price_precision
            except decimal.DivisionByZero:
                price_units = Decimal('0')
                print(f"警告：价格精度为0的交易对 {inst_id}")

            # 存储计算结果
            prices[inst_id] = {
                'buy_price': ask_px,
                'sell_price': bid_px,
                'base_currency': instrument_info['base_currency'],
                'quote_currency': instrument_info['quote_currency'],
                'ask_sz': ask_sz,
                'bid_sz': bid_sz,
                'inst_id': inst_id,
                # 滑点相关字段
                'spread': spread,
                'mid_price': mid_price,
                'slippage_percent': slippage_percent,
                'price_units': price_units,
                'price_precision': price_precision
            }
            valid_count += 1

        except Exception as e:
            print(f"处理交易对 {item.get('instId', '未知')} 时出错: {str(e)}")
            invalid_count += 1

    print(f"数据预处理完成 - 有效: {valid_count}, 无效: {invalid_count}")
    return prices


def build_currency_graph(prices, min_volume=100):
    """构建货币关系图（添加滑点信息）"""
    graph = defaultdict(dict)
    skipped = 0

    for inst_id, data in prices.items():
        base = data['base_currency']
        quote = data['quote_currency']

        # 卖出方向（base->quote）
        if data['bid_sz'] >= min_volume:
            graph[base][quote] = {
                'rate': Decimal('1') / data['sell_price'],
                'inst_id': inst_id,
                'type': 'sell',
                'volume': data['bid_sz'],
                'price': data['sell_price'],
                'from': base,
                'to': quote,
                # 添加滑点信息
                'slippage_percent': data['slippage_percent'],
                'price_units': data['price_units']
            }
        else:
            skipped += 1

        # 买入方向（quote->base）
        if data['ask_sz'] * data['buy_price'] >= min_volume:
            graph[quote][base] = {
                'rate': data['buy_price'],
                'inst_id': inst_id,
                'type': 'buy',
                'volume': data['ask_sz'] * data['buy_price'],
                'price': data['buy_price'],
                'from': quote,
                'to': base,
                # 添加滑点信息
                'slippage_percent': data['slippage_percent'],
                'price_units': data['price_units']
            }
        else:
            skipped += 1

    print(f"货币图构建完成 - 跳过{skipped}个低流动性交易对")
    return graph


def calculate_path_volume(path, start_amount=Decimal('1')):
    """计算路径最大可交易量（考虑滑点）"""
    current_amount = start_amount
    min_volume = None
    slippage_impact = Decimal('1')  # 滑点影响因子

    for step in path:
        # 应用滑点影响（示例：每步减少0.1%）
        slippage_impact *= Decimal('0.999')

        if step['type'] == 'sell':
            available = min(current_amount, step['volume'])
            if min_volume is None or available < min_volume:
                min_volume = available
            current_amount = available * step['rate'] * slippage_impact
        else:
            available = min(current_amount / step['price'], step['volume'])
            if min_volume is None or available < min_volume:
                min_volume = available
            current_amount = available * slippage_impact

    return min_volume if min_volume is not None else Decimal('0')


def decimal_exp(x: Decimal) -> Decimal:
    """优化指数计算稳定性"""
    try:
        x_float = float(x)
        if abs(x_float) > 20:
            return Decimal('Inf') if x_float > 0 else Decimal('0')
        return Decimal(math.exp(x_float)).normalize()
    except (OverflowError, ValueError):
        return Decimal('Inf') if x > 0 else Decimal('0')


def find_triangular_arbitrage(graph, start_currency='USDT', min_profit=0.01,
                              min_volume=50, max_slippage=1.0):
    """修复后的完整套利函数"""
    opportunities = []
    checked_paths = set()

    for first_currency, first_step in graph.get(start_currency, {}).items():
        for second_currency, second_step in graph.get(first_currency, {}).items():
            if second_currency == start_currency:
                continue

            third_step = graph.get(second_currency, {}).get(start_currency)
            if not third_step:
                continue

            path_id = f"{first_currency}-{second_currency}"
            if path_id in checked_paths:
                continue
            checked_paths.add(path_id)

            try:
                path = [first_step, second_step, third_step]

                # === 数据校验 ===
                for step in path:
                    if step.get('slippage_percent', Decimal('0')) < Decimal('0'):
                        raise ValueError(f"负滑点值 {step['slippage_percent']}%")
                    if step.get('rate', Decimal('0')) <= Decimal('0'):
                        raise ValueError(f"无效汇率 {step['rate']}")

                # === 滑点过滤 ===
                total_slippage = sum(s['slippage_percent'] for s in path)
                if total_slippage > Decimal(str(max_slippage)):
                    continue

                # === 收益率计算 ===
                fee = Decimal('0.001')
                amount = Decimal('1')
                factor = Decimal('0.05')

                for step in path:
                    # 滑点影响计算
                    slippage_pct = step['slippage_percent'] / Decimal('100')
                    exponent = -slippage_pct / (factor * (step['volume'].sqrt() + Decimal('0.1')))
                    slippage_impact = Decimal('1') - decimal_exp(exponent)

                    # 限制滑点影响范围
                    slippage_impact = max(min(slippage_impact, Decimal('0.1')), Decimal('0'))

                    amount *= step['rate'] * (Decimal('1') - fee) * (Decimal('1') - slippage_impact)

                # === 收益过滤 ===
                profit_percent = (amount - Decimal('1')) * Decimal('100')
                if profit_percent < Decimal(str(min_profit)):
                    continue

                # === 路径容量 ===
                path_volume = min(s['volume'] for s in path)
                if path_volume < Decimal(str(min_volume)):
                    continue

                # === 结果保存 ===
                opportunities.append({
                    'path': path,  # 保留完整的路径步骤信息，而不是只存inst_id
                    'profit_percent': float(profit_percent.quantize(Decimal('0.0001'))),
                    'final_amount': float(amount),
                    'path_volume': float(path_volume),
                    'total_slippage': float(total_slippage),
                    'max_step_slippage': float(max(s['slippage_percent'] for s in path))
                })


            except Exception as e:
                print(f"[ERROR] 路径 {path_id} 计算失败: {str(e)}")
                continue

    return sorted(opportunities, key=lambda x: x['profit_percent'], reverse=True)



def print_opportunities(opportunities, max_display=50):
    """修复后的专业打印函数"""
    if not opportunities:
        print("没有找到有效的三角套利机会")
        return

    print(f"\n找到 {len(opportunities)} 个套利机会:")
    print("=" * 120)
    for i, opp in enumerate(opportunities[:max_display], 1):
        print(f"\n机会 #{i}:")
        print(f"  ▪ 收益率: {opp['profit_percent']:.6f}%")
        print(f"  ▪ 总滑点: {opp['total_slippage']:.4f}% | 最大单步滑点: {opp['max_step_slippage']:.4f}%")

        try:
            # 获取基础货币
            base_currency = opp['path'][0]['from']
            print(f"  ▪ 路径容量: {opp['path_volume']:.4f} {base_currency}")

            print(" 路径步骤:")
            for idx, step in enumerate(opp['path'], 1):
                action = "卖出" if step['type'] == 'sell' else "买入"
                print(f"  步骤{idx}: {action} {step['from']} → {step['to']} ({step['inst_id']})")

            print(f"初始1 {base_currency} → 最终 {opp['final_amount']:.8f} {opp['path'][-1]['to']}")
        except KeyError as e:
            print(f"显示错误: 路径数据缺少必要字段 {str(e)}")
            continue

        print("-" * 120)


# 主函数保持不变
def analyze_arbitrage_opportunities():
    """主分析函数"""
    # 添加当前时间显示
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"开始分析套利机会... [当前时间: {current_time}]")

    price_data, instrument_data = load_data()
    if not price_data or not instrument_data:
        print("错误：无法加载数据文件")
        return

    prices = preprocess_data(price_data, instrument_data)
    if not prices:
        print("错误：预处理后无有效价格数据")
        return

    graph = build_currency_graph(prices)
    if not graph:
        print("错误：无法构建货币关系图")
        return

    # 调整参数示例（最大允许总滑点1%）
    opportunities = find_triangular_arbitrage(
        graph,
        max_slippage=1.0,
        min_profit=0.01,
        min_volume=20,
    )
    print_opportunities(opportunities)


if __name__ == "__main__":
    analyze_arbitrage_opportunities()
