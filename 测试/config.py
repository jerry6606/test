# 数据格式: { 交易所: { 网络: {"time": 分钟, "fee": USDT} } }
EXCHANGE_DEPOSIT_CONFIG = {
    "Binance": {
        "BSC": {"time": 1, "fee": 0},
        "TRC20": {"time": 1, "fee": 1},
        "SOL": {"time": 1, "fee": 0.5},
        "ERC20": {"time": 2, "fee": 2.5},
        "ARBITRUM": {"time": 1, "fee": 0.18},
        "OPBNB": {"time": 4, "fee": 0},
        "APTOS": {"time": 1, "fee": 0.04},
        "POLYGON": {"time": 1, "fee": 0.02},
        "AVAXC": {"time": 1, "fee": 0.081},
        "OPTIMISM": {"time": 1, "fee": 0.025},
        "EOS": {"time": 1, "fee": 1},
        "NEAR": {"time": 1, "fee": 0.2},
        "SCROLL": {"time": 1, "fee": 0.1},
        "XTZ": {"time": 5, "fee": 0.1}
    },
    "OKX": {
        "TRC20": {"time": 2, "fee": 2.5},
        "ERC20": {"time": 2, "fee": 1.44},
        "SOL": {"time": 2, "fee": 1},
        "ARBITRUM": {"time": 2, "fee": 0.1},
        "XLAYER": {"time": 2, "fee": 0.1},
        "APTOS": {"time": 2, "fee": 0.03},
        "AVAXC": {"time": 2, "fee": 0.22},
        "OPTIMISM": {"time": 2, "fee": 0.15},
        "POLYGON": {"time": 2, "fee": 0.8}
    },
    "HTX": {
        "TRC20": {"time": 3, "fee": 1.2},
        "ERC20": {"time": 3, "fee": 2.625},
        "SOL": {"time": 3, "fee": 1.649},
        "BSC": {"time": 3, "fee": 0.8},
        "AVAXC": {"time": 4, "fee": 0.21},
        "ARBITRUM": {"time": 39, "fee": 1}
    }
}


NETWORK_SELECTION_STRATEGY = {
    # 小金额策略 (<1000 USDT): 优先速度和低手续费
    "small_amount": {
        "USDT": ["SOL", "BSC", "TRC20", "ARBITRUM", "ERC20"],
        "USDC": ["SOL", "BSC", "ARBITRUM", "ERC20"],
        "USD": ["SOL", "BSC", "TRC20", "ARBITRUM", "ERC20"]
    },
    # 中金额策略 (1000-5000 USDT): 平衡速度和手续费
    "medium_amount": {
        "USDT": ["BSC", "TRC20", "SOL", "ARBITRUM"],
        "USDC": ["BSC", "SOL", "ARBITRUM"]
    },
    # 大金额策略 (>5000 USDT): 优先固定费率网络
    "large_amount": {
        "USDT": ["TRC20", "BSC", "ARBITRUM"],
        "USDC": ["BSC", "ARBITRUM"]
    },
    # 各网络基础手续费(USDT)
    "network_base_fee": {
        "SOL": 0.5, "BSC": 0, "TRC20": 1,
        "ARBITRUM": 0.1, "ERC20": 2.5, "POLYGON": 0.8
    }
}

def get_network_strategy(amount, currency):
    if amount < 1000:
        return NETWORK_SELECTION_STRATEGY["small_amount"].get(currency, [])
    elif 1000 <= amount <= 5000:
        return NETWORK_SELECTION_STRATEGY["medium_amount"].get(currency, [])
    else:
        return NETWORK_SELECTION_STRATEGY["large_amount"].get(currency, [])

#确保俩个交易所都支持的网络
def get_common_networks(exchange1, exchange2, amount, currency):
    strategy = get_network_strategy(amount, currency)
    ex1_networks = set(EXCHANGE_DEPOSIT_CONFIG.get(exchange1, {}).keys())
    ex2_networks = set(EXCHANGE_DEPOSIT_CONFIG.get(exchange2, {}).keys())
    common_networks = ex1_networks & ex2_networks
    return [net for net in strategy if net in common_networks]
