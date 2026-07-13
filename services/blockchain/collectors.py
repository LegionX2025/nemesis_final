import logging
import asyncio
import aiohttp
import os
import time
from datetime import datetime
from typing import List, Any

logger = logging.getLogger("NEMESIS_COLLECTOR")

class GBIOEdge:
    """Standardized representation of a blockchain transaction/transfer."""
    def __init__(self, tx_hash: str, src: str, dst: str, amt: float, ts: int):
        self.edge_id = tx_hash
        self.transaction_hash = tx_hash
        self.source_node_id = src
        self.target_node_id = dst
        self.amount_native = amt
        self.amount = amt
        self.timestamp = ts

async def _fetch_etherscan(session, address: str, chain: str) -> List[GBIOEdge]:
    """Real implementation for Etherscan/Blockscout API."""
    import os
    logger.info(f"Fetching trace for {address} on {chain} via Explorer APIs")
    
    chain = (chain or "").upper()
    api_url = "https://api.etherscan.io/api"
    api_key = os.getenv("ETHERSCAN_API_KEY", "")
    
    if chain in ["POLYGON", "MATIC"]:
        api_url = "https://api.polygonscan.com/api"
        api_key = os.getenv("POLYGONSCAN_API_KEY", "")
    elif chain in ["BSC", "BNB"]:
        api_url = "https://api.bscscan.com/api"
        api_key = os.getenv("BSCSCAN_API_KEY", "")
    elif chain in ["OPTIMISM", "OP"]:
        api_url = "https://api-optimism.etherscan.io/api"
        api_key = os.getenv("OPTIMISM_API_KEY", "")
    elif chain in ["ARBITRUM", "ARB"]:
        api_url = "https://api.arbiscan.io/api"
        api_key = os.getenv("ARBITRUM_API_KEY", "")
    elif chain in ["AVALANCHE", "AVAX"]:
        api_url = "https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api"
        api_key = os.getenv("SNOWTRACE_API_KEY", "")
    elif chain == "BASE":
        api_url = "https://api.basescan.org/api"
        api_key = os.getenv("BASESCAN_API_KEY", "")
    elif chain == "CELO":
        api_url = "https://api.celoscan.io/api"
        api_key = os.getenv("CELOSCAN_API_KEY", "")
    elif chain == "LINEA":
        api_url = "https://api.lineascan.build/api"
        api_key = os.getenv("LINEASCAN_API_KEY", "")

    edges = []
    
    # Normal Transactions
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": 50,
        "sort": "desc",
        "apikey": api_key
    }
    
    try:
        async with session.get(api_url, params=params) as resp:
            data = await resp.json()
            if data and data.get("status") == "1":
                for tx in data.get("result", []):
                    val = float(tx.get("value", 0)) / 1e18
                    if val > 0:
                        edges.append(GBIOEdge(
                            tx.get("hash"),
                            tx.get("from", "").lower(),
                            tx.get("to", "").lower(),
                            val,
                            int(tx.get("timeStamp", 0))
                        ))
    except Exception as e:
        logger.error(f"Error fetching normal txs: {e}")
        
    # ERC20 Token Transfers
    params["action"] = "tokentx"
    try:
        async with session.get(api_url, params=params) as resp:
            data = await resp.json()
            if data and data.get("status") == "1":
                for tx in data.get("result", []):
                    decimals = int(tx.get("tokenDecimal", 18))
                    val = float(tx.get("value", 0)) / (10 ** decimals)
                    if val > 0:
                        edge = GBIOEdge(
                            tx.get("hash"),
                            tx.get("from", "").lower(),
                            tx.get("to", "").lower(),
                            val,
                            int(tx.get("timeStamp", 0))
                        )
                        setattr(edge, "tokenSymbol", tx.get("tokenSymbol", ""))
                        setattr(edge, "tokenDecimal", decimals)
                        edges.append(edge)
    except Exception as e:
        logger.error(f"Error fetching token txs: {e}")
        
    return edges
async def run_collectors(session: aiohttp.ClientSession, address: str, chain: str = "eth"):
    """
    Replaces the broken bitquery_collectors.py logic.
    """
    try:
        edges = []
        # Run explorer collectors
        edges.extend(await _fetch_etherscan(session, address, chain))
        return edges
    except Exception as e:
        logger.error(f"Collector error for {address}: {e}")
        return []
