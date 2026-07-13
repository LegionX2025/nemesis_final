import os
import logging
import asyncio
import aiohttp
from typing import Dict, Any

from services.api_rotator import rotator
try:
    from scripts.oklink_scraper import scrape_oklink_tags
except ImportError:
    scrape_oklink_tags = None
from scripts.osint_orchestrator import aggregate_osint
try:
    from scripts.darknet import WalletEnrichmentEngine, UIEEngine
except ImportError:
    WalletEnrichmentEngine = None
    UIEEngine = None

logger = logging.getLogger("NEMESIS_INTELLIGENCE")

class IntelligencePipeline:
    """
    Enterprise Unified Intelligence Pipeline.
    Aggregates multi-domain intelligence from Graph, AI, OSINT engines, and Darknet.
    """
    @classmethod
    async def query_cached_intel(cls, address: str) -> Dict[str, Any]:
        """Queries MongoDB/Neo4J/CF for existing intelligence on this address."""
        try:
            from pymongo import MongoClient
            MONGO_URI = os.getenv("DATABASE_MONGO_URL", os.getenv("VITE_DATABASE_MONGO_URL", "mongodb://localhost:27017"))
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            db = client["blockchain_intel"]
            doc = db["entities"].find_one({"$or": [{"address": address}, {"addresses": address}]})
            if doc:
                return doc
        except Exception as e:
            logger.warning(f"Failed to query MongoDB cache: {e}")
        return None

    @classmethod
    async def run(cls, address: str, max_depth: int = 1) -> Dict[str, Any]:
        """Runs the intelligence pipeline for a specific address in parallel."""
        logger.info(f"Extracting Multi-Domain Intelligence for {address}...")
        
        # 1. Fast Cache Query (MongoDB / D1)
        cached_data = await asyncio.to_thread(cls.query_cached_intel, address)
        if cached_data:
            logger.info(f"Cache hit for {address} in MongoDB.")
        
        # 2. Parallel Task Execution for OSINT, On-Chain Labeling, and Darknet Enrichment
        oklink_task = asyncio.create_task(scrape_oklink_tags("eth", address))
        osint_task = asyncio.create_task(aggregate_osint("Unknown", "CRYPTO_WALLET", address, "ETHEREUM"))
        
        # Live Darknet Enrichment
        darknet_task = asyncio.create_task(WalletEnrichmentEngine().enrich_wallet_data(address))
        
        # Wait for all intelligence gathers
        oklink_data, osint_data, darknet_data = await asyncio.gather(
            oklink_task, osint_task, darknet_task, return_exceptions=True
        )
        
        # Safe unwrap if exceptions occurred during gather
        if isinstance(oklink_data, Exception) or not oklink_data:
            oklink_data = {"attributionTags": ["Unresolved"]}
        if isinstance(osint_data, Exception) or not osint_data:
            osint_data = {
                "entity_name": "Unknown Entity",
                "scores": {"threat": 0, "trust": 0},
                "negative_news": [],
                "crunchbase_summary": "",
                "logo": ""
            }
        if isinstance(darknet_data, Exception) or not darknet_data:
            darknet_data = {"threat_level": "Unknown", "darknet_mentions": 0}

        # Build Entity Data using OSINT as base and Oklink for tags
        tags = oklink_data.get("attributionTags", [])
        if cached_data and cached_data.get("tags"):
            tags.extend(cached_data.get("tags"))
            tags = list(set(tags)) # Deduplicate
            
        # Determine entity class
        entity_class = "EOA_WALLET"
        if any(tag.lower() in ["contract", "token", "dex", "router", "exchange"] for tag in tags):
            entity_class = "SMART_CONTRACT"

        entity_name = osint_data.get("entity_name")
        if not entity_name or entity_name == "Unknown Entity":
            if cached_data and cached_data.get("name"):
                entity_name = cached_data.get("name")
            elif tags and tags[0] != "Unresolved":
                entity_name = tags[0].title()
            else:
                entity_name = "Unknown Entity"
                
        # Merge threat scores from Darknet and OSINT
        risk_score = osint_data.get("scores", {}).get("threat", 15)
        if darknet_data.get("darknet_mentions", 0) > 0:
            risk_score = max(risk_score, 85)

        logo = osint_data.get("logo", "")
        if cached_data and cached_data.get("logo"):
            logo = cached_data.get("logo")

        # Prepare the response for the UI
        return {
            "address": address,
            "chain": "ETHEREUM",  # Can be parameterized
            "nodes": [
                {
                    "id": address,
                    "label": entity_name if entity_name != "Unknown Entity" else address, # Do not redact if known
                    "group": entity_class,
                    "title": f"Address: {address}<br>Entity: {entity_name}<br>Tags: {', '.join(tags)}<br>Threat: {risk_score}",
                    "logo": logo,
                    "entity_class": entity_class,
                    "risk_score": risk_score,
                    "properties": {
                        "name": entity_name,
                        "tags": tags,
                        "osint_summary": osint_data.get("crunchbase_summary", ""),
                        "negative_news": osint_data.get("negative_news", []),
                        "darknet_intel": darknet_data
                    }
                }
            ],
            "edges": [], # Edges are populated by the trace endpoint, not the dossier init
            "metadata": {
                "confidence": 0.85 if cached_data else 0.60,
                "sources": ["osint", "oklink", "playwright", "darknet", "mongodb"]
            }
        }
