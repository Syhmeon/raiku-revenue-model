"""
Classify 302 unclassified programs using behavioral fee data + address prefix patterns.
Also reclassify existing 314 programs with new RAIKU taxonomy.

Produces the final program_categories.csv (~616 rows).
"""
import csv
import json
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_MAP = PROJECT_ROOT / "data" / "mapping"
DATA_RAW = PROJECT_ROOT / "data" / "raw"


# ── TAXONOMY: single source of truth for 15 categories + unknown ──

TAXONOMY = {
    "prop_amm": {
        "subcategories": ["prop_amm", "proactive_mm"],
        "raiku_product": "aot",
        "tooltip": "Proprietary AMMs needing guaranteed top-of-block insertion for oracle updates.",
        "detail": (
            "Proprietary AMMs (single market maker, liquidity vault, no public frontend). "
            "Oracle updates must land to keep quotes fresh — stale quotes = adverse selection. "
            "AOT guarantees top-of-block insertion for every quote update. "
            "Examples: HumidiFi, BisonFi."
        ),
    },
    "quant_desk": {
        "subcategories": ["stat_arb", "momentum", "quantitative"],
        "raiku_product": "both",
        "tooltip": "Quantitative trading desks running algorithmic strategies.",
        "detail": (
            "Quantitative trading desks running algorithmic strategies (stat arb, momentum, etc.). "
            "AOT layers upstream of existing JIT pipeline — additive, not replacement. "
            "Higher P(execution) via AOT unlocks larger Kelly-sized positions. "
            "Still uses JIT as fallback."
        ),
    },
    "market_maker": {
        "subcategories": ["margin", "collateral", "settlement", "operational"],
        "raiku_product": "both",
        "tooltip": "Market maker operational txs: margin top-ups, collateral rebalancing, PnL settlement.",
        "detail": (
            "Market maker operational transactions: margin top-ups, collateral rebalancing, "
            "position rollovers, PnL settlement. These are NOT trading txs — they are housekeeping. "
            "CRITICAL ops use AOT reserved slots. IMPORTANT ops use AOT when available, JIT fallback. "
            "ROUTINE ops use standard submission."
        ),
    },
    "cranker": {
        "subcategories": ["keeper", "dca", "vault", "yield", "farming", "liquid", "restaking", "rebalance"],
        "raiku_product": "aot",
        "tooltip": "Protocol crankers/keepers submitting periodic state-transition transactions.",
        "detail": (
            "Protocol crankers/keepers: bots that submit periodic state-transition transactions "
            "on behalf of DeFi protocols. Cadence is predictable (every N slots), making AOT "
            "reservations highly efficient. Examples: Drift funding rate settlement, Jupiter DCA "
            "execution, Kamino vault rebalance, Marinade stake rebalancing, governance execution."
        ),
    },
    "arb_bot": {
        "subcategories": ["arbitrage", "mev", "sniper", "trading", "tips"],
        "raiku_product": "both",
        "tooltip": "Arbitrage bots and MEV searchers operating across DEX-DEX or DEX-CEX pairs.",
        "detail": (
            "Arbitrage bots and MEV searchers operating across DEX-DEX or DEX-CEX pairs. "
            "Current Jito JIT users. DEX-DEX: both legs on Solana, missed tx = missed opportunity only. "
            "DEX-CEX: CEX leg fills instantly, DEX leg MUST land or trader holds unhedged inventory "
            "— AOT critical. Includes snipers and MEV searchers."
        ),
    },
    "dex": {
        "subcategories": ["amm", "clmm", "orderbook", "aggregator", "liquidity"],
        "raiku_product": "both",
        "tooltip": "Decentralized exchange protocols: AMMs, CLMMs, orderbooks, aggregators.",
        "detail": (
            "Decentralized exchange protocols: AMMs, CLMMs, orderbooks, aggregators. "
            "LPs need reliable rebalancing (AOT). Traders need fast fills (JIT). "
            "Both execution paths relevant depending on tx type."
        ),
    },
    "lending": {
        "subcategories": ["lending", "borrowing", "structured_products", "options", "stablecoin", "bond"],
        "raiku_product": "both",
        "tooltip": "Lending, borrowing, and derivatives protocols with time-critical liquidations.",
        "detail": (
            "Lending, borrowing, and derivatives protocols. Liquidations are time-critical "
            "and benefit from AOT. Regular deposits/borrows use standard or JIT paths. "
            "Includes leveraged products and structured instruments."
        ),
    },
    "perps": {
        "subcategories": ["perpetuals", "futures", "funding"],
        "raiku_product": "both",
        "tooltip": "Perpetual futures protocols with funding rate settlements and position management.",
        "detail": (
            "Perpetual futures protocols. Funding rate settlements are cranker-type (AOT). "
            "Trader position opens/closes are JIT. Both paths relevant."
        ),
    },
    "oracle": {
        "subcategories": ["price_feed", "vrf", "data_feed"],
        "raiku_product": "aot",
        "tooltip": "Price oracle programs publishing on-chain feeds; stale prices cause bad liquidations.",
        "detail": (
            "Price oracle programs that publish on-chain price feeds. Updates must be timely "
            "and guaranteed — stale prices cause bad liquidations, incorrect funding rates, "
            "adverse selection. AOT reserved slots ensure oracle updates land in the correct slot."
        ),
    },
    "bridge": {
        "subcategories": ["cross_chain", "wormhole", "ntt"],
        "raiku_product": "both",
        "tooltip": "Cross-chain bridge programs; settlement finality benefits from AOT guarantees.",
        "detail": (
            "Cross-chain bridge programs. Settlement finality benefits from AOT guarantees. "
            "User-initiated transfers use standard or JIT paths."
        ),
    },
    "nft": {
        "subcategories": ["marketplace", "compressed", "lending"],
        "raiku_product": "both",
        "tooltip": "NFT marketplaces, NFT-backed lending, compressed NFT programs.",
        "detail": (
            "NFT marketplaces, NFT-backed lending, compressed NFT programs. "
            "High-value trades benefit from guaranteed execution. "
            "Standard listings use regular paths."
        ),
    },
    "gaming": {
        "subcategories": ["game", "casino", "prediction", "entertainment"],
        "raiku_product": "both",
        "tooltip": "On-chain gaming and entertainment requiring guaranteed tx timing.",
        "detail": (
            "On-chain gaming and entertainment. Real-time gameplay requires guaranteed tx "
            "timing (JIT). Scheduled game state updates and asset transfers can use AOT. "
            "Future DePIN/gaming convergence use case for Raiku."
        ),
    },
    "depin": {
        "subcategories": ["pow", "gpu_marketplace", "wireless", "iot", "network", "energy"],
        "raiku_product": "aot",
        "tooltip": "Decentralized physical infrastructure networks with scheduled coordination txs.",
        "detail": (
            "Decentralized physical infrastructure networks: GPU compute markets, wireless networks, "
            "IoT coordination, energy grids. Coordination transactions are scheduled and predictable "
            "— ideal for AOT. Examples: Helium, Hivemapper, Render."
        ),
    },
    "payments": {
        "subcategories": ["disbursement", "settlement", "cross_border"],
        "raiku_product": "aot",
        "tooltip": "Payment and settlement programs; disbursements can be scheduled ahead for AOT.",
        "detail": (
            "Payment and settlement programs. Disbursements can be scheduled up to 60s ahead "
            "— perfect AOT use case. Compliance-ready execution windows for regulated institutions "
            "and cross-border transfers."
        ),
    },
    "other": {
        "subcategories": [
            "staking", "token", "vote", "core", "metadata", "multisig",
            "governance", "infrastructure", "vesting", "program_deployment",
            "tips_distribution", "dao",
        ],
        "raiku_product": "neither",
        "tooltip": "Known programs with no direct Raiku AOT/JIT use case today.",
        "detail": (
            "All remaining programs with no direct Raiku AOT/JIT use case today. "
            "Includes system infrastructure, token programs, governance, staking, and "
            "other identified programs. Tracked for completeness and future re-classification."
        ),
    },
    "unknown": {
        "subcategories": ["unknown"],
        "raiku_product": "neither",
        "tooltip": "Unidentified programs — category and function not yet determined.",
        "detail": (
            "Programs whose function could not be identified from address prefix, name, "
            "or behavioral patterns. Require manual review or additional data to classify."
        ),
    },
}


# ── Manual overrides: programs identified via web research (Solscan, GitHub, web) ──

MANUAL_OVERRIDES = {
    # ── Confirmed via direct web research ──
    # breeze.baby — yield aggregator, auto-deposits
    "brzp8fNvHCBRi8UcCnTQUgQ2bQ4JnJTJtCvPzpKf2ty": ("Breeze", "cranker", "yield"),
    # magna.so — token vesting/distribution platform
    "magnaSHyAVk8E8FP7sHW2MrLnJjLVvf8nHsYGPD5YFq": ("Magna", "other", "vesting"),
    # openbook-dex/openbook-v2 GitHub — CLOB orderbook DEX
    "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb": ("OpenBook V2", "dex", "orderbook"),
    # onswig.com — smart wallet infrastructure
    "swigypWHEMPjCmQcJdMPKjJTMoCyPQsRj48jqcqWtip": ("Swig Wallet", "other", "infrastructure"),
    # Address suffix "pump" = pump.fun-deployed token program
    "cardWArq2BpdxYmrqpW5L4h8sPCxBLmEoZTVz2L4pump": ("Card (pump.fun)", "other", "token"),

    # ── Confirmed via tribixbite/Solana-programs.json gist ──
    # OpenOcean — DEX aggregator (cross-chain)
    "DF6c7dTBdZ9cb59pywKAVwy5NMSXiSfmXzYNwYFPNz9F": ("OpenOcean", "dex", "aggregator"),
    # Meteora Vault Program — automated liquidity vaults
    "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi": ("Meteora Vault", "cranker", "vault"),
    # Kamino Finance — yield vaults & DeFi automation
    "6LtLpnUFNByNXLyCoK9wA2MykKAmQNZKBdY8s47dehDc": ("Kamino", "cranker", "vault"),

    # ── Confirmed via Jito Foundation GitHub ──
    # Merkle Distributor — token airdrop/vesting distribution (Jito, Saber, OpenSea)
    "mERKcfxMC5SqJn4Ld4BUris3WKZZ1ojjWJ3A3J5CKxv": ("Merkle Distributor", "other", "token"),

    # ── Vanity prefix matches (same prefix as confirmed programs) ──
    # Second Magna program (same "magna" vanity prefix)
    "magnaSHyv8zzKJJmr8NSz5JXmtdGDTTFPEADmvNAwbj": ("Magna V2", "other", "vesting"),
    # Second Swig Wallet program (same "swigy" vanity prefix)
    "swigypWHEksbC64pWKwah1WTeh9JXwx8H1rJHLdbQMB": ("Swig Wallet V2", "other", "infrastructure"),
    # Second Card program (same "cardWArq" vanity prefix)
    "cardWArqhdV5jeRXXjUti7cHAa4mj41Nj3Apc6RPZH2": ("Card V2 (pump.fun)", "other", "token"),
    # SPL-related programs (SpL prefix = Solana Program Library utility)
    "SpLtnk7AMeg3WC8smGfPw43zFdeNeiYQSiecZM5tPBQ": ("SPL Utility", "other", "infrastructure"),
    "SpLtDgZYG9gn3UPjSsoBb4sjSbafT9ap9eXFU5UnScC": ("SPL Utility V2", "other", "infrastructure"),

    # ── Confirmed via Solscan labels + verified build ──
    # Solscan label: "Arbitrage Bot", verified build via Ellipsis Labs
    "SAbErai3UvzycbkooTMoQkD3Y7JVr5aEf7tEHBW1AWf": ("Verified Arb Bot (SAbEr)", "arb_bot", "arbitrage"),
    # Solscan: arbitrage_bot_icon in transactions, "IO" label, mintCredits instruction
    "idemJL67fKhpev5vKcxHrosuVyTat6wVC9sFfoPVg3Y": ("Arb Bot (idem)", "arb_bot", "arbitrage"),

    # ── Confirmed via GitHub repo ──
    # github.com/raelbob/bull-or-bear — prediction/gaming program, verified build
    "7FaMyGiTVdjm8dd3PxpjjCX15ibbmuE1zWVFX2PHxYUK": ("Bull-or-Bear", "gaming", "prediction"),

    # ── Confirmed via Solscan transaction analysis ──
    # Solscan label "PZ", instructions: join_tournament_with_sol, close_tournament
    # Interacts with cfldotfun.sol (Crypto Fantasy League — @cfldotfun on X)
    "pjwLcQDbzybW1FrHVbvkLzzk836S4TvXmtsqvYWC967": ("CFL (Crypto Fantasy League)", "gaming", "game"),

    # ── Confirmed via official documentation ──
    # docs.huma.finance/ecosystem-resources/smart-contracts — exact address match
    "HumaXepHnjaRCpjYTokxY4UtaJcmx41prQ8cxGmFC5fn": ("Huma Finance", "lending", "lending"),
    # Pyth PIP 5 + SolWatch spreadsheet — exact address match for Pyth Solana Receiver
    "rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ": ("Pyth Receiver", "oracle", "price_feed"),
}


# ── TASK 1: Reclassify existing 314 programs ──

def reclassify(old_cat, old_sub, pid, name):
    """Map old taxonomy to new RAIKU taxonomy."""
    # Manual overrides take priority
    if pid in MANUAL_OVERRIDES:
        _, cat, sub = MANUAL_OVERRIDES[pid]
        return cat, sub

    if old_cat == "trading_bot":
        if old_sub in ("arbitrage", "mev", "sniper", "trading", "tips"):
            return "arb_bot", old_sub
        elif old_sub == "keeper":
            return "cranker", "keeper"
        else:
            return "arb_bot", old_sub

    if old_cat == "dex":
        if old_sub in ("prop_amm", "proactive_mm"):
            return "prop_amm", old_sub
        elif old_sub == "governance":
            return "other", "governance"
        elif old_sub == "exchange":
            return "other", "infrastructure"
        else:
            return "dex", old_sub

    if old_cat == "defi":
        if old_sub == "vault":
            return "cranker", "vault"
        elif old_sub in ("yield", "yield_trading"):
            return "cranker", "yield" if old_sub == "yield_trading" else "yield"
        elif old_sub == "farming":
            return "cranker", "farming"
        elif old_sub in ("structured_products", "options", "stablecoin", "bond"):
            return "lending", old_sub
        elif old_sub == "liquidity":
            return "dex", "liquidity"
        elif old_sub == "prediction":
            return "gaming", "prediction"
        elif old_sub == "vesting":
            return "other", "vesting"
        elif old_sub == "unknown":
            return "unknown", "unknown"
        else:
            return "other", old_sub

    if old_cat == "staking":
        if old_sub == "liquid":
            return "cranker", "liquid"
        elif old_sub == "farming":
            return "cranker", "farming"
        else:
            return "other", "staking"

    if old_cat == "system":
        return "other", old_sub

    if old_cat == "mev":
        if old_sub == "tips":
            return "arb_bot", "tips"
        elif old_sub == "restaking":
            return "cranker", "restaking"
        elif old_sub == "tips_distribution":
            return "other", "tips_distribution"
        else:
            return "arb_bot", old_sub

    if old_cat == "mining":
        return "depin", "pow"
    if old_cat == "gambling":
        return "gaming", old_sub
    if old_cat == "infrastructure":
        return "other", "infrastructure"
    if old_cat == "governance":
        return "other", old_sub
    if old_cat == "compute":
        return "depin", "gpu_marketplace"
    if old_cat == "iot":
        return "depin", old_sub
    if old_cat == "unknown":
        return "unknown", "unknown"

    # Special case: other/unknown → unknown/unknown (separate from "other")
    if old_cat == "other" and old_sub == "unknown":
        return "unknown", "unknown"

    # Categories that stay (both old-that-persist AND new taxonomy categories)
    VALID_CATS = set(TAXONOMY.keys())  # all 16 valid categories
    if old_cat in VALID_CATS:
        return old_cat, old_sub

    return "unknown", "unknown"


# ── TASK 2: Behavioral classification for new programs ──

# Known prefixes from address patterns
PREFIX_MAP = {
    "arb1pg": ("Arbitrage Bot", "arb_bot", "arbitrage"),
    "trader": ("Trader Bot", "arb_bot", "trading"),
    "CCTPV2": ("Circle CCTP", "bridge", "cross_chain"),
    "Ccip84": ("Chainlink CCIP", "bridge", "cross_chain"),
    "stakev": ("Stake Program", "other", "staking"),
    "GovER5": ("Governance Program", "other", "governance"),
    "DCA265": ("DCA Program", "cranker", "dca"),
    "namesL": ("Solana Name Service", "other", "infrastructure"),
    "mine9Y": ("Mining Program", "depin", "pow"),
    "prgmQm": ("Program Manager", "other", "program_deployment"),
    "progSg": ("Program Deploy", "other", "program_deployment"),
    "Router": ("Router Program", "dex", "aggregator"),
    "SLendK": ("Solend v3", "lending", "lending"),
    "BLocKe": ("Blockworks", "other", "infrastructure"),
    "Priori": ("Priority Fee Program", "other", "infrastructure"),
    "TokExj": ("Token Extension", "other", "token"),
    "FLEET1": ("Star Atlas Fleet", "gaming", "game"),
    "save8R": ("Save Protocol", "lending", "lending"),
    "CndyV3": ("Candy Machine", "nft", "marketplace"),
    "LockrW": ("Lock Program", "other", "vesting"),
    "optsy3": ("Options Protocol", "lending", "options"),
    "AMMSgt": ("AMM Program", "dex", "amm"),
    "AMMJdE": ("AMM Program", "dex", "amm"),
    "goonu": ("GoonFi v1", "prop_amm", "prop_amm"),
    "eUSXyK": ("eUSD Protocol", "lending", "stablecoin"),
    "pyti8T": ("Pyth Integration", "oracle", "price_feed"),
    "pfeeUx": ("Priority Fee", "other", "infrastructure"),
    "migK82": ("Migration Program", "other", "infrastructure"),
    "tuktuk": ("TukTuk Program", "other", "infrastructure"),
    "WattJi": ("Watt Protocol", "depin", "network"),
    "JPDGXj": ("Jupiter DCA/Perps", "cranker", "dca"),
    "bidoyo": ("Bid Protocol", "unknown", "unknown"),
    "chopmf": ("Chop Protocol", "unknown", "unknown"),
    "GasB9d": ("Gas Protocol", "other", "infrastructure"),
    "foRGEL": ("Forge Protocol", "unknown", "unknown"),
    "CREWiq": ("Crew Protocol", "unknown", "unknown"),
    "BASKT7": ("Basket Protocol", "other", "token"),
    "EMBERp": ("Ember Protocol", "unknown", "unknown"),
    "UMBRA": ("Umbra Protocol", "unknown", "unknown"),
    "HumaXe": ("Huma Finance", "unknown", "unknown"),
    "mgrfTe": ("Manager Protocol", "other", "infrastructure"),
    "insco": ("Inscription Protocol", "unknown", "unknown"),
    "DERP2s": ("DERP Protocol", "unknown", "unknown"),
    "rec5EK": ("Receipt Program", "unknown", "unknown"),
    "SRSLY1": ("Seriously Protocol", "unknown", "unknown"),
    "SSSnRE": ("SSS Protocol", "unknown", "unknown"),
    "iLEPWo": ("iLEP Protocol", "unknown", "unknown"),
    "fragnA": ("Fragment Protocol", "unknown", "unknown"),
    "zoRabw": ("Zora Protocol", "unknown", "unknown"),
    "unpXTU": ("UNP Protocol", "unknown", "unknown"),
    "brcXo6": ("BRC Protocol", "unknown", "unknown"),
    "axjx66": ("AXJ Protocol", "unknown", "unknown"),
    "AUMinu": ("AUM Protocol", "unknown", "unknown"),
    "tbd3Qd": ("TBD Protocol", "unknown", "unknown"),
    "oxp5da": ("OXP Protocol", "unknown", "unknown"),
    "prm1az": ("Premium Protocol", "unknown", "unknown"),
    "QuaNTM": ("Quant Protocol", "unknown", "unknown"),
    "MashGQ": ("Mash Protocol", "unknown", "unknown"),
    "snapNQ": ("Snapshot Program", "other", "infrastructure"),
    "CMACYf": ("CMAC Program", "unknown", "unknown"),
    "CMAGAK": ("CMA Program", "unknown", "unknown"),
}

# NTT bridge prefixes (Wormhole NTT)
NTT_PREFIXES = ["NTTB7G", "NtTtwq", "NTTLfv", "nTT36H", "NtttGj", "nttLq3", "nttmCZ", "nttu74", "NttMjd", "nTtWMB"]
# Rate-related prefixes
RATE_PREFIXES = ["rateqk", "rateE3", "raTEMK", "raTEkE", "RateFz", "RatEqy", "RAte2g", "rAte14", "raTeAJ"]


def classify_behavioral(pid, tx, sx, pf, cu, avg_cu, fpc, med_fpc, blocks):
    """Classify a program by its on-chain behavioral patterns."""
    # Manual overrides take priority
    if pid in MANUAL_OVERRIDES:
        name, cat, sub = MANUAL_OVERRIDES[pid]
        return name, cat, sub, "manual_override"

    fail = (tx - sx) / tx * 100 if tx > 0 else 0

    # Check NTT bridge prefixes
    for ntt in NTT_PREFIXES:
        if pid.startswith(ntt):
            return "NTT Bridge", "bridge", "cross_chain", f"NTT prefix"

    # Check rate prefixes
    for rate in RATE_PREFIXES:
        if pid.startswith(rate):
            return "Rate Program", "other", "infrastructure", f"rate prefix"

    # Check known prefixes
    for prefix, (name, cat, sub) in PREFIX_MAP.items():
        if pid.startswith(prefix):
            return name, cat, sub, f"prefix:{prefix}"

    # Behavioral classification
    # Very high fail rate = arb bot
    if fail > 70:
        return f"Arb Bot ({pid[:5]})", "arb_bot", "arbitrage", f"{fail:.0f}% fail rate"

    # High fee/CU + moderate fail = MEV/arb
    if fpc > 10 and fail > 5:
        return f"MEV Bot ({pid[:5]})", "arb_bot", "mev", f"fpc={fpc:.1f} fail={fail:.0f}%"

    # High fee/CU + low fail + low tx = whale bot
    if fpc > 10 and tx < 5000:
        return f"Trading Bot ({pid[:5]})", "arb_bot", "trading", f"fpc={fpc:.1f} low_tx={tx}"

    # Tiny CU + high fee = tip program
    if avg_cu < 1000 and fpc > 5:
        return f"Tip Program ({pid[:5]})", "arb_bot", "tips", f"tiny_cu={avg_cu} fpc={fpc:.1f}"

    # Medium fail (20-50%) with volume = trading bot
    if 20 < fail <= 70 and tx > 1000:
        return f"Trading Bot ({pid[:5]})", "arb_bot", "trading", f"{fail:.0f}% fail"

    # Very consistent (low fail, many txs, small CU) = keeper/crank
    if fail < 5 and tx > 50000 and avg_cu < 20000:
        return f"Keeper ({pid[:5]})", "cranker", "keeper", f"consistent: {tx}tx {avg_cu}cu"

    # High CU + low fail = DeFi protocol
    if fail < 10 and avg_cu > 100000 and tx > 100:
        return f"DeFi Protocol ({pid[:5]})", "unknown", "unknown", f"high_cu={avg_cu}"

    # Default: unknown
    return f"Unknown ({pid[:5]})", "unknown", "unknown", f"tx={tx} fail={fail:.0f}% fpc={fpc:.1f}"


def main():
    # ── TASK 1: Reclassify existing 314 ──
    print("=== TASK 1: Reclassifying 314 existing programs ===")
    existing_rows = []
    with open(DATA_MAP / "program_categories.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            old_cat = row["raiku_category"]
            old_sub = row["subcategory"]
            pid = row["program_id"]
            new_cat, new_sub = reclassify(old_cat, old_sub, pid, row["program_name"])
            # Apply name from manual overrides if available
            prog_name = MANUAL_OVERRIDES[pid][0] if pid in MANUAL_OVERRIDES else row["program_name"]
            # Fix raiku_product for reclassified or empty-product programs
            product = row["raiku_product"]
            source = row["source"]
            # Manual overrides: update product from taxonomy if category changed
            if pid in MANUAL_OVERRIDES:
                new_tax_product = TAXONOMY.get(new_cat, {}).get("raiku_product", "neither")
                product = new_tax_product
                source = "manual_override"
            # Auto_classified with empty product: assign from taxonomy
            elif not product and source == "auto_classified":
                if new_cat in TAXONOMY:
                    product = TAXONOMY[new_cat]["raiku_product"]
                else:
                    product = "neither"
                # High-CU unknowns get "potential"
                if new_cat == "unknown":
                    product = "potential"
                    source = "behavioral"

            existing_rows.append({
                "program_id": pid,
                "program_name": prog_name,
                "raiku_category": new_cat,
                "subcategory": new_sub,
                "raiku_product": product,
                "source": source,
                "notes": row["notes"],
            })

    print(f"  Reclassified {len(existing_rows)} programs")
    old_cats = Counter(r["raiku_category"] for r in existing_rows)
    for cat, count in sorted(old_cats.items(), key=lambda x: -x[1]):
        print(f"    {cat:<15} {count:>4}")

    # ── TASK 2: Classify 302 new programs ──
    print("\n=== TASK 2: Classifying new programs ===")
    classified_ids = set(r["program_id"] for r in existing_rows)

    new_rows = []
    with open(DATA_RAW / "dune_program_fees_v2.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            pid = row["program_id"]
            if pid in classified_ids:
                continue

            tx = int(row["tx_count"])
            sx = int(row["success_count"])
            pf = float(row["priority_fees_sol"])
            cu = int(row["total_cu"])
            avg_cu = int(row["avg_cu_per_tx"])
            fpc = float(row["avg_priority_fee_per_cu_lamports"])
            med_fpc = float(row["median_priority_fee_per_cu_lamports"])
            blocks = int(row["blocks_touched"])

            name, cat, sub, reason = classify_behavioral(
                pid, tx, sx, pf, cu, avg_cu, fpc, med_fpc, blocks
            )

            # Assign raiku_product from taxonomy (or "potential" for unknown high-CU)
            if cat in TAXONOMY:
                product = TAXONOMY[cat]["raiku_product"]
            else:
                product = "neither"
            # High-CU unknowns get "potential" (they may be RAIKU customers)
            if cat == "unknown" and avg_cu > 100000:
                product = "potential"
                source = "behavioral"
            else:
                source = "auto_classified"

            new_rows.append({
                "program_id": pid,
                "program_name": name,
                "raiku_category": cat,
                "subcategory": sub,
                "raiku_product": product,
                "source": source,
                "notes": reason,
            })

    print(f"  Classified {len(new_rows)} new programs")
    new_cats = Counter(r["raiku_category"] for r in new_rows)
    for cat, count in sorted(new_cats.items(), key=lambda x: -x[1]):
        print(f"    {cat:<15} {count:>4}")

    # ── Preview ──
    print(f"\n--- PREVIEW: First 20 auto-classified ---")
    print(f"{'Name':<28} {'Category':<12} {'Sub':<15} {'Reason':<45}")
    print("-" * 100)
    for r in new_rows[:20]:
        print(f"{r['program_name']:<28} {r['raiku_category']:<12} {r['subcategory']:<15} {r['notes']:<45}")

    # ── Write final file ──
    all_rows = existing_rows + new_rows
    print(f"\n=== WRITING FINAL FILE: {len(all_rows)} programs ===")

    # Verify no old categories remain
    forbidden = {"trading_bot", "defi", "system", "mev", "mining", "gambling",
                 "infrastructure", "governance", "compute", "iot", "staking"}
    final_cats = set(r["raiku_category"] for r in all_rows)
    remaining = forbidden & final_cats
    if remaining:
        print(f"  WARNING: Old categories still present: {remaining}")
    else:
        print("  OK: All old categories eliminated")

    outpath = DATA_MAP / "program_categories.csv"
    fieldnames = ["program_id", "program_name", "raiku_category", "subcategory",
                  "raiku_product", "source", "notes"]
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"  Written to {outpath}")

    # ── Final summary ──
    total_cats = Counter(r["raiku_category"] for r in all_rows)
    print(f"\n=== FINAL CATEGORY DISTRIBUTION ({len(all_rows)} programs) ===")
    for cat, count in sorted(total_cats.items(), key=lambda x: -x[1]):
        print(f"  {cat:<15} {count:>4}")

    # Spot checks
    print(f"\n=== SPOT CHECKS ===")
    checks = {
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium V4",
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter V6",
        "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt": "Jito Tip Payment",
        "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH": "Pyth Oracle",
        "9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp": "HumidiFi",
    }
    for pid, expected_name in checks.items():
        match = [r for r in all_rows if r["program_id"] == pid]
        if match:
            r = match[0]
            print(f"  {expected_name}: {r['raiku_category']}/{r['subcategory']} (product={r['raiku_product']})")
        else:
            print(f"  {expected_name}: NOT FOUND!")


if __name__ == "__main__":
    main()
