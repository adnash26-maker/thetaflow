"""
ThetaFlow - Value Chain Graph

Maps macro themes and catalysts to investment opportunities across
the full value chain. Each chain defines the flow from raw inputs
to end consumers, with specific public tickers at each layer.

Example: AI Infrastructure
  Catalyst: "Microsoft announces $80B AI capex"
  → Power Generation (CEG, VST, NRG)
  → Cooling Systems (VRT, GNRC)
  → Chip Manufacturing (NVDA, AMD, AVGO)
  → Data Center REITs (EQIX, DLR)
  → Cloud Platforms (AMZN, MSFT, GOOGL)
  → AI SaaS Applications (CRM, PLTR, NOW)
  → End Users (enterprise, consumer)
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class ChainNode:
    """A single layer in a value chain."""
    layer: str           # e.g., "Power Generation"
    description: str     # What this layer does
    tickers: List[Dict]  # [{ticker, company, exposure, notes}]
    position: int        # 0 = upstream (raw), higher = downstream
    sensitivity: float   # 0-1, how sensitive to the catalyst (1 = most exposed)


@dataclass
class ValueChain:
    """A complete value chain from catalyst to end user."""
    id: str
    name: str            # e.g., "AI Infrastructure"
    description: str
    catalyst_keywords: List[str]  # Keywords that trigger this chain
    nodes: List[ChainNode]
    theme_color: str = "#6366f1"


# ── AI / Data Center Value Chain ──

AI_INFRASTRUCTURE = ValueChain(
    id="ai_infra",
    name="AI Infrastructure",
    description="The full stack from power generation to AI applications. Triggered by AI capex announcements, chip demand signals, and data center construction.",
    catalyst_keywords=[
        "ai capex", "data center", "gpu demand", "ai infrastructure",
        "nvidia earnings", "cloud spending", "ai investment",
        "data center construction", "hyperscaler", "ai chip",
        "machine learning infrastructure", "training compute",
    ],
    theme_color="#8b5cf6",
    nodes=[
        ChainNode(
            layer="Nuclear / Power Generation",
            description="AI data centers consume massive power. Nuclear provides baseload carbon-free energy.",
            position=0, sensitivity=0.7,
            tickers=[
                {"ticker": "CEG", "company": "Constellation Energy", "exposure": "high",
                 "notes": "Largest US nuclear fleet. Direct PPAs with data centers."},
                {"ticker": "VST", "company": "Vistra Corp", "exposure": "high",
                 "notes": "Nuclear + natural gas. Benefits from power demand surge."},
                {"ticker": "NRG", "company": "NRG Energy", "exposure": "medium",
                 "notes": "Power generation diversified across sources."},
                {"ticker": "SMR", "company": "NuScale Power", "exposure": "high",
                 "notes": "Small modular reactors — next-gen nuclear for data centers."},
            ]
        ),
        ChainNode(
            layer="Cooling & Power Infrastructure",
            description="Data centers need cooling systems and power distribution equipment.",
            position=1, sensitivity=0.8,
            tickers=[
                {"ticker": "VRT", "company": "Vertiv Holdings", "exposure": "high",
                 "notes": "#1 in data center cooling and power management."},
                {"ticker": "ETN", "company": "Eaton Corp", "exposure": "medium",
                 "notes": "Electrical infrastructure for data centers."},
                {"ticker": "GNRC", "company": "Generac", "exposure": "medium",
                 "notes": "Backup power systems for data center reliability."},
            ]
        ),
        ChainNode(
            layer="Semiconductor Manufacturing",
            description="AI chips (GPUs, TPUs, custom ASICs) are the core compute layer.",
            position=2, sensitivity=1.0,
            tickers=[
                {"ticker": "NVDA", "company": "NVIDIA", "exposure": "critical",
                 "notes": "Dominant AI GPU supplier. 80%+ data center AI market share."},
                {"ticker": "AMD", "company": "AMD", "exposure": "high",
                 "notes": "MI300X competing for AI training workloads."},
                {"ticker": "AVGO", "company": "Broadcom", "exposure": "high",
                 "notes": "Custom AI chips (TPUs for Google) + networking."},
                {"ticker": "TSM", "company": "TSMC", "exposure": "critical",
                 "notes": "Manufactures chips for NVDA, AMD, Apple. Single point of dependency."},
                {"ticker": "ASML", "company": "ASML", "exposure": "high",
                 "notes": "Sole supplier of EUV lithography machines for advanced chips."},
            ]
        ),
        ChainNode(
            layer="Networking & Memory",
            description="High-bandwidth networking and memory for AI cluster interconnects.",
            position=3, sensitivity=0.7,
            tickers=[
                {"ticker": "MRVL", "company": "Marvell Technology", "exposure": "high",
                 "notes": "Custom AI networking silicon, optical interconnects."},
                {"ticker": "MU", "company": "Micron Technology", "exposure": "high",
                 "notes": "HBM (High Bandwidth Memory) for AI GPUs."},
                {"ticker": "ANET", "company": "Arista Networks", "exposure": "high",
                 "notes": "Data center switching for AI clusters."},
            ]
        ),
        ChainNode(
            layer="Data Center REITs & Builders",
            description="Physical facilities that house AI compute infrastructure.",
            position=4, sensitivity=0.6,
            tickers=[
                {"ticker": "EQIX", "company": "Equinix", "exposure": "high",
                 "notes": "Largest data center REIT globally."},
                {"ticker": "DLR", "company": "Digital Realty", "exposure": "high",
                 "notes": "Major data center REIT, growing AI-ready capacity."},
                {"ticker": "AAON", "company": "AAON Inc", "exposure": "medium",
                 "notes": "HVAC systems increasingly sold to data centers."},
            ]
        ),
        ChainNode(
            layer="Cloud Platforms",
            description="Hyperscalers that sell AI compute as a service.",
            position=5, sensitivity=0.8,
            tickers=[
                {"ticker": "MSFT", "company": "Microsoft", "exposure": "critical",
                 "notes": "Azure + OpenAI partnership. Largest enterprise AI cloud."},
                {"ticker": "AMZN", "company": "Amazon", "exposure": "high",
                 "notes": "AWS is #1 cloud. Bedrock, SageMaker for AI."},
                {"ticker": "GOOGL", "company": "Alphabet", "exposure": "high",
                 "notes": "GCP, Gemini, TPU custom silicon."},
                {"ticker": "META", "company": "Meta Platforms", "exposure": "high",
                 "notes": "Massive AI infra spend for Llama models + ads."},
            ]
        ),
        ChainNode(
            layer="AI Application Layer (SaaS)",
            description="Companies building products on top of AI infrastructure.",
            position=6, sensitivity=0.5,
            tickers=[
                {"ticker": "CRM", "company": "Salesforce", "exposure": "high",
                 "notes": "Agentforce, Einstein AI across CRM suite."},
                {"ticker": "PLTR", "company": "Palantir", "exposure": "high",
                 "notes": "AIP platform for enterprise AI deployment."},
                {"ticker": "NOW", "company": "ServiceNow", "exposure": "high",
                 "notes": "AI-powered IT workflow automation."},
                {"ticker": "SNOW", "company": "Snowflake", "exposure": "medium",
                 "notes": "AI/ML on enterprise data warehouse."},
                {"ticker": "AI", "company": "C3.ai", "exposure": "high",
                 "notes": "Enterprise AI application platform."},
            ]
        ),
    ]
)

# ── Clean Energy / Electrification Value Chain ──

CLEAN_ENERGY = ValueChain(
    id="clean_energy",
    name="Clean Energy & Electrification",
    description="The transition from fossil fuels to renewable energy and electric vehicles.",
    catalyst_keywords=[
        "renewable energy", "solar", "wind power", "ev sales",
        "battery", "lithium", "electric vehicle", "clean energy",
        "energy storage", "grid modernization", "ira credits",
        "carbon reduction", "net zero", "green hydrogen",
    ],
    theme_color="#10b981",
    nodes=[
        ChainNode(
            layer="Critical Minerals & Mining",
            description="Lithium, cobalt, rare earths for batteries and magnets.",
            position=0, sensitivity=0.6,
            tickers=[
                {"ticker": "ALB", "company": "Albemarle", "exposure": "high",
                 "notes": "Largest lithium producer. Battery-grade lithium."},
                {"ticker": "SQM", "company": "Sociedad Quimica y Minera", "exposure": "high",
                 "notes": "Chilean lithium producer, low-cost brine extraction."},
                {"ticker": "MP", "company": "MP Materials", "exposure": "medium",
                 "notes": "Only US rare earth mine (Mountain Pass)."},
            ]
        ),
        ChainNode(
            layer="Battery Manufacturing",
            description="Cell production for EVs and grid storage.",
            position=1, sensitivity=0.8,
            tickers=[
                {"ticker": "PANW", "company": "Panasonic (via ADR)", "exposure": "high",
                 "notes": "Tesla battery cell supplier, expanding US capacity."},
                {"ticker": "QS", "company": "QuantumScape", "exposure": "medium",
                 "notes": "Solid-state battery technology. Pre-revenue but breakthrough potential."},
                {"ticker": "ENVX", "company": "Enovix", "exposure": "medium",
                 "notes": "Silicon-anode batteries for higher energy density."},
            ]
        ),
        ChainNode(
            layer="Solar & Wind Manufacturing",
            description="Panel, inverter, and turbine manufacturers.",
            position=2, sensitivity=0.9,
            tickers=[
                {"ticker": "FSLR", "company": "First Solar", "exposure": "critical",
                 "notes": "US solar panel manufacturer. IRA beneficiary."},
                {"ticker": "ENPH", "company": "Enphase Energy", "exposure": "high",
                 "notes": "Microinverters for residential/commercial solar."},
                {"ticker": "SEDG", "company": "SolarEdge", "exposure": "high",
                 "notes": "Solar inverters and power optimizers."},
                {"ticker": "GE", "company": "GE Vernova", "exposure": "high",
                 "notes": "Wind turbines and grid equipment spin-off."},
            ]
        ),
        ChainNode(
            layer="EV Manufacturers",
            description="Electric vehicle makers driving battery demand.",
            position=3, sensitivity=0.7,
            tickers=[
                {"ticker": "TSLA", "company": "Tesla", "exposure": "critical",
                 "notes": "Market leader in EVs, energy storage, and charging."},
                {"ticker": "RIVN", "company": "Rivian", "exposure": "high",
                 "notes": "EV trucks and SUVs. Amazon delivery van partnership."},
                {"ticker": "GM", "company": "General Motors", "exposure": "medium",
                 "notes": "Ultium platform, scaling EV production."},
            ]
        ),
        ChainNode(
            layer="Charging Infrastructure",
            description="EV charging networks and equipment.",
            position=4, sensitivity=0.5,
            tickers=[
                {"ticker": "CHPT", "company": "ChargePoint", "exposure": "high",
                 "notes": "Largest EV charging network in North America."},
                {"ticker": "TSLA", "company": "Tesla (Supercharger)", "exposure": "high",
                 "notes": "Opening Supercharger network to other EVs via NACS."},
            ]
        ),
        ChainNode(
            layer="Grid & Utilities",
            description="Utilities and grid operators managing the energy transition.",
            position=5, sensitivity=0.4,
            tickers=[
                {"ticker": "NEE", "company": "NextEra Energy", "exposure": "high",
                 "notes": "Largest wind/solar utility in the world."},
                {"ticker": "AES", "company": "AES Corp", "exposure": "medium",
                 "notes": "Renewable energy and battery storage developer."},
            ]
        ),
    ]
)

# ── Biotech / GLP-1 / Healthcare Value Chain ──

BIOTECH_GLP1 = ValueChain(
    id="biotech_glp1",
    name="GLP-1 & Obesity Treatment",
    description="The GLP-1 weight loss drug revolution and its ripple effects across healthcare, food, and fitness.",
    catalyst_keywords=[
        "glp-1", "ozempic", "wegovy", "mounjaro", "weight loss drug",
        "obesity treatment", "semaglutide", "tirzepatide",
        "novo nordisk", "eli lilly", "obesity epidemic",
    ],
    theme_color="#ec4899",
    nodes=[
        ChainNode(
            layer="Drug Manufacturers",
            description="Companies developing and selling GLP-1 drugs.",
            position=0, sensitivity=1.0,
            tickers=[
                {"ticker": "NVO", "company": "Novo Nordisk", "exposure": "critical",
                 "notes": "Ozempic, Wegovy. Dominant GLP-1 franchise."},
                {"ticker": "LLY", "company": "Eli Lilly", "exposure": "critical",
                 "notes": "Mounjaro, Zepbound. Fastest-growing pharma."},
                {"ticker": "AMGN", "company": "Amgen", "exposure": "medium",
                 "notes": "MariTide — oral obesity drug in development."},
            ]
        ),
        ChainNode(
            layer="Drug Delivery & CDMO",
            description="Companies manufacturing injectable devices and drug substances.",
            position=1, sensitivity=0.7,
            tickers=[
                {"ticker": "BDX", "company": "Becton Dickinson", "exposure": "medium",
                 "notes": "Pre-filled syringe manufacturing for injectables."},
                {"ticker": "WST", "company": "West Pharma", "exposure": "medium",
                 "notes": "Injectable drug delivery components."},
            ]
        ),
        ChainNode(
            layer="Telehealth & Digital Health",
            description="Platforms enabling GLP-1 prescriptions and weight management.",
            position=2, sensitivity=0.6,
            tickers=[
                {"ticker": "HIMS", "company": "Hims & Hers", "exposure": "high",
                 "notes": "Compounded semaglutide prescriptions driving growth."},
                {"ticker": "TDOC", "company": "Teladoc", "exposure": "medium",
                 "notes": "Virtual visits for weight management programs."},
            ]
        ),
        ChainNode(
            layer="Disrupted Industries (Short Side)",
            description="Industries potentially hurt by widespread GLP-1 adoption.",
            position=3, sensitivity=0.5,
            tickers=[
                {"ticker": "DXCM", "company": "DexCom", "exposure": "negative",
                 "notes": "Glucose monitors — less diabetics = less demand. Potential short."},
                {"ticker": "ISRG", "company": "Intuitive Surgical", "exposure": "negative",
                 "notes": "Bariatric surgery volumes declining as GLP-1s replace surgery."},
                {"ticker": "MCD", "company": "McDonald's", "exposure": "negative",
                 "notes": "Reduced calorie consumption thesis — debated but real risk."},
            ]
        ),
    ]
)

# ── Cybersecurity Value Chain ──

CYBERSECURITY = ValueChain(
    id="cybersecurity",
    name="Cybersecurity",
    description="Growing cyber threats drive spending across endpoint, cloud, identity, and network security.",
    catalyst_keywords=[
        "cyber attack", "data breach", "ransomware", "cybersecurity spending",
        "zero trust", "security breach", "hacking", "cyber threat",
        "soc", "siem", "endpoint security", "identity security",
    ],
    theme_color="#f59e0b",
    nodes=[
        ChainNode(
            layer="Endpoint & XDR",
            description="Protecting devices and detecting threats across endpoints.",
            position=0, sensitivity=0.9,
            tickers=[
                {"ticker": "CRWD", "company": "CrowdStrike", "exposure": "critical",
                 "notes": "Leader in cloud-native endpoint security (Falcon platform)."},
                {"ticker": "S", "company": "SentinelOne", "exposure": "high",
                 "notes": "AI-powered autonomous endpoint protection."},
            ]
        ),
        ChainNode(
            layer="Network & Firewall",
            description="Network perimeter security and firewall infrastructure.",
            position=1, sensitivity=0.8,
            tickers=[
                {"ticker": "PANW", "company": "Palo Alto Networks", "exposure": "critical",
                 "notes": "Largest pure-play cybersecurity company. Platformization strategy."},
                {"ticker": "FTNT", "company": "Fortinet", "exposure": "high",
                 "notes": "Firewall and network security appliances."},
            ]
        ),
        ChainNode(
            layer="Identity & Access",
            description="Zero-trust identity verification and access management.",
            position=2, sensitivity=0.7,
            tickers=[
                {"ticker": "OKTA", "company": "Okta", "exposure": "high",
                 "notes": "Cloud identity and access management leader."},
                {"ticker": "CYBR", "company": "CyberArk", "exposure": "high",
                 "notes": "Privileged access management. Critical for zero-trust."},
            ]
        ),
        ChainNode(
            layer="Cloud Security",
            description="Securing cloud workloads, containers, and SaaS applications.",
            position=3, sensitivity=0.6,
            tickers=[
                {"ticker": "ZS", "company": "Zscaler", "exposure": "high",
                 "notes": "Cloud security gateway. Zero-trust network access."},
                {"ticker": "NET", "company": "Cloudflare", "exposure": "high",
                 "notes": "Edge security, DDoS protection, zero-trust platform."},
            ]
        ),
    ]
)


# ── Chain Registry ──

ALL_CHAINS: Dict[str, ValueChain] = {
    "ai_infra": AI_INFRASTRUCTURE,
    "clean_energy": CLEAN_ENERGY,
    "biotech_glp1": BIOTECH_GLP1,
    "cybersecurity": CYBERSECURITY,
}


def find_chains_for_event(headline: str) -> List[Dict]:
    """Match a news headline to relevant value chains.
    Returns list of {chain_id, chain_name, matched_keywords, relevance_score}."""
    headline_lower = headline.lower()
    matches = []

    for chain_id, chain in ALL_CHAINS.items():
        matched_keywords = []
        for kw in chain.catalyst_keywords:
            if kw in headline_lower:
                matched_keywords.append(kw)

        if matched_keywords:
            # More keyword matches = higher relevance
            relevance = min(len(matched_keywords) / 3, 1.0)
            matches.append({
                "chain_id": chain_id,
                "chain_name": chain.name,
                "matched_keywords": matched_keywords,
                "relevance_score": round(relevance, 2),
                "theme_color": chain.theme_color,
            })

    matches.sort(key=lambda m: m["relevance_score"], reverse=True)
    return matches


def get_chain_tickers(chain_id: str) -> List[Dict]:
    """Get all tickers in a value chain with their layer info."""
    chain = ALL_CHAINS.get(chain_id)
    if not chain:
        return []

    tickers = []
    for node in chain.nodes:
        for t in node.tickers:
            tickers.append({
                **t,
                "layer": node.layer,
                "position": node.position,
                "sensitivity": node.sensitivity,
            })
    return tickers
