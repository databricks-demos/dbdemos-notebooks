"""SAP Financial Services industry use case responses and examples for SAP mode."""

SAP_FINS_RESPONSES = [
    {
        "question": "FinBot, given a 2% drop in GDP and 5% inflation over the next 6 months, simulate the impact on our cash flow by business unit and generate three potential mitigation strategies.",
        "data_scientist": "FinBot, given a 2% drop in GDP and 5% inflation over the next 6 months, simulate the impact on our cash flow by business unit and generate three potential mitigation strategies.",
        "non_intelligent_answer": "I'll analyze the economic scenario and estimate the impact on cash flow. Please provide more details about your business units for a more accurate analysis.",
        "tools": [
            {
                "tool_name": "simulate_economic_impact",
                "description": "Simulate cash flow impact in SAP FSCM",
                "type": "SAP_ANALYTICS",
                "reasoning": "Simulating the effect of macroeconomic conditions on different business units using SAP Financial Supply Chain Management",
                "informations": [
                    "SAP financial simulations integrate economic indicators with internal business data to project impacts across different organizational units.\n[Learn more about SAP financial simulation](https://www.databricks.com/product/solutions/finance)"
                ]
            },
            {
                "tool_name": "generate_mitigation_strategies",
                "description": "Create data-driven financial recommendations via SAP BPC",
                "type": "SAP_PLANNING",
                "reasoning": "Using SAP Business Planning and Consolidation to generate mitigation strategies based on the simulation results",
                "informations": [
                    "AI-powered recommendations through SAP can analyze multiple scenarios and propose optimized mitigation strategies tailored to specific business contexts"
                ]
            }
        ],
        "finbot": "**Simulation complete.** Here's the projected cash flow impact by business unit under the given macroeconomic conditions:\n\n**Retail BU:** -**14%** cash flow due to reduced consumer spending\n\n**Manufacturing BU:** -**9%** driven by rising raw material costs\n\n**Services BU:** -**5%** from delayed customer payments\n\n**📊 Identified Risks:**\n• Potential liquidity gap of **$20M** in Q3\n• Operating margin compression by **2.3%**\n• Increased reliance on short-term credit lines\n\n**💡 Mitigation Strategies:**\n1️⃣ Defer **$8M** in discretionary CAPEX projects\n2️⃣ Renegotiate supplier contracts to hedge input costs\n3️⃣ Introduce dynamic discounting to accelerate receivables",
        "final_answer": "**Simulation complete.** Here's the projected cash flow impact by business unit under the given macroeconomic conditions:\n\n**Retail BU:** -**14%** cash flow due to reduced consumer spending\n\n**Manufacturing BU:** -**9%** driven by rising raw material costs\n\n**Services BU:** -**5%** from delayed customer payments\n\n**📊 Identified Risks:**\n• Potential liquidity gap of **$20M** in Q3\n• Operating margin compression by **2.3%**\n• Increased reliance on short-term credit lines\n\n**💡 Mitigation Strategies:**\n1️⃣ Defer **$8M** in discretionary CAPEX projects\n2️⃣ Renegotiate supplier contracts to hedge input costs\n3️⃣ Introduce dynamic discounting to accelerate receivables",
        "final_informations": [
            "SAP financial simulations integrate real-time economic indicators with business data to provide actionable insights during uncertain economic conditions"
        ]
    },
    {
        "question": "Can you break down the projected liquidity gap by month and flag which BUs contribute most to the shortfall?",
        "data_scientist": "Can you break down the projected liquidity gap by month and flag which BUs contribute most to the shortfall?",
        "non_intelligent_answer": "To analyze the liquidity gap by month and business unit, I'll need to access our financial projections. Let me break down the data we have available.",
        "tools": [
            {
                "tool_name": "analyze_liquidity_forecast",
                "description": "Analyze detailed liquidity projections in SAP Treasury",
                "type": "SAP_FINANCE",
                "reasoning": "Examining granular liquidity forecasts from SAP Treasury to break down the gap by month and business unit",
                "informations": [
                    "SAP Treasury management provides detailed liquidity forecasting capabilities that help identify cash flow patterns and risks across different time horizons"
                ]
            },
            {
                "tool_name": "identify_risk_contributors",
                "description": "Identify main contributing factors to liquidity risk via SAP GRC",
                "type": "SAP_RISK_ANALYSIS",
                "reasoning": "Using SAP Governance, Risk and Compliance to identify which business units and factors contribute most to the liquidity shortfall",
                "informations": [
                    "Risk factor analysis in SAP systems can identify root causes of financial challenges and prioritize mitigation efforts"
                ]
            }
        ],
        "finbot": "**📈 Monthly Breakdown of $20M Liquidity Gap:**\n\n**July:** **$4M** shortfall\n• Retail BU: **60%**\n• Manufacturing: **30%**\n• Services: **10%**\n\n**August:** **$6M** shortfall\n• Retail BU: **50%**\n• Manufacturing: **35%**\n• Services: **15%**\n\n**September:** **$10M** shortfall\n• Retail BU: **45%**\n• Manufacturing: **40%**\n• Services: **15%**\n\n**🔍 Top Contributors:**\n\n**Retail BU:** **$9.5M** total gap\n• Primary factor: Inventory buildup and slower sales\n\n**Manufacturing BU:** **$7M** total\n• Primary factor: Increased procurement costs\n\n**Services BU:** **$3.5M** total\n• Primary factor: Delayed customer payments",
        "final_answer": "**📈 Monthly Breakdown of $20M Liquidity Gap:**\n\n**July:** **$4M** shortfall\n• Retail BU: **60%**\n• Manufacturing: **30%**\n• Services: **10%**\n\n**August:** **$6M** shortfall\n• Retail BU: **50%**\n• Manufacturing: **35%**\n• Services: **15%**\n\n**September:** **$10M** shortfall\n• Retail BU: **45%**\n• Manufacturing: **40%**\n• Services: **15%**\n\n**🔍 Top Contributors:**\n\n**Retail BU:** **$9.5M** total gap\n• Primary factor: Inventory buildup and slower sales\n\n**Manufacturing BU:** **$7M** total\n• Primary factor: Increased procurement costs\n\n**Services BU:** **$3.5M** total\n• Primary factor: Delayed customer payments",
        "final_informations": [
            "Detailed liquidity analysis through SAP systems helps pinpoint specific cash flow challenges and prioritize mitigation strategies by business unit"
        ]
    },
    {
        "question": "What would be the cash flow impact if we applied a 10% opex reduction across all BUs starting next month?",
        "data_scientist": "What would be the cash flow impact if we applied a 10% opex reduction across all BUs starting next month?",
        "non_intelligent_answer": "A 10% reduction in operating expenses would likely improve cash flow. To provide specific numbers, I'll need to analyze your current expense structure and business unit financials.",
        "tools": [
            {
                "tool_name": "simulate_cost_reduction",
                "description": "Simulate cost-cutting scenario in SAP BPC",
                "type": "SAP_SIMULATION",
                "reasoning": "Creating a simulation in SAP Business Planning and Consolidation to model the impact of an opex reduction across different business units",
                "informations": [
                    "SAP financial planning allows for granular scenario modeling to test various cost reduction strategies and their impacts on cash flow"
                ]
            },
            {
                "tool_name": "calculate_net_impact",
                "description": "Calculate resulting liquidity position via SAP FI",
                "type": "SAP_FINANCE",
                "reasoning": "Using SAP Financial Accounting to calculate the net impact on the company's liquidity position after the proposed cost reductions",
                "informations": [
                    "Comprehensive financial impact assessments through SAP help quantify benefits while identifying any potential operational constraints"
                ]
            }
        ],
        "finbot": "**💰 Cash Flow Uplift (Next 6 Months):**\n\n**Retail BU:** +**$4.2M**\n\n**Manufacturing BU:** +**$3.5M**\n\n**Services BU:** +**$2.1M**\n\n**Total Net Improvement:** +**$9.8M**\n\n**📊 Revised Liquidity Position:**\n• Original gap: **$20M**\n• New gap: **$10.2M** (49% reduction)\n\n**⚠️ Note:** This analysis assumes no negative impact on revenue from cost-saving measures.",
        "final_answer": "**💰 Cash Flow Uplift (Next 6 Months):**\n\n**Retail BU:** +**$4.2M**\n\n**Manufacturing BU:** +**$3.5M**\n\n**Services BU:** +**$2.1M**\n\n**Total Net Improvement:** +**$9.8M**\n\n**📊 Revised Liquidity Position:**\n• Original gap: **$20M**\n• New gap: **$10.2M** (49% reduction)\n\n**⚠️ Note:** This analysis assumes no negative impact on revenue from cost-saving measures.",
        "final_informations": [
            "Data-driven cost reduction strategies through SAP financial systems can precisely quantify cash flow improvements while highlighting any operational trade-offs"
        ]
    }
] 