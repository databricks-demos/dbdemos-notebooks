"""SAP Financial Services industry use case responses and examples for SAP mode."""

SAP_FINS_RESPONSES = [
    {
        "question": "FinBot, given a 2% drop in GDP and 5% inflation over the next 6 months, simulate the impact on our cash flow by business unit and generate three potential mitigation strategies.",
        "data_scientist": "FinBot, given a 2% drop in GDP and 5% inflation over the next 6 months, simulate the impact on our cash flow by business unit and generate three potential mitigation strategies.",
        "non_intelligent_answer": "I'll analyze the economic scenario and estimate the impact on cash flow. Please provide more details about your business units for a more accurate analysis.",
        "tools": [
            {
                "tool_name": "get_economic_data and monte_carlo_simulation",
                "description": "Simulate cash flow impact using the distributed compute power of PySpark",
                "type": "SAP_ANALYTICS",
                "reasoning": "Simulating the effect of macroeconomic conditions on cash flow of different business units",
                "informations": [
                    "Monte Carlo simulation optionally supports specifying one of three dozen statistical distributions.\n[Learn more about Monte Carlos simulation using PySpark](https://community.databricks.com/t5/technical-blog/migrating-ml-workload-to-databricks/ba-p/114180#toc-hId--1941021199)"
                ]
            },
            {
                "tool_name": "generate_mitigation_strategies",
                "description": "AI-generated recommendations to address the identified risk factors",
                "type": "SAP_PLANNING",
                "reasoning": "GenAI-powered analysis offers tailored recommendations to address the specific business context",
                "informations": [
                    "AI-powered recommendations through SAP can analyze multiple scenarios and propose optimized mitigation strategies tailored to specific business contexts"
                ]
            }
        ],
        "finbot": "**Simulation complete.** Here's the projected cash flow impact by business unit under the given macroeconomic conditions:\n\n**Retail BU:** -**14%** cash flow due to reduced consumer spending\n\n**Manufacturing BU:** -**9%** driven by rising raw material costs\n\n**Services BU:** -**5%** from delayed customer payments\n\n**📊 Identified Risks:**\n• Cost of goods sold (COGS) increases by **7%**\n• Sales declines by **4%**\n• Working capital reduction of **$20M** in Q3\n\n**💡 Mitigation Strategies:**\n1️⃣ Renegotiate supplier contracts to lengthen Accounts Payable (AP) Net Terms\n2️⃣ Accelerate Accounts Receivables (AR) collection through early payment discounts\n3️⃣ Reduce inventory levels",
        "final_answer": "**Simulation complete.** Here's the projected cash flow impact by business unit under the given macroeconomic conditions:\n\n**Retail BU:** -**14%** cash flow due to reduced consumer spending\n\n**Manufacturing BU:** -**9%** driven by rising raw material costs\n\n**Services BU:** -**5%** from delayed customer payments\n\n**📊 Identified Risks:**\n• Cost of goods sold (COGS) increases by **7%**\n• Sales declines by **4%**\n• Working capital reduction of **$20M** in Q3\n\n**💡 Mitigation Strategies:**\n1️⃣ Renegotiate supplier contracts to lengthen Accounts Payable (AP) Net Terms\n2️⃣ Accelerate Accounts Receivables (AR) collection through early payment discounts\n3️⃣ Reduce inventory levels",
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
                "tool_name": "disaggregate_monte_carlo",
                "description": "Disaggregate Monte Carlos simulation results",
                "type": "SAP_FINANCE",
                "reasoning": "Display the by month by BU Monte Carlo cash flow simulation result",
                "informations": [
                    "Monte Carlo simulation provides detailed breakdowns of financial projections across different time periods and business units"
                ]
            },
            {
                "tool_name": "key_factor_summarization",
                "description": "Summarize key contributing factors",
                "type": "SAP_RISK_ANALYSIS",
                "reasoning": "GenAI-powered analysis of changes in various attributes to determine the key drivers for the target attribute of cash flow",
                "informations": [
                    "Risk factor analysis can identify root causes of financial challenges and prioritize mitigation efforts"
                ]
            }
        ],
        "finbot": "**📈 Monthly Breakdown of $20M Liquidity Gap:**\n\n**July:** **$4M** shortfall\n• Retail BU: **60%**\n• Manufacturing: **30%**\n• Services: **10%**\n\n**August:** **$6M** shortfall\n• Retail BU: **50%**\n• Manufacturing: **35%**\n• Services: **15%**\n\n**September:** **$10M** shortfall\n• Retail BU: **45%**\n• Manufacturing: **40%**\n• Services: **15%**\n\n**🔍 Top Contributors:**\n\n**Retail BU:** **$9.5M** total gap\n• Primary factor: Inventory buildup and slower sales\n\n**Manufacturing BU:** **$7M** total\n• Primary factor: Increased procurement costs\n\n**Services BU:** **$3.5M** total\n• Primary factor: Delayed customer payments",
        "final_answer": "**📈 Monthly Breakdown of $20M Liquidity Gap:**\n\n**July:** **$4M** shortfall\n• Retail BU: **60%**\n• Manufacturing: **30%**\n• Services: **10%**\n\n**August:** **$6M** shortfall\n• Retail BU: **50%**\n• Manufacturing: **35%**\n• Services: **15%**\n\n**September:** **$10M** shortfall\n• Retail BU: **45%**\n• Manufacturing: **40%**\n• Services: **15%**\n\n**🔍 Top Contributors:**\n\n**Retail BU:** **$9.5M** total gap\n• Primary factor: Inventory buildup and slower sales\n\n**Manufacturing BU:** **$7M** total\n• Primary factor: Increased procurement costs\n\n**Services BU:** **$3.5M** total\n• Primary factor: Delayed customer payments",
        "final_informations": [
            "Detailed liquidity analysis helps pinpoint specific cash flow challenges and prioritize mitigation strategies by business unit"
        ]
    },
    {
        "question": "What would be the cash flow impact if we applied a 10% opex reduction across all BUs starting next month?",
        "data_scientist": "What would be the cash flow impact if we applied a 10% opex reduction across all BUs starting next month?",
        "non_intelligent_answer": "A 10% reduction in operating expenses would likely improve cash flow. To provide specific numbers, I'll need to analyze your current expense structure and business unit financials.",
        "tools": [
            {
                "tool_name": "sacp_create_plan",
                "description": "Plan cost cutting in SAP Analytics Cloud - Planning (SAC-P)",
                "type": "SAP_SIMULATION",
                "reasoning": "Create a planning scenario in SAC-P and allocate the opex reduction across the BUs",
                "informations": [
                    "SAP Analytics Cloud Planning enables comprehensive scenario modeling to test various cost reduction strategies and their impacts on cash flow"
                ]
            },
            {
                "tool_name": "analyze_financial_plan",
                "description": "Summarize the output of the SAC-P generated plan",
                "type": "SAP_FINANCE",
                "reasoning": "AI-generated summary of the by-BU impact on cash flow",
                "informations": [
                    "Comprehensive financial impact assessments help quantify benefits while identifying any potential operational constraints"
                ]
            }
        ],
        "finbot": "**💰 Cash Flow Uplift (Next 6 Months):**\n\n**Retail BU:** +**$4.2M**\n\n**Manufacturing BU:** +**$3.5M**\n\n**Services BU:** +**$2.1M**\n\n**Total Net Improvement:** +**$9.8M**\n\n**📊 Revised Liquidity Position:**\n• Original gap: **$20M**\n• New gap: **$10.2M** (49% reduction)\n\n**⚠️ Note:** This analysis assumes no negative impact on revenue from cost-saving measures.",
        "final_answer": "**💰 Cash Flow Uplift (Next 6 Months):**\n\n**Retail BU:** +**$4.2M**\n\n**Manufacturing BU:** +**$3.5M**\n\n**Services BU:** +**$2.1M**\n\n**Total Net Improvement:** +**$9.8M**\n\n**📊 Revised Liquidity Position:**\n• Original gap: **$20M**\n• New gap: **$10.2M** (49% reduction)\n\n**⚠️ Note:** This analysis assumes no negative impact on revenue from cost-saving measures.",
        "final_informations": [
            "Data-driven cost reduction strategies can precisely quantify cash flow improvements while highlighting any operational trade-offs"
        ]
    }
] 