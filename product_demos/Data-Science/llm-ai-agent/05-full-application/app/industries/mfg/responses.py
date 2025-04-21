"""Manufacturing industry use case responses and examples."""

MFG_RESPONSES = [
    {
        "question": "Production order #MFG-2025-789 is showing delays. The customer needs their order by next week. Can you help me understand what's causing the delay?",
        "non_intelligent_answer": "I can check the production order status for delays. Please allow me to review the order details.",
        "tools": [
            {
                "tool_name": "query_production_status",
                "description": "Query production order status and timeline",
                "type": "DATABASE",
                "reasoning": "I need to access detailed production information to identify delay causes. Calling query_production_status",
                "informations": [
                    "Databricks Enforce security with Access Control across all components, ensuring data privacy.\nThe Agent won't be able to see sensitive production information.\n[Discover how to enforce security](https://www.databricks.com/product/security)"
                ]
            },
            {
                "tool_name": "analyze_supply_chain",
                "description": "Analyze supply chain dependencies",
                "type": "ML_MODEL",
                "reasoning": "Let's check for any supply chain bottlenecks affecting this order",
                "informations": [
                    "Manufacturing data requires real-time analysis.\nAll your data sources can be ingested and centralized for instant analytics [Discover how to ingest data with LakeFlow](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "I've analyzed order #MFG-2025-789 and here's what I found:\n\n| Production Stage | Status | Timeline Impact |\n| ------------| ------ | ------- |\n| Material Procurement | ⚠️ Delayed | +2 days |\n| Assembly Line | 🟡 Pending | Dependent |\n| Quality Control | 🟡 Not Started | - |\n\nThe delay is primarily due to a shortage of component C-789X from our supplier. However, I've identified some mitigation options:\n\n1. We have a compatible alternative component (C-789Y) in stock\n2. Express shipping is available from our backup supplier\n3. We can prioritize this order in the assembly queue\n\nWould you like me to proceed with any of these options to meet next week's deadline?",
        "final_informations": [
            "Databricks enables real-time production tracking while maintaining data security [Learn about manufacturing analytics](https://www.databricks.com/solutions/industries/manufacturing)"
        ]
    },
    {
        "question": "The quality control report for batch B-456 shows some anomalies in the measurements. The customer is concerned about product reliability.",
        "non_intelligent_answer": "I'll check the quality control report for batch B-456 and review the measurements.",
        "tools": [
            {
                "tool_name": "analyze_qc_data",
                "description": "Analyze quality control measurements",
                "type": "ML_MODEL",
                "reasoning": "Let's analyze the QC data to identify any significant deviations",
                "informations": [
                    "Databricks ML models can detect anomalies while maintaining data integrity [Learn about ML in manufacturing](https://www.databricks.com/solutions/industries/manufacturing)"
                ]
            },
            {
                "tool_name": "generate_qc_report",
                "description": "Generate detailed quality control report",
                "type": "LLM",
                "reasoning": "Based on the analysis, we'll provide a comprehensive quality assessment",
                "informations": [
                    "Automated quality control reporting improves accuracy and response time"
                ]
            }
        ],
        "final_answer": "I've analyzed the quality control data for batch B-456. Here's what we found:\n\nQuality Metrics:\n- Dimensional Accuracy: 99.7% (Within spec)\n- Material Strength: 98.5% (Within spec)\n- Surface Finish: 96.8% (⚠️ Slight deviation)\n\nThe anomalies are limited to minor surface finish variations that are:\n1. Within acceptable tolerance ranges\n2. Don't affect product functionality or reliability\n3. Present in only 3% of the batch\n\nRecommended Actions:\n1. Provide detailed QC documentation to the customer\n2. Offer additional testing for concerned units\n3. Schedule a technical review meeting if desired\n\nWould you like me to prepare any of these materials or schedule the review?",
        "final_informations": [
            "Databricks enables comprehensive quality analytics while ensuring traceability [Learn about quality control analytics](https://www.databricks.com/solutions/industries/manufacturing)"
        ]
    },
    {
        "question": "We're running low on raw materials RM-101 and RM-102. Can you help optimize our inventory and prevent production stoppage?",
        "non_intelligent_answer": "I can check the inventory levels and help with ordering more materials.",
        "tools": [
            {
                "tool_name": "forecast_inventory",
                "description": "Forecast inventory requirements",
                "type": "FORECASTING_MODEL",
                "reasoning": "Let's predict future inventory needs based on production schedule",
                "informations": [
                    "GenAI relies on specialized forecasting models for inventory optimization.\nDatabricks offers advanced forecasting capabilities with MLOps support."
                ]
            },
            {
                "tool_name": "optimize_ordering",
                "description": "Generate optimal order recommendations",
                "type": "ML_MODEL",
                "reasoning": "We'll calculate optimal order quantities and timing",
                "informations": [
                    "Databricks can integrate with your ERP and supply chain systems"
                ]
            }
        ],
        "final_answer": "I've analyzed our inventory situation and production schedule. Here's the current status:\n\nRM-101 Status:\n- Current Stock: 500 units\n- Daily Usage: 100 units\n- Reorder Point: 600 units\n- Lead Time: 5 days\n\nRM-102 Status:\n- Current Stock: 300 units\n- Daily Usage: 75 units\n- Reorder Point: 450 units\n- Lead Time: 7 days\n\nRecommended Actions:\n1. ⚠️ Immediate order for RM-102 (1000 units)\n2. 🔄 Schedule RM-101 order for next week (1500 units)\n3. 📊 Adjust reorder points based on new usage patterns\n\nI can also suggest alternative suppliers or materials if needed. Would you like me to proceed with any of these recommendations?",
        "final_informations": [
            "Databricks enables real-time inventory optimization with predictive analytics [Learn about supply chain analytics](https://www.databricks.com/solutions/industries/manufacturing)"
        ]
    }
] 