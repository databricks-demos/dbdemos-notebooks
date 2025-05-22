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
        "question": "What will be the impact of the substitution to this Order?",
        "non_intelligent_answer": "There will be no impact to this order because the substitution is approved by Engineering",
        "tools": [
            {
                "tool_name": "material_cost_calculation",
                "description": "Calculate the change in costs from this substitution",
                "type": "FUNCTION",
                "reasoning": "Let's calculate the cost difference between the original part and the substitute part, and multiply by the quantity required for each unit of the product ",
                "informations": [
                    "You can create user-defined functions that perform the calculations using data in Delta Tables.(https://www.databricks.com/solutions/industries/manufacturing)"
                ]
            },
            {
                "tool_name": "identify_qc_process",
                "description": "Identify additional QC steps to handle substitutions",
                "type": "VECTOR_SEARCH",
                "reasoning": "Need to check QC process manuals for any part substitutions",
                "informations": [
                    "Quality Control process compliance is critical from a safety and auditability point of view"
                ]
            }
        ],
        "final_answer": "I have analyzed the cost and process impact of this change. Here's what I found:\n\nCost impact:\n- The unit cost for P-ABC123 increases by $0.70 or 10% from this substitution. Since the #MFG-2025-789 has 25,000 units, that increases the order cost by $17,500 or 10%. \n\nQuality impact:\n- Substituting this part means a slight change in the in-line QC process. While this will not change the number of samples to be QC checked, the line supervisor needs to refer to the Product Engineering manual for the QC steps.\n\nWould you like me to go ahead and prepare the memo to swap the parts?",
        "final_informations": [
            "Databricks enables comprehensive quality analytics while ensuring traceability [Learn about quality control analytics](https://www.databricks.com/solutions/industries/manufacturing)"
        ]
    },
    {
        "question": "How will this impact the stocks for the substitution part?",
        "non_intelligent_answer": "The stock of the substitution part will come down by 17,500 because of the unplanned use of the part",
        "tools": [
            {
                "tool_name": "Stock_burndown_calculation",
                "description": "Calculate consumption of part based on projected usage",
                "type": "FORECASTING_MODEL",
                "reasoning": "I need to calculate the usage of the part based on the planned forecast and the current order",
                "informations": [
                    "GenAI relies on Material Requirement Processes (MRP) to compute part usage based on forecasted demand generated from ML Models.\nDatabricks offers advanced forecasting capabilities with MLOps support."
                ]
            },
            {
                "tool_name": "project_usage",
                "description": "Generate projected usage and the date by which the part will run out of stock",
                "type": "ML_MODEL",
                "reasoning": "I will need to calculate the modified part usage over time and identify the date by which the inventory will come down to 0",
                "informations": [
                    "Databricks can integrate with your ERP and supply chain systems"
                ]
            },
            {
                "tool_name": "order_backlog",
                "description": "Identify open Purchase Orders for a part",
                "type": "FUNCTION",
                "reasoning": "I need to identify Purchase Orders for the part that can impact the inventory position of the part",
                "informations": [
                    "Databricks can integrate with your ERP and supply chain systems"
                ]
            }
        ],
        "final_answer": "I have analyzed the inventory situation. Based on the production schedule, if we go ahead with this substitution, the plant will run out of stock for C-789Y 4 days sooner than planned. To mitigate this, you can choose to expedite the following Purchase Orders: O-7831 and O-7838. Would you like me to proceed with this recommendation?",
        "final_informations": [
            "Databricks enables real-time scenario planning with predictive analytics [Learn about supply chain analytics](https://www.databricks.com/solutions/industries/manufacturing)"
        ]
    }
] 