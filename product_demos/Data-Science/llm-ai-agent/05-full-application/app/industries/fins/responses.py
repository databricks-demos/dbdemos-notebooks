"""Financial Services industry use case responses and examples."""

FINS_RESPONSES = [
    {
        "question": "A customer is reporting unusual charges on their credit card from last week. Can you help me investigate?",
        "non_intelligent_answer": "I understand your concern about the unusual charges. Please provide the account number and transaction details so we can investigate further.",
        "tools": [
            {
                "tool_name": "query_customer_details",
                "description": "Query customer information",
                "type": "DATABASE",
                "reasoning": "I need to access customer information to verify identity and account details",
                "informations": [
                    "Databricks Enforce security with Access Control across all components, ensuring the LLM doesn't access private information.\nThe Agent won't be able to see sensitive customer details.\n[Discover how to enforce security](https://www.databricks.com/product/security)"
                ]
            },
            {
                "tool_name": "query_transaction_history",
                "description": "Fetch detailed transaction history",
                "type": "DATABASE",
                "reasoning": "Let's analyze recent transaction patterns to identify suspicious activity",
                "informations": [
                    "Transaction data is processed in real-time through our fraud detection system.\nAll data sources can be ingested and centralized for immediate analysis [Discover how to ingest data with LakeFlow](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "I've reviewed the account for Sarah Chen, and here are the recent transactions flagged as unusual:\n\n| Date | Merchant | Amount | Location | Status |\n| ---- | -------- | ------ | -------- | ------ |\n| 03/15/2024 | Online Electronics | $899.99 | Online/UK | Flagged |\n| 03/15/2024 | Digital Games Store | $159.99 | Online/UK | Flagged |\n| 03/16/2024 | International Travel | $450.00 | London, UK | Flagged |\n\nThese transactions appear suspicious because:\n1. They're from a different country than usual spending patterns\n2. Higher amounts than typical for this account\n3. Multiple transactions in quick succession\n\nWould you like me to help initiate our fraud protection protocol and guide you through disputing these charges?",
        "final_informations": [
            "AI-powered fraud detection helps identify suspicious patterns in real-time, protecting customer accounts while maintaining service quality."
        ]
    },
    {
        "question": "Yes, please help me with the fraud protection steps and create a dispute for these transactions.",
        "non_intelligent_answer": "To dispute charges, we'll need to document the unauthorized transactions and submit a formal dispute. Please have the customer verify their recent legitimate purchases.",
        "tools": [
            {
                "tool_name": "initiate_fraud_protection",
                "description": "Activate enhanced security measures",
                "type": "SECURITY",
                "reasoning": "We need to secure the account immediately to prevent further unauthorized charges",
                "informations": [
                    "Automated security protocols can be triggered to protect customer accounts while maintaining accessibility for legitimate transactions"
                ]
            },
            {
                "tool_name": "create_dispute_case",
                "description": "Generate dispute documentation",
                "type": "PROCESS",
                "reasoning": "Creating formal dispute cases for each unauthorized transaction",
                "informations": [
                    "Dispute process is automated but requires careful documentation to ensure customer protection"
                ]
            }
        ],
        "final_answer": "I've taken the following actions to protect the account:\n\n1. **Card Security:**\n   - Temporarily locked the compromised card\n   - Initiated expedited replacement card delivery\n   - Added extra verification for future transactions\n\n2. **Dispute Process:**\n   - Created Case #FD-2024-0319-001\n   - Documented all three suspicious transactions\n   - Initiated chargeback process\n\n3. **Next Steps:**\n   - New card will arrive in 2-3 business days\n   - Temporary virtual card number generated for essential purchases\n   - Provisional credit of $1,509.98 applied within 24-48 hours\n\nWould you like me to draft a detailed explanation email for the customer?",
        "final_informations": [
            "Automated security responses help protect customers while maintaining clear communication and service quality"
        ]
    },
    {
        "question": "Yes, please draft an email explaining the situation and next steps.",
        "non_intelligent_answer": "I'll draft a basic email explaining the fraud protection measures and next steps for the customer.",
        "tools": [
            {
                "tool_name": "check_customer_status",
                "description": "Verify customer relationship status",
                "type": "DATABASE",
                "reasoning": "Let's check the customer's status to personalize our response and offer appropriate additional services",
                "informations": [
                    "Customer relationship data helps provide personalized service and appropriate security measures"
                ]
            },
            {
                "tool_name": "generate_security_tips",
                "description": "Create customized security recommendations",
                "type": "AI_MODEL",
                "reasoning": "Providing personalized security advice based on the incident",
                "informations": [
                    "AI models can generate contextual security advice based on specific incident patterns"
                ]
            }
        ],
        "final_answer": "I see Sarah is a Premium Banking member. I've drafted this comprehensive email:\n\n---\n**Subject:** We've Secured Your Account & Next Steps 🔒\n\nDear Sarah,\n\nThank you for promptly reporting the suspicious activity on your account. We've taken immediate action to protect your funds and resolve this situation.\n\n**Actions We've Taken:**\n- Secured your current card to prevent further unauthorized charges\n- Initiated rush delivery of your replacement card\n- Created dispute cases for the suspicious transactions:\n  • Online Electronics ($899.99)\n  • Digital Games Store ($159.99)\n  • International Travel ($450.00)\n\n**What to Expect:**\n1. Your new card will arrive within 2-3 business days\n2. A provisional credit of $1,509.98 will be applied to your account within 48 hours\n3. You can continue essential purchases using a secure virtual card number (sent separately)\n\n**Enhanced Security Features:**\n- We've activated Premium Transaction Alerts\n- Added extra verification for international purchases\n- Enabled real-time fraud monitoring\n\n**Protecting Your Account:**\n• Review your digital banking settings\n• Update your banking passwords\n• Enable two-factor authentication if not already active\n\nAs a Premium Banking member, you have priority access to our security team 24/7 at 1-800-XXX-XXXX.\n\nWe value your security and trust in us. Please don't hesitate to reach out if you have any questions.\n\nBest regards,\nYour Financial Security Team",
        "final_informations": [
            "Personalized communication helps maintain customer trust while addressing security concerns effectively"
        ]
    }
] 