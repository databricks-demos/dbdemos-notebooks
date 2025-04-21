"""Healthcare and Life Sciences (HLS) industry use case responses and examples."""

HLS_RESPONSES = [
    {
        "question": "A patient is complaining about unexpected charges on their recent hospital visit bill. They were told their procedure was covered but now see a $3,000 charge. How should I help them?",
        "non_intelligent_answer": "I understand your concern about the unexpected charges. Please review your explanation of benefits or contact our billing department for assistance.",
        "tools": [
            {
                "tool_name": "query_patient_coverage",
                "description": "Query patient insurance coverage details",
                "type": "DATABASE",
                "reasoning": "I need to access patient coverage information to understand their benefits. Calling query_patient_coverage",
                "informations": [
                    "Databricks Enforce security with Access Control across all components, ensuring HIPAA compliance.\nThe Agent won't be able to see sensitive patient information.\n[Discover how to enforce security](https://www.databricks.com/product/security)"
                ]
            },
            {
                "tool_name": "query_billing_details",
                "description": "Fetch detailed billing information",
                "type": "DATABASE",
                "reasoning": "Let's get the complete billing breakdown for this patient. Calling query_billing_details",
                "informations": [
                    "Healthcare data requires special handling.\nAll your data sources can be ingested and centralized with proper security controls [Discover how to ingest data with LakeFlow](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "I've reviewed the billing information for patient ID **redacted**. Here's what I found:\n\n| Service Description | Amount | Coverage Status |\n| ------------| ------ | ------- |\n| Pre-operative Assessment | $500.00 | Covered |\n| Main Procedure | $2,500.00 | Pending Prior Authorization |\n| Post-op Care | $0.00 | Covered |\n\nIt appears the $3,000 charge is due to a missing prior authorization for the main procedure. Here are the immediate steps we can take:\n\n1. Submit an urgent prior authorization request\n2. Place the account on hold for 30 days while we resolve this\n3. Once approved, we'll reprocess the claim\n\nWould you like me to initiate these steps now?",
        "final_informations": [
            "Healthcare data requires strict privacy controls. Databricks provides a defense layer, securing PHI while tracking usage and compliance.\n[Discover Databricks AI Governance](https://www.databricks.com/product/ai-governance)"
        ]
    },
    {
        "question": "The patient says they did get prior authorization, and they have the approval number: AUTH-2025-123456. Can you help resolve this?",
        "non_intelligent_answer": "I'll look into the prior authorization number you provided. Please allow me to review your records.",
        "tools": [
            {
                "tool_name": "verify_authorization",
                "description": "Verify prior authorization details",
                "type": "DATABASE",
                "reasoning": "Let's validate the authorization number and its status",
                "informations": [
                    "Databricks enables real-time verification of healthcare authorizations [Learn about real-time processing](https://www.databricks.com/product/lakehouse)"
                ]
            },
            {
                "tool_name": "reprocess_claim",
                "description": "Trigger claim reprocessing",
                "type": "EXTERNAL_API",
                "reasoning": "Once we verify the authorization, we'll need to reprocess the claim",
                "informations": [
                    "Automated claim processing reduces manual intervention and improves accuracy"
                ]
            }
        ],
        "final_answer": "I've verified the authorization number AUTH-2025-123456 and found that it was indeed approved on 01/15/2025 but wasn't properly linked to the claim. I've taken the following actions:\n\n1. ✅ Linked the authorization to the claim\n2. ✅ Triggered automatic reprocessing\n3. ✅ Added a priority flag for processing within 48 hours\n\nYou should see the updated claim status within 2 business days. I've also added a note to send an automated notification once the reprocessing is complete.\n\nIs there anything else you need assistance with regarding this claim?",
        "final_informations": [
            "Databricks enables end-to-end tracking of healthcare claims, ensuring nothing falls through the cracks [Discover MLFlow tracking](https://www.databricks.com/product/mlflow)"
        ]
    },
    {
        "question": "We have a patient asking about their medication adherence score, they're worried about their insurance coverage. Can you help explain their situation?",
        "non_intelligent_answer": "I can check your medication adherence score. Please provide your member ID for review.",
        "tools": [
            {
                "tool_name": "analyze_adherence",
                "description": "Analyze patient medication adherence",
                "type": "ML_MODEL",
                "reasoning": "Let's check the patient's adherence patterns and risk factors",
                "informations": [
                    "Databricks ML models can predict patient outcomes while maintaining privacy [Learn about ML in healthcare](https://www.databricks.com/solutions/industries/healthcare-and-life-sciences)"
                ]
            },
            {
                "tool_name": "generate_care_recommendations",
                "description": "Generate personalized care recommendations",
                "type": "LLM",
                "reasoning": "Based on the adherence analysis, we'll provide tailored recommendations",
                "informations": [
                    "Personalized healthcare recommendations improve patient outcomes"
                ]
            }
        ],
        "final_answer": "I've analyzed the medication adherence data and here's what I found:\n\nCurrent Adherence Score: 72%\nRisk Level: Moderate\n\nKey Factors:\n- Consistent with primary medication (95%)\n- Some gaps in secondary medications (65%)\n- Refill patterns show occasional delays\n\nRecommendations:\n1. Set up automatic refills for all medications\n2. Schedule a medication review with your pharmacist\n3. Consider our mobile app for reminders\n\nGood news: Your current adherence score is still within acceptable range for full coverage. However, to maintain optimal coverage and health outcomes, I'd recommend implementing the above suggestions. Would you like me to help set up any of these services?",
        "final_informations": [
            "Databricks enables real-time health analytics while maintaining HIPAA compliance [Learn about healthcare analytics](https://www.databricks.com/solutions/industries/healthcare-and-life-sciences)"
        ]
    }
] 