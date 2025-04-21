"""TELCO industry use case responses and examples."""

TELCO_RESPONSES = [
    {
        "question": "I had a customer asking about her high bill last month after upgrading to a new phone. Could you give me the support steps?",
        "non_intelligent_answer": "Your bill may be higher due to charges related to the phone upgrade. Please review your billing statement or contact support for more details.",
        "tools": [
            {
                "tool_name": "query_customer_details",
                "description": "Query customer information",
                "type": "DATABASE",
                "reasoning": "I need to access customer information to answer this question. Calling the tool query_customer_details",
                "informations": [
                    "Databricks Enforce security with Access Control across all components, ensuring the LLM doesn't access private information.\nThe Agent won't be able to see sensitive customer details.\n[Discover how to enforce security](https://www.databricks.com/product/security)"
                ]
            },
            {
                "tool_name": "query_billing",
                "description": "Fetch customer billing history details",
                "type": "DATABASE",
                "reasoning": "Let's get the latest billing information for this customer. Calling the tool query_billing",
                "informations": [
                    "Agents need to have access to a variety of data.\nAll your data sources can be ingested and centralized, instantly available for Analytics and GenAI usecases [Discover how to ingest data with LakeFlow ](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "Let's get the latest billing information for this customer. Calling the tool query_billing...\n\nI see that this billing data is for Layla Yang, home address **redacted**, billing cycle starting 01/20/2025.\nHere's a summary of the current charges:\n\n| Bill Description | Amount | Notes |\n| ------------| ------ | ------- |\n| Plan Charge (Unlimited Max) | $85.00 | Recurring monthly |\n| Device Installment (S25 Ultra) | $50.00 | Phone upgrade  |\n| Taxes & Surcharges | $19.99 | Recurring monthly |\n\nIt looks like the promotion for the S25 Ultra wasn't applied, so the customer is currently being billed the full device installment — which explains the $50 increase they're seeing. Would you like me to troubleshoot the issue instead? We might be able to avoid the return.",
        "final_informations": [
            "GenAI can make the system vulnerable to leaking **sensitive PII** information. Databricks provides a defense layer, securing your business integrity while tracking usage and cost.\n[Discover Databricks AI Governance](https://www.databricks.com/product/ai-governance)"
        ]
    },
    {
        "question": "The customer says the phone bill increased by $50 and the customer's new phone is the item 'Samsung Galaxy S25 Ultra'. She also says she used the promotion coupon to upgrade for free.",
        "non_intelligent_answer": "The $50 increase could be related to charges from the phone upgrade or plan changes. Please refer to your bill for more details.\n\nHere's a general guideline to review phone bill while we find out more:\n\n### 🛠️General Guideline to Review a Phone Bill Increase\nReview the billing statement for any new or changed charges.\nCheck for recent upgrades, plan changes, or added services.\nCompare with the previous bill to spot differences.\nIf unclear, direct them to **return** the new purchased device.",
        "tools": [
            {
                "tool_name": "fetch_product_documentation",
                "description": "Perform RAG on PDF documentation",
                "type": "DATABASE",
                "reasoning": "Let's try to troubleshoot the issue before asking for a return. ",
                "informations": [
                    "Databricks makes it easy to plug any unstructured data sources (Past tickets, PDF, Office documents and other)[Discover how to perform RAG and Vector Search ](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "Based on the billing details, the increase is due to the promotion not being applied to the current billing cycle. As a result, the customer was charged the full monthly device installment of $50.00 for the Samsung Galaxy S25 Ultra.\n\nOnce the promo is correctly applied, this charge should be removed or credited in future cycles, and the customer's bill will return to the expected amount",
        "final_informations": [
            "Databricks lets you plug any LLM foundation model (openai, llama, anthropic...). But these LLMs are quickly evolving! You need to make sure your genAI application will provide relevant information overtime and focus on reducing return. Databricks lets you benchmark your applications against your dataset and instruction, making software iteration easy and safe.[Discover how MLFlow Evaluate enforces your quality](https://www.databricks.com/product/mlflow)"
        ]
    },
    {
        "question": "Ok, the customer seems angry and has **churn risk**, can you craft a friendly message for me to handle that?",
        "non_intelligent_answer": "# Customer Service Response\n\nDear [Customer's Name],\n\nThanks for contacting us.\nWe understand there may be some confusion with your recent bill after the phone upgrade. Please note that charges may vary depending on your account activity.\nIf you're not satisfied with the new phone, we recommend returning it and reviewing your billing details in your online account.\n\nLet us know if you have further questions.",
        "tools": [
            {
                "tool_name": "query_customer_propensity",
                "description": "Check customer propensity score",
                "type": "FORECASTING_MODEL",
                "reasoning": "I need to generate a service message for the customer — but first, let's check if this customer qualifies for any incentives, as we'll adjust the tone and offer accordingly. Calling check_propensity_score...",
                "informations": [
                    "GenAI relies on classic, specialized models such as forecasting, customer segmentation or CLV.\nDatabricks offers a full platform to build classic ML models, with all the traditional MLOps capabilities to enforce trust and simplify the deployment."
                ]
            },
            {
                "tool_name": "generate_coupon_code",
                "description": "Generate unique coupon code",
                "type": "EXTERNAL_API",
                "reasoning": "The model flags Layla as a high-value long term customer, with high projected spend in upcoming cycles",
                "informations": [
                    "Databricks can be plugged with any other external systems, including partner solutions"
                ]
            }
        ],
        "final_answer": "It looks like this customer is a loyal high-value client. I recommend offering a 10% discount coupon for 1 month.\n\nHere's the message I've drafted for the customer service team:\n---\n\n**Subject:** We Fixed the Upgrade — and Added a Little Thank You 🎁\n\nDear Layla,\n\nThanks again for reaching out — I understand how frustrating a surprise bill can be. I took another look and made sure the promotion was correctly applied to your upgrade.\nThat means your phone is now fully covered under the promo, and you won't see any increase related to the device going forward.\n\nAs a thank you for your patience, I've also added a 10% discount coupon to your account, which will be applied to your next bill.\n\nWe really appreciate having you with us, and I'm here if there's anything else I can help with.\n"
    }
] 