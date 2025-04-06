USE_CASES = {
    "telco": {
        "name": "Telco Subscription Support",
        "responses": [
            {
                "question": "I had a customer asking about his high bill last month after upgrading to a new phone. Could you give me the support steps?",
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
                    "GenAI can make it very easy to leak sensitive PII information. Databricks provides a defense layer, securing your business integrity while tracking usage and cost.\n[Discover Databricks AI Governance](https://www.databricks.com/product/ai-governance)"
                ]
            },
            {
                "question": "The customer says the phone bill increased by $50 and the customer's new phone is the item 'Samsung Galaxy S25 Ultra'. He also says he used the promotion coupon to upgrade for free.",
                "non_intelligent_answer": "The $50 increase could be related to charges from the phone upgrade or plan changes. Please refer to your bill for more details. To assist further, I'd need to know which promotion you used.\n\nHere's a general guideline to review phone bill while we find out more:\n\n## 🛠️General Guideline to Review a Phone Bill Increase\nReview the billing statement for any new or changed charges.\nCheck for recent upgrades, plan changes, or added services.\nCompare with the previous bill to spot differences.\nIf unclear, request more information from the customer or direct them to return the new purchased device.",
                "tools": [
                    {
                        "tool_name": "fetch_product_documentation",
                        "description": "Perform RAG on PDF documentation",
                        "type": "DATABASE",
                        "reasoning": "Let's try to troubleshoot the issue before asking for a return. I need to fetch the product documentation for the headphones Wireless Headphones soundWAVE PRO X5.",
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
                "question": "Ok, the customer seems angry and has churn risk, can you craft a friendly message for me to handle that?",
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
                "final_answer": "It looks like this customer is a loyal high-value client. I recommend offering a 5% discount coupon for 1 month.\n\nHere's the message I've drafted for the customer service team:\n---\n\n**Subject:** We Fixed the Upgrade — and Added a Little Thank You 🎁\n\nDear Layla,\n\nThanks again for reaching out — I understand how frustrating a surprise bill can be. I took another look and made sure the promotion was correctly applied to your upgrade.\nThat means your phone is now fully covered under the promo, and you won't see any increase related to the device going forward.\n\nAs a thank you for your patience, I've also added a 10% discount coupon to your account, which will be applied to your next bill.\n\nWe really appreciate having you with us, and I'm here if there's anything else I can help with.\n\nLet me know if there is anything else I can do for you."
            }
        ]
    },
    "retail": {
        "name": "Retail Order Support",
        "responses": [
            {
                "question": "I have a customer asking about his order number #1231, can you get me the order details?",
                "non_intelligent_answer": "I can't directly access order systems or databases. But if you can share the order details or data (e.g. a spreadsheet, database export, or screenshot), I can help you look it up or extract the relevant info for order #1231. Want to upload something?",
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
                        "tool_name": "query_customer_invoice_history",
                        "description": "Fetch order details",
                        "type": "DATABASE",
                        "reasoning": "Let's get the latest orders for this customer. Calling the tool query_customer_orders",
                        "informations": [
                            "Agents need to have access to a variety of data.\nAll your data sources can be ingested and centralized, instantly available for Analytics and GenAI usecases [Discover how to ingest data with LakeFlow ](https://www.databricks.com/product/lakehouse)"
                        ]
                    }
                ],
                "final_answer": "I see that this order is from Quentin Ambard, the 12/23/205, delivery address is *** MASKED DATA ***.\n\nHere is the order detail:\n\n| Product | Quantity | Price | Status |\n| ------- | -------- | ----- | ------ |\n| Wireless Headphones soundWAVE PRO X5 | 1 | $129.99 | Returned Requested |\n| Phone Charger | 2 | $24.99 | Shipped |\n| Screen Protector | 1 | $19.99 | Processing |\n\nI see the customer asked for a return on the Headphones. Would you like me to troubleshoot the issue instead? We might be able to avoid the return.",
                "final_informations": [
                    "GenAI can make it very easy to leak sensitive PII information. Databricks provides a defense layer, securing your business integrity while tracking usage and cost.\n[Discover Databricks AI Governance](https://www.databricks.com/product/ai-governance)"
                ]
            },
            {
                "question": "Yes, the customer says they have an error code E12, what could we suggest?",
                "non_intelligent_answer": "To help with error code E12, I'd need to know the type of product or system the customer is referring to (e.g., a coffee machine, HVAC unit, industrial sensor, etc.), since error codes can vary widely between manufacturers and products.\n\nBut here's a general approach while we find out more:\n\n## 🛠️ General Troubleshooting Steps for Error Code E12\n\n1. Check the Manual: If available, look up error code E12 in the product's manual or support site.\n2. Restart the Device: A power cycle often resolves temporary glitches.\n3. Common E12 Meanings (by type of device):\n   - Appliances (e.g., dishwashers, coffee machines): Often linked to water flow, temperature sensor failure, or internal communication issues.\n   - Industrial/IoT Equipment: Might mean sensor disconnection, data timeout, or firmware mismatch.\n   - Electronics (e.g., routers or controllers): Could be a firmware or internal component issue.",
                "tools": [
                    {
                        "tool_name": "fetch_product_documentation",
                        "description": "Perform RAG on PDF documentation",
                        "type": "DATABASE",
                        "reasoning": "Let's try to troubleshoot the issue before asking for a return. I need to fetch the product documentation for the headphones Wireless Headphones soundWAVE PRO X5.",
                        "informations": [
                            "Databricks makes it easy to plug any unstructured data sources (Past tickets, PDF, Office documents and other)[Discover how to perform RAG and Vector Search ](https://www.databricks.com/product/lakehouse)"
                        ]
                    }
                ],
                "final_answer": "From the headphone manual ([headphone-001-AZE1.pdf](https://www.databricks.com/product/lakehouse)), I see that the following can be performed to do a hard reset:\n\n- press the play button for 30 seconds\n- if it doesn't work, use the pin to reset the headphone to factory settings\n- Make sure the user installed the application in his mobile phone to do the latest firmware update\n\nLet me know if it helps",
                "final_informations": [
                    "Databricks lets you plug any LLM foundation model (openai, llama, anthropic...). But these LLMs are quickly evolving! You need to make sure your genAI application will provide relevant information overtime and focus on reducing return. Databricks lets you benchmark your applications against your dataset and instruction, making software iteration easy and safe.[Discover how MLFlow Evaluate enforces your quality](https://www.databricks.com/product/mlflow)"
                ]
            },
            {
                "question": "Ok, the customer seems upset, can you craft an email for me?",
                "non_intelligent_answer": "# Customer Service Response\n\nDear [Customer's Name],\n\nThank you for reaching out, and I'm very sorry to hear about the trouble you've encountered with your order #1231.\n\nWe understand how frustrating this situation must be, and we want to make it right. We've gone ahead and approved the return for your item. Please follow the return instructions below to send the product back to us:\n\n## Return Instructions\n[Insert return steps, address, or link to return portal]\n\nOnce we receive the item, we'll process your refund or replacement promptly, depending on your preference.\n\nIf there's anything else we can do in the meantime, please don't hesitate to let me know. We really appreciate your patience and understanding.",
                "tools": [
                    {
                        "tool_name": "forecast_customer_consumption",
                        "description": "Forecast future consumption",
                        "type": "FORECASTING_MODEL",
                        "reasoning": "I need to generate an email, first let's if this customer qualifies for any insentive as we'll adjust the email accordingly...",
                        "informations": [
                            "GenAI relies on classic, specialized models such as forecasting or CLV.\nDatabricks offers a full platform to build classic ML models, with all the traditional MLOps capabilities to enforce trust and simplify the deployment."
                        ]
                    },
                    {
                        "tool_name": "generate_coupon_code",
                        "description": "Generate unique coupon code",
                        "type": "EXTERNAL_API",
                        "reasoning": "This seems to be a premium customer, as per guidance let's generate a 5% discount coupon for 1 month using the COUPON API",
                        "informations": [
                            "Databricks can be plugged with any other external systems, including partner solutions"
                        ]
                    }
                ],
                "final_answer": "It looks like this customer is a loyal high-value client. I recommend offering a 5% discount coupon for 1 month.\n\nHere's the email I've drafted for you:\n---\n\n**Subject:** Quick Fix + A Special Thank You for Your Loyalty 🎧\n\nDear Quentin,\n\nThank you for reaching out regarding your recent order. We're happy to assist!\n\nBased on your inquiry about the headphones, here are a few steps you can perform that should solve the issue:\n\n- Press and hold the **play** button for 30 seconds\n- If that doesn't work, use a pin to press the **reset** hole (usually near the charging port)\n- Lastly, Ensure the **official mobile app is installed** to update to the latest firmware\n\nWe also noticed your continued trust in our products — and we truly appreciate it!\nAs a token of our gratitude, please enjoy **5% off your next order** using the exclusive code:\n\n🎁 **THANKYOU-QA5DISC**\n*(Valid for one month from today)*\n\nIf you have any further questions, don't hesitate to reach out — we're always here to help!\n\nWarm regards,\nThe Customer Success Team\n\n---\nLet me know if you'd like to modify the tone or highlight other products based on recent activity."
            }
        ]
    }
} 