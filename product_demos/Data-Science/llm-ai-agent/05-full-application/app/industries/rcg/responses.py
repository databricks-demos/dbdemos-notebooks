"""Retail and Consumer Goods industry use case responses and examples."""

RCG_RESPONSES = [
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
        "non_intelligent_answer": "To help with error code E12, I'd need to know the type of product or system the customer is referring to (e.g., a coffee machine, HVAC unit, industrial sensor, etc.), since error codes can vary widely between manufacturers and products.",
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
        "non_intelligent_answer": "# Customer Service Response\n\nDear [Customer's Name],\n\nThank you for reaching out, and I'm very sorry to hear about the trouble you've encountered with your order #1231.",
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
        "final_answer": "It looks like this customer is a loyal high-value client. I recommend offering a 5% discount coupon for 1 month.\n\nHere's the email I've drafted for you:\n---\n\n**Subject:** Quick Fix + A Special Thank You for Your Loyalty 🎧\n\nDear Quentin,\n\nThank you for reaching out regarding your recent order. We're happy to assist!\n\nBased on your inquiry about the headphones, here are a few steps you can perform that should solve the issue:\n\n- Press and hold the **play** button for 30 seconds\n- If that doesn't work, use a pin to press the **reset** hole (usually near the charging port)\n- Lastly, Ensure the **official mobile app is installed** to update to the latest firmware\n\nWe also noticed your continued trust in our products — and we truly appreciate it!\nAs a token of our gratitude, please enjoy **5% off your next order** using the exclusive code:\n\n🎁 **THANKYOU-QA5DISC**\n*(Valid for one month from today)*\n\nIf you have any further questions, don't hesitate to reach out — we're always here to help!\n\nWarm regards,\nThe Customer Success Team",
        "final_informations": [
            "Customer satisfaction and retention are key metrics in retail. The AI assistant helps maintain high standards while providing personalized support."
        ]
    }
] 