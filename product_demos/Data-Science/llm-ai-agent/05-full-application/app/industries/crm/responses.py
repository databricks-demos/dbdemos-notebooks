"""CRM industry use case responses and examples."""

# AI Assist responses - for customer service representatives
CRM_AI_ASSIST_RESPONSES = [
    {
        "question": "I have a lead who's been inactive for 3 months and is now showing interest again. What's the best approach to re-engage them?",
        "non_intelligent_answer": "For inactive leads showing renewed interest, it's best to review their previous interactions and reach out with personalized content. Check their engagement history and tailor your approach accordingly.",
        "tools": [
            {
                "tool_name": "query_lead_profile",
                "description": "Query lead information and interaction history",
                "type": "DATABASE",
                "reasoning": "I need to access the lead's profile and interaction history to provide personalized re-engagement recommendations. Calling the tool query_lead_profile",
                "informations": [
                    "Databricks ensures **secure data access** with granular permissions, preventing unauthorized access to sensitive customer information.\nLead data remains protected while enabling AI insights.\n[Discover how to enforce security](https://www.databricks.com/product/security)"
                ]
            },
            {
                "tool_name": "analyze_engagement_patterns",
                "description": "Analyze lead engagement and interaction patterns",
                "type": "DATABASE",
                "reasoning": "Let me analyze this lead's engagement patterns to understand what type of content and timing works best. Calling the tool analyze_engagement_patterns",
                "informations": [
                    "CRM data needs to be enriched and accessible for AI analysis.\nDatabricks enables real-time processing of customer interaction data for personalized experiences [Discover LakeFlow for real-time processing](https://www.databricks.com/product/lakehouse)"
                ]
            }
        ],
        "final_answer": "Let me analyze this lead's profile and engagement patterns...\n\nBased on the data for Sarah Chen from TechFlow Solutions, here's what I found:\n\n**Lead Profile Summary:**\n- Company: TechFlow Solutions (50-100 employees)\n- Role: VP of Operations\n- Previous interest: Marketing automation platform\n- Last engagement: Downloaded whitepaper 3 months ago\n- Recent activity: Visited pricing page twice this week\n\n**Recommended Re-engagement Strategy:**\n1. **Immediate follow-up**: Send a personalized email referencing her previous whitepaper download\n2. **Value proposition**: Focus on ROI and operational efficiency (matches her role)\n3. **Timing**: Reach out Tuesday-Thursday, 10-11 AM (her highest engagement window)\n4. **Content**: Offer a live demo or case study from similar-sized companies\n\nHer recent pricing page visits suggest she's in evaluation mode - strike while the interest is hot!",
        "final_informations": [
            "AI-powered CRM insights require **robust data governance** to ensure customer privacy and compliance.\nDatabricks provides enterprise-grade security for customer data analytics.\n[Discover Databricks AI Governance](https://www.databricks.com/product/ai-governance)"
        ]
    },
    {
        "question": "Our client TechFlow Solutions is threatening to cancel their contract due to poor service delivery. How should I handle this escalation?",
        "non_intelligent_answer": "For contract cancellation threats, immediately escalate to management and review the client's service history. Document all issues and prepare a remediation plan to address their concerns.",
        "tools": [
            {
                "tool_name": "query_client_history",
                "description": "Retrieve comprehensive client service history",
                "type": "DATABASE",
                "reasoning": "I need to review TechFlow Solutions' complete service history, support tickets, and satisfaction scores to understand the root cause of their dissatisfaction.",
                "informations": [
                    "Customer service data from multiple touchpoints needs to be unified for comprehensive analysis.\nDatabricks centralizes all customer interaction data for 360-degree insights [Discover Data Integration](https://www.databricks.com/product/data-integration)"
                ]
            },
            {
                "tool_name": "generate_retention_strategy",
                "description": "Generate personalized retention strategy",
                "type": "FORECASTING_MODEL",
                "reasoning": "Based on TechFlow Solutions' client profile and history, I'll generate a data-driven retention strategy with specific remediation steps.",
                "informations": [
                    "Retention strategies benefit from predictive analytics to identify at-risk accounts early.\nDatabricks ML models can predict churn risk and recommend proactive interventions."
                ]
            }
        ],
        "final_answer": "This is a critical escalation with TechFlow Solutions that requires immediate action. Remember, we successfully re-engaged Sarah Chen 6 months ago, and they became a valued client. Here's your recommended approach:\n\n**Client: TechFlow Solutions**\n- Contract value: $180K annually (signed after our successful re-engagement)\n- Relationship: 6 months since conversion from lead\n- Recent issues: 2 service outages in Q4, delayed onboarding of their HR module\n- Key contact: Sarah Chen, VP of Operations (our original lead)\n\n**Immediate Actions (Next 24 hours):**\n1. **Personal outreach to Sarah**: Leverage your existing relationship\n2. **Service recovery**: Implement temporary workarounds for HR module issues\n3. **Executive involvement**: Have our VP call their CEO to show commitment\n\n**Retention Strategy:**\n- Offer 10% service credit for recent outages\n- Assign dedicated customer success manager (they've been managed by general support)\n- Implement bi-weekly check-ins for next 90 days\n- Fast-track HR module deployment (was delayed due to resource constraints)\n\n**Success probability**: 85% based on strong relationship foundation with Sarah and their initial positive ROI results.\n\n⚠️ **Critical**: Sarah specifically mentioned they chose us over competitors because of our initial responsiveness - we need to prove that wasn't a one-time occurrence.",
        "final_informations": [
            "Real-time customer health scoring helps identify at-risk accounts before they escalate.\nDatabricks enables continuous monitoring of customer satisfaction metrics and predictive churn analysis."
        ]
    },
    {
        "question": "I need to prepare for a quarterly business review with TechFlow Solutions after resolving their service issues. What data should I focus on?",
        "non_intelligent_answer": "For QBRs, focus on key performance metrics, ROI analysis, usage statistics, and future roadmap alignment. Prepare data on service performance, achievements, and growth opportunities.",
        "tools": [
            {
                "tool_name": "generate_qbr_analytics",
                "description": "Generate comprehensive QBR analytics dashboard",
                "type": "DATABASE",
                "reasoning": "I'll compile comprehensive analytics for the QBR including usage metrics, ROI analysis, and performance indicators specific to TechFlow Solutions, highlighting both challenges overcome and successes achieved.",
                "informations": [
                    "QBR analytics require aggregating data from multiple business systems for comprehensive insights.\nDatabricks enables unified analytics across all customer touchpoints [Discover Unified Analytics](https://www.databricks.com/product/lakehouse)"
                ]
            },
            {
                "tool_name": "identify_growth_opportunities",
                "description": "Identify expansion and growth opportunities",
                "type": "FORECASTING_MODEL",
                "reasoning": "Using predictive analytics to identify potential upsell opportunities with TechFlow Solutions, especially given their successful recovery and strong relationship with Sarah Chen.",
                "informations": [
                    "Growth opportunity identification benefits from machine learning models that analyze usage patterns and market trends.\nDatabricks ML capabilities enable sophisticated customer expansion modeling."
                ]
            }
        ],
        "final_answer": "Perfect timing for this QBR with **TechFlow Solutions**! After successfully navigating the service challenges, this is your opportunity to strengthen the relationship and drive expansion. Here's your comprehensive preparation guide:\n\n## 📊 Key Performance Metrics to Highlight\n\n**Recovery & Transformation Story:**\n- **Service improvement**: 99.8% uptime since implementing dedicated support (vs. 97.2% during issues)\n- **Issue resolution**: Average response time reduced from 4 hours to 45 minutes\n- **User satisfaction**: CSAT score improved from 6.2 to 8.7/10\n\n**Business Impact Achieved:**\n- **25% reduction** in manual processing time (Sarah's original goal)\n- **$85K operational cost savings** in first 6 months\n- **92% user adoption** rate across their operations team\n- **3,200+ monthly active workflows** (exceeded projections by 40%)\n\n## 🎯 Strategic Discussion Points\n\n1. **Challenges Overcome**: Address the Q4 service issues head-on and show concrete improvements\n2. **ROI Validation**: 280% ROI achieved vs. 200% originally projected to Sarah\n3. **Success Partnership**: Highlight how Sarah's feedback drove product improvements\n4. **Future Vision**: Present Q2 roadmap features that align with their expansion plans\n\n## 💡 Expansion Opportunities (Based on Sarah's Team Feedback)\n- **HR Department Integration**: Sarah mentioned HR struggles with similar manual processes (potential $75K expansion)\n- **Advanced Analytics Package**: Operations team requesting custom dashboards\n- **Multi-location Rollout**: They're planning to expand to 3 new offices\n\n## 🤝 Relationship Highlights\n- **Journey together**: From lead re-engagement → service recovery → strategic partnership\n- **Sarah as advocate**: She's already recommended us to 2 industry contacts\n- **Executive relationship**: Strong CEO-to-CEO connection established during escalation\n\n**Meeting outcome goal**: Secure 12-month renewal + 35% expansion (HR module + analytics package)\n\n💬 **Opening line suggestion**: \"Sarah, it's incredible to see how far we've come together - from that initial conversation about operational efficiency to now being a strategic partner in TechFlow's growth story...\"",
        "final_informations": [
            "Effective QBRs rely on real-time, accurate data visualization and predictive insights.\nDatabricks provides enterprise-grade analytics platforms for comprehensive customer success reporting."
        ]
    }
]

# AI Autopilot responses - for direct customer interaction
CRM_AI_AUTOPILOT_RESPONSES = [
    {
        "question": "I'm looking for a CRM solution that can help us manage our growing sales pipeline better. What features should I prioritize?",
        "non_intelligent_answer": "For sales pipeline management, focus on features like lead tracking, opportunity management, sales forecasting, and reporting capabilities. Consider your team size, integration needs, and budget when evaluating options.",
        "tools": [
            {
                "tool_name": "analyze_business_requirements",
                "description": "Analyze business size and CRM requirements",
                "type": "DATABASE",
                "reasoning": "I need to understand your specific business context and requirements to recommend the most suitable CRM features for your sales pipeline management.",
                "informations": [
                    "CRM selection requires careful analysis of business requirements and scalability needs.\nDatabricks helps organizations make data-driven technology decisions through comprehensive analytics [Discover Business Intelligence](https://www.databricks.com/product/lakehouse)"
                ]
            },
            {
                "tool_name": "match_crm_features",
                "description": "Match CRM features to business needs",
                "type": "DATABASE",
                "reasoning": "Based on your business profile, I'll identify the most critical CRM features that will drive sales pipeline efficiency and growth.",
                "informations": [
                    "Modern CRM systems integrate with multiple data sources for comprehensive customer insights.\nDatabricks enables seamless integration of CRM data with other business systems for unified analytics."
                ]
            }
        ],
        "final_answer": "Based on your business profile, here are the **essential CRM features** you should prioritize for sales pipeline management:\n\n## 🎯 Core Pipeline Features\n**Must-Have:**\n- **Visual Pipeline Management**: Drag-and-drop deal stages\n- **Lead Scoring**: Automated prioritization based on engagement\n- **Sales Forecasting**: Predictive revenue projections\n- **Activity Tracking**: Calls, emails, meetings in one view\n\n## 📊 Analytics & Reporting\n- **Pipeline Analytics**: Conversion rates by stage\n- **Sales Performance Dashboards**: Individual and team metrics\n- **Revenue Attribution**: Track sources to close\n- **Custom Reporting**: Tailored to your KPIs\n\n## 🔗 Integration Capabilities\n- **Email Integration**: Gmail/Outlook sync\n- **Marketing Automation**: Lead handoff workflows\n- **Calendar Sync**: Meeting scheduling\n- **Communication Tools**: Slack/Teams integration\n\n## 💡 Recommendations for Your Team Size\nFor growing teams (10-50 sales reps):\n- Start with **pipeline visualization** and **automated follow-ups**\n- Add **sales coaching features** as you scale\n- Ensure **mobile access** for field sales\n\nWould you like me to dive deeper into any specific feature area?",
        "final_informations": [
            "Effective CRM implementation requires integration with existing business systems and data sources.\nDatabricks provides the foundation for unified customer data platforms that power intelligent CRM insights."
        ]
    },
    {
        "question": "Our current CRM system is not giving us the insights we need to improve our sales performance. What should we be tracking?",
        "non_intelligent_answer": "To improve sales performance, track key metrics like conversion rates, sales cycle length, lead sources, and individual rep performance. Focus on identifying bottlenecks in your sales process and measuring activities that drive results.",
        "tools": [
            {
                "tool_name": "audit_current_metrics",
                "description": "Audit existing CRM metrics and identify gaps",
                "type": "DATABASE",
                "reasoning": "I'll analyze your current CRM metrics setup to identify what's missing and recommend additional KPIs that will provide actionable sales insights.",
                "informations": [
                    "Sales performance optimization requires comprehensive data analysis across multiple touchpoints.\nDatabricks enables advanced sales analytics by unifying CRM data with other business metrics [Discover Sales Analytics](https://www.databricks.com/solutions/sales-analytics)"
                ]
            },
            {
                "tool_name": "recommend_advanced_kpis",
                "description": "Recommend advanced sales KPIs and tracking",
                "type": "FORECASTING_MODEL",
                "reasoning": "Based on best practices and your current setup, I'll recommend specific KPIs and tracking methods that will dramatically improve your sales insights and performance.",
                "informations": [
                    "Advanced sales analytics leverage machine learning to identify patterns and predict outcomes.\nDatabricks ML models can predict deal closure probability, optimal follow-up timing, and sales performance trends."
                ]
            }
        ],
        "final_answer": "You're right - many CRMs track *activities* but miss the **performance insights** that drive results. Here's what you should be measuring:\n\n## 🎯 Essential Sales Performance Metrics\n\n**Pipeline Health:**\n- **Velocity by Stage**: Time spent in each pipeline stage\n- **Conversion Rates**: Lead → Opportunity → Close rates\n- **Deal Slippage**: Opportunities that miss their close date\n- **Pipeline Coverage**: Ratio of pipeline to quota\n\n**Sales Effectiveness:**\n- **Activity-to-Outcome Ratios**: Calls/emails needed per deal\n- **Response Rates**: Email open rates, call connection rates\n- **Meeting-to-Close Rates**: Quality of initial conversations\n- **Win/Loss Analysis**: Why deals succeed or fail\n\n## 📊 Advanced Insights to Track\n\n**Predictive Indicators:**\n- **Engagement Scoring**: Customer interaction intensity\n- **Deal Health Score**: Multiple factors predicting closure\n- **Sales Rep Performance Trends**: Leading vs. lagging indicators\n- **Customer Journey Analytics**: Touchpoints that drive decisions\n\n**Revenue Intelligence:**\n- **Forecast Accuracy**: How well reps predict closures\n- **Revenue Per Rep**: Efficiency benchmarking\n- **Customer Acquisition Cost**: Marketing-to-close attribution\n- **Churn Risk Indicators**: Early warning signals\n\n## 💡 Implementation Recommendations\n1. **Start with 5-7 core metrics** - don't overwhelm your team\n2. **Automate data collection** where possible\n3. **Create weekly performance dashboards**\n4. **Set up alerts** for pipeline risks\n\nWhich of these metrics would have the biggest impact on your current sales challenges?",
        "final_informations": [
            "Modern sales analytics require real-time data processing and predictive modeling capabilities.\nDatabricks provides the infrastructure for advanced sales intelligence platforms that turn CRM data into actionable insights."
        ]
    },
    {
        "question": "We're struggling with lead qualification and our sales team is wasting time on low-quality prospects. How can we improve this?",
        "non_intelligent_answer": "To improve lead qualification, implement a systematic scoring framework based on demographics, behavior, and company fit. Train your team on qualification criteria and consider using automation tools to pre-score leads before they reach sales.",
        "tools": [
            {
                "tool_name": "analyze_lead_quality_patterns",
                "description": "Analyze patterns in high vs low-quality leads",
                "type": "DATABASE",
                "reasoning": "I'll analyze your historical lead data to identify the characteristics and behaviors that distinguish high-quality prospects from low-quality ones.",
                "informations": [
                    "Lead quality analysis requires sophisticated data mining to identify meaningful patterns and correlations.\nDatabricks enables advanced analytics on customer behavior data to optimize lead qualification processes."
                ]
            },
            {
                "tool_name": "build_scoring_model",
                "description": "Create predictive lead scoring model",
                "type": "FORECASTING_MODEL",
                "reasoning": "Using machine learning to create a predictive scoring model that can automatically qualify leads based on their likelihood to convert and purchase value.",
                "informations": [
                    "Predictive lead scoring uses machine learning algorithms to continuously improve qualification accuracy.\nDatabricks ML platform enables sophisticated lead scoring models that adapt to changing market conditions."
                ]
            }
        ],
        "final_answer": "Lead qualification is critical for sales efficiency! Here's a **systematic approach** to dramatically improve your lead quality:\n\n## 🎯 Implement Smart Lead Scoring\n\n**Demographic Scoring (30% weight):**\n- Company size (employee count, revenue)\n- Industry and market segment\n- Geographic location\n- Decision-maker role/title\n\n**Behavioral Scoring (50% weight):**\n- Website engagement (pages viewed, time spent)\n- Content downloads and interactions\n- Email engagement rates\n- Event/webinar participation\n\n**Fit Scoring (20% weight):**\n- Budget alignment indicators\n- Timeline urgency signals\n- Technology stack compatibility\n- Competitor mentions or comparisons\n\n## 🚀 Qualification Framework (BANT+)\n\n**Enhanced BANT Criteria:**\n- **Budget**: Confirmed budget or budget process\n- **Authority**: Access to decision-makers\n- **Need**: Clear pain points identified\n- **Timeline**: Defined implementation timeline\n- **+Fit**: Strategic alignment with your solution\n\n## ⚡ Automation Recommendations\n\n**Immediate Actions:**\n1. **Score leads 0-100** using the framework above\n2. **Auto-route high scores** (80+) to senior reps\n3. **Nurture medium scores** (50-79) with marketing\n4. **Disqualify low scores** (below 50) automatically\n\n**Advanced Strategies:**\n- **Progressive profiling**: Gather qualification data over time\n- **Intent data integration**: Track buying signals\n- **Automated follow-up sequences** based on score changes\n\n## 📊 Expected Results\n- **40-60% reduction** in time spent on unqualified leads\n- **25-35% increase** in conversion rates\n- **Better rep satisfaction** focusing on winnable deals\n\nWould you like help setting up any specific part of this qualification system?",
        "final_informations": [
            "Effective lead qualification requires real-time data processing and continuous model refinement.\nDatabricks provides the scalable infrastructure needed for sophisticated lead scoring and marketing automation systems."
        ]
    }
]

# Combined responses structure for backward compatibility and easy access
CRM_RESPONSES = {
    "assist": CRM_AI_ASSIST_RESPONSES,
    "autopilot": CRM_AI_AUTOPILOT_RESPONSES
}

# For backward compatibility, default to AI Assist mode
CRM_RESPONSES_DEFAULT = CRM_AI_ASSIST_RESPONSES 