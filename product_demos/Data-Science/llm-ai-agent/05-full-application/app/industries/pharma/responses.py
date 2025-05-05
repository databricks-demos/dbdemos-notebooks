"""Pharmaceutical industry use case responses and examples: Xarelto (rivaroxaban) in renal impairment."""

PHARMA_RESPONSES = [
    {
        "question": "I have a patient with atrial fibrillation and a CrCl of 35 mL/min. How should I adjust their Xarelto dose compared to someone with normal renal function?",
        "non_intelligent_answer": "I'll review the latest guidelines and pharmacokinetic data for Xarelto dosing in renal impairment.",
        "tools": [
            {
                "tool_name": "query_clinical_guidelines",
                "description": "Retrieve dosing recommendations for Xarelto in renal impairment",
                "type": "DATABASE",
                "reasoning": "Accessing up-to-date clinical guidelines for anticoagulation in atrial fibrillation with renal impairment.",
                "mock_response": {
                    "indication": "Non-valvular atrial fibrillation",
                    "normal_renal_function": "20 mg once daily with the evening meal (CrCl ≥50 mL/min)",
                    "moderate_impairment": "15 mg once daily with the evening meal (CrCl 15–49 mL/min)",
                    "severe_impairment": "Avoid use if CrCl <15 mL/min",
                    "exposure_increase": "~52% increase in systemic exposure at CrCl 15–49 mL/min"
                },
                "informations": [
                    "Guidelines recommend Xarelto 15 mg once daily for CrCl 15–49 mL/min in non-valvular AF."
                ]
            }
        ],
        "final_answer": "For non-valvular AF, Xarelto 15 mg once daily is recommended for CrCl 15–49 mL/min, taken with the evening meal. This accounts for increased systemic exposure due to reduced renal clearance, which rises by ~52% in moderate renal impairment. Avoid use in CrCl <15 mL/min due to insufficient data and heightened bleeding risk.",
        "final_informations": [
            "Dosing recommendations are based on clinical guidelines and pharmacokinetic studies.",
            "See [Xarelto Prescribing Information](https://www.janssenlabels.com/package-insert/product-monograph/prescribing-information/XARELTO-pi.pdf) for details."
        ]
    },
    {
        "question": "What monitoring parameters are critical here?",
        "non_intelligent_answer": "I'll summarize the key monitoring parameters for Xarelto in renal impairment.",
        "tools": [
            {
                "tool_name": "summarize_monitoring_guidelines",
                "description": "Summarize monitoring recommendations for Xarelto in renal impairment",
                "type": "LLM",
                "reasoning": "Providing evidence-based monitoring guidance for patients on Xarelto with reduced renal function.",
                "mock_response": {
                    "monitoring": [
                        "Renal function (especially if CrCl approaches 30 mL/min)",
                        "Signs of bleeding (anemia, hypotension, overt bleeding)",
                        "Consider therapeutic drug monitoring in severe impairment if bleeding occurs"
                    ],
                    "exposure_increase": "1.6-fold increase in plasma levels for CrCl 15–29 mL/min"
                },
                "informations": [
                    "Close monitoring is essential in patients with moderate to severe renal impairment."
                ]
            }
        ],
        "final_answer": "Regularly assess renal function—especially if CrCl approaches 30 mL/min—and watch for bleeding signs (e.g., anemia, hypotension). Patients with CrCl 15–29 mL/min have a 1.6-fold increase in rivaroxaban plasma levels, elevating hemorrhage risk. Consider therapeutic drug monitoring in severe impairment if bleeding occurs.",
        "final_informations": [
            "Monitoring recommendations are based on clinical guidelines and pharmacokinetic data."
        ]
    },
    {
        "question": "How does Xarelto compare to warfarin in advanced CKD?",
        "non_intelligent_answer": "I'll compare the safety and efficacy of Xarelto and warfarin in advanced CKD based on available studies.",
        "tools": [
            {
                "tool_name": "compare_anticoagulants_ckd",
                "description": "Compare Xarelto and warfarin outcomes in advanced CKD",
                "type": "ML_MODEL",
                "reasoning": "Synthesizing evidence from clinical studies on anticoagulant use in advanced CKD.",
                "mock_response": {
                    "major_bleeding_risk": "2.4x higher with Xarelto in severe CKD vs mild/no CKD, similar to warfarin",
                    "renal_decline": "Xarelto may reduce renal function decline compared to warfarin, especially in Asian populations",
                    "alternative": "For CrCl <30 mL/min, apixaban may offer a safer profile"
                },
                "informations": [
                    "Both Xarelto and warfarin carry increased bleeding risk in advanced CKD."
                ]
            }
        ],
        "final_answer": "While Xarelto avoids INR monitoring, it carries a 2.4x higher major bleeding risk in severe CKD versus mild/no CKD, similar to warfarin. However, studies suggest Xarelto may reduce renal function decline compared to warfarin, particularly in Asian populations. For CrCl <30 mL/min, apixaban may offer a safer profile.",
        "final_informations": [
            "Comparative data are based on observational studies and meta-analyses."
        ]
    },
    {
        "question": "Are there drug interactions that exacerbate renal risks?",
        "non_intelligent_answer": "I'll identify drug interactions that may increase renal or bleeding risk with Xarelto.",
        "tools": [
            {
                "tool_name": "analyze_drug_interactions",
                "description": "Analyze drug interactions relevant to Xarelto in renal impairment",
                "type": "ML_MODEL",
                "reasoning": "Identifying interactions that increase rivaroxaban exposure or bleeding risk in renal impairment.",
                "mock_response": {
                    "avoid": [
                        "Strong CYP3A4/P-gp inhibitors (e.g., ketoconazole) in moderate-severe renal impairment",
                        "NSAIDs or antiplatelets—assess bleeding risk"
                    ],
                    "mechanism": "CYP3A4/P-gp inhibitors further elevate rivaroxaban levels"
                },
                "informations": [
                    "Drug interactions can significantly increase bleeding risk in renal impairment."
                ]
            }
        ],
        "final_answer": "Yes. Avoid concomitant use of strong CYP3A4/P-gp inhibitors (e.g., ketoconazole) in moderate-severe renal impairment, as they further elevate rivaroxaban levels. For patients on NSAIDs or antiplatelets, weigh bleeding risks carefully.",
        "final_informations": [
            "Interaction data are based on pharmacokinetic and clinical studies."
        ]
    },
    {
        "question": "What if renal function deteriorates during treatment?",
        "non_intelligent_answer": "I'll provide guidance on managing Xarelto if renal function worsens during therapy.",
        "tools": [
            {
                "tool_name": "manage_acute_renal_decline",
                "description": "Provide recommendations for Xarelto management in acute renal decline",
                "type": "LLM",
                "reasoning": "Offering evidence-based recommendations for anticoagulation management in acute renal failure.",
                "mock_response": {
                    "action": "Discontinue Xarelto if CrCl <15 mL/min or acute renal failure develops. Transition to alternative anticoagulation (e.g., heparin) if needed. Re-evaluate once renal function stabilizes."
                },
                "informations": [
                    "Acute renal decline requires prompt reassessment of anticoagulation strategy."
                ]
            }
        ],
        "final_answer": "Discontinue Xarelto if CrCl drops below 15 mL/min or acute renal failure develops. Transition to alternative anticoagulation (e.g., heparin) if needed, and re-evaluate once renal function stabilizes.",
        "final_informations": [
            "Management recommendations are based on clinical guidelines and expert consensus."
        ]
    },
    {
        "question": "Any advice for patients with fluctuating CrCl near 30 mL/min?",
        "non_intelligent_answer": "I'll provide dosing and monitoring advice for patients with variable renal function.",
        "tools": [
            {
                "tool_name": "personalize_dosing_monitoring",
                "description": "Personalize dosing and monitoring for fluctuating renal function",
                "type": "LLM",
                "reasoning": "Tailoring recommendations for patients with unstable renal function.",
                "mock_response": {
                    "dosing": "Use a 15 mg dose if CrCl is 30–49 mL/min and monitor closely during illness or dehydration. For CrCl 15–29 mL/min, limit use to essential cases and consider apixaban if bleeding risk is high."
                },
                "informations": [
                    "Close monitoring and individualized therapy are critical in fluctuating renal function."
                ]
            }
        ],
        "final_answer": "Use a 15 mg dose if CrCl is 30–49 mL/min and monitor closely during illness or dehydration. For CrCl 15–29 mL/min, limit use to essential cases and consider apixaban if bleeding risk is high.",
        "final_informations": [
            "Recommendations are based on expert consensus and clinical experience."
        ]
    }
] 