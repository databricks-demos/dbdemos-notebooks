"""Pharmaceutical industry use case responses and examples."""

PHARMA_RESPONSES = [
    {
        "question": "I'm a pharmacist and I need information about ZEPHYR-XR (zephyroxin). I have a patient who's on warfarin and has severe renal impairment. What should I consider?",
        "non_intelligent_answer": "I'll look up the drug interaction and dosing information for ZEPHYR-XR.",
        "tools": [
            {
                "tool_name": "query_patient_profile",
                "description": "Query patient medical profile",
                "type": "DATABASE",
                "reasoning": "Retrieving patient-specific medical history and current medications",
                "mock_response": {
                    "patient_id": "PT-2024-0472",
                    "age": 68,
                    "weight": "72 kg",
                    "current_medications": [
                        {"drug": "Warfarin", "dose": "5mg", "frequency": "daily", "last_INR": 2.4, "target_INR": "2.0-3.0"},
                        {"drug": "Lisinopril", "dose": "10mg", "frequency": "daily"},
                        {"drug": "Metformin", "dose": "1000mg", "frequency": "twice daily"}
                    ],
                    "allergies": ["penicillin"],
                    "conditions": ["atrial fibrillation", "type 2 diabetes", "CKD stage 4"],
                    "lab_values": {
                        "creatinine": 2.8,
                        "eGFR": 22,
                        "last_lab_date": "2024-02-15"
                    }
                },
                "informations": [
                    "Databricks enables secure access to patient records while maintaining HIPAA compliance. [Learn about Databricks HIPAA compliance](https://www.databricks.com/trust/compliance/hipaa)"
                ]
            },
            {
                "tool_name": "analyze_drug_interactions",
                "description": "Analyze drug interactions for patient profile",
                "type": "ML_MODEL",
                "reasoning": "Evaluating potential interactions with current medications",
                "mock_response": {
                    "primary_interaction": {
                        "drugs": ["ZEPHYR-XR", "Warfarin"],
                        "severity": "MODERATE",
                        "mechanism": "CYP2C9 inhibition",
                        "effect_size": "25% increase in warfarin exposure",
                        "evidence_level": "Level 2 (well-documented)"
                    },
                    "secondary_interactions": [
                        {
                            "drugs": ["ZEPHYR-XR", "Metformin"],
                            "severity": "MINOR",
                            "notes": "Monitor blood glucose"
                        }
                    ]
                },
                "informations": [
                    "ML-powered interaction analysis considers patient-specific factors"
                ]
            }
        ],
        "final_answer": "I've reviewed your patient's profile (PT-2024-0472) and analyzed the specific considerations for ZEPHYR-XR:\n\n1. Patient-Specific Factors:\n   - 68-year-old with CKD stage 4 (eGFR: 22)\n   - Current INR: 2.4 (within target 2.0-3.0)\n   - Multiple comorbidities: AF, T2DM, CKD\n\n2. Drug Interactions Analysis:\n   Primary Concern - Warfarin:\n   - Moderate interaction (Level 2 evidence)\n   - Expected 25% increase in warfarin exposure\n   - Current dose: 5mg daily\n   - Recent INR well-controlled at 2.4\n\n3. Renal Dosing Requirements:\n   Based on eGFR 22 mL/min:\n   - Reduce ZEPHYR-XR to 50% of normal dose\n   - Maximum daily dose: 25mg\n   - Normal dosing interval maintained\n\n4. Monitoring Plan:\nA. Warfarin-related:\n   - Current INR: 2.4 → Recheck in 4-5 days\n   - Consider 20% warfarin dose reduction if INR rises\n   - More frequent INR monitoring for first 2 weeks\n\nB. Renal-related:\n   - Last creatinine: 2.8 (2024-02-15)\n   - Schedule next renal panel for [Current Date + 14 days]\n   - Monitor for drug accumulation\n\nNote: Secondary interaction with metformin noted - will need blood glucose monitoring.\n\nWould you like me to provide the specific preparation instructions for the dose-adjusted oral suspension?",
        "final_informations": [
            "Databricks enables personalized medication management with real-time clinical decision support [Learn about precision medicine](https://www.databricks.com/solutions/industries/healthcare)",
            "Databricks is HIPAA compliant for handling protected health information [HIPAA compliance details](https://www.databricks.com/trust/compliance/hipaa)"
        ]
    },
    {
        "question": "Yes, please provide the preparation instructions for the oral suspension. We'll need to adjust the dose for this patient.",
        "non_intelligent_answer": "I'll check the preparation and stability requirements for ZEPHYR-XR oral suspension.",
        "tools": [
            {
                "tool_name": "calculate_patient_dose",
                "description": "Calculate patient-specific dose",
                "type": "ML_MODEL",
                "reasoning": "Computing adjusted dose based on patient parameters",
                "mock_response": {
                    "patient_weight": "72 kg",
                    "standard_dose": {"amount": "50mg", "frequency": "daily"},
                    "adjusted_dose": {"amount": "25mg", "frequency": "daily"},
                    "suspension_concentration": {
                        "standard": "5mg/mL",
                        "adjusted": "2.5mg/mL"
                    },
                    "daily_volume": "10mL",
                    "preparation_batch": {
                        "reconstitution_volume": "60mL",
                        "final_volume": "120mL",
                        "duration": "14 days"
                    }
                },
                "informations": [
                    "Precision dosing algorithms account for patient-specific factors"
                ]
            },
            {
                "tool_name": "generate_preparation_label",
                "description": "Generate pharmacy preparation label",
                "type": "LLM",
                "reasoning": "Creating patient-specific preparation instructions",
                "mock_response": {
                    "rx_number": "RX-2024-1857",
                    "preparation_date": "[Current Date]",
                    "expiration_date": "[Current Date + 14 days]",
                    "pharmacist": "RPh: Sarah Johnson",
                    "verification_required": ["concentration", "volume", "expiration"]
                },
                "informations": [
                    "Automated label generation ensures accurate preparation documentation"
                ]
            }
        ],
        "final_answer": "Here's the patient-specific preparation guide for ZEPHYR-XR oral suspension (RX-2024-1857):\n\nPatient-Specific Dosing:\n- Prescribed dose: 25mg daily\n- Required volume: 10mL of 2.5mg/mL suspension\n\nReconstitution Instructions:\n1. Initial Preparation:\n   - Add 60mL purified water to powder\n   - Shake vigorously 60 seconds\n   - Stand 5 minutes\n   - Initial concentration: 5mg/mL\n\n2. Renal Dose Adjustment:\n   - Transfer 60mL of 5mg/mL suspension to calibrated vessel\n   - Add 60mL of provided diluent\n   - Final concentration: 2.5mg/mL\n   - Total volume: 120mL (14-day supply)\n\nQuality Control Checks:\n✓ Initial concentration: 5mg/mL\n✓ Final concentration: 2.5mg/mL\n✓ Final volume: 120mL\n✓ Verification by: RPh Sarah Johnson\n\nLabel Information:\n- Preparation Date: [Current Date]\n- Expiration Date: [Current Date + 14 days]\n- Store refrigerated (2-8°C)\n- Shake well before use\n\nWould you like me to provide the patient-specific administration instructions, including the monitoring schedule for their warfarin and renal function?",
        "final_informations": [
            "Databricks enables personalized medication management with real-time clinical decision support [Learn about precision medicine](https://www.databricks.com/solutions/industries/healthcare)",
            "Databricks is HIPAA compliant for handling protected health information [HIPAA compliance details](https://www.databricks.com/trust/compliance/hipaa)"
        ]
    },
    {
        "question": "Yes, please provide the patient administration instructions, including any specific monitoring they should be aware of.",
        "non_intelligent_answer": "I'll provide the patient-specific administration and monitoring instructions.",
        "tools": [
            {
                "tool_name": "generate_monitoring_schedule",
                "description": "Generate patient monitoring schedule",
                "type": "ML_MODEL",
                "reasoning": "Creating personalized monitoring plan based on patient profile",
                "mock_response": {
                    "patient_id": "PT-2024-0472",
                    "scheduled_tests": [
                        {"test": "INR", "current": 2.4, "next_date": "[Current Date + 4]", "frequency": "2x/week"},
                        {"test": "Renal Panel", "last_date": "2024-02-15", "next_date": "[Current Date + 14]"},
                        {"test": "Drug Levels", "baseline": "needed", "next_date": "[Current Date + 5]"}
                    ],
                    "clinic_appointments": {
                        "primary_care": {"date": "[Current Date + 14]", "provider": "Dr. Martinez"},
                        "anticoagulation": {"date": "[Current Date + 7]", "provider": "Coag Clinic"}
                    }
                },
                "informations": [
                    "AI-powered scheduling optimizes patient monitoring"
                ]
            },
            {
                "tool_name": "create_patient_calendar",
                "description": "Create patient calendar with reminders",
                "type": "LLM",
                "reasoning": "Organizing monitoring schedule into patient-friendly format",
                "mock_response": {
                    "calendar_link": "mychart.health.org/PT-2024-0472/calendar",
                    "sms_reminders": "Enabled",
                    "email_alerts": "patient@email.com"
                },
                "informations": [
                    "Digital patient engagement improves medication adherence"
                ]
            }
        ],
        "final_answer": "Here are your personalized instructions for ZEPHYR-XR:\n\nDaily Medication Schedule:\n1. ZEPHYR-XR Suspension:\n   - Take 10mL once daily at 9:00 AM\n   - Shake bottle well before measuring\n   - Use provided oral syringe\n   - Take 2 hours after warfarin\n\n2. Current Medications:\n   - Warfarin 5mg: Continue at 7:00 AM\n   - Lisinopril 10mg: Continue as prescribed\n   - Metformin 1000mg: Maintain schedule\n\nUpcoming Appointments:\n1. THIS WEEK:\n   - INR Check: [Current Date + 4] at Coag Clinic\n   - ZEPHYR-XR Level: [Current Date + 5] at Lab\n\n2. NEXT WEEK:\n   - Anticoagulation Review: [Current Date + 7]\n   - Dr. Martinez Follow-up: [Current Date + 14]\n\nMonitoring Instructions:\nCall Immediately for:\n- Unusual bleeding/bruising\n- Dark/bloody stools\n- Severe drowsiness\n- Unusual stomach pain\n- Fever or infection signs\n\nDaily Monitoring:\n- Blood pressure: Morning and evening\n- Blood sugar: Continue usual schedule\n- Weight: Daily before breakfast\n\nMedication Storage:\n- Keep ZEPHYR-XR refrigerated\n- Check expiration: [Current Date + 14]\n- Maintain warfarin supply\n\nDigital Resources:\n- Calendar: mychart.health.org/PT-2024-0472/calendar\n- SMS reminders enabled\n- Email alerts set up\n\nEmergency Contacts:\n- Pharmacy: [Your pharmacy number]\n- Anticoagulation Clinic: [Clinic number]\n- After Hours: [Emergency contact]\n\nNext Steps:\n1. Pick up medication today\n2. Start ZEPHYR-XR tomorrow morning\n3. First INR check in 4 days\n4. Bring medication diary to all appointments",
        "final_informations": [
            "Databricks enables comprehensive patient care coordination with integrated monitoring [Learn about connected care](https://www.databricks.com/solutions/industries/healthcare)",
            "Databricks is HIPAA compliant for handling protected health information [HIPAA compliance details](https://www.databricks.com/trust/compliance/hipaa)"
        ]
    }
] 