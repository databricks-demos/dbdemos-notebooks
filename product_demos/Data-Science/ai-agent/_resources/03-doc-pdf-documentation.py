# Databricks notebook source
# MAGIC %pip install faker weasyprint mlflow>=3.1.0 openai
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

"""
Telco PDF Documentation Generator
This script generates comprehensive PDF documentation for telco equipment and services.
"""

import random
import os
import requests
import json
from datetime import date, datetime, timedelta
from typing import Dict, List, Any

import pandas as pd
from faker import Faker

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def get_documentation_entities():
    """Generate list of documentation entities for telco equipment and services."""
    
    return [
        # Hardware Equipment Manuals
        {
            "title": "Fiber Optic Modem User Manual",
            "category": "Hardware",
            "model": "FOM-2000X",
            "description": "High-speed fiber optic modem for residential and business use"
        },
        {
            "title": "ADSL Router Troubleshooting Guide",
            "category": "Hardware",
            "model": "ADSL-R500",
            "description": "Comprehensive troubleshooting for ADSL router connectivity issues"
        },
        {
            "title": "Mobile Hotspot Device Manual",
            "category": "Hardware",
            "model": "MHD-4G-Pro",
            "description": "4G/5G mobile hotspot device for on-the-go internet access"
        },
        {
            "title": "WiFi Extender Setup Guide",
            "category": "Hardware",
            "model": "WIFI-EXT-300",
            "description": "WiFi range extender installation and configuration manual"
        },
        {
            "title": "VoIP Phone System Manual",
            "category": "Hardware",
            "model": "VOIP-PRO-100",
            "description": "Voice over IP phone system setup and configuration"
        },
        {
            "title": "Cable Modem Troubleshooting",
            "category": "Hardware",
            "model": "CABLE-MODEM-500",
            "description": "Cable modem connectivity and performance troubleshooting"
        },
        
        # Technical Documentation
        {
            "title": "Internet Service Error Codes Reference",
            "category": "Technical",
            "model": "ERR-REF-2024",
            "description": "Complete reference guide for all internet service error codes"
        },
        {
            "title": "Network Security Configuration Manual",
            "category": "Security",
            "model": "SEC-NET-2024",
            "description": "Network security setup and configuration for telco equipment"
        },
        {
            "title": "Router Firmware Update Guide",
            "category": "Maintenance",
            "model": "FIRMWARE-UPDATE-001",
            "description": "Safe router firmware update procedures and troubleshooting"
        },
        {
            "title": "Network Monitoring Tools Manual",
            "category": "Monitoring",
            "model": "NET-MON-2024",
            "description": "Network monitoring and diagnostic tools usage guide"
        },
        
        # Installation & Configuration
        {
            "title": "Fiber Installation Guide",
            "category": "Installation",
            "model": "FIBER-INST-001",
            "description": "Step-by-step fiber optic installation and setup guide"
        },
        {
            "title": "Home Network Configuration Manual",
            "category": "Configuration",
            "model": "HOME-NET-001",
            "description": "Complete home network setup and configuration guide"
        },
        {
            "title": "Smart Home Network Setup",
            "category": "Configuration",
            "model": "SMART-HOME-NET",
            "description": "Smart home device network integration and setup"
        },
        
        # Business Services
        {
            "title": "Business Internet Setup Guide",
            "category": "Business",
            "model": "BIZ-INT-2024",
            "description": "Business-grade internet service setup and management"
        },
        {
            "title": "Internet Backup Solutions Guide",
            "category": "Business",
            "model": "BACKUP-INT-001",
            "description": "Internet backup solutions for business continuity"
        },
        {
            "title": "Internet Service Level Agreement Guide",
            "category": "Business",
            "model": "SLA-GUIDE-2024",
            "description": "Understanding and monitoring internet service level agreements"
        },
        
        # Diagnostics & Optimization
        {
            "title": "Internet Speed Test Guide",
            "category": "Diagnostics",
            "model": "SPEED-TEST-2024",
            "description": "How to test and optimize internet connection speeds"
        },
        {
            "title": "Network Performance Optimization",
            "category": "Optimization",
            "model": "PERF-OPT-2024",
            "description": "Network performance optimization techniques and best practices"
        },
        {
            "title": "Emergency Internet Connectivity Guide",
            "category": "Emergency",
            "model": "EMERG-INT-001",
            "description": "Emergency internet connectivity solutions and procedures"
        },
        {
            "title": "Internet Security Best Practices",
            "category": "Security",
            "model": "SEC-BEST-2024",
            "description": "Internet security best practices for home and business users"
        },
        
        # Customer Service & Support Documentation
        {
            "title": "Customer Refund Policy Guide",
            "category": "Customer Service",
            "model": "REFUND-POLICY-2024",
            "description": "Complete guide to customer refund policies, procedures, and eligibility criteria"
        },
        {
            "title": "Equipment Return Policy Manual",
            "category": "Customer Service",
            "model": "RETURN-POLICY-2024",
            "description": "Equipment return procedures, restocking fees, and return authorization process"
        },
        {
            "title": "Service Cancellation Procedures",
            "category": "Customer Service",
            "model": "CANCEL-PROC-2024",
            "description": "Step-by-step service cancellation procedures and early termination fees"
        },
        {
            "title": "Customer Complaint Resolution Guide",
            "category": "Customer Service",
            "model": "COMPLAINT-RES-2024",
            "description": "Customer complaint handling procedures and escalation protocols"
        },
        {
            "title": "Service Upgrade and Downgrade Guide",
            "category": "Customer Service",
            "model": "SERVICE-CHANGE-2024",
            "description": "Service plan changes, upgrade procedures, and proration policies"
        },
        {
            "title": "Payment Plan and Billing Support Guide",
            "category": "Customer Service",
            "model": "BILLING-SUPPORT-2024",
            "description": "Payment plan options, billing dispute resolution, and payment methods"
        },
        {
            "title": "Customer Loyalty Program Guide",
            "category": "Customer Service",
            "model": "LOYALTY-PROG-2024",
            "description": "Loyalty program benefits, tier requirements, and reward redemption"
        },
        {
            "title": "Technical Support Escalation Procedures",
            "category": "Customer Service",
            "model": "ESCALATION-PROC-2024",
            "description": "When and how to escalate technical issues to higher support tiers"
        },
        {
            "title": "Customer Account Management Guide",
            "category": "Customer Service",
            "model": "ACCOUNT-MGMT-2024",
            "description": "Account changes, authorized users, and account security procedures"
        },
        {
            "title": "Service Outage Communication Protocol",
            "category": "Customer Service",
            "model": "OUTAGE-COMM-2024",
            "description": "How to communicate service outages and estimated restoration times"
        },
        {
            "title": "Customer Satisfaction Survey Procedures",
            "category": "Customer Service",
            "model": "SAT-SURVEY-2024",
            "description": "Customer satisfaction survey administration and follow-up procedures"
        },
        {
            "title": "Service Credit and Compensation Guide",
            "category": "Customer Service",
            "model": "SERVICE-CREDIT-2024",
            "description": "When and how to offer service credits for service issues"
        },
        {
            "title": "Customer Retention Strategies Guide",
            "category": "Customer Service",
            "model": "RETENTION-STRAT-2024",
            "description": "Customer retention strategies, win-back offers, and loyalty incentives"
        },
        {
            "title": "Multi-Language Support Procedures",
            "category": "Customer Service",
            "model": "MULTI-LANG-2024",
            "description": "Supporting customers in multiple languages and translation services"
        },
        {
            "title": "Accessibility Support Guidelines",
            "category": "Customer Service",
            "model": "ACCESSIBILITY-2024",
            "description": "Supporting customers with disabilities and accessibility requirements"
        },
        {
            "title": "Customer Privacy and Data Protection Guide",
            "category": "Customer Service",
            "model": "PRIVACY-PROT-2024",
            "description": "Customer data protection, privacy policies, and GDPR compliance"
        },
        {
            "title": "Social Media Customer Support Guide",
            "category": "Customer Service",
            "model": "SOCIAL-SUPPORT-2024",
            "description": "Handling customer inquiries and complaints on social media platforms"
        },
        {
            "title": "Customer Education and Training Materials",
            "category": "Customer Service",
            "model": "CUST-EDU-2024",
            "description": "Educational materials and training resources for customers"
        }
    ]


def get_documentation_prompt(entity):
    """Generate the prompt for a specific documentation entity."""
    
    base_prompt = """
Generate a highly structured, comprehensive, and professional HTML5 documentation manual for the following telco equipment or service. This document is intended to be used in a Retrieval-Augmented Generation (RAG) application, so all relevant information must be explicitly written and deeply detailed to answer any user question.

TITLE: {title}
CATEGORY: {category}
MODEL: {model}
DESCRIPTION: {description}

## REQUIREMENTS:

### 1. STRUCTURE & HTML OUTPUT
- Generate a complete and valid HTML5 document with `<!DOCTYPE html>`, proper `<head>`, metadata, and `<body>`.
- Include embedded CSS styles for a clean, readable PDF layout (e.g., margins, font sizes, headings, tables).
- Start with a title page including model, title, and version info.
- Include an auto-generated **Table of Contents** with clickable anchor links.
- Use `<h1>` to `<h4>` consistently for logical sectioning.
- Provide page-break support (`page-break-before/after`) for clean PDF exports.

### 2. CONTENT SECTIONS (Mandatory when applicable):
- Executive Summary
- Technical Specifications
- Installation & Setup Instructions (detailed steps, environment requirements)
- Configuration & Management Guide
- Error Code Reference
  - For each error code:
    - Code, description, symptoms, root causes
    - Step-by-step resolution procedure
    - Escalation policy if unresolved
- Troubleshooting (diagnostic steps, flowcharts, scenarios)
- Maintenance & Firmware Update Procedures
- Network Diagrams (ASCII or table-based if no images)
- Performance Optimization Tips
- Compliance, Regulatory & Safety Warnings
- Security Configuration (firewall, VPN, user access control)
- Compatibility & Integration Matrix
- Warranty, Return, and Refund Policies
- Frequently Asked Questions (include at least 10 real-world scenarios with answers)
- Glossary of Technical Terms
- Support & Escalation Contacts
- Changelog or Revision History (optional)

### 3. TECHNICAL DEPTH
- Use formal, professional language suitable for enterprise documentation.
- Provide exact command-line examples, config snippets, or UI navigation paths where applicable.
- Include measurable specs (e.g., “up to 1.2 Gbps over 5 GHz”).
- Ensure each error code and support topic has complete resolution coverage — never assume the reader already knows the steps.

### 4. RAG OPTIMIZATION
This document is used as source material for a LLM-powered agent. Therefore:
- Every topic should be self-contained and clearly titled.
- Avoid vague instructions. Prefer: “Navigate to Settings > Network > VPN” over “Go to VPN settings.”
- Make sure that **all policies, procedures, exceptions, and escalation steps are explicitly listed**.
- Anticipate questions from:
  - End users (e.g., “How do I reset the modem?”)
  - Technicians (e.g., “What’s error 1042 and how do I fix it?”)
  - Customer service (e.g., “When is a refund eligible?”)
  - Managers (e.g., “Is this device GDPR compliant?”)

### 5. LENGTH & COMPLETENESS
- The output should generate a document equivalent to **20–30 PDF pages** with full coverage.
- Include real-world examples, configuration walkthroughs, and sample scenarios.
- Use numbered lists for procedures, bullet lists for requirements, and tables for comparisons.

### 6. VISUAL ENHANCEMENT
- Use `<hr>` to break sections visually.
- Use `<table>` for structured data like specs and codes.
- Simulate diagrams using ASCII if no images can be used.
- Apply headers/footers where possible for printing.

DO NOT LEAVE OUT any expected section or common user concern. This document will be the **only source** for AI to answer questions — make sure it is fully comprehensive.

Now generate the full HTML documentation for the product/service listed above.
"""
    
    return base_prompt.format(**entity)


def call_llm_api(prompt: str) -> str:
    """
    Call OpenAI API to generate HTML content.
    """
    from openai import OpenAI
    import os
    
    # Set your API key (recommended: use an environment variable)
    # client = OpenAI(api_key=dbutils.secrets.get(scope="dbdemos", key="openai_key"))  # or set directly: "your-api-key-here"

    client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url="https://e2-demo-field-eng.cloud.databricks.com//serving-endpoints"
    )
    model_name = 'databricks-meta-llama-3-3-70b-instruct' # "gpt-4.1-nano"
    
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You are an expert technical documentation specialist with 15+ years of experience writing professional HTML5 manuals for telecommunications equipment and services. Your goal is to generate complete, deeply detailed, well-structured HTML documentation that is immediately usable and optimized for PDF conversion and retrieval-augmented generation (RAG) use cases.\n\nInstructions:\n- Output only valid, clean HTML — do NOT wrap output in triple backticks or markdown.\n- Always generate a full HTML5 document including <!DOCTYPE html>, <html>, <head>, <style>, and <body>.\n- Use embedded CSS for simple, elegant styling optimized for PDF (e.g. clean fonts, spacing, page breaks).\n- Include a generated <nav> Table of Contents with anchor links to each section.\n- Use <h1> to <h4> headings consistently, semantic <section> tags when possible.\n- Write content that fills approximately 20–30 pages in PDF format.\n- Include all expected sections: installation, troubleshooting, error codes, safety, warranty, FAQs, user scenarios, configuration steps, etc.\n- Ensure every error code includes its cause, symptoms, and detailed resolution steps.\n- Avoid vague phrases. Use clear, numbered instructions.\n- Your output should be clean HTML, copy-paste ready, with no extra markdown or formatting artifacts.\n\nYou write with a professional, technical tone suitable for customers, technicians, and management. Ensure the HTML is designed for full-page PDF rendering using A4 page size. Font size should be small, 12px max."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2,
        max_tokens=8192 # 32768
    )
    
    return response.choices[0].message.content


def convert_html_to_pdf(html_content: str, output_path: str):
    """Convert HTML content to PDF using WeasyPrint and save to the specified path."""
    try:
        from weasyprint import HTML
        HTML(string=html_content).write_pdf(output_path)
        print(f"  ✓ PDF saved: {output_path}")
    except ImportError:
        print("WeasyPrint not available. Install it with: pip install weasyprint")
        raise
    except Exception as e:
        print(f"  ✗ Error converting to PDF: {str(e)}")
        raise


def generate_documentation_manuals(path: str):
    """Generate comprehensive documentation manuals for telco equipment and services."""
    
    # Create target folder
    dbutils.fs.mkdirs(f'{path}/pdf_documentation')
    
    # Get documentation entities
    documentation_entities = get_documentation_entities()
    
    print(f"Generating {len(documentation_entities)} documentation manuals...")
    
    for i, entity in enumerate(documentation_entities, 1):
        print(f"Generating manual {i}/{len(documentation_entities)}: {entity['title']}")
        
        try:
            # Get prompt for this entity
            prompt = get_documentation_prompt(entity)
            
            # Call Claude API
            html_content = call_llm_api(prompt)
            
            # Generate filename
            filename = f"{entity['model'].replace('-', '_').lower()}_manual"
            pdf_path = f'{path}/pdf_documentation/{filename}.pdf'
            
            # Convert to PDF and save
            convert_html_to_pdf(html_content, pdf_path)
            
        except Exception as e:
            print(f"  ✗ Error generating {entity['title']}: {str(e)}")
    
    print(f"\nDocumentation generation complete! Files saved in '{path}/pdf_documentation' folder.")


# COMMAND ----------

print("Generating telco documentation...")

# Generate documentation
path = f'/Volumes/{catalog}/{schema}/{volume_name}'
generate_documentation_manuals(path)

print("\nDocumentation generation complete!")

# COMMAND ----------

pdf = call_llm_api("generate a html documentation page on a wifi router. It should be 5 page and then we'll generate it as pdf.")


# COMMAND ----------

pdf

# COMMAND ----------

dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume_name}/pdf_documentation")
convert_html_to_pdf(pdf, f"/Volumes/{catalog}/{schema}/{volume_name}/pdf_documentation/wifi_router_manual.pdf")
