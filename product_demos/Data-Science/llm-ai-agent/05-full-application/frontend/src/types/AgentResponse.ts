export interface ToolCall {
  tool_name: string;
  description: string;
  type: 'DATABASE' | 'KNOWLEDGE_BASE' | 'REASONING' | 'WEB_SEARCH' | 'FORECASTING_MODEL' | 'EXTERNAL_API';
  reasoning: string;
  informations: string[];
}

export interface AgentResponse {
  question: string;
  tools: ToolCall[];
  final_answer: string;
  final_informations?: string[];
  non_intelligent_answer?: string;
} 