import type { AgentResponse } from '@/types/AgentResponse';

export interface Message {
  text: string;
  sender: 'user' | 'bot';
  timestamp: Date;
  agentResponse?: AgentResponse | null;
}

export interface ApiMessage {
  role: 'user' | 'assistant';
  content: string;
} 