import { ref } from 'vue';
import type { AgentResponse, ToolCall } from '@/types/AgentResponse';
import type { ApiMessage } from '@/types/ChatMessage';

// Types definition for the API
export interface Message {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

// Track the current agent response being shown
export const currentAgentResponse = ref<AgentResponse | null>(null);

// Create event emitter to stream agent results
export const agentResultsEmitter = {
  listeners: new Map<string, Function>(),
  
  addListener(id: string, callback: Function) {
    this.listeners.set(id, callback);
  },
  
  removeListener(id: string) {
    this.listeners.delete(id);
  },
  
  emit(id: string, data: any) {
    const callback = this.listeners.get(id);
    if (callback) {
      callback(data);
    }
  }
};

/**
 * Send a message to the backend and get a streaming response
 * The function will emit agent results progressively
 * @param messages The conversation history
 * @param messageId The ID of the message being responded to
 * @param intelligenceEnabled Whether to show the full intelligence process
 * @param useCase The use case to process messages for
 * @param demoType The demo type: 'assist' or 'autopilot'
 * @returns A promise that resolves when all agent results are emitted
 */
export const sendMessageToAgent = async (
  messages: ApiMessage[], 
  messageId: string, 
  intelligenceEnabled: boolean = true,
  useCase: string = "telco",
  demoType: string = "assist"
): Promise<void> => {
  try {
    const response = await fetch('/api/agent/chat', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        messages,
        intelligence_enabled: intelligenceEnabled,
        use_case: useCase,
        demo_type: demoType
      })
    });

    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('No reader available');
    }

    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const jsonData = line.slice(6); // Remove 'data: ' prefix
            const data = JSON.parse(jsonData);
            agentResultsEmitter.emit(messageId, data);
          } catch (e) {
            console.error('Error parsing SSE data:', e);
          }
        }
      }
    }
  } catch (error) {
    console.error('Error in sendMessageToAgent:', error);
    throw error;
  }
};

// Function to get predefined questions from the backend
export const getPredefinedQuestions = async (useCase: string = "telco", demoType: string = "assist") => {
  try {
    const response = await fetch(`/api/agent/questions?use_case=${useCase}&demo_type=${demoType}`);
    if (!response.ok) {
      throw new Error('Failed to fetch questions');
    }
    return await response.json();
  } catch (error) {
    console.error('Error fetching predefined questions:', error);
    return [];
  }
};
