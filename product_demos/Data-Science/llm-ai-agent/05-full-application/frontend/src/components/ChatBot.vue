<template>
  <div class="chat-container" @use-case-change="onUseCaseChange">
    <div class="messages-container" ref="messagesContainer">
      <div 
        v-for="(message, index) in messages" 
        :key="message.id" 
        :class="['message', message.sender === 'user' ? 'user-message' : 'bot-message']"
      >
        <div class="message-content">
          <div class="message-avatar">
            <v-avatar size="32" :color="message.sender === 'user' ? 'primary' : 'secondary'">
              <span class="text-white">{{ message.sender === 'user' ? 'U' : 'B' }}</span>
            </v-avatar>
          </div>
          <div class="message-bubble">
            <div v-if="message.sender === 'bot'" class="message-with-intelligence">
              <div v-html="formatMessage(message.text)" class="message-text"></div>
              <button 
                v-if="responsesMap.get(message.id)"
                @click="showIntelligence(message.id)"
                class="intelligence-button"
                title="View intelligence behind this answer"
              >
                <IconBrainCircuit class="intelligence-icon" />
                <span class="intelligence-text">Show Intelligence</span>
              </button>
            </div>
            <div v-else v-html="formatMessage(message.text)"></div>
          </div>
        </div>
        <div class="message-time">{{ formatTime(message.timestamp) }}</div>
      </div>
      
      <!-- Word-by-word typing animation -->
      <div v-if="isShowingTypingAnimation" class="message bot-message typing-animation">
        <div class="message-content">
          <div class="message-avatar">
            <v-avatar size="32" color="secondary">
              <span class="text-white">B</span>
            </v-avatar>
          </div>
          <div class="message-bubble">
            <p v-html="formatMessage(currentTypingMessage)"></p>
            <span class="typing-cursor"></span>
          </div>
        </div>
        <div class="message-time">{{ formatTime(new Date()) }}</div>
      </div>
      
      <!-- Standard typing indicator -->
      <div v-else-if="isTyping && !isShowingTypingAnimation" class="message bot-message typing-indicator">
        <div class="message-content">
          <div class="message-avatar">
            <v-avatar size="32" color="secondary">
              <span class="text-white">B</span>
            </v-avatar>
          </div>
          <div class="message-bubble">
            <span class="loading-dots">
              <span></span>
              <span></span>
              <span></span>
            </span>
          </div>
        </div>
      </div>
    </div>
    
    <div class="chat-input">
      <v-form @submit.prevent="sendMessage">
        <div class="d-flex align-center">
          <v-text-field
            v-model="userInput"
            placeholder="Type your message here..."
            variant="outlined"
            density="comfortable"
            hide-details
            autocomplete="off"
            @keydown.enter.prevent="sendMessage"
            class="mr-2"
          />
          <v-btn 
            color="primary" 
            icon 
            @click="sendMessage" 
            :disabled="!userInput.trim()"
          >
            <v-icon>mdi-send</v-icon>
          </v-btn>
        </div>
      </v-form>
      
      <!-- Pre-defined questions section -->
      <div class="predefined-questions mt-3">
        <div class="text-caption text-medium-emphasis mb-2">Quick questions:</div>
        <div class="d-flex flex-wrap">
          <v-tooltip
            v-for="(question, index) in predefinedQuestions"
            :key="index"
            location="top"
            :text="question.text"
          >
            <template v-slot:activator="{ props }">
              <v-chip
                v-bind="props"
                color="primary"
                variant="outlined"
                size="small"
                class="mb-2 mr-2"
                @click="selectPredefinedQuestion(question)"
              >
                {{ question.preview }}
              </v-chip>
            </template>
          </v-tooltip>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick, watch } from 'vue';
import { marked } from 'marked';
import { type ApiMessage } from '@/types/ChatMessage';
import { sendMessageToAgent, agentResultsEmitter, getPredefinedQuestions } from '@/services/api';
import type { AgentResponse, ToolCall } from '@/types/AgentResponse';
import IconBrainCircuit from './icons/IconBrainCircuit.vue';

const props = defineProps<{
  thinking: boolean;
  agents: ToolCall[];
  finalAnswer: string;
  finalInformations: string[];
  intelligenceEnabled: boolean;
}>();

const emit = defineEmits<{
  (e: 'update:thinking', value: boolean): void;
  (e: 'update:agents', value: ToolCall[]): void;
  (e: 'update:final-answer', value: string): void;
  (e: 'update:final-informations', value: string[]): void;
  (e: 'use-case-change', value: string): void;
}>();

// Store agent responses in a map keyed by message ID
const responsesMap = ref<Map<string, AgentResponse>>(new Map());

// Store the current streaming response (tools as they arrive)
const currentStreamingTools = ref<ToolCall[]>([]);
const currentFinalAnswer = ref<string>('');
const currentFinalInformations = ref<string[]>([]);

// Generate a unique ID for each message
const generateMessageId = (): string => {
  return Date.now().toString() + Math.random().toString(36).substring(2, 9);
};

// Interface for chat messages with a unique ID
interface Message {
  id: string;
  text: string;
  sender: 'user' | 'bot';
  timestamp: Date;
  agentResponse?: AgentResponse | null;
}

const messages = ref<Message[]>([
  {
    id: generateMessageId(),
    text: "Hello! I'm your AI assistant. How can I help you today?",
    sender: 'bot',
    timestamp: new Date()
  }
]);

const userInput = ref('');
const messagesContainer = ref<HTMLElement | null>(null);
const isTyping = ref(false);
const isThinking = ref(false);

// Add new reactive variables for typing animation
const currentTypingMessage = ref('');
const isShowingTypingAnimation = ref(false);
const fullMessageText = ref('');
const typingSpeed = 2; // Much faster typing speed
// Instead of individual words, we'll group the text into chunks for faster display
const chunkSize = 4; // Number of words to display at once
const wordSplitRegex = /(\S+\s*)/g; // Regex to split text into words with spacing

// Add a new responsive variable to track the selected agent response for display
const selectedAgentResponse = ref<AgentResponse | null>(null);

// Add ref to store the current message ID during typing
const currentMessageId = ref<string>('');

interface PredefinedQuestion {
  preview: string;
  text: string;
}

// Get predefined questions from the API
const predefinedQuestions = ref<PredefinedQuestion[]>([]);

// Add useCase ref
const useCase = ref('telco');

// Handle use case changes
const handleUseCaseChange = async (newUseCase: string) => {
  useCase.value = newUseCase;
  
  // Reset chat to initial state
  messages.value = [{
    id: generateMessageId(),
    text: "Hello! I'm your AI assistant. How can I help you today?",
    sender: 'bot',
    timestamp: new Date()
  }];
  
  // Clear any ongoing state
  currentStreamingTools.value = [];
  currentFinalAnswer.value = '';
  currentFinalInformations.value = [];
  isTyping.value = false;
  isThinking.value = false;
  emit('update:thinking', false);
  emit('update:agents', []);
  emit('update:final-answer', '');
  emit('update:final-informations', []);
  
  // Clear the responses map
  responsesMap.value.clear();
  
  try {
    // Fetch new questions for the use case
    const questions = await getPredefinedQuestions(newUseCase);
    predefinedQuestions.value = questions;
  } catch (error) {
    console.error('Error fetching predefined questions:', error);
    predefinedQuestions.value = [];
  }
  
  // Scroll to top since we're resetting
  await nextTick();
  if (messagesContainer.value) {
    messagesContainer.value.scrollTop = 0;
  }
};

// Load predefined questions on mount
onMounted(async () => {
  predefinedQuestions.value = await getPredefinedQuestions(useCase.value);
  scrollToBottom();
});

// Method to handle selecting a pre-defined question
const selectPredefinedQuestion = (question: { preview: string, text: string }) => {
  userInput.value = question.text;
  // Use nextTick to ensure the input is updated before sending
  nextTick(() => {
    sendMessage();
  });
};

// Convert chat messages to API message format
const convertToApiMessages = (chatMessages: Message[]): ApiMessage[] => {
  return chatMessages.map(msg => ({
    role: msg.sender === 'user' ? 'user' : 'assistant',
    content: msg.text
  }));
};

// Modified sendMessage to include use case
const sendMessage = async () => {
  if (!userInput.value.trim()) return;
  
  // Add user message with ID
  const userMessageId = generateMessageId();
  messages.value.push({
    id: userMessageId,
    text: userInput.value,
    sender: 'user',
    timestamp: new Date()
  });
  
  // Clear input
  const userMessage = userInput.value;
  userInput.value = '';
  
  // Scroll to bottom
  await scrollToBottom();
  
  // Show typing indicator and thinking state immediately
  isTyping.value = true;
  isThinking.value = true;
  emit('update:thinking', true);
  emit('update:agents', []);
  
  try {
    // Reset current streaming state
    currentStreamingTools.value = [];
    currentFinalAnswer.value = '';
    currentFinalInformations.value = [];
    
    // Convert messages to API format
    const apiMessages = convertToApiMessages(messages.value);
    
    // Call the API with the user message ID
    agentResultsEmitter.addListener(userMessageId, handleAgentStreamingResponse);
    await sendMessageToAgent(apiMessages, userMessageId, props.intelligenceEnabled, useCase.value);
    agentResultsEmitter.removeListener(userMessageId);
    
  } catch (error) {
    console.error('Error getting bot response:', error);
    isTyping.value = false;
    isThinking.value = false;
    emit('update:thinking', false);
    
    // Add error message with ID
    messages.value.push({
      id: generateMessageId(),
      text: "I'm sorry, I encountered an error. Please try again.",
      sender: 'bot',
      timestamp: new Date()
    });
    
    await scrollToBottom();
  }
};

// Handle streaming responses from the agent emitter
const handleAgentStreamingResponse = (event: any) => {
  switch (event.type) {
    case 'thinking-start':
      currentStreamingTools.value = [];
      emit('update:agents', []);
      emit('update:final-answer', '');
      emit('update:final-informations', []);
      isTyping.value = true;
      break;
      
    case 'tool':
      if (props.intelligenceEnabled) {
        currentStreamingTools.value = [...currentStreamingTools.value, event.data];
        emit('update:agents', currentStreamingTools.value);
      }
      break;
      
    case 'final-answer':
      // Generate message ID when we actually receive the answer
      currentMessageId.value = generateMessageId();
      
      currentFinalAnswer.value = event.data.final_answer;
      currentFinalInformations.value = event.data.final_informations || [];
      emit('update:final-answer', currentFinalAnswer.value);
      emit('update:final-informations', currentFinalInformations.value);
      
      // Store the response with the new message ID
      responsesMap.value.set(currentMessageId.value, {
        question: messages.value[messages.value.length - 1]?.text || '',
        tools: currentStreamingTools.value,
        final_answer: currentFinalAnswer.value,
        final_informations: currentFinalInformations.value,
        non_intelligent_answer: event.data.final_answer
      });
      
      startTypingAnimation(event.data.final_answer, !props.intelligenceEnabled);
      isTyping.value = false;
      break;
  }
};

// Word-by-word typing animation function
const startTypingAnimation = async (text: string, isNonIntelligent = false) => {
  // Create the message first with empty text
  const messageId = currentMessageId.value;
  messages.value.push({
    id: messageId,
    text: '',
    sender: 'bot',
    timestamp: new Date()
  });

  isShowingTypingAnimation.value = false;
  fullMessageText.value = text;
  currentTypingMessage.value = '';
  isTyping.value = false;
  
  await scrollToBottom();
  
  const words = text.match(wordSplitRegex) || [];
  const actualChunkSize = isNonIntelligent ? chunkSize * 2 : chunkSize;
  const actualSpeed = isNonIntelligent ? typingSpeed / 2 : typingSpeed;
  
  // Find the message index
  const messageIndex = messages.value.findIndex(m => m.id === messageId);
  if (messageIndex === -1) return;

  for (let i = 0; i < words.length; i += actualChunkSize) {
    const chunk = words.slice(i, i + actualChunkSize).join('');
    // Update the message text directly
    messages.value[messageIndex].text += chunk;
    await new Promise(resolve => setTimeout(resolve, actualSpeed * chunk.length));
    await scrollToBottom();
  }
  
  await scrollToBottom();
};

// New function to handle non-intelligent answers
const handleNonIntelligentAnswer = (answer: string) => {
  // Clear any current animations
  isShowingTypingAnimation.value = false;
  currentTypingMessage.value = '';
  
  // Hide thinking indicators
  isTyping.value = false;
  isThinking.value = false;
  emit('update:thinking', false);
  
  // Wait just 1 second for natural conversation pacing
  setTimeout(() => {
    // Start typing animation for the non-intelligent answer, with flag for faster typing
    startTypingAnimation(answer, true);
  }, 1000);
};

// Function to show intelligence for a specific message
const showIntelligence = (messageId: string) => {
  // Get the agent response from our map
  const agentResponse = responsesMap.value.get(messageId);
  
  if (!agentResponse) {
    console.error('No agent response found for message ID:', messageId);
    return;
  }
  
  // Set the selected agent response
  selectedAgentResponse.value = agentResponse;
  
  // Reset all panels first to ensure clean state
  emit('update:thinking', false);
  
  // Short delay to ensure UI resets before showing the intelligence
  setTimeout(() => {
    // Show the intelligence panel with the stored data - immediately displayed
    emit('update:thinking', true);
    
    // Send all data at once for immediate display
    emit('update:agents', agentResponse.tools);
    emit('update:final-answer', agentResponse.final_answer);
    emit('update:final-informations', agentResponse.final_informations || []);
  }, 50);
};

const scrollToBottom = async () => {
  await nextTick();
  if (messagesContainer.value) {
    // Force layout recalculation to ensure proper scrolling
    const height = messagesContainer.value.scrollHeight;
    messagesContainer.value.scrollTop = height;
  }
};

const formatTime = (date: Date) => {
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
};

const formatMessage = (text: string): string => {
  // Make sure we're using marked to parse the markdown
  return marked.parse(text) as string;
};

// Auto-scroll when new messages arrive
watch(messages, () => {
  scrollToBottom();
}, { deep: true });

onMounted(() => {
  scrollToBottom();
});

// Add method to get current use case
const getCurrentUseCase = () => useCase.value;

// Expose both methods
defineExpose({
  handleUseCaseChange,
  getCurrentUseCase
});

// Add event handler for use case changes
const onUseCaseChange = (useCase: string) => {
  handleUseCaseChange(useCase);
};
</script>

<style scoped>
.chat-container {
  display: flex;
  flex-direction: column;
  height: 70vh; /* Use viewport height to be more responsive */
  min-height: 500px; /* Set minimum height */
  max-height: 900px; /* Set maximum height */
  background-color: #f9f9f9;
  border-radius: 8px;
  overflow: hidden;
}

.messages-container {
  flex: 1;
  overflow-y: scroll; /* Force scrollbar to be visible */
  padding: 16px;
  scroll-behavior: smooth;
  background-color: #f9f9f9;
  scrollbar-width: thin;
  scrollbar-color: rgba(0, 0, 0, 0.3) transparent;
}

/* Customizing scrollbar for WebKit browsers (Chrome, Safari, Edge) */
.messages-container::-webkit-scrollbar {
  width: 8px; /* Slightly wider for better visibility */
  background-color: rgba(0, 0, 0, 0.05);
}

.messages-container::-webkit-scrollbar-track {
  background: rgba(0, 0, 0, 0.05);
  border-radius: 4px;
}

.messages-container::-webkit-scrollbar-thumb {
  background-color: rgba(0, 0, 0, 0.3);
  border-radius: 4px;
  min-height: 40px; /* Ensure thumb is always visible */
}

/* For WebKit browsers: Ensure scrollbar is always visible */
.messages-container::-webkit-scrollbar-track-piece {
  background-color: rgba(0, 0, 0, 0.05);
}

.message {
  margin-bottom: 16px;
  max-width: 80%;
  animation: fadeIn 0.3s ease-in-out;
}

.user-message {
  margin-left: auto;
  margin-right: 0;
  text-align: right;
}

.bot-message {
  margin-right: auto;
  margin-left: 0;
  text-align: left;
}

.message-content {
  display: flex;
  align-items: flex-start;
}

.user-message .message-content {
  flex-direction: row-reverse;
}

.message-avatar {
  margin-right: 8px;
  flex-shrink: 0;
}

.user-message .message-avatar {
  margin-right: 0;
  margin-left: 8px;
}

.message-bubble {
  background-color: #fff;
  border-radius: 18px;
  padding: 12px 16px;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
}

.user-message .message-bubble {
  background-color: #4f6df5;
  color: white;
}

.message-time {
  font-size: 0.7rem;
  color: #888;
  margin-top: 4px;
  margin-left: 40px;
}

.user-message .message-time {
  margin-left: 0;
  margin-right: 40px;
  text-align: right;
}

.chat-input {
  padding: 16px;
  background-color: white;
  border-top: 1px solid #eee;
  flex-shrink: 0; /* Prevent shrinking */
  z-index: 1; /* Ensure it stays above other content */
}

.typing-indicator {
  opacity: 0.7;
}

/* Chat bot specific overrides for loading dots */
.message-bubble .loading-dots {
  height: 20px;
}

.message-bubble .loading-dots span {
  background-color: #888;
  margin-right: 4px;
  animation: thinkingAnimation 1.4s infinite;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* Cursor animation */
.typing-cursor {
  display: inline-block;
  width: 2px;
  height: 18px;
  background-color: currentColor;
  margin-left: 2px;
  vertical-align: middle;
  animation: blinkCursor 0.8s infinite;
}

@keyframes blinkCursor {
  0%, 100% { opacity: 1; }
  50% { opacity: 0; }
}

/* Make sure markdown formatting works properly */
.message-bubble :deep(p) {
  margin: 0;
}

.message-bubble :deep(pre) {
  background-color: rgba(0, 0, 0, 0.05);
  padding: 8px;
  border-radius: 4px;
  overflow-x: auto;
  margin: 8px 0;
}

.user-message .message-bubble :deep(pre) {
  background-color: rgba(255, 255, 255, 0.2);
}

.message-bubble :deep(code) {
  font-family: monospace;
}

.message-bubble :deep(ul), .message-bubble :deep(ol) {
  margin: 8px 0;
  padding-left: 24px;
}

.typing-animation .message-bubble {
  position: relative;
  min-height: 20px; /* Ensure height is consistent during typing */
  min-width: 60px; /* Ensure there's a minimum width during typing */
}

/* Add these styles to properly position the intelligence button */
.message-with-intelligence {
  position: relative;
}

.message-with-intelligence .message-text {
  display: block;
}

/* Add padding-bottom only when the intelligence button exists */
.message-with-intelligence:has(.intelligence-button) {
  padding-bottom: 28px;
}

.intelligence-button {
  position: absolute;
  bottom: -8px;
  right: -8px;
  color: #666;
  background-color: #f0f4ff;
  transition: all 0.2s;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 4px 10px;
  border-radius: 12px;
  border: 1px solid #e0e8ff;
  cursor: pointer;
  font-size: 0.75rem;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
}

.intelligence-button:hover {
  color: #4f6df5;
  background-color: #e5ebff;
  border-color: #d0dcff;
  box-shadow: 0 2px 4px rgba(79, 109, 245, 0.1);
}

.message-text {
  display: inline-block;
}

.intelligence-icon {
  width: 14px;
  height: 14px;
  margin-right: 4px;
}

.intelligence-text {
  font-weight: 500;
}

.message-text :deep(p) {
  margin: 0;
}

/* Add these styles to the existing <style scoped> section */
.predefined-questions {
  border-top: 1px solid rgba(0, 0, 0, 0.06);
  padding-top: 8px;
}

.predefined-questions .v-chip {
  cursor: pointer;
  transition: all 0.2s ease;
  max-width: 100%;
  text-overflow: ellipsis;
  overflow: hidden;
  white-space: nowrap;
}

.predefined-questions .v-chip:hover {
  background-color: rgba(79, 109, 245, 0.1);
  transform: translateY(-1px);
}

@media (max-width: 600px) {
  .predefined-questions .d-flex {
    flex-direction: column;
    align-items: stretch;
  }
  
  .predefined-questions .v-chip {
    width: 100%;
    margin-right: 0 !important;
  }
}

.message-bubble :deep(table) {
  border-collapse: collapse;
  width: 100%;
  margin: 8px 0;
  background-color: #fff;
}

.message-bubble :deep(th), .message-bubble :deep(td) {
  border: 1px solid #eee;
  padding: 8px 12px;
  text-align: left;
}

.message-bubble :deep(th) {
  background-color: #f5f5f5;
  font-weight: 500;
}

.message-bubble :deep(tr:nth-child(even)) {
  background-color: #fafafa;
}

.user-message .message-bubble :deep(table) {
  color: white;
  background-color: transparent;
}

.user-message .message-bubble :deep(th), .user-message .message-bubble :deep(td) {
  border-color: rgba(255, 255, 255, 0.2);
}

.user-message .message-bubble :deep(th) {
  background-color: rgba(255, 255, 255, 0.1);
}

.user-message .message-bubble :deep(tr:nth-child(even)) {
  background-color: rgba(255, 255, 255, 0.05);
}
</style> 