<template>
  <div class="intelligent-panel">
    <div class="panel-header">
      <h2 class="text-h6">Databricks Intelligence</h2>
    </div>
    
    <div class="thinking-container" v-if="showThinking">
      <div class="agents-container">
        <!-- Thinking indicator while loading -->
        <div v-if="isThinking && !hasLoadedAnyAgents" class="thinking-indicator">
          <div class="loading-dots">
            <span></span>
            <span></span>
            <span></span>
          </div>
          <div class="thinking-text">Thinking...</div>
        </div>
        
        <!-- Agents and loading indicators -->
        <div>
          <transition-group name="fade" tag="div" class="agents-transition-group">
            <Agent
              v-for="(tool, index) in visibleTools"
              :key="`tool-${index}`"
              :agent-number="index + 1"
              :title="tool.tool_name"
              :description="tool.description"
              :content="tool.reasoning"
              :type="tool.type as any"
              :informations="tool.informations"
              :show-arrow="true"
              :is-last-visible-agent="index === visibleTools.length - 1"
              :show-loading="index === visibleTools.length - 1 && isLoadingNextAgent"
            />
            
            <!-- Final step block -->
            <Agent
              v-if="showFinalStep"
              key="final-step"
              :agent-number="tools.length + 1"
              title=""
              content=""
              :is-final-step="true"
              :informations="finalInformations"
              :show-arrow="false"
              :is-last-visible-agent="false"
              :show-loading="false"
            />
          </transition-group>
        </div>
      </div>
    </div>
    
    <div v-else class="panel-empty">
      <div class="empty-state">
        <v-icon size="48" color="grey-lighten-1" class="mb-4">mdi-brain</v-icon>
        <p class="text-center text-subtitle-1">
          Ask a question to see the AI reasoning process
        </p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, nextTick } from 'vue';
import Agent from './Agent.vue';
import { type ToolCall } from '@/types/AgentResponse';

interface Props {
  showThinking?: boolean;
  tools?: ToolCall[];
  finalAnswer?: string;
  finalInformations?: string[];
}

const props = withDefaults(defineProps<Props>(), {
  showThinking: false,
  tools: () => [],
  finalAnswer: '',
  finalInformations: () => []
});

// Track which tools are visible with staggered animation
const visibleTools = ref<ToolCall[]>([]);
const showFinalStep = ref(false);
const isThinking = ref(true);
const hasLoadedAnyAgents = computed(() => visibleTools.value.length > 0 || showFinalStep.value);
// Add an explicit flag to track when we're waiting for the next agent
const isWaitingForNextAgent = ref(false);

// Add a computed property to determine if we should show loading dots
const isLoadingNextAgent = computed(() => isWaitingForNextAgent.value && !showFinalStep.value);

// Add a ref to track if we're showing a previously generated response
const isShowingStoredResponse = ref(false);

// Detect when we're showing a stored response
const isStoredResponseView = computed(() => {
  // If we have both tools and final answer at once, it's a stored response
  return props.showThinking && props.tools.length > 0 && props.finalAnswer && props.finalAnswer.length > 0;
});

// Immediately show a stored response without any animation delay
const showStoredResponse = () => {
  // Clear any existing state first
  visibleTools.value = [];
  showFinalStep.value = false;
  
  // Use nextTick to ensure DOM has updated before showing stored response
  nextTick(() => {
    visibleTools.value = [...props.tools];
    showFinalStep.value = true;
    isThinking.value = false;
  });
};

// Watch for changes in the tools array
watch(() => props.tools, (newTools, oldTools) => {
  // Skip if not showing anything
  if (!props.showThinking) return;
  
  // Compare current and previous tool counts
  const oldLength = oldTools?.length || 0;
  const newLength = newTools.length;
  
  if (isStoredResponseView.value) {
    // Stored response - show all at once
    isShowingStoredResponse.value = true;
    isWaitingForNextAgent.value = false;
    showStoredResponse();
  } else if (newTools.length > 0) {
    // Streaming response - handling tool updates
    isShowingStoredResponse.value = false;
    
    // Show all tools that are currently available
    if (visibleTools.value.length < newTools.length) {
      visibleTools.value = [...newTools];
      isThinking.value = true;
      
      // Set waiting state if we just received a new tool
      if (newLength > oldLength) {
        isWaitingForNextAgent.value = true;
      }
    }
  } else {
    // Reset when tools are cleared
    visibleTools.value = [];
    showFinalStep.value = false;
    isThinking.value = true;
    isWaitingForNextAgent.value = false;
  }
}, { deep: true });

// Watch for changes in the final answer
watch(() => props.finalAnswer, (newAnswer) => {
  if (!props.showThinking) return;
  
  if (newAnswer && newAnswer.length > 0 && !isShowingStoredResponse.value) {
    // Final answer received - stop loading state but keep content visible
    showFinalStep.value = true;
    isThinking.value = false;
    isWaitingForNextAgent.value = false;
  }
});

// Watch for changes in the showThinking property
watch(() => props.showThinking, (isShowing) => {
  if (!isShowing) {
    // Don't reset when panel is hidden, keep the last state
    isWaitingForNextAgent.value = false;
  } else if (isStoredResponseView.value) {
    // Show stored response immediately
    isShowingStoredResponse.value = true;
    showStoredResponse();
    isWaitingForNextAgent.value = false;
  } else {
    // New thinking session started
    isWaitingForNextAgent.value = true;
  }
});

// Simplified debug logging
watch(isLoadingNextAgent, (isLoading) => {
  console.log(`Loading indicator state: ${isLoading ? 'SHOWING' : 'HIDDEN'}`);
});

// Debug state function
const debugState = () => {
  console.log({
    isThinking: isThinking.value,
    isLoadingNext: isLoadingNextAgent.value,
    visibleTools: visibleTools.value.length,
    totalTools: props.tools.length,
    isFinalStep: showFinalStep.value
  });
};

// Call debug on mounted and whenever visible tools change
watch(visibleTools, () => {
  debugState();
});
</script>

<style scoped>
.intelligent-panel {
  height: 100%;
  background-color: white;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
}

.panel-header {
  padding: 12px 16px;
  border-bottom: 1px solid #eee;
  flex-shrink: 0;
}

.thinking-container {
  flex: 1;
  padding: 14px 16px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  background-color: #fafafa;
}

.agents-container {
  display: flex;
  flex-direction: column;
  padding-bottom: 12px;
}

.panel-empty {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #888;
  background-color: #fafafa;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 32px;
  max-width: 300px;
  text-align: center;
}

/* Thinking indicator */
.thinking-indicator {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 32px 0;
}

.loading-dots {
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 8px;
}

.loading-dots span {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #2196F3;
  margin: 0 4px;
  opacity: 0.6;
  animation: thinkingAnimation 1.4s infinite;
}

.loading-dots span:nth-child(2) {
  animation-delay: 0.2s;
}

.loading-dots span:nth-child(3) {
  animation-delay: 0.4s;
}

.thinking-text {
  font-size: 0.9rem;
  color: #666;
  margin-top: 8px;
}

/* Override loading-dots animation for thinking indicator */
.thinking-indicator .loading-dots span {
  opacity: 0.6;
  animation: thinkingAnimation 1.4s infinite;
}

/* Transition group container */
.agents-transition-group {
  position: relative;
}

/* Ensure transitions are contained properly */
.fade-leave-to {
  position: absolute;
  width: 100%;
}
</style> 