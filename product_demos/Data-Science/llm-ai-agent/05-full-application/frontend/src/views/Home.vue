<template>
  <v-container fluid class="fill-height pa-0 pa-sm-4">
    <v-row class="fill-height ma-0">
      <v-col cols="12" lg="8" class="fill-height pa-0 pa-sm-2">
        <v-card class="fill-height d-flex flex-column" elevation="2">
          <v-card-title class="pb-0 pt-4 px-4 d-flex align-center justify-space-between">
            <div>
              <h1 class="text-h5">AI Chat Assistant</h1>
              <p class="text-body-2 text-medium-emphasis mt-1">
                Ask me anything and I'll do my best to help you.
              </p>
            </div>
            <div class="intelligence-toggle">
              <v-tooltip
                location="bottom"
                text="Toggle AI intelligence visualization"
              >
                <template v-slot:activator="{ props }">
                  <div class="d-flex align-center">
                    <span class="mr-2">Intelligence:</span>
                    <v-switch
                      v-bind="props"
                      v-model="intelligenceEnabled"
                      color="success"
                      hide-details
                      density="compact"
                      inset
                    ></v-switch>
                    <span 
                      class="ml-1 text-caption" 
                      :style="{ 
                        color: intelligenceEnabled ? '#2e7d32' : '#757575', 
                        fontWeight: 500
                      }"
                    >
                      {{ intelligenceEnabled ? 'On' : 'Off' }}
                    </span>
                  </div>
                </template>
              </v-tooltip>
            </div>
          </v-card-title>
          
          <v-card-text class="flex-grow-1 d-flex flex-column pa-0 pa-sm-4">
            <ChatBot 
              ref="chatBot"
              class="fill-height" 
              :thinking="isThinking"
              :agents="agentTools"
              :finalAnswer="finalAnswer"
              :finalInformations="finalInformations"
              :intelligenceEnabled="intelligenceEnabled"
              @update:thinking="handleThinkingUpdate"
              @update:agents="handleAgentsUpdate"
              @update:final-answer="handleFinalAnswerUpdate"
              @update:final-informations="handleFinalInformationsUpdate"
              @use-case-change="handleUseCaseChange"
            />
          </v-card-text>
        </v-card>
      </v-col>
      
      <v-col cols="12" lg="4" class="fill-height pa-0 pa-sm-2" :class="{ 'd-none': !intelligenceEnabled, 'd-lg-block': intelligenceEnabled }">
        <IntelligentPanel 
          :show-thinking="isThinking"
          :tools="agentTools"
          :final-answer="finalAnswer"
          :final-informations="finalInformations"
        />
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import ChatBot from '@/components/ChatBot.vue'
import IntelligentPanel from '@/components/IntelligentPanel.vue'
import type { ToolCall } from '@/types/AgentResponse'

// Define emits for component communication
const emit = defineEmits(['update:title'])

const chatBot = ref<InstanceType<typeof ChatBot> | null>(null);
const isThinking = ref(false);
const agentTools = ref<ToolCall[]>([]);
const finalAnswer = ref('');
const finalInformations = ref<string[]>([]);
const intelligenceEnabled = ref(true); // Default state for the toggle

const handleThinkingUpdate = (value: boolean) => {
  isThinking.value = value;
  
  // When thinking stops, reset the agent data after a delay
  if (!value) {
    setTimeout(() => {
      if (!isThinking.value) {
        agentTools.value = [];
        finalAnswer.value = '';
        finalInformations.value = [];
      }
    }, 5000);
  }
};

// Watch for changes to the intelligence toggle
watch(intelligenceEnabled, (newValue) => {
  console.log(`Intelligence panel ${newValue ? 'enabled' : 'disabled'}`);
  
  // Reset the thinking state when toggling intelligence mode
  isThinking.value = false;
  
  // Clear the previous agents and responses when toggling
  agentTools.value = [];
  finalAnswer.value = '';
  finalInformations.value = [];
  
  // This would be a good place to add a snackbar notification in the future
});

const handleAgentsUpdate = (tools: ToolCall[]) => {
  agentTools.value = tools;
};

const handleFinalAnswerUpdate = (answer: string) => {
  finalAnswer.value = answer;
};

const handleFinalInformationsUpdate = (informations: string[]) => {
  finalInformations.value = informations;
};

// Handle use case changes from Header
const handleUseCaseChange = (useCase: string) => {
  if (chatBot.value) {
    chatBot.value.handleUseCaseChange(useCase);
  }
};

onMounted(() => {
  // Emit title update to App.vue
  emit('update:title', 'AI Chat Assistant')
})
</script>

<style scoped>
.fill-height {
  height: 100%;
}

.intelligence-toggle {
  flex-shrink: 0;
  margin-left: 16px;
  border-radius: 8px;
}

@media (max-width: 600px) {
  .v-card-title {
    flex-direction: column;
    align-items: flex-start;
  }
  
  .intelligence-toggle {
    margin-left: 0;
    margin-top: 12px;
    align-self: flex-end;
  }
}
</style> 