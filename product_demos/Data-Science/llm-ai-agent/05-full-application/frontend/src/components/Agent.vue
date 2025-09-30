<template>
  <div class="agent-card" :class="typeColorClass">
    <div class="agent-header">
      <v-avatar size="30" :color="typeColor" class="mr-2">
        <v-icon v-if="isFinalStep" size="small" color="white">mdi-check-circle</v-icon>
        <v-icon v-else-if="type === 'DATABASE'" size="small" color="white">mdi-database</v-icon>
        <v-icon v-else-if="type === 'KNOWLEDGE_BASE'" size="small" color="white">mdi-book-open-variant</v-icon>
        <v-icon v-else-if="type === 'REASONING'" size="small" color="white">mdi-brain</v-icon>
        <v-icon v-else-if="type === 'WEB_SEARCH'" size="small" color="white">mdi-web</v-icon>
        <span v-else class="text-caption text-white">{{ agentNumber }}</span>
      </v-avatar>
      <div class="agent-title">
        {{ isFinalStep ? "I have everything I need, let's answer the user" : 
          `${description || content.split('.')[0]} (tool: ${title})` }}
      </div>
    </div>
    
    <div class="agent-content">
      <p v-if="!isFinalStep">{{ content }}</p>
    </div>
    
    <div v-if="informations && informations.length > 0" class="agent-informations">
      <div 
        v-for="(info, infoIndex) in informations" 
        :key="infoIndex" 
        class="information-block"
      >
        <div class="information-header">
          <v-avatar size="22" color="#76c166">
            <v-icon size="x-small" color="white">mdi-information</v-icon>
          </v-avatar>
        </div>
        <div 
          class="information-content"
          v-html="formatMarkdown(info)"
        ></div>
      </div>
    </div>
    
    <div v-if="!isFinalStep && showLinks && links && links.length > 0" class="agent-links">
      <a 
        v-for="(link, index) in links" 
        :key="index" 
        :href="link.url" 
        target="_blank" 
        class="agent-link"
      >
        {{ link.text }}
      </a>
    </div>
    
    <!-- Arrow will appear on all cards when not showing loading indicator -->
    <div v-if="showArrow && !(isLastVisibleAgent && showLoading) && !isFinalStep" class="agent-arrow"></div>
    
    <!-- Show loading indicator if this is the last visible agent and showLoading is true -->
    <div v-if="isLastVisibleAgent && showLoading" class="agent-loading-indicator">
      <div class="loading-dots">
        <span></span>
        <span></span>
        <span></span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { marked } from 'marked';

interface Link {
  text: string;
  url: string;
}

interface Props {
  agentNumber: number;
  title: string;
  content: string;
  description?: string;
  type?: 'DATABASE' | 'KNOWLEDGE_BASE' | 'REASONING' | 'WEB_SEARCH' | 'FINAL_STEP';
  links?: Link[];
  showLinks?: boolean;
  isFinalStep?: boolean;
  informations?: string[];
  showArrow?: boolean;
  isLastVisibleAgent?: boolean;
  showLoading?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  showLinks: true,
  links: () => [],
  type: 'DATABASE',
  isFinalStep: false,
  informations: () => [],
  description: '',
  showArrow: true,
  isLastVisibleAgent: false,
  showLoading: false
});

const typeColor = computed(() => {
  if (props.isFinalStep) return '#2196F3'; // blue for final step
  return '#76c166'; // consistent green for all agents
});

const typeColorClass = computed(() => {
  if (props.isFinalStep) return 'agent-card-final';
  return 'agent-card-standard'; // consistent class for all agents
});

const formatMarkdown = (text: string): string => {
  return marked.parse(text) as string;
};
</script>

<style scoped>
.agent-card {
  background-color: #fff;
  border-radius: 8px;
  padding: 14px;
  margin-bottom: 30px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  position: relative;
}

/* Use a separate div for the arrow instead of pseudo-element */
.agent-arrow {
  position: absolute;
  bottom: -30px;
  left: 50%;
  transform: translateX(-50%);
  width: 24px;
  height: 28px;
  background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="28" viewBox="0 0 24 28" fill="none" stroke="%23AAAAAA" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="2" x2="12" y2="22"></line><polyline points="19 15 12 22 5 15"></polyline></svg>');
  background-position: center;
  background-repeat: no-repeat;
  z-index: 5;
}

.agent-card-standard {
  /* No specific styling needed */
}

.agent-card-final {
  background: linear-gradient(135deg, #f0f7ff 0%, #e6f0ff 100%);
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(33, 150, 243, 0.15);
  margin-top: 8px;
}

.agent-header {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
}

.agent-title {
  font-weight: 500;
  color: #333;
}

.agent-content {
  font-size: 0.9rem;
  color: #333;
  margin-bottom: 12px;
}

.agent-final-message {
  font-weight: 500;
  font-size: 1rem;
  color: #1a73e8;
  text-align: left;
  display: flex;
  align-items: center;
}

.agent-informations {
  margin-top: 12px;
}

.information-block {
  background-color: #edf7ec;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 12px;
  border-left: 4px solid #76c166;
  margin-top: 12px;
  display: flex;
  align-items: flex-start;
}

.information-header {
  margin-right: 10px;
  flex-shrink: 0;
}

.information-content {
  font-size: 0.9rem;
  color: #333;
  line-height: 1.5;
  flex-grow: 1;
}

.information-content :deep(a) {
  color: #056df1;
  text-decoration: none;
  font-weight: 500;
  display: block;
  margin-top: 8px;
  position: relative;
  padding-left: 0;
  transition: all 0.2s ease;
}

.information-content :deep(a:hover) {
  text-decoration: underline;
  color: #0747a6;
}

.agent-content p {
  margin-top: 0;
  margin-bottom: 8px;
}

.agent-links {
  margin-top: 12px;
}

.agent-link {
  display: block;
  color: #3a66db;
  font-size: 0.85rem;
  text-decoration: none;
  margin-bottom: 4px;
}

.agent-link:hover {
  text-decoration: underline;
}

/* Loading indicator styling */
.agent-loading-indicator {
  position: absolute;
  bottom: -25px;
  left: 0;
  right: 0;
  display: flex;
  justify-content: center;
  z-index: 25; /* Ensure it's above arrows */
}

/* Agent-specific styling for loading dots */
.agent-loading-indicator .loading-dots {
  background-color: white;
  padding: 3px 8px;
  border-radius: 12px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}
</style> 