<template>
  <v-app>
    <Header 
      :title="appTitle"
      @refresh="handleRefresh"
      @use-case-change="handleUseCaseChange"
    />
    
    <v-main>
      <router-view 
        ref="homeView"
        @update:title="updateTitle"
      />
    </v-main>
  </v-app>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRoute } from 'vue-router'
import Header from '@/components/Header.vue'
import type { Component } from 'vue'

const route = useRoute()
const appTitle = ref('AI Chat Assistant')
const homeView = ref<Component | null>(null)

// Update app title
const updateTitle = (title: string) => {
  appTitle.value = title
}

// Handle refresh
const handleRefresh = () => {
  window.location.reload()
}

// Handle use case changes
const handleUseCaseChange = (useCase: string) => {
  // Pass the use case change to the Home component
  if (homeView.value && 'handleUseCaseChange' in homeView.value) {
    (homeView.value as any).handleUseCaseChange(useCase);
  }
}
</script>

<style>
:root {
  font-family: -apple-system, BlinkMacSystemFont, sans-serif;
}

html, body {
  height: 100%;
}

.v-application {
  background-color: #f5f7fa;
}
</style> 