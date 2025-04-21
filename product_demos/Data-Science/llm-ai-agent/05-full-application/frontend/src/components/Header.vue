<template>
  <v-app-bar 
    elevation="1"
    color="secondary"
  >
    <div class="px-4 d-flex align-center justify-space-between w-100">
      <!-- Left section - back to align-center for vertical centering -->
      <div class="d-flex align-center">
        <!-- Home button -->
        <v-btn 
          icon
          variant="text"
          color="white"
          class="mr-2"
          to="/"
          title="Go to home"
        >
          <v-icon>mdi-home</v-icon>
        </v-btn>
        
        <!-- Databricks Logo -->
        <img 
          src="@/assets/databricks-logo.svg" 
          alt="Databricks Logo" 
          class="ml-2 mr-4" 
          height="20"
        />
        
        <!-- Title and subtitle wrapper -->
        <div class="d-flex align-baseline">
          <h1 class="text-h6 font-weight-medium text-white mb-0 mr-3">
            {{ title || "AI Chat Assistant" }}
          </h1>
          
          <span class="text-caption text-white text-opacity-80">
            Powered by Databricks Genai Agents
          </span>
        </div>
      </div>

      <!-- Right section with use case selector and admin button -->
      <div class="d-flex align-center">
        <v-select
          v-model="selectedUseCase"
          :items="useCases"
          item-title="label"
          item-value="value"
          label="Select your use-case"
          variant="outlined"
          density="compact"
          hide-details
          class="use-case-select mr-6"
          @update:model-value="handleUseCaseChange"
        ></v-select>
        
        <div class="admin-dashboard-divider"></div>
        
        <!-- Add Admin Dashboard button -->
        <v-btn
          color="white"
          variant="flat"
          class="admin-dashboard-btn ml-6"
          href="https://e2-demo-field-eng.cloud.databricks.com/dashboardsv3/01f0187a33881d0db7fea91f596f8bda/published/pages/468b8ea5?o=1444828305810485"
          target="_blank"
          elevation="0"
        >
          <v-icon icon="mdi-view-dashboard" class="mr-2" />
          Admin Dashboard (AI/BI)
        </v-btn>
      </div>
    </div>
  </v-app-bar>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()

interface Props {
  title?: string;
}

const props = defineProps<Props>()

const emit = defineEmits<{
  'refresh': [],
  'use-case-change': [useCase: string]
}>()

const useCases = [
  { label: 'Telco Subscription', value: 'telco' },
  { label: 'Healthcare Member Support', value: 'hls' },
  { label: 'Financial Services', value: 'fins' },
  { label: 'Manufacturing Support', value: 'mfg' },
  { label: 'Retail Order Support', value: 'retail' }
]

const selectedUseCase = ref('telco')

// Emit initial value on mount
onMounted(() => {
  emit('use-case-change', selectedUseCase.value)
})

const handleUseCaseChange = (value: string) => {
  emit('use-case-change', value)
}
</script>

<style scoped>
.use-case-select {
  width: 300px;
}

.admin-dashboard-divider {
  width: 1px;
  height: 24px;
  background-color: rgba(255, 255, 255, 0.2);
}

.admin-dashboard-btn {
  font-weight: 500;
  text-transform: none;
  letter-spacing: 0;
  border: 1px solid rgba(255, 255, 255, 0.2);
  transition: all 0.2s ease;
  padding: 0 20px;
  height: 36px;
}

.admin-dashboard-btn:hover {
  background-color: rgba(255, 255, 255, 0.1) !important;
  border-color: rgba(255, 255, 255, 0.3);
}
</style> 