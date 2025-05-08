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
        
        <!-- Databricks Logo with click counter for secret button -->
        <img 
          src="@/assets/databricks-logo.svg" 
          alt="Databricks Logo" 
          class="ml-2 mr-4 clickable-logo" 
          height="20"
          @click="handleLogoClick"
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
        
        <!-- Toggle SAP mode button (only visible when logo clicked 5 times) -->
        <v-btn
          v-if="showSapToggle"
          color="secondary"
          variant="outlined"
          size="small"
          class="ml-3"
          @click="toggleSapUseCase"
          title="Toggle SAP mode"
        >
          <v-icon size="small" :color="sapModeEnabled ? 'success' : 'white'">
            {{ sapModeEnabled ? 'mdi-toggle-switch' : 'mdi-toggle-switch-off' }}
          </v-icon>
          <span class="ml-1 text-caption">SAP</span>
        </v-btn>
        
        <!-- SAP mode indicator -->
        <v-chip
          v-if="sapModeEnabled"
          color="success"
          size="small"
          class="ml-2"
          title="SAP mode is enabled"
        >
          SAP
        </v-chip>
      </div>
    </div>
    
    <!-- Toast notification for SAP mode toggle -->
    <v-snackbar
      v-model="showSapNotification"
      :timeout="2000"
      color="success"
      location="top"
    >
      {{ sapModeEnabled ? 'SAP mode enabled' : 'SAP mode disabled' }}
    </v-snackbar>
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

const useCases = ref([
  { label: 'Telco Subscription', value: 'telco' },
  { label: 'Healthcare Member Support', value: 'hls' },
  { label: 'Financial Services', value: 'fins' },
  { label: 'Manufacturing Support', value: 'mfg' },
  { label: 'Retail Order Support', value: 'retail' },
  { label: 'Pharmaceutical Support', value: 'pharma' }
])

const selectedUseCase = ref('telco')
const sapModeEnabled = ref(false)
const showSapNotification = ref(false)
const showSapToggle = ref(false)
const logoClickCount = ref(0)
const logoClickTimer = ref<number | null>(null)

// Emit initial value on mount
onMounted(() => {
  emit('use-case-change', selectedUseCase.value)
})

const handleLogoClick = () => {
  logoClickCount.value++
  
  // Reset click count after 2 seconds of inactivity
  if (logoClickTimer.value) {
    window.clearTimeout(logoClickTimer.value)
  }
  
  logoClickTimer.value = window.setTimeout(() => {
    logoClickCount.value = 0
  }, 2000)
  
  // After 5 clicks, show the SAP toggle button
  if (logoClickCount.value >= 5) {
    showSapToggle.value = true
    logoClickCount.value = 0
  }
}

const toggleSapUseCase = () => {
  sapModeEnabled.value = !sapModeEnabled.value
  showSapNotification.value = true
  
  if (sapModeEnabled.value) {
    // When SAP mode is enabled, if the user is in the financial services use case,
    // switch to the SAP use case
    if (selectedUseCase.value === 'fins') {
      selectedUseCase.value = 'sap'
      emit('use-case-change', selectedUseCase.value)
    }
    
    // Add SAP option to the dropdown if it's not already there
    if (!useCases.value.some(uc => uc.value === 'sap')) {
      useCases.value.push({ label: 'SAP Financial Services', value: 'sap' })
    }
  } else {
    // When SAP mode is disabled, switch back to the regular financial services use case
    const sapIndex = useCases.value.findIndex(uc => uc.value === 'sap')
    if (sapIndex !== -1) {
      useCases.value.splice(sapIndex, 1)
      
      // If currently selected usecase is SAP, reset to regular financial services
      if (selectedUseCase.value === 'sap') {
        selectedUseCase.value = 'fins'
        emit('use-case-change', selectedUseCase.value)
      }
    }
  }
}

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

.clickable-logo {
  cursor: pointer;
}
</style> 