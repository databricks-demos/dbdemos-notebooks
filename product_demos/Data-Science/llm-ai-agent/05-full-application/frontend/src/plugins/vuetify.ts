import 'vuetify/styles'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { aliases, mdi } from 'vuetify/iconsets/mdi'
import '@mdi/font/css/materialdesignicons.css'

export default createVuetify({
  components,
  directives,
  icons: {
    defaultSet: 'mdi',
    aliases,
    sets: {
      mdi,
    },
  },
  theme: {
    themes: {
      light: {
        colors: {
          primary: '#FF3621',
          secondary: '#1B3139',
          'primary-darken-1': '#FF5F46',
          'secondary-darken-1': '#0B2026',
          background: '#F9F7F4',
          surface: '#FFFFFF',
          success: '#00a972',
          'success-lighten-1': '#33ba8c',
          info: '#1b5162',
          divider: '#EEEDE9',
        },
      },
    },
    defaultTheme: 'light',
  },
}) 