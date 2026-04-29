<script setup>
import axios from 'axios'
import { computed, onMounted, reactive, ref } from 'vue'
import InteractiveChart from './components/InteractiveChart.vue'

const apiBaseUrl = import.meta.env.VITE_API_URL || '/api/analytics'

const filters = reactive({
  section: '',
  kcsr_code: '',
  indicator_type: '',
})

const chartType = ref('bar')
const summaryRows = ref([])
const sections = ref([])
const indicators = ref([])
const loading = ref(false)
const exporting = ref(false)
const apiError = ref('')

const chartTypes = [
  { value: 'bar', label: 'Bar' },
  { value: 'line', label: 'Line' },
  { value: 'pie', label: 'Pie' },
]

const totalAmount = computed(() =>
  summaryRows.value.reduce((sum, row) => sum + Number(row.amount || 0), 0),
)

const formattedTotal = computed(() =>
  new Intl.NumberFormat('ru-RU', {
    maximumFractionDigits: 2,
  }).format(totalAmount.value),
)

const loadDictionaries = async () => {
  try {
    const [sectionsResponse, indicatorsResponse] = await Promise.all([
      axios.get(`${apiBaseUrl}/sections`),
      axios.get(`${apiBaseUrl}/indicators`),
    ])
    sections.value = sectionsResponse.data || []
    indicators.value = indicatorsResponse.data || []
  } catch (error) {
    apiError.value = 'Не удалось загрузить фильтры'
  }
}

const clearFilters = () => {
  filters.section = ''
  filters.kcsr_code = ''
  filters.indicator_type = ''
}

const updateRows = (rows) => {
  summaryRows.value = rows
  apiError.value = ''
}

const handleChartError = () => {
  apiError.value = 'Не удалось загрузить сводку'
}

const exportReport = async () => {
  exporting.value = true
  apiError.value = ''

  try {
    const params = {}
    if (filters.section) {
      params.section = filters.section
    }
    if (filters.kcsr_code) {
      params.kcsr_code = filters.kcsr_code
    }

    const response = await axios.get(`${apiBaseUrl}/export`, {
      params,
      responseType: 'blob',
    })
    const blob = new Blob([response.data], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = 'nerpochka_analytics_export.xlsx'
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  } catch (error) {
    apiError.value = 'Не удалось сформировать выгрузку'
  } finally {
    exporting.value = false
  }
}

onMounted(loadDictionaries)
</script>

<template>
  <main class="app">
    <header class="page-header">
      <div>
        <p class="eyebrow">nerpochka</p>
        <h1>Бюджетная аналитика</h1>
      </div>
      <div class="metric">
        <span>Сумма</span>
        <strong>{{ formattedTotal }}</strong>
      </div>
    </header>

    <section class="toolbar">
      <label class="field">
        <span>Раздел</span>
        <select v-model="filters.section">
          <option value="">Все</option>
          <option
            v-for="item in sections"
            :key="item.section"
            :value="item.section"
          >
            {{ item.section }}
          </option>
        </select>
      </label>

      <label class="field">
        <span>КЦСР</span>
        <input
          v-model.trim="filters.kcsr_code"
          type="search"
          placeholder="Например, 6105"
        />
      </label>

      <label class="field">
        <span>Показатель</span>
        <select v-model="filters.indicator_type">
          <option value="">Все</option>
          <option
            v-for="item in indicators"
            :key="item.indicator_type"
            :value="item.indicator_type"
          >
            {{ item.indicator_type }}
          </option>
        </select>
      </label>

      <div class="chart-toggle" aria-label="Тип графика">
        <button
          v-for="type in chartTypes"
          :key="type.value"
          type="button"
          :class="{ active: chartType === type.value }"
          @click="chartType = type.value"
        >
          {{ type.label }}
        </button>
      </div>

      <button class="ghost-button" type="button" @click="clearFilters">
        Сбросить
      </button>

      <button
        class="primary-button"
        type="button"
        :disabled="exporting"
        @click="exportReport"
      >
        {{ exporting ? 'Формирование...' : 'Выгрузить XLSX' }}
      </button>
    </section>

    <p v-if="apiError" class="notice">{{ apiError }}</p>

    <section class="panel">
      <InteractiveChart
        :filters="filters"
        :chart-type="chartType"
        @data-loaded="updateRows"
        @loading-change="loading = $event"
        @error="handleChartError"
      />
    </section>

    <section class="table-section">
      <div class="table-header">
        <h2>Данные</h2>
        <span>{{ loading ? 'Обновление...' : `${summaryRows.length} строк` }}</span>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Раздел</th>
              <th>КЦСР</th>
              <th>Объект</th>
              <th>Показатель</th>
              <th class="number-cell">Сумма</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="summaryRows.length === 0">
              <td colspan="5" class="empty-row">Нет данных</td>
            </tr>
            <tr v-for="(row, index) in summaryRows" :key="`${row.kcsr_code}-${row.indicator_type}-${index}`">
              <td>{{ row.section || '-' }}</td>
              <td>{{ row.kcsr_code || '-' }}</td>
              <td>{{ row.object_name || '-' }}</td>
              <td>{{ row.indicator_type || '-' }}</td>
              <td class="number-cell">
                {{
                  new Intl.NumberFormat('ru-RU', {
                    maximumFractionDigits: 2,
                  }).format(Number(row.amount || 0))
                }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>
</template>
