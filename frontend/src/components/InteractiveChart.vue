<script setup>
import { BarChart, LineChart, PieChart } from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import axios from 'axios'
import { computed, ref, watch } from 'vue'

use([
  CanvasRenderer,
  BarChart,
  LineChart,
  PieChart,
  GridComponent,
  LegendComponent,
  TooltipComponent,
])

const props = defineProps({
  filters: {
    type: Object,
    default: () => ({}),
  },
  chartType: {
    type: String,
    default: 'bar',
  },
})

const emit = defineEmits(['data-loaded', 'error', 'loading-change'])

const apiBaseUrl = import.meta.env.VITE_API_URL || '/api/analytics'
const rows = ref([])
const loading = ref(false)
const errorMessage = ref('')

const formatAmount = (value) =>
  new Intl.NumberFormat('ru-RU', {
    maximumFractionDigits: 2,
  }).format(Number(value || 0))

const compactName = (row) => row.object_name || row.kcsr_code || 'Без объекта'

const queryParams = computed(() => {
  const params = {}
  for (const [key, value] of Object.entries(props.filters || {})) {
    if (value !== null && value !== undefined && String(value).trim() !== '') {
      params[key] = value
    }
  }
  return params
})

const loadSummary = async () => {
  loading.value = true
  errorMessage.value = ''
  emit('loading-change', true)

  try {
    const response = await axios.get(`${apiBaseUrl}/summary`, {
      params: queryParams.value,
    })
    rows.value = Array.isArray(response.data) ? response.data : []
    emit('data-loaded', rows.value)
  } catch (error) {
    rows.value = []
    errorMessage.value = 'Не удалось загрузить данные'
    emit('data-loaded', [])
    emit('error', error)
  } finally {
    loading.value = false
    emit('loading-change', false)
  }
}

watch(
  () => ({ ...queryParams.value }),
  loadSummary,
  { immediate: true },
)

const chartOption = computed(() => {
  if (props.chartType === 'pie') {
    const totals = new Map()
    for (const row of rows.value) {
      const key = row.indicator_type || 'unknown'
      totals.set(key, (totals.get(key) || 0) + Number(row.amount || 0))
    }

    return {
      tooltip: {
        trigger: 'item',
        valueFormatter: formatAmount,
      },
      legend: {
        type: 'scroll',
        bottom: 0,
      },
      series: [
        {
          name: 'Сумма',
          type: 'pie',
          radius: ['38%', '68%'],
          center: ['50%', '45%'],
          avoidLabelOverlap: true,
          data: Array.from(totals, ([name, value]) => ({ name, value })),
        },
      ],
    }
  }

  const categorySet = new Set()
  const indicatorSet = new Set()
  const matrix = new Map()

  for (const row of rows.value) {
    const category = compactName(row)
    const indicator = row.indicator_type || 'unknown'
    const value = Number(row.amount || 0)
    categorySet.add(category)
    indicatorSet.add(indicator)
    matrix.set(`${indicator}\u0000${category}`, (matrix.get(`${indicator}\u0000${category}`) || 0) + value)
  }

  const categories = Array.from(categorySet)
  const indicators = Array.from(indicatorSet)

  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: props.chartType === 'bar' ? 'shadow' : 'line',
      },
      valueFormatter: formatAmount,
    },
    legend: {
      type: 'scroll',
      top: 0,
    },
    grid: {
      top: 56,
      right: 24,
      bottom: 96,
      left: 80,
    },
    xAxis: {
      type: 'category',
      data: categories,
      axisLabel: {
        hideOverlap: true,
        interval: 0,
        width: 120,
        overflow: 'truncate',
      },
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        formatter: (value) => formatAmount(value),
      },
    },
    series: indicators.map((indicator) => ({
      name: indicator,
      type: props.chartType,
      smooth: props.chartType === 'line',
      data: categories.map((category) => matrix.get(`${indicator}\u0000${category}`) || 0),
    })),
  }
})
</script>

<template>
  <div class="chart-shell">
    <div v-if="loading" class="chart-state">Загрузка...</div>
    <div v-else-if="errorMessage" class="chart-state chart-state-error">
      {{ errorMessage }}
    </div>
    <div v-else-if="rows.length === 0" class="chart-state">Нет данных</div>
    <VChart v-else class="chart" :option="chartOption" autoresize />
  </div>
</template>
