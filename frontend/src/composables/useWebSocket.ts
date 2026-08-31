import { onUnmounted, ref } from 'vue'
import { useAuthStore } from '@/stores/auth'
import router from '@/router'

export interface StatusMessage {
  type: 'status'
  dlna_running: boolean
  renderers_count: number
  speakers: {
    did: string
    dlna_name: string
    transport_state: string
    current_uri: string
    airplay_active: boolean
    spotify_paired: boolean
    spotify_playing: boolean
    spotify_track: string
    spotify_artist: string
  }[]
}

export interface LogMessage {
  type: 'log'
  line: string
}

/** 连接后端 WebSocket, 实时接收状态与日志 (token 通过查询参数传递) */
export function useWebSocket() {
  const connected = ref(false)
  const status = ref<StatusMessage | null>(null)
  const logLines = ref<string[]>([])
  let ws: WebSocket | null = null
  let retryTimer: number | null = null
  let disposed = false

  function connect() {
    const auth = useAuthStore()
    if (!auth.token || disposed) return

    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    ws = new WebSocket(`${proto}://${location.host}/api/v1/ws?token=${auth.token}`)

    ws.onopen = () => {
      connected.value = true
      // 每次连接后端都会重放最近日志, 清空旧数据避免重连后重复堆积
      logLines.value.length = 0
    }
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data)
        if (msg.type === 'status') {
          status.value = msg
        } else if (msg.type === 'log') {
          logLines.value.push(msg.line)
          if (logLines.value.length > 500) logLines.value.shift()
        }
      } catch {
        /* 忽略非法消息 */
      }
    }
    ws.onclose = (ev) => {
      connected.value = false
      if (disposed) return
      // 4401: token 无效/过期, 不再重连, 清登录态回登录页 (避免无限重连被拒)
      if (ev.code === 4401) {
        const auth = useAuthStore()
        auth.logout()
        if (router.currentRoute.value.name !== 'login') {
          router.push({ name: 'login' })
        }
        return
      }
      // 其它原因断线自动重连
      retryTimer = window.setTimeout(connect, 3000)
    }
    ws.onerror = () => ws?.close()
  }

  function disconnect() {
    disposed = true
    if (retryTimer) clearTimeout(retryTimer)
    ws?.close()
    ws = null
  }

  connect()
  onUnmounted(disconnect)

  return { connected, status, logLines }
}
