import './fonts.css'
import './framework/styles.css'
import Assembly from './framework/assembly'
import { Bridge } from './framework/types/modules'
import LiveBridge from './live_bridge'
import FakeBridge from './fake_bridge'

const rootElement = document.getElementById('root') as HTMLElement

const workerFile = new URL('./framework/processing/py_worker.js', import.meta.url)
const worker = new Worker(workerFile)

let assembly: Assembly

const run = (bridge: Bridge, locale: string): void => {
  // Extract platform from URL query params or environment variable
  // URL params take precedence: ?platform=facebook
  // Environment variable fallback: REACT_APP_RELEASE_PLATFORM (for npm scripts)
  const params = new URLSearchParams(window.location.search)
  const releasePlatform = params.get('platform') || process.env.REACT_APP_RELEASE_PLATFORM || 'all'
  
  assembly = new Assembly(worker, bridge, releasePlatform)
  assembly.visualisationEngine.start(rootElement, locale)
  assembly.processingEngine.start()
}

if (process.env.REACT_APP_BUILD !== 'standalone' && process.env.NODE_ENV === 'production') {
  // Setup embedded mode (requires to be embedded in iFrame)
  console.log('Initializing bridge system')
  LiveBridge.create(window, run)
} else {
  // Setup local development mode
  console.log('Running with fake bridge')
  const [langPart] = navigator.language.split('-')
  const browserLang = langPart !== '' ? langPart : 'en'
  run(new FakeBridge(), browserLang)
}

const observer = new ResizeObserver(() => {
  const height = window.document.body.scrollHeight
  const action = 'resize'
  window.parent.postMessage({ action, height }, '*')
})

observer.observe(window.document.body)
