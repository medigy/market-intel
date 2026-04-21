import { LitElement, html, css } from 'lit';
import { createRoot, Root } from 'react-dom/client';
import React, { useEffect, useRef, useState } from 'react';

// Your existing assistant-ui pieces
import { AssistantRuntimeProvider } from '@assistant-ui/react';
import { useChatRuntime, AssistantChatTransport } from '@assistant-ui/react-ai-sdk';
import { Thread } from '../components/assistant-ui/thread';
import { TooltipProvider } from '../components/ui/tooltip';
import { TooltipIconButton } from '../components/assistant-ui/tooltip-icon-button';
import { XIcon, MessageCircleIcon } from 'lucide-react';

// Vite ?inline trick: converts CSS into a string for shadow DOM injection
import indexStyles from '../index.css?inline';
import componentStyles from './ai-chat-styles.css?inline';

// ── Recreating AssistantModal Logic (Shadow-DOM Friendly) ───────────────────
const ShadowAssistantModal = () => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div style={{
      position: 'fixed',
      bottom: 0,
      right: 0,
      zIndex: 50000,
      display: 'flex',
      flexDirection: 'column-reverse',
      alignItems: 'flex-end',
      gap: '16px',
      pointerEvents: 'none',
    }}>
      {/* Modal Content */}
      {isOpen && (
        <div style={{
          marginRight: '16px',
          marginBottom: '8px',
          height: '700px',
          width: '400px',
          overflow: 'hidden',
          borderRadius: '1rem',
          border: '1px solid rgba(0,0,0,0.1)',
          background: '#ffffff',
          boxShadow: '0 25px 50px rgba(0,0,0,0.15)',
          display: 'flex',
          flexDirection: 'column',
          pointerEvents: 'auto',
          animation: 'fadeIn 0.2s ease',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', padding: '16px', borderBottom: '1px solid rgba(0,0,0,0.08)', background: 'rgba(255,255,255,0.8)', backdropFilter: 'blur(8px)', position: 'sticky', top: 0, zIndex: 10, flexShrink: 0 }}>
            <div style={{ width: 44, height: 44, borderRadius: '0.75rem', overflow: 'hidden', border: '1px solid rgba(0,0,0,0.1)', background: '#fff', padding: 4, flexShrink: 0 }}>
              <img src="https://qualityfolio.dev/favicon.png" alt="Logo" style={{ width: '100%', height: '100%', objectFit: 'contain', transform: 'scale(1.25)' }} />
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: '#111' }}>Ask AI</h3>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, paddingTop: 2 }}>
                <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', background: '#22c55e' }}></span>
                <span style={{ fontSize: 11, color: '#888', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>active</span>
              </div>
            </div>
            <button
              onClick={() => setIsOpen(false)}
              style={{ marginLeft: 'auto', background: 'none', border: 'none', cursor: 'pointer', padding: 8, borderRadius: 8, color: '#888', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
              title="Close"
            >
              <XIcon style={{ width: 18, height: 18 }} />
            </button>
          </div>
          <div style={{ flex: 1, overflow: 'hidden' }}>
            <Thread />
          </div>
        </div>
      )}

      {/* Trigger Tab — solid purple, white icon, no border */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        style={{
          position: 'relative',
          height: '58px',
          width: '72px',
          borderRadius: '1rem 0 0 1rem',
          background: '#7c3aed',
          border: 'none',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer',
          boxShadow: '-4px 4px 24px rgba(124,58,237,0.4)',
          transition: 'width 0.3s ease, background 0.2s ease',
          pointerEvents: 'auto',
        }}
        onMouseEnter={e => { e.currentTarget.style.width = '80px'; e.currentTarget.style.background = '#6d28d9'; }}
        onMouseLeave={e => { e.currentTarget.style.width = '72px'; e.currentTarget.style.background = '#7c3aed'; }}
      >
        <MessageCircleIcon style={{ width: 28, height: 28, color: '#ffffff' }} strokeWidth={2.5} />
      </button>
    </div>

  );
};

function AssistantApp({ apiUrl }: { apiUrl: string }) {
  const runtime = useChatRuntime({
    transport: new AssistantChatTransport({ api: apiUrl }),
  });

  const STORAGE_KEY = "assistant-messages";
  const loadedRef = useRef(false);

  // Restore history from localStorage on first mount
  useEffect(() => {
    if (loadedRef.current) return;
    loadedRef.current = true;
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const state = JSON.parse(saved);
        runtime.thread.importExternalState(state);
      }
    } catch {
      // ignore corrupted state
    }
  }, [runtime]);

  // Persist to localStorage on every change
  useEffect(() => {
    return runtime.thread.subscribe(() => {
      try {
        const state = runtime.thread.exportExternalState();
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
      } catch {
        // ignore write errors
      }
    });
  }, [runtime]);

  return (
    <TooltipProvider>
      <AssistantRuntimeProvider runtime={runtime}>
        <ShadowAssistantModal />
      </AssistantRuntimeProvider>
    </TooltipProvider>
  );
}

// ── The Lit custom element shell ──────────────────────────────────────────────
class AiChatElement extends LitElement {
  static properties = {
    apiUrl: { type: String, attribute: 'api-url' },
    theme:  { type: String, attribute: 'theme' },
  };

  static styles = css`
    :host {
      display: block;
      position: fixed;
      bottom: 0;
      right: 0;
      width: 0;
      height: 0;
      z-index: 10000;
      pointer-events: none;
    }
  `;

  apiUrl: string;
  theme: string;
  private _reactRoot: Root | null = null;

  constructor() {
    super();
    this.apiUrl = '/api/chat';
    this.theme  = 'light';
  }

  createRenderRoot() {
    return this.attachShadow({ mode: 'open' });
  }

  connectedCallback() {
    super.connectedCallback();
    this._mount();
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    if (this._reactRoot) {
      this._reactRoot.unmount();
      this._reactRoot = null;
    }
  }

  private _mount() {
    const shadow = this.shadowRoot;
    if (!shadow) return;

    // 1. Inject Styles into Shadow DOM ONLY
    const styleEl = document.createElement('style');
    // Map :root variables to :host so Tailwind bg-background etc works inside shadow
    const hostVars = indexStyles
        .replace(/:root/g, ':host')
        .replace(/body/g, ':host');

    styleEl.textContent = hostVars + '\n' + componentStyles;
    shadow.appendChild(styleEl);

    // 2. Create Mount Point
    const mountPoint = document.createElement('div');
    mountPoint.className = this.theme;
    mountPoint.style.cssText = 'pointer-events: auto;';
    shadow.appendChild(mountPoint);

    // 3. Mount React
    this._reactRoot = createRoot(mountPoint);
    this._renderReact();
  }

  private _renderReact() {
    if (!this._reactRoot) return;
    this._reactRoot.render(
      <React.StrictMode>
        <AssistantApp apiUrl={this.apiUrl} />
      </React.StrictMode>
    );
  }

  updated(changedProperties: Map<string, any>) {
    if (changedProperties.has('theme') && this.shadowRoot) {
      const mountPoint = this.shadowRoot.querySelector('div');
      if (mountPoint) mountPoint.className = this.theme;
    }
    if (changedProperties.has('apiUrl') || changedProperties.has('theme')) {
      this._renderReact();
    }
  }

  render() {
    return html``;
  }
}

// Register the custom element
if (!customElements.get('ai-chat')) {
  customElements.define('ai-chat', AiChatElement);
}

export { AiChatElement };
