import { LitElement, html, css } from 'lit';
import { createRoot, Root } from 'react-dom/client';
import React, { useState, forwardRef } from 'react';

// Your existing assistant-ui pieces
import { AssistantRuntimeProvider } from '@assistant-ui/react';
import { useChatRuntime, AssistantChatTransport } from '@assistant-ui/react-ai-sdk';
import { Thread } from '../components/assistant-ui/thread';
import { TooltipProvider } from '../components/ui/tooltip';
import { TooltipIconButton } from '../components/assistant-ui/tooltip-icon-button';
import { XIcon } from 'lucide-react';

// Vite ?inline trick: converts CSS into a string for shadow DOM injection
import indexStyles from '../index.css?inline';
import componentStyles from './ai-chat-styles.css?inline';

// ── Recreating AssistantModal Logic (Shadow-DOM Friendly) ───────────────────
const ShadowAssistantModal = () => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col items-end gap-4">
      {/* Modal Content */}
      {isOpen && (
        <div 
          className="h-[700px] w-[400px] overflow-hidden rounded-2xl border border-border bg-background shadow-2xl flex flex-col animate-in fade-in zoom-in-95 duration-200"
        >
          <div className="flex items-center gap-3 p-4 border-b border-border/50 bg-background/50 backdrop-blur-md sticky top-0 z-10 shrink-0">
            <div className="size-11 rounded-xl overflow-hidden shadow-xl shadow-primary/10 border border-border/50 bg-white p-1">
              <img 
                src="https://qualityfolio.dev/favicon.png" 
                alt="Medigy Market Intelligence Logo" 
                className="size-full object-contain scale-125"
              />
            </div>
            <div className="flex flex-col">
              <h3 className="text-[15px] font-bold tracking-tight text-foreground leading-tight">Ask AI</h3>
              <div className="flex items-center gap-1.5 pt-0.5">
                <div className="relative flex size-1.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full size-1.5 bg-green-500"></span>
                </div>
                <span className="text-[11px] text-muted-foreground/90 font-semibold tracking-wide uppercase">active</span>
              </div>
            </div>
            <div className="ml-auto">
              <TooltipIconButton 
                tooltip="Close" 
                variant="ghost" 
                className="size-9 rounded-xl text-muted-foreground hover:text-foreground"
                onClick={() => setIsOpen(false)}
              >
                 <XIcon className="size-4" />
              </TooltipIconButton>
            </div>
          </div>
          
          <div className="flex-1 overflow-hidden">
            <Thread />
          </div>
        </div>
      )}

      {/* Trigger Button */}
      <TooltipIconButton
        variant="default"
        tooltip="Chat Assistant"
        className="size-14 rounded-full shadow-lg bg-gradient-to-br from-[#2f10a0] to-[#7c3aed] hover:shadow-xl transition-all overflow-hidden p-0 flex items-center justify-center pointer-events-auto"
        onClick={() => setIsOpen(!isOpen)}
      >
        {isOpen ? (
          <XIcon className="size-6 text-white" />
        ) : (
          <svg 
            width="26" 
            height="26" 
            viewBox="0 0 24 24" 
            fill="none" 
            stroke="white" 
            strokeWidth="2" 
            strokeLinecap="round" 
            strokeLinejoin="round"
          >
            <path d="M7.9 20A9 9 0 1 0 4 16.1L2 22Z"/>
            <path d="M8 12h.01"/><path d="M12 12h.01"/><path d="M16 12h.01"/>
          </svg>
        )}
      </TooltipIconButton>
    </div>
  );
};

function AssistantApp({ apiUrl }: { apiUrl: string }) {
  const runtime = useChatRuntime({
    transport: new AssistantChatTransport({ api: apiUrl }),
  });

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

  render() {
    return html``;
  }
}

// Register the custom element
if (!customElements.get('ai-chat')) {
  customElements.define('ai-chat', AiChatElement);
}

export { AiChatElement };
