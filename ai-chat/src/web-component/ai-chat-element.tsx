import { LitElement, html, css } from 'lit';
import { createRoot, Root } from 'react-dom/client';
import React, { useEffect, useRef, useState } from 'react';

// Your existing assistant-ui pieces
import { AssistantRuntimeProvider } from '@assistant-ui/react';
import { useChatRuntime, AssistantChatTransport } from '@assistant-ui/react-ai-sdk';
import { AssistantModal } from '../components/assistant-ui/assistant-modal';
import { PortalContainerProvider } from '../components/ui/portal-container';
import { TooltipProvider } from '../components/ui/tooltip';
import { MyCustomUploadAdapter } from '../lib/attachment-adapter';

// Vite ?inline trick: converts CSS into a string for shadow DOM injection
import indexStyles from '../index.css?inline';
import componentStyles from './ai-chat-styles.css?inline';

// Replaced custon ShadowAssistantModal with standard AssistantModal for minimal customization

function AssistantApp({
  apiUrl,
  tenantId,
  chatToken,
  portalContainer,
}: {
  apiUrl: string;
  tenantId?: string;
  chatToken?: string;
  portalContainer: Element | DocumentFragment | null;
}) {
  const uploadUrl = new URL(apiUrl.replace("/api/chat", "/api/upload"), window.location.origin);
  if (tenantId) uploadUrl.searchParams.append("tenantId", tenantId);
  if (chatToken) uploadUrl.searchParams.append("chatToken", chatToken);

  const apiEndpoint = new URL(apiUrl, window.location.origin);
  if (tenantId) apiEndpoint.searchParams.append("tenantId", tenantId);
  if (chatToken) apiEndpoint.searchParams.append("chatToken", chatToken);

  const runtime = useChatRuntime({
    transport: new AssistantChatTransport({ 
      api: apiEndpoint.toString(),
      headers: {
        ...(tenantId ? { "x-tenant-id": tenantId } : {}),
        ...(chatToken ? { "x-chat-token": chatToken } : {})
      }
    }),
    adapters: {
      attachments: new MyCustomUploadAdapter(uploadUrl.toString()),
    },
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
    <PortalContainerProvider container={portalContainer}>
      <TooltipProvider>
        <AssistantRuntimeProvider runtime={runtime}>
          <AssistantModal />
        </AssistantRuntimeProvider>
      </TooltipProvider>
    </PortalContainerProvider>
  );
}

// ── The Lit custom element shell ──────────────────────────────────────────────
class AiChatElement extends LitElement {
  static properties = {
    apiUrl: { type: String, attribute: 'api-url' },
    theme:  { type: String, attribute: 'theme' },
    tenantId: { type: String, attribute: 'tenant-id' },
    chatToken: { type: String, attribute: 'chat-token' }
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
  tenantId?: string;
  chatToken?: string;
  private _reactRoot: Root | null = null;
  private _mountPoint: HTMLDivElement | null = null;

  constructor() {
    super();
    this.apiUrl = import.meta.env.VITE_API_URL ?? '/api/chat';
    this.theme  = 'light';
  }

  createRenderRoot() {
    return this.attachShadow({ mode: 'open' });
  }

  connectedCallback() {
    super.connectedCallback();
    this.classList.add(this.theme); // Apply theme class to host
    this._mount();
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    if (this._reactRoot) {
      this._reactRoot.unmount();
      this._reactRoot = null;
    }
    this._mountPoint = null;
  }

  private _mount() {
    const shadow = this.shadowRoot;
    if (!shadow) return;

    // 1. Inject Styles into Shadow DOM ONLY
    const styleEl = document.createElement('style');
    // Map global selectors to :host and children so they work correctly in Shadow DOM.
    // Tailwind v4 uses :root for variables, which we move to :host.
    const hostVars = indexStyles
        .replace(/:root/g, ':host')
        .replace(/\bbody\b/g, ':host')
        .replace(/\bhtml\b/g, ':host');

    styleEl.textContent = hostVars + '\n' + componentStyles;
    shadow.appendChild(styleEl);

    // 2. Create Mount Point
    const mountPoint = document.createElement('div');
    mountPoint.setAttribute('data-aui-root', '');
    mountPoint.className = this.theme;
    mountPoint.style.cssText = 'pointer-events: auto;';
    shadow.appendChild(mountPoint);
    this._mountPoint = mountPoint;

    // 3. Mount React
    this._reactRoot = createRoot(mountPoint);
    this._renderReact();
  }

  private _renderReact() {
    if (!this._reactRoot) return;
    this._reactRoot.render(
      <React.StrictMode>
        <AssistantApp 
          apiUrl={this.apiUrl} 
          tenantId={this.tenantId}
          chatToken={this.chatToken}
          portalContainer={this._mountPoint} 
        />
      </React.StrictMode>
    );
  }

  updated(changedProperties: Map<string, any>) {
    if (changedProperties.has('theme')) {
      // Update theme class on host
      this.classList.remove('light', 'dark');
      this.classList.add(this.theme);
      
      if (this.shadowRoot) {
        const mountPoint = this.shadowRoot.querySelector('[data-aui-root]');
        if (mountPoint) mountPoint.className = this.theme;
      }
    }
    if (changedProperties.has('apiUrl') || changedProperties.has('theme') || changedProperties.has('tenantId') || changedProperties.has('chatToken')) {
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
