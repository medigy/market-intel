import { useChatRuntime, AssistantChatTransport } from "@assistant-ui/react-ai-sdk";
import { AssistantRuntimeProvider } from "@assistant-ui/react";
import { AssistantModal } from "@/components/assistant-ui/assistant-modal";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useEffect, useRef } from "react";

import { MyCustomUploadAdapter } from "@/lib/attachment-adapter";

const STORAGE_KEY = "assistant-messages";
const API_URL = import.meta.env.VITE_API_URL ?? "/api/chat";

export default function RootPage() {
  const uploadUrl = API_URL.replace("/api/chat", "/api/upload");

  const runtime = useChatRuntime({
    transport: new AssistantChatTransport({ api: API_URL }),
    adapters: {
      attachments: new MyCustomUploadAdapter(uploadUrl),
    },
  });

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
        <AssistantModal />
      </AssistantRuntimeProvider>
    </TooltipProvider>
  );
}
