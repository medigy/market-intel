

import { BotIcon, XIcon, MessageCircleIcon } from "lucide-react";
import { forwardRef } from "react";
import { AssistantModalPrimitive } from "@assistant-ui/react";
import { Thread } from "@/components/assistant-ui/thread";
import { TooltipIconButton } from "@/components/assistant-ui/tooltip-icon-button";
import { usePortalContainer } from "@/components/ui/portal-container";

const AssistantModal = () => {
  const portalContainer = usePortalContainer();

  return (
    <AssistantModalPrimitive.Root>
      <AssistantModalPrimitive.Anchor className="fixed bottom-0 right-0 z-50 pointer-events-none">
        <AssistantModalTrigger />
      </AssistantModalPrimitive.Anchor>
      <AssistantModalPrimitive.Content
        sideOffset={16}
        portalProps={portalContainer ? { container: portalContainer } : undefined}
        className="aui-modal-content z-50 mr-4 mb-2 h-[720px] w-[420px] overflow-hidden rounded-2xl border border-border bg-background shadow-2xl flex flex-col data-[state=closed]:animate-out data-[state=open]:animate-in data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95"
      >
        <AssistantModalHeader />
        <Thread />
      </AssistantModalPrimitive.Content>
    </AssistantModalPrimitive.Root>
  );
};

const AssistantModalHeader = () => {
  return (
    <div className="aui-header-section flex items-center gap-3 p-4 border-b border-border/50 bg-background/50 backdrop-blur-md sticky top-0 z-10 shrink-0">
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
      <div className="ml-auto" />
    </div>
  )
}

const AssistantModalTrigger = forwardRef<
  HTMLButtonElement,
  Record<string, never>
>((props, ref) => {
  return (
    <AssistantModalPrimitive.Trigger asChild>
      <button
        {...props}
        ref={ref}
        className="group relative flex items-center justify-center transition-all duration-300 ease-out pointer-events-auto
                   h-[58px] w-[72px] rounded-l-2xl bg-[#7c3aed] text-white shadow-[-4px 4px 24px rgba(124,58,237,0.4)] hover:w-[80px]
                   data-[state=open]:mr-4 data-[state=open]:size-14 data-[state=open]:rounded-full data-[state=open]:bg-background data-[state=open]:border data-[state=open]:border-border data-[state=open]:text-[#7c3aed] data-[state=open]:shadow-lg data-[state=open]:hover:bg-muted"
      >
        <div 
          className="absolute -top-[12px] right-0 w-[14px] h-[12px] bg-[#6d28d9] group-data-[state=open]:opacity-0 transition-opacity duration-300"
          style={{ clipPath: 'polygon(0% 100%, 100% 0%, 100% 100%)' }}
        />
        
        <div className="group-data-[state=open]:hidden flex items-center justify-center relative">
          <MessageCircleIcon className="size-8" strokeWidth={2.5} />
        </div>
        
        <div className="hidden group-data-[state=open]:block">
          <XIcon className="size-6" />
        </div>
      </button>
    </AssistantModalPrimitive.Trigger>
  );
});

AssistantModalTrigger.displayName = "AssistantModalTrigger";

export { AssistantModal };
