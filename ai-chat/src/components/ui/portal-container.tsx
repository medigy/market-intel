import * as React from "react";

type PortalContainer = Element | DocumentFragment | null;

const PortalContainerContext = React.createContext<PortalContainer>(null);

function PortalContainerProvider({
  container,
  children,
}: {
  container: PortalContainer;
  children: React.ReactNode;
}) {
  return (
    <PortalContainerContext.Provider value={container}>
      {children}
    </PortalContainerContext.Provider>
  );
}

function usePortalContainer() {
  return React.useContext(PortalContainerContext);
}

export { PortalContainerProvider, usePortalContainer };