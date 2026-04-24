import { type FC } from "react";
import { MessagePartPrimitive } from "@assistant-ui/react";

export const ImagePart: FC = () => {
  return (
    <MessagePartPrimitive.Image className="my-2 first:mt-0 last:mb-0 max-w-full rounded-lg border shadow-sm" />
  );
};
