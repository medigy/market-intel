import { AttachmentAdapter, PendingAttachment, CompleteAttachment } from "@assistant-ui/react";

export class MyCustomUploadAdapter implements AttachmentAdapter {
  accept = "*";
  private uploadUrl: string;
  private tenantId?: string;
  private chatToken?: string;

  constructor(uploadUrl: string = "/api/upload", tenantId?: string, chatToken?: string) {
    this.uploadUrl = uploadUrl;
    this.tenantId = tenantId;
    this.chatToken = chatToken;
  }

  async *add({ file }: { file: File }) {
    const id = Math.random().toString(36).slice(2);
    yield {
      id,
      type: "file" as const,
      name: file.name,
      file,
      status: { type: "running" as const, reason: "uploading" as const, progress: 0 },
    };
  }

  async send(attachment: PendingAttachment): Promise<CompleteAttachment> {
    console.log("📤 Attempting to upload file to:", this.uploadUrl, attachment.name);
    const formData = new FormData();
    formData.append("file", attachment.file);
    if (this.tenantId) formData.append("tenantId", this.tenantId);
    if (this.chatToken) formData.append("chatToken", this.chatToken);

    const response = await fetch(this.uploadUrl, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      console.error("❌ Upload failed with status:", response.status);
      throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
    }

    const result = await response.json();
    console.log("✅ Upload successful, fileId:", result.fileId);
    
    // Create a local blob URL for the UI to show a preview immediately
    const localUrl = URL.createObjectURL(attachment.file);

    return {
      id: attachment.id,
      type: attachment.type,
      name: attachment.name,
      url: localUrl, // This property is used by AttachmentPrimitive.unstable_Thumb
      content: [
        { 
          type: "file", 
          data: result.fileId,
          mimeType: attachment.file.type 
        }
      ],
      status: { type: "complete" },
    } as any as CompleteAttachment;
  }

  async remove() {
    // No-op for now
  }
}
