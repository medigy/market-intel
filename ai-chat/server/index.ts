import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import {
  streamText,
  stepCountIs,
} from "ai";
import path from "path";
import fs from "fs";
import multer from "multer";
import { createMCPClient } from "@ai-sdk/mcp";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

// Load .env from parent directory with absolute path
const envPath = path.join(process.cwd(), "..", ".env");
console.log("📂 Loading .env from:", envPath);
console.log("📂 File exists:", fs.existsSync(envPath));
dotenv.config({ path: envPath });

console.log("✅ Environment loaded");
console.log("🔑 Key variables:", {
  RSSD_PATH: process.env.RSSD_PATH ? "✓" : "✗",
  LITELLM_BASE_URL: process.env.LITELLM_BASE_URL ? "✓" : "✗",
  AI_MODEL: process.env.AI_MODEL ? "✓" : "✗",
});

const app = express();
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));

// Request logger
app.use((req, res, next) => {
  console.log(`📡 ${req.method} ${req.url}`);
  next();
});

app.use(express.json({ limit: "20mb" }));

// Configure multer to save files to an 'uploads' directory
const upload = multer({ 
  dest: "uploads/",
  limits: { fileSize: 20 * 1024 * 1024 } // 20MB
});

// Ensure uploads directory exists
if (!fs.existsSync("uploads")) {
  fs.mkdirSync("uploads");
}

app.post("/api/upload", upload.single("file"), (req, res) => {
  if (!req.file) {
    res.status(400).json({ error: "No file uploaded" });
    return;
  }

  res.json({ 
    ok: true, 
    fileId: req.file.filename,
    filename: req.file.originalname, 
    mimeType: req.file.mimetype
  });
});

// Alias for compatibility
app.post("/upload-csv", upload.single("file"), (req, res) => {
  if (!req.file) {
    res.status(400).json({ error: "No file uploaded" });
    return;
  }

  res.json({ 
    ok: true, 
    fileId: req.file.filename,
    filename: req.file.originalname, 
    mimeType: req.file.mimetype
  });
});

app.get("/", (req, res) => {
  res.send("Assistant UI Backend is running. Access the frontend at http://localhost:5173");
});

let cachedTableNames: string[] | null = null;

async function getKnownTables(mcpTools: Record<string, { execute?: Function }>): Promise<string[]> {
  if (cachedTableNames) return cachedTableNames;

  const querySqlTool = mcpTools.query_sql;
  if (!querySqlTool?.execute) return [];

  try {
    const result = await querySqlTool.execute(
      {
        sql: "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
        limit: 200,
      },
      {
        toolCallId: "known-tables-cache",
        messages: [],
      },
    );

    const textContent = (
      result?.content as Array<{ type?: string; text?: string }> | undefined
    )?.find((entry) => entry.type === "text")?.text;

    if (!textContent) return [];

    const parsed = JSON.parse(textContent) as {
      rows?: Array<{ name?: string }>;
    };

    cachedTableNames =
      parsed.rows
        ?.map((row) => row.name)
        .filter((name): name is string => Boolean(name)) ?? [];

    return cachedTableNames;
  } catch {
    return [];
  }
}

const open_model = createOpenAICompatible({
  baseURL: process.env.LITELLM_BASE_URL!,
  name: process.env.AI_MODEL!,
  apiKey: process.env.LITELLM_API_KEY!,
});

let tools: any;
let mcpClient: any;

async function initMCP() {
  if (!mcpClient) {
    console.log("🔌 Initializing MCP Client...");
    console.log("📁 RSSD Path:", path.resolve(process.cwd(), process.env.RSSD_PATH!));
    
    try {
      mcpClient = await createMCPClient({
        transport: new StdioClientTransport({
          command: "surveilr",
          args: ["mcp", "server", "-d", path.resolve(process.cwd(), process.env.RSSD_PATH!)],
        }),
      });
      console.log("✅ MCP Client created successfully");
      
      tools = await mcpClient.tools();
      console.log("✅ Tools loaded:", Object.keys(tools).length, "tools available");
      console.log("📋 Available tools:", Object.keys(tools).join(", "));
    } catch (err) {
      console.error("❌ MCP Initialization failed:", err);
      throw err;
    }
  }
  return tools;
}

function getFriendlyErrorMessage(err: unknown): string {
  const error = err instanceof Error ? err : new Error(String(err));
  const message = error.message.toLowerCase();

  if (message.includes("429") || message.includes("too many requests") || message.includes("rate limit")) {
    return "Rate limit reached. Please wait a moment before sending another message.";
  }
  if (message.includes("408") || message.includes("504") || message.includes("timeout") || message.includes("deadline exceeded")) {
    return "The request timed out. Please try again in a few moments.";
  }
  if (message.includes("401") || message.includes("403") || message.includes("auth") || message.includes("api key")) {
    return "Authentication failed. Please check your API configuration.";
  }
  if (message.includes("quota") || message.includes("insufficient")) {
    return "Quota exceeded. Please check your account limits.";
  }

  return "An unexpected error occurred. Please try again.";
}

// ─── HELPER: resolve a file reference or data URI into a model-ready part ───
// Defined OUTSIDE the request handler so it's always available.
function resolveAttachment(
  name: string | undefined,
  contentType: string | undefined,
  dataUri: string
): { type: string; image?: string; mediaType?: string; text?: string } | null {
  if (!dataUri || typeof dataUri !== "string") return null;

  // ── File reference (multer hash, not a data URI or http URL) ──
  if (!dataUri.startsWith("data:") && !dataUri.startsWith("http")) {
    const filePath = path.join("uploads", dataUri);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File not found on disk: ${filePath}`);
      return null;
    }

    const isImage =
      contentType?.startsWith("image/") ||
      name?.toLowerCase().match(/\.(jpg|jpeg|png|gif|webp)$/) != null;

    if (isImage) {
      const mediaType = contentType || "image/jpeg";
      const imageBytes = fs.readFileSync(filePath); // raw Buffer
      console.log(`🖼️  File → image: ${dataUri} (${mediaType}, ${Math.round(imageBytes.length / 1024)} KB)`);
      return { type: "image", image: imageBytes, mediaType };
    } else {
      const text = fs.readFileSync(filePath, "utf-8");
      const truncated = text.length > 100_000;
      console.log(`📄 File → text: ${name || dataUri} (${text.length} chars${truncated ? ", truncated" : ""})`);
      return {
        type: "text",
        text: `\n\n[Attached File: ${name || dataUri}${truncated ? " (TRUNCATED)" : ""}]\n${
          truncated ? text.substring(0, 100_000) + "\n... [truncated] ..." : text
        }\n[End of File]\n`,
      };
    }
  }

  // ── Data URI ──
  if (dataUri.startsWith("data:")) {
    const isImageUri =
      dataUri.includes("image/") || (contentType && contentType.includes("image"));

    if (isImageUri) {
      try {
        const mediaType = dataUri.match(/:(.*?);/)?.[1] || contentType || "image/jpeg";
        const base64 = dataUri.split(",")[1];
        const imageBytes = Buffer.from(base64, "base64");
        return { type: "image", image: imageBytes, mediaType };
      } catch (e) {
        console.error("Failed to parse image data URI:", e);
      }
    } else {
      // Text / CSV / other
      try {
        const base64 = dataUri.split(",")[1];
        const text = Buffer.from(base64, "base64").toString("utf-8");
        return {
          type: "text",
          text: `\n\n[Attached File: ${name || "document"}]\n${text}\n[End of File]\n`,
        };
      } catch (e) {
        console.error("Failed to parse text data URI:", e);
      }
    }
  }

  return null;
}

app.post("/api/chat", async (req, res) => {
  try {
    console.log("📨 Chat request received");
    const currentTools = await initMCP();
    console.log("✅ MCP initialized for this request");
    
    const { messages } = req.body;
    console.log("📝 Messages count:", messages?.length || 0);
    console.log("📥 Raw incoming messages:", JSON.stringify(messages, (key, value) => 
      (typeof value === "string" && value.length > 100) ? value.substring(0, 50) + "..." : value, 2));

    if (!messages || !Array.isArray(messages)) {
      res.status(400).json({ error: "Invalid messages" });
      return;
    }
    
    const knownTables = await getKnownTables(currentTools as Record<string, { execute?: Function }>);
    console.log("📊 Known tables:", knownTables.length);

    const tableHint = knownTables.length
      ? `\n\nAvailable tables and views in the RSSD (use exact names):\n${knownTables.join(", ")}.`
      : "";

    const systemPrompt = `You are an AI assistant connected to a surveilr Resource Surveillance State Database (RSSD) via an MCP server. Your primary capability is answering questions by generating and executing SQL queries against the RSSD — a read-only SQLite database.
 
Use a "Progressive Discovery" strategy: start with lightweight tools and escalate only when needed. You have a maximum of 15 tool calls per response — use them efficiently.
 
Core Constraints:
- Read-only: Only SELECT statements are permitted. Never attempt INSERT, UPDATE, DELETE, DROP, or any DDL.
- Row limits: Queries return 10 rows by default, max 50 rows. Request more explicitly only when truly necessary.
- Text truncation: All text fields are truncated at 200 characters. If a value ends with "... (N chars total)", the full value is longer than displayed.
- Step budget: You have at most 15 tool calls per response. Prefer the minimum number of calls needed.
 
Available MCP Tools:
1. Schema Discovery (use these FIRST):
   - list_tables(): ~50-100 tokens. Use at the start of a new conversation to see what tables exist.
   - get_table_columns(table_name): ~50-200 tokens. Use once you know which tables are relevant.
   - get_table_metadata(table_name): Detailed column definitions for a specific table.
   - get_schema_compact(): ~2k-5k tokens. Use when you need a broad overview of the full database structure.
   - get_schema(): ~25k-80k tokens. Use only when full metadata and row counts are explicitly required.
 
2. Data Sampling:
   - get_table_sample(table_name): Returns first 3 rows from a table; text fields truncated to 200 chars.
   - get_table_stats(table_name): Get row count and basic stats for a table.
 
3. Query Execution:
   - query_sql(sql, limit?): Execute a SELECT query. Default 10 rows, max 50 rows.
 
4. Ontology Tools:
   - query_ontology(concept): Look up a concept in the RSSD ontology.
   - explore_concept(class_name): Explore relationships connected to an ontology class.
   - list_ontology(): List available ontology classes.
 
Optimal Text-to-SQL Workflow:
1. MAP: Call list_tables() first to identify candidate tables.
2. DRILL: Call get_table_columns(table_name) for 1-2 relevant tables.
3. INSPECT: Call get_table_sample(table_name) to see example values.
4. QUERY: Use query_sql with narrow SELECT statements and specific WHERE clauses.
 
Analysis & Recommendations:
- After retrieving data, ALWAYS provide analysis and actionable recommendations.
- Never refuse to provide recommendations simply because you are a database tool.
- If the data is insufficient, state what data was found and what additional data would help.
 
Behavioral Rules:
1. Always start with list_tables() on the FIRST turn of a conversation.
2. Never call get_schema() unless the user explicitly asks for full schema metadata.
3. Chain tools efficiently: list_tables -> get_table_columns -> query_sql.
4. Validate before querying: Confirm table and column names exist.
5. Explain truncation: If a text result ends with "... (N chars total)", inform the user.
6. Limit discipline: Default to limit=10. Only increase to max 50 if needed.
7. SQL safety: Never generate or execute non-SELECT SQL.
8. Surface ontology when relevant for concepts, classifications, or taxonomy.
9. Empty results: If a query returns no rows, suggest possible reasons.
10. Silent execution: Never narrate tool calls or intermediate findings.${tableHint}`;

    // ─── Message sanitization ────────────────────────────────────────────────
    // The AI SDK UIMessage format uses "parts" (not "content").
    // convertToModelMessages re-processes parts and corrupts our base64,
    // so we build the final model messages manually instead.

    const sanitizedMessages = messages.map((m: any) => {
      const experimental_attachments: any[] = m.experimental_attachments || [];

      // ── Read from "parts" (UIMessage) OR "content" (legacy) ──
      const rawParts: any[] = Array.isArray(m.parts)
        ? m.parts
        : Array.isArray(m.content)
        ? m.content
        : typeof m.content === "string"
        ? [{ type: "text", text: m.content }]
        : [];

      // ── STEP 1: Resolve file references and data URIs into model-ready parts ──
      let resolvedParts: any[] = rawParts.map((c: any) => {
        // ── Case A: { type: "file", url: "<multerHash>", mediaType, filename }
        //    This is exactly what assistant-ui sends after a custom upload adapter.
        if (c.type === "file" && typeof c.url === "string") {
          console.log(`🔍 File part detected: url=${c.url}, mediaType=${c.mediaType}, filename=${c.filename}`);
          console.log(`🗂️  Checking disk: ${path.join("uploads", c.url)} exists=${fs.existsSync(path.join("uploads", c.url))}`);

          const resolved = resolveAttachment(
            c.filename || c.name,
            c.mediaType || c.contentType,
            c.url
          );
          if (resolved) return resolved;
          return { type: "text", text: `[Attachment not found: ${c.filename || c.url}]` };
        }

        // ── Case B: already a clean text part ──
        if (c.type === "text") return c;

        // ── Case C: image or other part with embedded data URI / file ref ──
        let dataUri: string | null = null;
        if (typeof c.image === "string") dataUri = c.image;
        else if (typeof c.data === "string") dataUri = c.data;
        else if (c.attachment?.url) dataUri = c.attachment.url;
        else if (c.attachment?.data) dataUri = c.attachment.data;

        if (dataUri) {
          const name = c.name || c.filename;
          const contentType = c.contentType || c.mimeType || c.mediaType;
          const resolved = resolveAttachment(name, contentType, dataUri);
          if (resolved) return resolved;
          // Strip bad data
          return { type: "text", text: `[Attachment: ${name || "unsupported"}]` };
        }

        return c;
      });

      // ── STEP 2: Convert experimental_attachments → resolved parts ──
      const fromAttachments: any[] = experimental_attachments
        .map((a: any) =>
          resolveAttachment(a.name || a.filename, a.contentType || a.mediaType, a.url)
        )
        .filter(Boolean);

      const allParts = [...resolvedParts, ...fromAttachments];

      // ── STEP 3: Build the final model message content array ──
      // Map our resolved parts into the format the model API expects:
      // text → { type: "text", text }
      // image → { type: "image", image: <URL | base64 string> }
      const modelContent = allParts.map((p: any) => {
        if (p.type === "text") {
          return { type: "text" as const, text: p.text ?? "" };
        }
        if (p.type === "image") {
          // p.image is a raw Buffer (Uint8Array), p.mediaType is e.g. "image/jpeg"
          // The AI SDK image part accepts: { type: "image", image: Uint8Array, mimeType: string }
          return { type: "image" as const, image: p.image, mimeType: p.mediaType || "image/jpeg" };
        }
        // Fallback: drop unknown part types silently
        return null;
      }).filter(Boolean);

      return {
        role: m.role as "user" | "assistant" | "system",
        content: modelContent,
      };
    });
    // ────────────────────────────────────────────────────────────────────────

    // Debug log — confirm base64 is real (should start with /9j/ for JPEG, iVBOR for PNG)
    console.log("📤 Sending messages to model:", JSON.stringify(
      sanitizedMessages.map((m) => ({
        role: m.role,
        content: Array.isArray(m.content)
          ? m.content.map((c: any) =>
              c.type === "image"
                ? { type: "image", mimeType: c.mimeType, sizeBytes: c.image?.length }
                : c
            )
          : m.content,
      })),
      null,
      2
    ));

    const result = streamText({
      model: open_model(process.env.AI_MODEL!),
      tools: currentTools,
      messages: sanitizedMessages as any,
      system: systemPrompt,
      stopWhen: stepCountIs(15),
      onStepFinish: async ({ toolResults }) => {
        if (toolResults.length) {
          console.log("📋 Tool Results:", JSON.stringify(toolResults, null, 2));
        }
      },
    });

    result.pipeUIMessageStreamToResponse(res);
  } catch (err) {
    console.error("❌ API ERROR:", err);
    const friendlyMessage = getFriendlyErrorMessage(err);
    
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.status(200);
    res.write(`data: ${JSON.stringify({ type: "error", errorText: friendlyMessage })}\n\n`);
    res.write("data: [DONE]\n\n");
    res.end();
    console.error("⚠️ Error sent to client:", friendlyMessage);
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log("\n" + "=".repeat(50));
  console.log(`✅ Backend on http://localhost:${PORT}`);
  console.log("🔧 Configuration:");
  console.log("   - LITELLM:", process.env.LITELLM_BASE_URL);
  console.log("   - Model:", process.env.AI_MODEL);
  console.log("   - Database:", process.env.RSSD_PATH);
  console.log("🚀 Ready to handle chat requests");
  console.log("=".repeat(50) + "\n");
});