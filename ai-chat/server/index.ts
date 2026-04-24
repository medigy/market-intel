import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import {
  streamText,
  convertToModelMessages,
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

// Cache for file metadata if needed, but we can just use the filesystem
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

app.post("/api/chat", async (req, res) => {
  try {
    console.log("📨 Chat request received");
    const currentTools = await initMCP();
    console.log("✅ MCP initialized for this request");
    
    const { messages } = req.body;
    console.log("📝 Messages count:", messages?.length || 0);

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

    // Robust sanitization and attachment handling
    const processedMessages = messages.map((m: any) => {
      const experimental_attachments = m.experimental_attachments || [];
      let contentParts = Array.isArray(m.content) ? [...m.content] : (typeof m.content === 'string' ? [{ type: 'text', text: m.content }] : []);

      // Function to process a data URI into a content part
      const processAttachment = (name: string | undefined, contentType: string | undefined, dataUri: string) => {
        if (!dataUri || typeof dataUri !== 'string') return null;

        // Handle File References (not data URIs)
        if (!dataUri.startsWith("data:")) {
          console.log(`🔍 Processing file reference: ${dataUri} (Name: ${name}, Type: ${contentType})`);
          const filePath = path.join("uploads", dataUri);
          if (fs.existsSync(filePath)) {
            const isImage = contentType?.includes("image") || name?.toLowerCase().match(/\.(jpg|jpeg|png|gif|webp)$/);
            
            if (isImage) {
              const mediaType = contentType || (name?.endsWith(".png") ? "image/png" : name?.endsWith(".webp") ? "image/webp" : name?.endsWith(".gif") ? "image/gif" : "image/jpeg");
              const base64 = fs.readFileSync(filePath).toString("base64");
              console.log(`🖼️ Loading image reference: ${dataUri} (Media: ${mediaType}, Size: ${Math.round(base64.length / 1024)}KB)`);
              return {
                type: "image",
                image: Buffer.from(base64, "base64"),
                mediaType,
              };
            } else {
              // Assume text/CSV
              const text = fs.readFileSync(filePath, "utf-8");
              const truncated = text.length > 100000;
              const displayText = truncated ? text.substring(0, 100000) : text;
              console.log(`📄 Loading text reference: ${name || dataUri} (${text.length} chars)`);
              return {
                type: "text",
                text: `\n\n[Attached File: ${name || 'document'}${truncated ? ' (TRUNCATED)' : ''}]\n${displayText}\n${truncated ? '... [Content truncated for length] ...\n' : ''}[End of File]\n`
              };
            }
          }
          return null;
        }

        // Handle Data URIs (Legacy support)
        if (dataUri.startsWith("data:")) {
          // ... (existing data URI handling logic) ...
          // I will keep the original logic here but simplified for the diff
          if (dataUri.includes("text/csv") || dataUri.includes("text/plain") || dataUri.includes("application/octet-stream") || (contentType && (contentType.includes("text") || contentType.includes("csv")))) {
            try {
              const [header, base64] = dataUri.split(",");
              const text = Buffer.from(base64, "base64").toString("utf-8");
              return { type: "text", text: `\n\n[Attached File: ${name || 'document'}]\n${text}\n[End of File]\n` };
            } catch (e) { console.error(e); }
          }
          if (dataUri.includes("image/") || (contentType && contentType.includes("image"))) {
            try {
              const [header, base64] = dataUri.split(",");
              const mediaType = header.match(/:(.*?);/)?.[1] || contentType || "image/jpeg";
              return { type: "image", image: `data:${mediaType};base64,${base64}`, mediaType };
            } catch (e) { console.error(e); }
          }
        }
        return null;
      };

      // 1. Process experimental_attachments and move them to content parts
      const newFromAttachments = experimental_attachments
        .map((a: any) => processAttachment(a.name || a.filename, a.contentType || a.mediaType, a.url))
        .filter(Boolean);

      // 2. Sanitize existing content parts and STRIP all data URIs
      const sanitizedExistingContent = contentParts.map((c: any) => {
        // Find if this part contains a data URI in any known property
        let dataUri: string | null = null;
        if (typeof c.image === 'string' && c.image.startsWith('data:')) dataUri = c.image;
        else if (typeof c.url === 'string' && (c.url.startsWith('data:') || !c.url.includes(":"))) dataUri = c.url;
        else if (c.image && typeof c.image.url === 'string' && (c.image.url.startsWith('data:') || !c.image.url.includes(":"))) dataUri = c.image.url;
        else if (c.data && typeof c.data === 'string') dataUri = c.data;

        if (dataUri) {
          const name = c.name || c.filename || c.itemName;
          const contentType = c.contentType || c.mimeType || c.mediaType;
          
          console.log(`📎 Found data reference in part: ${dataUri.substring(0, 50)}... (Type: ${c.type}, Name: ${name}, ContentType: ${contentType})`);
          
          const processed = processAttachment(name, contentType, dataUri);
          if (processed) {
            console.log(`✅ Successfully processed attachment part: ${dataUri.substring(0, 20)} -> ${processed.type}`);
            return processed;
          }
          
          console.log(`⚠️ Failed to process attachment part: ${dataUri.substring(0, 20)}, stripping data`);
          // If we couldn't process it but it HAS a data URI/file reference, we MUST strip it to avoid errors
          const { image, url, data, ...rest } = c;
          return { ...rest, type: rest.type || 'text', text: rest.text || `[Attachment: ${name || 'unsupported'}]` };
        }
        return c;
      });

      // Combine and clear experimental_attachments to prevent AI SDK from downloading
      return { 
        ...m, 
        content: [...sanitizedExistingContent, ...newFromAttachments],
        experimental_attachments: [] 
      };
    });

    const sanitizedMessages = await convertToModelMessages(processedMessages);
    
    // Log message structure for debugging (without the big image data)
    console.log("📤 Sending messages to model:", JSON.stringify(sanitizedMessages.map(m => ({
      role: m.role,
      content: Array.isArray(m.content) 
        ? m.content.map((c: any) => c.type === 'image' ? { type: 'image', mediaType: c.mediaType, size: c.image?.length } : c)
        : (typeof m.content === 'string' ? m.content.substring(0, 100) + '...' : typeof m.content)
    })), null, 2));

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

    // Stream the result to the Express response
    result.pipeUIMessageStreamToResponse(res);
  } catch (err) {
    console.error("❌ API ERROR:", err);
    const friendlyMessage = getFriendlyErrorMessage(err);
    
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.status(200);
    res.write(`data: ${JSON.stringify({
      type: "error",
      errorText: friendlyMessage,
    })}\n\n`);
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
