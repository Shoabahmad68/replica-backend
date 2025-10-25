// index.js — fixed version for Cloudflare Worker
import { XMLParser } from "fast-xml-parser";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 🔹 1. Handle Tally Push
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();

        // Decompress XML from base64 gzip
        const decompress = (b64) => {
          if (!b64) return "";
          const binary = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
          const ds = new DecompressionStream("gzip");
          const stream = new Blob([binary]).stream().pipeThrough(ds);
          return new Response(stream).text();
        };

        const salesXml = await decompress(body.salesXml);
        const purchaseXml = await decompress(body.purchaseXml);
        const mastersXml = await decompress(body.mastersXml);

        // Parse XML safely
        const parser = new XMLParser({
          ignoreAttributes: false,
          attributeNamePrefix: "",
          allowBooleanAttributes: true,
        });

        const sales = salesXml ? parser.parse(salesXml) : {};
        const purchase = purchaseXml ? parser.parse(purchaseXml) : {};
        const masters = mastersXml ? parser.parse(mastersXml) : {};

        const flatRows = [];

        // Prepare KV JSON
        const jsonData = {
          status: "ok",
          time: new Date().toISOString(),
          rows: {
            sales,
            purchase,
            masters,
          },
          flatRows,
        };

        // Save to KV
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(jsonData));

        return new Response(
          JSON.stringify({ status: "ok", saved: true, size: JSON.stringify(jsonData).length }),
          { headers: { "Content-Type": "application/json" } }
        );
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500 });
      }
    }

    // 🔹 2. Handle Frontend Fetch
    if (url.pathname === "/api/imports/latest") {
      const kv = await env.REPLICA_DATA.get("latest_tally_json");
      if (!kv)
        return new Response(
          JSON.stringify({ status: "error", message: "No data found in KV" }),
          { status: 404 }
        );

      return new Response(kv, { headers: { "Content-Type": "application/json" } });
    }

    // Default
    return new Response("Replica backend active");
  },
};
