// ===== index.js =====
// Author: Shoaib Ahamad
// Purpose: Universal Tally Data Receiver + Cloudflare KV Sync

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    };

    // CORS preflight
    if (request.method === "OPTIONS")
      return new Response(null, { headers: cors });

    // Root endpoint
    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    // Test route
    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({
        status: "success",
        message: "Backend connected successfully",
        time: new Date().toISOString()
      }), { headers: { "Content-Type": "application/json", ...cors } });

    // =========================================================
    // RECEIVE XML DATA FROM PUSHER
    // =========================================================
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const xml = await request.text();
        if (!xml.includes("<ENVELOPE>")) {
          return new Response(JSON.stringify({ error: "Invalid XML format" }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...cors }
          });
        }

        // Define tag list (multi-report support)
        const tags = [
          "VOUCHER",
          "LEDGER",
          "STOCKITEM",
          "COMPANY",
          "GROUP",
          "COSTCENTRE",
          "UNIT",
          "EMPLOYEE",
          "GODOWN"
        ];

        const records = [];
        const getVal = (block, tag) => {
          const match = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return match ? match[1].trim() : "";
        };

        // Loop through every tag and extract records
        for (const tag of tags) {
          const blocks = xml.match(new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi")) || [];
          for (const block of blocks) {
            records.push({
              type: tag,
              Name: getVal(block, "NAME"),
              Date: getVal(block, "DATE"),
              Amount: getVal(block, "AMOUNT"),
              Party: getVal(block, "PARTYNAME"),
              Item: getVal(block, "STOCKITEMNAME"),
              Qty: getVal(block, "BILLEDQTY"),
              State: getVal(block, "PLACEOFSUPPLY"),
              Salesman: getVal(block, "BASICSALESNAME")
            });
          }
        }

        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: records
        };

        // Store in KV
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(JSON.stringify({
          success: true,
          message: "XML parsed successfully",
          count: records.length
        }), { headers: { "Content-Type": "application/json", ...cors } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message || "Processing failed" }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors }
        });
      }
    }

    // =========================================================
    // FETCH LATEST IMPORTED DATA
    // =========================================================
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(JSON.stringify({ status: "empty", rows: [] }), {
          headers: { "Content-Type": "application/json", ...cors }
        });

      return new Response(data, { headers: { "Content-Type": "application/json", ...cors } });
    }

    // Default 404
    return new Response("404 Not Found", { status: 404, headers: cors });
  }
};
