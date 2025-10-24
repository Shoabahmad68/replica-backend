// index.js (Cloudflare Worker) - accepts XML or combined JSON from pusher
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return new Response(
        JSON.stringify({
          status: "success",
          message: "Backend connected",
          time: new Date().toISOString(),
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    // === POST from pusher.js ===
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";

        // ------------------------
        // ✅ 1. New JSON payload
        // ------------------------
        if (ct.includes("application/json")) {
          const body = await request.json();

          const xml = body.salesXml || "";
          const rows = [];

          if (xml && xml.includes("<VOUCHER")) {
            const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
            for (const v of vouchers) {
              const get = (tag) => {
                const m = v.match(
                  new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i")
                );
                return m ? m[1].trim() : "";
              };

              rows.push({
                type: "VOUCHER",
                Date: get("DATE"),
                Party: get("PARTYNAME"),
                Item: get("STOCKITEMNAME"),
                Qty: get("BILLEDQTY"),
                Amount: get("AMOUNT"),
                State: get("PLACEOFSUPPLY"),
                Salesman: get("BASICSALESNAME"),
              });
            }
          }

          const payload = {
            status: "ok",
            time: new Date().toISOString(),
            rows,
          };

          await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

          return new Response(
            JSON.stringify({
              success: true,
              parsed: rows.length,
              message: "Parsed XML from JSON body and stored",
            }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );
        }

        // ------------------------
        // ✅ 2. Old XML POST
        // ------------------------
        else {
          const xml = await request.text();
          if (!xml || !xml.includes("<ENVELOPE>"))
            return new Response(
              JSON.stringify({ error: "Invalid XML format" }),
              { status: 400, headers: { "Content-Type": "application/json", ...cors } }
            );

          const tags = ["VOUCHER", "COMPANY", "LEDGER", "STOCKITEM", "GROUP", "UNIT"];
          const records = [];

          const getVal = (block, tag) => {
            const match = block.match(
              new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i")
            );
            return match ? match[1].trim() : "";
          };

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
                Salesman: getVal(block, "BASICSALESNAME"),
              });
            }
          }

          const payload = {
            status: "ok",
            time: new Date().toISOString(),
            rows: records,
          };

          await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

          return new Response(
            JSON.stringify({
              success: true,
              message: "XML parsed and stored",
              count: records.length,
            }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );
        }
      } catch (err) {
        return new Response(
          JSON.stringify({
            error: err.message || "Processing failed",
          }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // === GET for frontend ===
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(
          JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      return new Response(data, {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    // Default 404
    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
