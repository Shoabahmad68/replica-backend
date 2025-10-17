// Cloudflare Worker – replica-backend (final fixed version)

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    };

    if (request.method === "OPTIONS")
      return new Response(null, { headers: cors });

    // root
    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    // test route
    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({
        status: "success",
        message: "Backend connected successfully",
        time: new Date().toISOString()
      }), { headers: { "Content-Type": "application/json", ...cors } });

    // receive from Tally pusher
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const xml = await request.text();
        if (!xml || !xml.includes("<ENVELOPE>"))
          return new Response(JSON.stringify({ error: "Invalid or empty XML" }),
            { status: 400, headers: { "Content-Type": "application/json", ...cors } });

        // safe regex XML parsing
        const vouchers = [];
        const voucherBlocks = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
        const get = (block, tag) => {
          const m = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return m ? m[1].trim() : "";
        };
        for (const b of voucherBlocks) {
          vouchers.push({
            Date: get(b, "DATE"),
            Party: get(b, "PARTYNAME"),
            Item: get(b, "STOCKITEMNAME"),
            Quantity: get(b, "BILLEDQTY"),
            Amount: get(b, "AMOUNT"),
            City: get(b, "PLACEOFSUPPLY"),
            Salesman: get(b, "BASICSALESNAME")
          });
        }

        const payload = { status: "ok", time: new Date().toISOString(), rows: vouchers };
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(JSON.stringify({
          success: true,
          message: "XML parsed and stored successfully",
          count: vouchers.length
        }), { headers: { "Content-Type": "application/json", ...cors } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message || "Processing failed" }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } });
      }
    }

    // serve latest data
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } });
      return new Response(data, { headers: { "Content-Type": "application/json", ...cors } });
    }

    // optional report
    if (url.pathname === "/api/reports/source" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      const json = data ? JSON.parse(data) : { rows: [] };
      return new Response(JSON.stringify({
        success: !!data,
        source: data ? "tally" : "none",
        message: data ? "Data loaded from Cloudflare KV" : "No data found",
        data: json
      }), { headers: { "Content-Type": "application/json", ...cors } });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  }
};
