export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    };

    if (request.method === "OPTIONS")
      return new Response(null, { headers: cors });

    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({
        status: "success",
        message: "Backend connected successfully",
        time: new Date().toISOString()
      }), { headers: { "Content-Type": "application/json", ...cors } });

    // Receive data from Tally Pusher
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const xml = await request.text();
        if (!xml.includes("<ENVELOPE>"))
          return new Response(JSON.stringify({ error: "Invalid XML" }),
            { status: 400, headers: { "Content-Type": "application/json", ...cors } });

        // Regex parsing for VOUCHER blocks
        const vouchers = [];
        const blocks = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
        const get = (b, tag) => {
          const m = b.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return m ? m[1].trim() : "";
        };

        for (const b of blocks) {
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

    // Serve latest data
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } });
      return new Response(data, { headers: { "Content-Type": "application/json", ...cors } });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  }
};
