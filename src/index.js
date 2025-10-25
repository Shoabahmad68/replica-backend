// index.js — force show all KV content raw in frontend
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // ✅ 1. root check
    if (url.pathname === "/")
      return new Response("Replica RAW Backend Active ✅", { headers: cors });

    // ✅ 2. save from pusher.js (as-is)
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.text();
      await env.REPLICA_DATA.put("latest_tally_raw", body);
      return new Response(JSON.stringify({ ok: true, size: body.length }), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    // ✅ 3. frontend fetch (return full raw)
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      let raw = await env.REPLICA_DATA.get("latest_tally_raw");
      if (!raw)
        raw = await env.REPLICA_DATA.get("latest_tally_json");
      if (!raw)
        return new Response(JSON.stringify({ status: "empty", rows: [], flatRows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });

      // show full raw as table rows
      const chunk = raw.slice(0, 100000); // prevent browser freeze
      const rows = [{ RAW_DATA: chunk }];
      return new Response(
        JSON.stringify({
          status: "ok",
          rows: { sales: [], purchase: [], masters: [], outstanding: [] },
          flatRows: rows,
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
