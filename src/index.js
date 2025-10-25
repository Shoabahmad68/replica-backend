// replica-backend/index.js — Raw Data Pass-Through (for debugging)
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (url.pathname === "/") return new Response("Replica Backend Active ✅", { headers: cors });

    // 🔹 /api/push/tally — just save whatever pusher sends
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.text(); // raw capture
        await env.REPLICA_DATA.put("latest_tally_raw", body);
        return new Response(JSON.stringify({ success: true, note: "Raw saved" }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // 🔹 /api/imports/latest — return raw saved data directly
    if (url.pathname === "/api/imports/latest") {
      const raw = await env.REPLICA_DATA.get("latest_tally_raw");
      if (!raw)
        return new Response(JSON.stringify({ status: "empty", rows: [], flatRows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });

      // Try to parse JSON if it looks like JSON; else wrap as text
      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        data = { raw };
      }

      // Wrap so frontend table can at least iterate keys/values
      const flatRows = [{ RAW_CONTENT: JSON.stringify(data).slice(0, 50000) }];
      return new Response(
        JSON.stringify({
          status: "ok",
          raw: true,
          flatRows,
          rows: { sales: [], purchase: [], masters: [], outstanding: [] },
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
