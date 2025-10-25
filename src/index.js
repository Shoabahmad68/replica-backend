export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "*",
    };

    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: cors });
    }

    try {
      const url = new URL(request.url);
      const path = url.pathname;

      // Health check
      if (path === "/") {
        return new Response("Replica Backend Active ✅", { headers: cors });
      }

      // Data fetch
      if (path === "/api/imports/latest") {
        // try both keys
        const data =
          (await env.REPLICA_DATA.get("latest_tally_json")) ||
          (await env.REPLICA_DATA.get("latest_tally_raw"));

        if (!data) {
          return new Response(
            JSON.stringify({ status: "empty", rows: [] }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );
        }

        let parsed;
        try {
          parsed = JSON.parse(data);
        } catch {
          parsed = { raw: data };
        }

        return new Response(JSON.stringify(parsed), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      }

      // Push endpoint
      if (path === "/api/push/tally" && request.method === "POST") {
        const body = await request.text();
        await env.REPLICA_DATA.put("latest_tally_json", body);
        return new Response(
          JSON.stringify({ status: "ok", saved: true }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      }

      // Fallback
      return new Response("Not Found", { status: 404, headers: cors });
    } catch (err) {
      return new Response(
        JSON.stringify({ status: "error", message: err.message }),
        { status: 500, headers: { "Content-Type": "application/json", ...cors } }
      );
    }
  },
};
