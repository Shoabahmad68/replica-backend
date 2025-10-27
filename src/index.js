// ✅ index.js — FULL FIXED BACKEND (for latest_tally_json only)
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // Root test
    if (url.pathname === "/")
      return new Response("Replica Unified Backend Active ✅", { headers: cors });

    // -------------------- TEST ROUTE --------------------
    if (url.pathname === "/api/test") {
      return new Response(
        JSON.stringify({
          status: "ok",
          message: "Backend Live",
          time: new Date().toISOString(),
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );
    }

    // -------------------- MAIN PUSH ENDPOINT --------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid Content-Type", { status: 400, headers: cors });

        const data = await request.json();
        const jsonStr = JSON.stringify(data);
        const sizeMB = (Buffer.byteLength(jsonStr) / 1024 / 1024).toFixed(2);

        if (sizeMB < 0.05)
          return new Response("Empty or invalid data", { status: 400, headers: cors });

        // 🔹 Store ONLY one unified key in KV
        await env.REPLICA_DATA.put("latest_tally_json", jsonStr, {
          expirationTtl: 60 * 60 * 24 * 7, // keep 7 days
        });

        return new Response(
          JSON.stringify({
            success: true,
            message: "Tally full payload stored successfully.",
            sizeMB,
            keys: Object.keys(data),
            time: new Date().toISOString(),
          }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      } catch (err) {
        return new Response(
          JSON.stringify({ error: err.message || "Processing failed" }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // -------------------- FETCH LATEST DATA ENDPOINT --------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const kvValue = await env.REPLICA_DATA.get("latest_tally_json");
      if (!kvValue) {
        return new Response(
          JSON.stringify({ status: "empty", message: "No data found." }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      }

      let parsed;
      try {
        parsed = JSON.parse(kvValue);
      } catch {
        parsed = { raw: kvValue };
      }

      return new Response(JSON.stringify(parsed), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    // -------------------- 404 DEFAULT --------------------
    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
