export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS")
      return new Response(null, { headers: cors });

    if (url.pathname === "/")
      return new Response("Replica Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.json();
      await env.REPLICA_DATA.put("latest_data", JSON.stringify(body));
      return Response.json({ success: true, stored: Object.keys(body.allData) }, { headers: cors });
    }

    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_data");
      if (!data)
        return Response.json({ status: "empty" }, { headers: cors });
      const parsed = JSON.parse(data);
      return Response.json(parsed, { headers: cors });
    }

    return new Response("Not found", { status: 404, headers: cors });
  }
};
