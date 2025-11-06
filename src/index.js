export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    const url = new URL(request.url);

    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.json();
      const data = body.data || {};

      const storeData = {
        storedAt: new Date().toISOString(),
        source: body.source || "tally-odbc",
        total: Object.values(data).reduce((a, b) => a + (b.length || 0), 0),
        data
      };

      await env.REPLICA_DATA.put("latest_data", JSON.stringify(storeData));

      return Response.json({
        success: true,
        message: "Tally ODBC JSON stored successfully",
        totalRecords: storeData.total,
        storedAt: storeData.storedAt
      }, { headers: cors });
    }

    if (url.pathname === "/api/imports/latest") {
      const data = await env.REPLICA_DATA.get("latest_data");
      return Response.json(data ? JSON.parse(data) : { status: "empty" }, { headers: cors });
    }

    if (url.pathname === "/api/summary") {
      const data = await env.REPLICA_DATA.get("latest_data");
      if (!data) return Response.json({ status: "no_data" }, { headers: cors });
      const parsed = JSON.parse(data);
      return Response.json({
        status: "available",
        storedAt: parsed.storedAt,
        source: parsed.source,
        total: parsed.total
      }, { headers: cors });
    }

    return Response.json({ message: "Replica Backend Active" }, { headers: cors });
  }
};
