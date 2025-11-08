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

    // Receive new data from Tally connector
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.json();
      let existing = await env.REPLICA_DATA.get("latest_data");
      let newData = {};

      try {
        if (existing) newData = JSON.parse(existing);
      } catch (e) {}

      // Auto-append or update incoming data
      newData = {
        ...newData,
        company: body.company,
        lastSync: new Date().toISOString(),
        allData: {
          ...newData.allData,
          ...body.allData
        }
      };

      await env.REPLICA_DATA.put("latest_data", JSON.stringify(newData));
      return Response.json({ success: true, stored: Object.keys(newData.allData) }, { headers: cors });
    }

    // Fetch latest synced data
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
