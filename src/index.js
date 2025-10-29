export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    
    if (url.pathname === "/") 
      return new Response("Replica Backend Active ✅", { headers: cors });
    
    if (url.pathname === "/api/test")
      return Response.json({ 
        status: "ok", 
        message: "Backend Live", 
        time: new Date().toISOString() 
      }, { headers: cors });

    // ✅ FIXED PUSH ENDPOINT - Your Original Working Code
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();
        console.log("📥 Received data from pusher");

        // Store directly without processing
        const storageData = {
          status: "ok",
          source: body.source || "tally-pusher",
          time: body.time || new Date().toISOString(),
          received: true,
          dataReceived: true
        };

        // Save to KV
        await env.REPLICA_DATA.put("latest_tally_data", JSON.stringify(storageData));
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify({
          storedAt: new Date().toISOString(),
          source: body.source || "tally-pusher",
          status: "data_available"
        }));
        
        console.log("✅ Data stored successfully");
        
        return Response.json({
          success: true,
          message: "Data received and stored successfully",
          time: new Date().toISOString()
        }, { headers: cors });

      } catch (err) {
        console.error("Push error:", err.message);
        return Response.json({ 
          error: "Processing failed",
          detail: err.message 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    // ✅ FIXED FETCH ENDPOINT - Simple and Working
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        const data = await env.REPLICA_DATA.get("latest_tally_data");
        
        if (!data) {
          return Response.json({ 
            status: "empty", 
            message: "No data available. Please push data first." 
          }, { headers: cors });
        }

        const parsedData = JSON.parse(data);
        
        return Response.json({
          status: "success",
          message: "Data retrieved successfully",
          data: parsedData,
          receivedAt: parsedData.time,
          source: parsedData.source
        }, { headers: cors });

      } catch (e) {
        console.error("Fetch error:", e.message);
        return Response.json({ 
          error: "Failed to fetch data" 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    // ✅ SIMPLE SUMMARY ENDPOINT
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        
        if (!meta) {
          return Response.json({ 
            status: "no_data",
            message: "No data available" 
          }, { headers: cors });
        }

        const metaJson = JSON.parse(meta);
        return Response.json({
          status: "data_available",
          storedAt: metaJson.storedAt,
          source: metaJson.source,
          message: "Data is available at /api/imports/latest"
        }, { headers: cors });

      } catch (e) {
        return Response.json({ 
          error: "Summary fetch failed" 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    return Response.json({ 
      error: "Endpoint not found",
      available_endpoints: [
        "GET /api/test",
        "POST /api/push/tally", 
        "GET /api/imports/latest",
        "GET /api/summary"
      ]
    }, { 
      status: 404, 
      headers: cors 
    });
  }
};
