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

    // ✅ ULTRA SIMPLE PUSH ENDPOINT
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        console.log("📥 PUSH ENDPOINT CALLED");
        
        const body = await request.json();
        console.log("✅ Data received from Pusher");
        
        // Store simple confirmation
        const storageData = {
          status: "ok",
          source: body.source || "tally-pusher",
          time: new Date().toISOString(),
          message: "Data successfully received",
          data_size: JSON.stringify(body).length,
          fields_received: Object.keys(body)
        };

        // Save to KV
        await env.REPLICA_DATA.put("latest_data", JSON.stringify(storageData));
        await env.REPLICA_DATA.put("latest_meta", JSON.stringify({
          storedAt: new Date().toISOString(),
          status: "data_available",
          source: body.source || "tally-pusher"
        }));
        
        console.log("💾 Data saved to KV");
        
        return Response.json({
          success: true,
          message: "Data stored successfully",
          received_at: new Date().toISOString(),
          data_size: storageData.data_size
        }, { headers: cors });

      } catch (err) {
        console.error("❌ Push error:", err.message);
        return Response.json({ 
          error: "Failed to process data",
          detail: err.message 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    // ✅ ULTRA SIMPLE FETCH ENDPOINT
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        console.log("📤 FETCH ENDPOINT CALLED");
        
        const data = await env.REPLICA_DATA.get("latest_data");
        
        if (!data) {
          console.log("❌ No data in KV");
          return Response.json({ 
            status: "empty", 
            message: "No data available" 
          }, { headers: cors });
        }

        const parsedData = JSON.parse(data);
        console.log("✅ Data found in KV");
        
        return Response.json({
          status: "success",
          message: "Data retrieved successfully",
          data: parsedData,
          retrieved_at: new Date().toISOString()
        }, { headers: cors });

      } catch (e) {
        console.error("❌ Fetch error:", e.message);
        return Response.json({ 
          error: "Failed to fetch data" 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    // ✅ SIMPLE SUMMARY
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_meta");
        
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
          message: "Data is available"
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
      error: "Endpoint not found" 
    }, { 
      status: 404, 
      headers: cors 
    });
  }
};
