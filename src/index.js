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

    // ✅ PERFECT PUSH ENDPOINT - Exact field names matching Pusher.js
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();
        console.log("📥 Received data from Pusher.js");

        // ✅ EXACT FIELD NAMES MATCHING PUSHER.JS
        const receivedData = {
          salesXml: body.salesXml || "",
          purchaseXml: body.purchaseXml || "", 
          receiptXml: body.receiptXml || "",
          paymentXml: body.paymentXml || "",
          journalXml: body.journalXml || "",
          debitXml: body.debitXml || "",
          creditXml: body.creditXml || "",
          mastersXml: body.mastersXml || "",
          outstandingXml: body.outstandingXml || ""
        };

        console.log("📊 Data received with fields:", Object.keys(receivedData));

        // Create success response
        const successData = {
          status: "ok",
          source: body.source || "tally-pusher-final",
          time: body.time || new Date().toISOString(),
          message: "Data received successfully",
          receivedFields: Object.keys(receivedData).filter(key => receivedData[key])
        };

        // Save to KV
        await env.REPLICA_DATA.put("latest_tally_data", JSON.stringify(successData));
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify({
          storedAt: new Date().toISOString(),
          source: body.source || "tally-pusher-final",
          status: "data_available",
          counts: {
            sales: receivedData.salesXml ? 1 : 0,
            purchase: receivedData.purchaseXml ? 1 : 0,
            receipt: receivedData.receiptXml ? 1 : 0,
            payment: receivedData.paymentXml ? 1 : 0,
            journal: receivedData.journalXml ? 1 : 0,
            debit: receivedData.debitXml ? 1 : 0,
            credit: receivedData.creditXml ? 1 : 0,
            masters: receivedData.mastersXml ? 1 : 0,
            outstanding: receivedData.outstandingXml ? 1 : 0
          }
        }));
        
        console.log("✅ Data stored in KV successfully");
        
        return Response.json({
          success: true,
          message: "Data received and stored successfully",
          receivedFields: successData.receivedFields,
          time: successData.time
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

    // ✅ SIMPLE FETCH ENDPOINT
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
          counts: metaJson.counts,
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
