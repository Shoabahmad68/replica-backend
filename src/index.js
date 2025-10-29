export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    
    // Root endpoint - quick response
    if (url.pathname === "/") 
      return new Response("Replica Unified Backend Active ✅", { headers: cors });
    
    // Test endpoint
    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({ 
        status: "ok", 
        message: "Backend Live", 
        time: new Date().toISOString() 
      }), {
        headers: { "Content-Type": "application/json", ...cors },
      });

    // ------------------ FIXED FETCH ENDPOINT ------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        console.log("📥 Fetching latest data from KV...");
        
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        if (!meta) {
          return new Response(JSON.stringify({ 
            status: "empty", 
            message: "No data available" 
          }), { 
            headers: { "Content-Type": "application/json", ...cors } 
          });
        }

        const metaJson = JSON.parse(meta);
        console.log("📊 Metadata found:", metaJson);

        // If data is chunked, assemble it
        if (metaJson.parts && Number.isInteger(metaJson.parts) && metaJson.parts > 0) {
          let merged = "";
          for (let i = 0; i < metaJson.parts; i++) {
            const partKey = `latest_tally_json_part_${i}`;
            const part = await env.REPLICA_DATA.get(partKey);
            if (part) {
              merged += part; // ✅ FIXED: += instead of !=
              console.log(`✅ Loaded part ${i}: ${part.length} chars`);
            } else {
              console.warn(`⚠️ Part ${i} not found`);
            }
          }
          
          if (merged) {
            console.log(`✅ Returning merged data: ${merged.length} chars`);
            return new Response(merged, { 
              headers: { "Content-Type": "application/json", ...cors } 
            });
          }
        }

        // fallback: return metadata if chunks not found
        return new Response(JSON.stringify(metaJson), { 
          headers: { "Content-Type": "application/json", ...cors } 
        });

      } catch (e) {
        console.error("❌ Fetch error:", e);
        return new Response(JSON.stringify({ 
          error: "Failed to fetch latest", 
          detail: e?.message || e 
        }), { 
          status: 500, 
          headers: { "Content-Type": "application/json", ...cors } 
        });
      }
    }

    // ------------------ PUSH ENDPOINT (Keep your existing code) ------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      // YOUR EXISTING PUSH CODE HERE - NO CHANGES NEEDED
      try {
        const body = await request.json();
        // ... [rest of your existing push code] ...
        
        // After processing, return success
        return new Response(JSON.stringify({ 
          success: true, 
          message: "Data stored successfully",
          counts: finalPayload.counts 
        }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
        
      } catch (err) {
        return new Response(JSON.stringify({ 
          error: err?.message || "Processing failed" 
        }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // Simple data summary endpoint
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        if (!meta) {
          return new Response(JSON.stringify({ 
            status: "no_data",
            message: "No data available yet" 
          }), { 
            headers: { "Content-Type": "application/json", ...cors } 
          });
        }

        const metaJson = JSON.parse(meta);
        return new Response(JSON.stringify({
          status: "data_available",
          storedAt: metaJson.storedAt,
          counts: metaJson.counts,
          parts: metaJson.parts
        }), { 
          headers: { "Content-Type": "application/json", ...cors } 
        });

      } catch (e) {
        return new Response(JSON.stringify({ 
          error: "Summary fetch failed" 
        }), { 
          status: 500, 
          headers: { "Content-Type": "application/json", ...cors } 
        });
      }
    }

    return new Response("404 Not Found - Use /api/imports/latest or /api/push/tally", { 
      status: 404, 
      headers: cors 
    });
  },
};
