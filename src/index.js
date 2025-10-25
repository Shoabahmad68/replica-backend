export default {
  async fetch(request, env) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      const url = new URL(request.url);
      const path = url.pathname;

      // ✅ Health check
      if (path === "/")
        return new Response("Replica Backend Active ✅", { headers: corsHeaders });

      // ✅ Push data from Tally pusher
      if (path === "/api/push/tally" && request.method === "POST") {
        const body = await request.text();
        await env.REPLICA_DATA.put("latest_tally_raw", body);
        return new Response(
          JSON.stringify({ status: "ok", saved: body.length }),
          { headers: { "Content-Type": "application/json", ...corsHeaders } }
        );
      }

      // ✅ Fetch latest imports
      if (path === "/api/imports/latest") {
        const raw = await env.REPLICA_DATA.get("latest_tally_raw");
        if (!raw) {
          return new Response(
            JSON.stringify({ status: "empty", rows: [] }),
            { headers: { "Content-Type": "application/json", ...corsHeaders } }
          );
        }

        let parsed = {};
        try {
          parsed = JSON.parse(raw);
        } catch {
          return new Response(
            JSON.stringify({ status: "corrupt", rows: [] }),
            { headers: { "Content-Type": "application/json", ...corsHeaders } }
          );
        }

        async function decompressBase64(b64) {
          try {
            const binary = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
            const ds = new DecompressionStream("gzip");
            const ab = await new Response(new Blob([binary]).stream().pipeThrough(ds)).arrayBuffer();
            return new TextDecoder().decode(ab);
          } catch {
            return "";
          }
        }

        const salesXml = parsed.salesXml ? await decompressBase64(parsed.salesXml) : "";
        const purchaseXml = parsed.purchaseXml ? await decompressBase64(parsed.purchaseXml) : "";
        const mastersXml = parsed.mastersXml ? await decompressBase64(parsed.mastersXml) : "";

        function parseXML(xml) {
          if (!xml.includes("<VOUCHER")) return [];
          const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
          return vouchers.map((v) => {
            const extract = (t) => {
              const m = v.match(new RegExp(`<${t}[^>]*>([\\s\\S]*?)<\\/${t}>`, "i"));
              return m ? m[1].trim() : "";
            };
            return {
              VoucherType: extract("VOUCHERTYPENAME"),
              Date: extract("DATE"),
              Party: extract("PARTYNAME"),
              Item: extract("STOCKITEMNAME"),
              Amount: extract("AMOUNT"),
            };
          });
        }

        const rows = [
          ...parseXML(salesXml),
          ...parseXML(purchaseXml),
          ...parseXML(mastersXml),
        ];

        return new Response(
          JSON.stringify({ status: "ok", count: rows.length, rows }),
          { headers: { "Content-Type": "application/json", ...corsHeaders } }
        );
      }

      // 404 fallback
      return new Response("Not Found", { status: 404, headers: corsHeaders });
    } catch (e) {
      return new Response(
        JSON.stringify({ status: "error", message: e.message }),
        { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }
  },
};
