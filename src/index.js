export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: cors });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // ✅ Health check
      if (path === "/") {
        return new Response("Replica Backend Active ✅", { headers: cors });
      }

      // ✅ Push from Tally pusher
      if (path === "/api/push/tally" && request.method === "POST") {
        const body = await request.text();
        await env.REPLICA_DATA.put("latest_tally_raw", body);
        return new Response(
          JSON.stringify({ status: "ok", saved: body.length }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      }

      // ✅ Read + parse + send
      if (path === "/api/imports/latest") {
        const raw = await env.REPLICA_DATA.get("latest_tally_raw");
        if (!raw) {
          return new Response(JSON.stringify({ status: "empty", rows: [] }), {
            headers: { "Content-Type": "application/json", ...cors },
          });
        }

        let parsed;
        try {
          parsed = JSON.parse(raw);
        } catch {
          parsed = { rawText: raw };
        }

        // ✅ Decompress helper
        async function decompressBase64(b64) {
          try {
            const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
            const ds = new DecompressionStream("gzip");
            const ab = await new Response(new Blob([bin]).stream().pipeThrough(ds)).arrayBuffer();
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
            const pick = (t) => {
              const m = v.match(new RegExp(`<${t}[^>]*>([\\s\\S]*?)<\\/${t}>`, "i"));
              return m ? m[1].trim() : "";
            };
            return {
              VoucherType: pick("VOUCHERTYPENAME"),
              Date: pick("DATE"),
              Party: pick("PARTYNAME"),
              Item: pick("STOCKITEMNAME"),
              Amount: pick("AMOUNT"),
            };
          });
        }

        const rows = [
          ...parseXML(salesXml),
          ...parseXML(purchaseXml),
          ...parseXML(mastersXml),
        ];

        return new Response(
          JSON.stringify({ status: "ok", rows }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      }

      return new Response("Not Found", { status: 404, headers: cors });
    } catch (e) {
      return new Response(
        JSON.stringify({ status: "error", message: e.message }),
        { status: 500, headers: { "Content-Type": "application/json", ...cors } }
      );
    }
  },
};
