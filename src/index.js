export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // ✅ Home
    if (url.pathname === "/")
      return new Response("Replica Backend Active ✅", { headers: corsHeaders });

    // ✅ Save Raw Data
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.text();
      await env.REPLICA_DATA.put("latest_tally_raw", body);
      return new Response(
        JSON.stringify({ success: true, message: "Raw data saved." }),
        { headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    // ✅ Fetch Structured Data
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const raw = await env.REPLICA_DATA.get("latest_tally_raw");
      if (!raw)
        return new Response(
          JSON.stringify({ status: "empty", rows: [], flatRows: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders } }
        );

      let parsed;
      try {
        parsed = JSON.parse(raw);
      } catch {
        parsed = { raw };
      }

      async function decompressBase64(base64str) {
        try {
          const binary = Uint8Array.from(atob(base64str), (c) => c.charCodeAt(0));
          const ds = new DecompressionStream("gzip");
          const decompressed = await new Response(
            new Blob([binary]).stream().pipeThrough(ds)
          ).arrayBuffer();
          return new TextDecoder().decode(decompressed);
        } catch {
          return "";
        }
      }

      const salesXml = parsed.salesXml ? await decompressBase64(parsed.salesXml) : "";
      const purchaseXml = parsed.purchaseXml ? await decompressBase64(parsed.purchaseXml) : "";
      const mastersXml = parsed.mastersXml ? await decompressBase64(parsed.mastersXml) : "";

      function extractTag(xml, tag) {
        const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "gi");
        const result = [];
        let m;
        while ((m = re.exec(xml))) result.push(m[1].trim());
        return result;
      }

      function parseXML(xml) {
        if (!xml || !xml.includes("<VOUCHER")) return [];
        const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
        const rows = [];
        for (const v of vouchers) {
          const row = {
            Date: extractTag(v, "DATE")[0] || "",
            Party: extractTag(v, "PARTYNAME")[0] || "",
            Item: extractTag(v, "STOCKITEMNAME")[0] || "",
            Amount: extractTag(v, "AMOUNT")[0] || "",
            VoucherType: extractTag(v, "VOUCHERTYPENAME")[0] || "",
          };
          if (Object.values(row).some(Boolean)) rows.push(row);
        }
        return rows;
      }

      const rows = {
        sales: parseXML(salesXml),
        purchase: parseXML(purchaseXml),
        masters: parseXML(mastersXml),
      };

      const flatRows = [
        ...(rows.sales || []),
        ...(rows.purchase || []),
        ...(rows.masters || []),
      ];

      const payload = {
        status: "ok",
        time: new Date().toISOString(),
        rows,
        flatRows,
      };

      await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

      return new Response(JSON.stringify(payload), {
        headers: { "Content-Type": "application/json", ...corsHeaders },
      });
    }

    return new Response("404 Not Found", { status: 404, headers: corsHeaders });
  },
};
