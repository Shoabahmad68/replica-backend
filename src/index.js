// replica-backend/index.js — Cloudflare Workers compatible decompression + XML parse
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

    // ✅ test
    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    // ✅ save incoming raw data
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.text();
      await env.REPLICA_DATA.put("latest_tally_raw", body);
      return new Response(JSON.stringify({ ok: true, saved: body.length }), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    // ✅ fetch and decompress + parse
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const raw = await env.REPLICA_DATA.get("latest_tally_raw");
      if (!raw)
        return new Response(JSON.stringify({ status: "empty", rows: [], flatRows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });

      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        data = { raw };
      }

      // Cloudflare Workers में gzip decompress करने का helper
      async function decompressBase64(base64str) {
        try {
          const bin = Uint8Array.from(atob(base64str), (c) => c.charCodeAt(0));
          const ds = new DecompressionStream("gzip");
          const decompressed = await new Response(
            new Blob([bin]).stream().pipeThrough(ds)
          ).arrayBuffer();
          return new TextDecoder().decode(decompressed);
        } catch (e) {
          return "";
        }
      }

      const salesXml = await decompressBase64(data.salesXml || "");
      const purchaseXml = await decompressBase64(data.purchaseXml || "");
      const mastersXml = await decompressBase64(data.mastersXml || "");

      // XML to JSON rows
      function extractTag(xml, tag) {
        const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "gi");
        const out = [];
        let m;
        while ((m = re.exec(xml))) out.push(m[1].trim());
        return out;
      }

      function parseXML(xml) {
        if (!xml.includes("<VOUCHER")) return [];
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
          if (Object.values(row).some((x) => x)) rows.push(row);
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
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
