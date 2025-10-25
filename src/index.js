// replica-backend/index.js — decompress + parse XML to table
import { gunzipSync } from "zlib";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // ✅ Home
    if (url.pathname === "/")
      return new Response("Replica Backend Active ✅", { headers: cors });

    // ✅ Receive data from pusher
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      const body = await request.text();
      await env.REPLICA_DATA.put("latest_tally_raw", body);
      return new Response(JSON.stringify({ ok: true, saved: body.length }), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    // ✅ Provide structured data to frontend
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const raw = await env.REPLICA_DATA.get("latest_tally_raw");
      if (!raw)
        return new Response(
          JSON.stringify({ status: "empty", rows: [], flatRows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );

      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        data = { raw };
      }

      // Decode + decompress XML
      const decodeGzip = (base64str) => {
        try {
          const buf = Buffer.from(base64str, "base64");
          return gunzipSync(buf).toString("utf8");
        } catch {
          return "";
        }
      };

      const salesXml = decodeGzip(data.salesXml || "");
      const purchaseXml = decodeGzip(data.purchaseXml || "");
      const mastersXml = decodeGzip(data.mastersXml || "");

      // XML to table rows
      const extractTag = (xml, tag) => {
        const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "gi");
        const matches = [];
        let m;
        while ((m = regex.exec(xml))) matches.push(m[1].trim());
        return matches;
      };

      const parseXML = (xml) => {
        if (!xml.includes("<VOUCHER")) return [];
        const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
        const rows = [];
        for (const v of vouchers) {
          const row = {
            Date: extractTag(v, "DATE")[0] || "",
            Party: extractTag(v, "PARTYNAME")[0] || "",
            Amount: extractTag(v, "AMOUNT")[0] || "",
            Item: extractTag(v, "STOCKITEMNAME")[0] || "",
            VoucherType: extractTag(v, "VOUCHERTYPENAME")[0] || "",
          };
          if (Object.values(row).some((x) => x)) rows.push(row);
        }
        return rows;
      };

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
