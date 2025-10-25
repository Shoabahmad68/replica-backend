// replica-backend/index.js — final universal gzip-safe version
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (url.pathname === "/") return new Response("Replica Backend Active ✅", { headers: cors });

    // =============== GZIP DECOMPRESS HELPER =================
    async function decompressBase64Gzip(b64) {
      if (!b64 || typeof b64 !== "string") return "";
      if (b64.includes("<ENVELOPE")) return b64; // already XML
      try {
        const bin = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
        const ds = new DecompressionStream("gzip");
        const decompressed = await new Response(new Blob([bin]).stream().pipeThrough(ds)).text();
        return decompressed;
      } catch {
        try {
          // fallback for older runtimes
          const binary = atob(b64);
          const text = new TextDecoder().decode(Uint8Array.from(binary, c => c.charCodeAt(0)));
          return text.includes("<ENVELOPE") ? text : "";
        } catch {
          return "";
        }
      }
    }

    // =============== LIGHTWEIGHT XML PARSER =================
    const getField = (src, tag) => {
      if (!src) return "";
      const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
      const m = src.match(re);
      return m ? m[1].trim() : "";
    };
    const getAny = (src, tags) => {
      for (const t of tags) {
        const v = getField(src, t);
        if (v) return v;
      }
      return "";
    };
    const parseNumber = (s) => {
      if (!s) return 0;
      const cleaned = String(s).replace(/,/g, "").replace(/[^\d.\-]/g, "");
      const n = parseFloat(cleaned);
      return isNaN(n) ? 0 : n;
    };
    const parseXML = (xml) => {
      if (!xml || !xml.includes("<VOUCHER")) return [];
      const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
      const rows = [];
      for (const v of vouchers) {
        const amtRaw = getAny(v, ["AMOUNT", "BILLEDAMOUNT"]);
        let amt = parseNumber(amtRaw);
        const deem = (getField(v, "ISDEEMEDPOSITIVE") || "").toLowerCase();
        if (deem === "yes" && amt > 0) amt = -amt;

        const r = {
          "Voucher Type": getAny(v, ["VOUCHERTYPENAME", "VCHTYPE"]),
          Date: getAny(v, ["DATE", "VCHDATE"]),
          Party: getAny(v, ["PARTYNAME", "LEDGERNAME"]),
          Item: getAny(v, ["STOCKITEMNAME", "ITEMNAME"]),
          Qty: getAny(v, ["BILLEDQTY", "ACTUALQTY"]),
          Amount: amt,
          State: getAny(v, ["PLACEOFSUPPLY", "STATENAME"]),
          Salesman: getAny(v, ["BASICSALESNAME", "SALESMANNAME"]),
        };
        const keep = Object.values(r).some(v => String(v || "").trim() !== "" && v !== 0);
        if (keep) rows.push(r);
      }
      return rows;
    };

    // =============== PUSH ENDPOINT =================
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();

        // decompress every field safely
        const [salesXml, purchaseXml, mastersXml] = await Promise.all([
          decompressBase64Gzip(body.salesXml || ""),
          decompressBase64Gzip(body.purchaseXml || ""),
          decompressBase64Gzip(body.mastersXml || ""),
        ]);

        const parsed = {
          sales: parseXML(salesXml),
          purchase: parseXML(purchaseXml),
          masters: parseXML(mastersXml),
        };

        const header = {
          "Voucher Type": "Voucher Type",
          Date: "Date",
          Party: "Party",
          Item: "Item",
          Qty: "Qty",
          Amount: "Amount",
          State: "State",
          Salesman: "Salesman",
        };

        const mkCategory = (arr) => (arr.length ? [header, ...arr] : []);
        const rows = {
          sales: mkCategory(parsed.sales),
          purchase: mkCategory(parsed.purchase),
          masters: mkCategory(parsed.masters),
        };

        const flatRows = [...rows.sales, ...rows.purchase, ...rows.masters];

        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows,
          flatRows,
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));
        return new Response(JSON.stringify({ success: true, total: flatRows.length }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // =============== IMPORT ENDPOINT =================
    if (url.pathname === "/api/imports/latest") {
      const raw = await env.REPLICA_DATA.get("latest_tally_json");
      if (!raw)
        return new Response(JSON.stringify({ status: "empty", rows: {}, flatRows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      return new Response(raw, { headers: { "Content-Type": "application/json", ...cors } });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
