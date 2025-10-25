// replica-backend/index.js — Complete fixed worker
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

    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json", ...cors },
      });

    // util: attempt to decompress base64-gzip, fallback to raw
    async function tryDecompressMaybe(b64OrXml) {
      if (!b64OrXml) return "";
      // If already looks like XML, return as-is
      if (typeof b64OrXml === "string" && b64OrXml.includes("<ENVELOPE")) return b64OrXml;
      try {
        // convert base64 -> Uint8Array
        const binary = Uint8Array.from(atob(String(b64OrXml)), (c) => c.charCodeAt(0));
        // Make a stream, pipe through DecompressionStream (gzip)
        const blob = new Blob([binary]);
        const ds = blob.stream().pipeThrough(new DecompressionStream("gzip"));
        const text = await new Response(ds).text();
        if (text && text.includes("<ENVELOPE")) return text;
        // if decompressed text doesn't look like XML, fallback to raw input interpreted as text
      } catch (e) {
        // ignore and try fallback
      }
      // fallback: maybe input was not base64 but url-encoded or raw; return original string
      return String(b64OrXml || "");
    }

    // safe field extractors (from your original)
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
      if (s == null) return 0;
      try {
        const cleaned = String(s).replace(/,/g, "").replace(/[^\d\.\-]/g, "");
        const n = parseFloat(cleaned);
        return Number.isNaN(n) ? 0 : n;
      } catch {
        return 0;
      }
    };

    // robust voucher parser
    const parseXML = (xml) => {
      if (!xml || typeof xml !== "string" || !xml.includes("<ENVELOPE")) return [];
      const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
      const rows = [];
      for (const v of vouchers) {
        const amtRaw = getAny(v, ["AMOUNT", "BILLEDAMOUNT", "AMT"]);
        let amt = parseNumber(amtRaw || "0");
        const deem = (getField(v, "ISDEEMEDPOSITIVE") || "").toLowerCase();
        if (deem === "yes" && amt > 0) amt = -amt;

        const row = {
          "Voucher Type": getAny(v, ["VOUCHERTYPENAME", "VCHTYPE", "VOUCHERNAME"]),
          Date: getAny(v, ["DATE", "VCHDATE"]),
          Party: getAny(v, ["PARTYNAME", "LEDGERNAME"]),
          Item: getAny(v, ["STOCKITEMNAME", "ITEMNAME"]),
          Qty: getAny(v, ["BILLEDQTY", "ACTUALQTY", "QTY"]),
          Amount: amt,
          State: getAny(v, ["PLACEOFSUPPLY", "STATENAME"]),
          Salesman: getAny(v, ["BASICSALESNAME", "SALESMANNAME"]),
        };

        const hasMeaning = Object.values(row).some((val) => {
          if (val === null || val === undefined) return false;
          if (typeof val === "number") return val !== 0;
          const s = String(val).trim();
          if (!s) return false;
          if (/voucher ?type/i.test(s) && s.length < 30) return false;
          if (/date/i.test(s) && s.length < 6) return false;
          return true;
        });

        if (hasMeaning) rows.push(row);
      }
      return rows;
    };

    // ----- PUSH endpoint -----
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response(JSON.stringify({ error: "Invalid content type" }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...cors },
          });

        const body = await request.json();

        // support both compressed base64 fields and raw xml fields
        const rawSales = body.salesXml || body.sales || "";
        const rawPurchase = body.purchaseXml || body.purchase || "";
        const rawMasters = body.mastersXml || body.masters || "";
        const rawOutstanding = body.outstandingXml || body.outstanding || "";

        // decompress/normalize
        const [salesXml, purchaseXml, mastersXml, outstandingXml] = await Promise.all([
          tryDecompressMaybe(rawSales),
          tryDecompressMaybe(rawPurchase),
          tryDecompressMaybe(rawMasters),
          tryDecompressMaybe(rawOutstanding),
        ]);

        // parse
        const parsed = {
          sales: parseXML(salesXml),
          purchase: parseXML(purchaseXml),
          masters: parseXML(mastersXml),
          outstanding: parseXML(outstandingXml),
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
        const mkCategory = (arr) => (arr && arr.length ? [header, ...arr] : []);

        const rowsForKV = {
          sales: mkCategory(parsed.sales),
          purchase: mkCategory(parsed.purchase),
          masters: mkCategory(parsed.masters),
          outstanding: mkCategory(parsed.outstanding),
        };

        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: rowsForKV,
          flatRows: [
            ...(rowsForKV.sales || []),
            ...(rowsForKV.purchase || []),
            ...(rowsForKV.masters || []),
            ...(rowsForKV.outstanding || []),
          ],
          meta: {
            source: "tally-pusher",
            originalSizes: {
              salesRaw: (rawSales || "").length,
              purchaseRaw: (rawPurchase || "").length,
              mastersRaw: (rawMasters || "").length,
            },
          },
        };

        // save to KV (string)
        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(
          JSON.stringify({
            success: true,
            message: "Tally data saved successfully",
            total:
              (parsed.sales?.length || 0) +
              (parsed.purchase?.length || 0) +
              (parsed.masters?.length || 0) +
              (parsed.outstanding?.length || 0),
          }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      } catch (err) {
        return new Response(JSON.stringify({ error: err?.message || String(err) }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // ----- IMPORT endpoint -----
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        const raw = await env.REPLICA_DATA.get("latest_tally_json");
        if (!raw)
          return new Response(JSON.stringify({ status: "empty", rows: [], flatRows: [] }), {
            headers: { "Content-Type": "application/json", ...cors },
          });

        let data = JSON.parse(raw);
        if (typeof data.rows === "string") {
          try {
            data.rows = JSON.parse(data.rows);
          } catch {
            /* ignore */
          }
        }
        if (!data.flatRows && data.rows) {
          data.flatRows = [
            ...(data.rows.sales || []),
            ...(data.rows.purchase || []),
            ...(data.rows.masters || []),
            ...(data.rows.outstanding || []),
          ];
        }

        return new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json", ...cors } });
      } catch (e) {
        return new Response(JSON.stringify({ status: "corrupt", rows: [], flatRows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
