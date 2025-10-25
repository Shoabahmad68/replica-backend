// replica-backend index.js — fixed: no header-only rows, robust parser
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

    if (url.pathname === "/")
      return new Response("Replica Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json", ...cors },
      });

    // ----- PUSH endpoint -----
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid content type", { status: 400, headers: cors });

        const body = await request.json();
        const xmlBlocks = {
          salesXml: body.salesXml || "",
          purchaseXml: body.purchaseXml || "",
          mastersXml: body.mastersXml || "",
          outstandingXml: body.outstandingXml || "",
        };

        // helper: safe field extractor
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
            // remove commas and non-number chars
            const cleaned = String(s).replace(/,/g, "").replace(/[^\d\.\-]/g, "");
            const n = parseFloat(cleaned);
            return Number.isNaN(n) ? 0 : n;
          } catch { return 0; }
        };

        // robust voucher parser
        const parseXML = (xml) => {
          if (!xml || typeof xml !== "string" || !xml.includes("<ENVELOPE>")) return [];
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

            // keep only if there's at least one meaningful field
            const hasMeaning = Object.values(row).some((val) => {
              if (val === null || val === undefined) return false;
              if (typeof val === "number") return val !== 0;
              const s = String(val).trim();
              if (!s) return false;
              // ignore template header strings (like "Voucher Type" that sometimes appear)
              if (/voucher ?type/i.test(s) && s.length < 30) return false;
              if (/date/i.test(s) && s.length < 6) return false;
              return true;
            });

            if (hasMeaning) rows.push(row);
          }
          return rows;
        };

        const parsed = {
          sales: parseXML(xmlBlocks.salesXml),
          purchase: parseXML(xmlBlocks.purchaseXml),
          masters: parseXML(xmlBlocks.mastersXml),
          outstanding: parseXML(xmlBlocks.outstandingXml),
        };

        // only add a header if that category has real rows (frontend expects consistent keys)
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
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(JSON.stringify({
          success: true,
          message: "Tally data saved successfully",
          total:
            (parsed.sales?.length || 0) +
            (parsed.purchase?.length || 0) +
            (parsed.masters?.length || 0) +
            (parsed.outstanding?.length || 0),
        }), { headers: { "Content-Type": "application/json", ...cors } });

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
        if (!raw) return new Response(JSON.stringify({ status: "empty", rows: [] }), {
          headers: { "Content-Type": "application/json", ...cors }
        });

        let data = JSON.parse(raw);
        // auto-fix if rows stored as string
        if (typeof data.rows === "string") {
          try { data.rows = JSON.parse(data.rows); } catch { /* ignore */ }
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
        return new Response(JSON.stringify({ status: "corrupt", rows: [] }), {
          headers: { "Content-Type": "application/json", ...cors }
        });
      }
    }

    // default
    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
