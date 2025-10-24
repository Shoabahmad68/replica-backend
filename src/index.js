// index.js — Corrected unified backend with flatRows support
export default {
  async fetch(request, env, ctx) {
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
      return new Response(
        JSON.stringify({ status: "ok", time: new Date().toISOString() }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    // ------------ Main Upload Handler ------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid content-type", {
            status: 400,
            headers: cors,
          });

        const body = await request.json();
        const xmlBlocks = {
          salesXml: body.salesXml || "",
          purchaseXml: body.purchaseXml || "",
          mastersXml: body.mastersXml || "",
          outstandingXml: body.outstandingXml || "",
        };

        const tags = ["VOUCHER", "LEDGER", "STOCKITEM", "GROUP"];
        const parseXML = (xml) => {
          const rows = [];
          if (!xml || !xml.includes("<ENVELOPE>")) return rows;
          const getVal = (b, t) => {
            const m = b.match(new RegExp(`<${t}>([\\s\\S]*?)<\\/${t}>`, "i"));
            return m ? m[1].trim() : "";
          };
          for (const tag of tags) {
            const blocks = xml.match(
              new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi")
            );
            if (blocks) {
              for (const b of blocks) {
                let amt = parseFloat(getVal(b, "AMOUNT") || "0");
                if (getVal(b, "ISDEEMEDPOSITIVE") === "Yes" && amt > 0)
                  amt = -amt;
                rows.push({
                  "Voucher Type": getVal(b, "VOUCHERTYPENAME"),
                  Date: getVal(b, "DATE"),
                  Party: getVal(b, "PARTYNAME"),
                  Item: getVal(b, "STOCKITEMNAME"),
                  Qty: getVal(b, "BILLEDQTY"),
                  Amount: amt,
                  State: getVal(b, "PLACEOFSUPPLY"),
                  Salesman: getVal(b, "BASICSALESNAME"),
                });
              }
            }
          }
          return rows;
        };

        // parse all
        const parsed = {
          sales: parseXML(xmlBlocks.salesXml),
          purchase: parseXML(xmlBlocks.purchaseXml),
          masters: parseXML(xmlBlocks.mastersXml),
          outstanding: parseXML(xmlBlocks.outstandingXml),
        };

        const blank = {};
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

        // Correct structure (object, not string)
        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: {
            sales: [blank, header, ...parsed.sales],
            purchase: [blank, header, ...parsed.purchase],
            masters: [blank, header, ...parsed.masters],
            outstanding: [blank, header, ...parsed.outstanding],
          },
        };

        // backward compatibility for frontend
        payload.flatRows = [
          ...payload.rows.sales,
          ...payload.rows.purchase,
          ...payload.rows.masters,
          ...payload.rows.outstanding,
        ];

        await env.REPLICA_DATA.put(
          "latest_tally_json",
          JSON.stringify(payload)
        );

        return new Response(
          JSON.stringify({
            success: true,
            message: "Data stored in correct format",
            total:
              parsed.sales.length +
              parsed.purchase.length +
              parsed.masters.length +
              parsed.outstanding.length,
          }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      } catch (err) {
        return new Response(
          JSON.stringify({ error: err.message }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // ------------ Fetch Latest Data ------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(
          JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      return new Response(data, {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
