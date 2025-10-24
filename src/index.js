// index.js — Backend output in Excel-style format (row1 blank, row2 header)
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (url.pathname === "/") return new Response("Replica Backend Active ✅", { headers: cors });

    // Test API
    if (url.pathname === "/api/test")
      return new Response(
        JSON.stringify({ status: "ok", time: new Date().toISOString() }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    // Main API — data receiver
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid type", { status: 400, headers: cors });

        const body = await request.json();
        const xml = body.salesXml || "";
        const rows = [];

        if (xml && xml.includes("<VOUCHER")) {
          const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];

          for (const v of vouchers) {
            const get = (tag) => {
              const m = v.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
              return m ? m[1].trim() : "";
            };

            const isPositive = get("ISDEEMEDPOSITIVE");
            let amount = parseFloat(get("AMOUNT") || "0");
            if (isPositive === "Yes" && amount > 0) amount = -amount;

            rows.push({
              "Voucher Type": get("VOUCHERTYPENAME"),
              Date: get("DATE"),
              Party: get("PARTYNAME"),
              Item: get("STOCKITEMNAME"),
              Qty: get("BILLEDQTY"),
              Amount: amount,
              State: get("PLACEOFSUPPLY"),
              Salesman: get("BASICSALESNAME"),
            });
          }
        }

        // Excel-style alignment
        const blankRow = {}; // old Excel export में row 1 खाली रहती थी
        const headerRow = {
          "Voucher Type": "Voucher Type",
          Date: "Date",
          Party: "Party",
          Item: "Item",
          Qty: "Qty",
          Amount: "Amount",
          State: "State",
          Salesman: "Salesman",
        };

        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: [blankRow, headerRow, ...rows],
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(
          JSON.stringify({
            success: true,
            message: "Excel-style JSON stored",
            total: rows.length,
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

    // Latest data fetcher
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const data = await env.REPLICA_DATA.get("latest_tally_json");
      if (!data)
        return new Response(JSON.stringify({ status: "empty", rows: [] }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      return new Response(data, { headers: { "Content-Type": "application/json", ...cors } });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
