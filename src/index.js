// index.js – Tally → JSON with header row for frontend compatibility
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (url.pathname === "/")
      return new Response("Replica Cloudflare Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return new Response(
        JSON.stringify({
          status: "success",
          message: "Backend connected",
          time: new Date().toISOString(),
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          const body = await request.json();
          const xml = body.salesXml || "";
          const rows = [];

          if (xml && xml.includes("<VOUCHER")) {
            const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];

            for (const v of vouchers) {
              const get = (tag) => {
                const m = v.match(
                  new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i")
                );
                return m ? m[1].trim() : "";
              };

              const voucherType = get("VOUCHERTYPENAME");
              const isPositive = get("ISDEEMEDPOSITIVE");
              let amount = parseFloat(get("AMOUNT") || "0");
              if (isPositive === "Yes" && amount > 0) amount = -amount;

              rows.push({
                "Voucher Type": voucherType,
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

          // Add header row as 2nd row reference
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
            rows: [headerRow, ...rows], // <--- Insert header as first usable row
          };

          await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));
          return new Response(
            JSON.stringify({
              success: true,
              message: "Header row added, JSON stored",
              count: rows.length,
            }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );
        }

        return new Response(
          JSON.stringify({ error: "Invalid request format" }),
          { status: 400, headers: { "Content-Type": "application/json", ...cors } }
        );
      } catch (err) {
        return new Response(
          JSON.stringify({ error: err.message }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

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
