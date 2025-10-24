// ✅ Final Stable Version — auto string fix + flatRows + full compatibility
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

    // -----------------------------------------------------
    // PUSH endpoint
    // -----------------------------------------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid content type", {
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

        const parseXML = (xml) => {
          if (!xml || !xml.includes("<ENVELOPE>")) return [];
          const get = (src, tag) => {
            const m = src.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
            return m ? m[1].trim() : "";
          };
          const blocks = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
          const rows = [];
          for (const b of blocks) {
            let amt = parseFloat(get(b, "AMOUNT") || "0");
            if (get(b, "ISDEEMEDPOSITIVE") === "Yes" && amt > 0) amt = -amt;
            rows.push({
              "Voucher Type": get(b, "VOUCHERTYPENAME"),
              Date: get(b, "DATE"),
              Party: get(b, "PARTYNAME"),
              Item: get(b, "STOCKITEMNAME"),
              Qty: get(b, "BILLEDQTY"),
              Amount: amt,
              State: get(b, "PLACEOFSUPPLY"),
              Salesman: get(b, "BASICSALESNAME"),
            });
          }
          return rows;
        };

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

        // add flatRows for frontend (unified)
        payload.flatRows = [
          ...payload.rows.sales,
          ...payload.rows.purchase,
          ...payload.rows.masters,
          ...payload.rows.outstanding,
        ];

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));

        return new Response(
          JSON.stringify({
            success: true,
            message: "Tally data saved successfully",
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

    // -----------------------------------------------------
    // IMPORT endpoint
    // -----------------------------------------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const raw = await env.REPLICA_DATA.get("latest_tally_json");
      if (!raw)
        return new Response(
          JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );

      let data = {};
      try {
        data = JSON.parse(raw);
        // if rows accidentally stored as string, auto-fix it
        if (typeof data.rows === "string") {
          data.rows = JSON.parse(data.rows);
        }
        if (!data.flatRows && data.rows) {
          data.flatRows = [
            ...(data.rows.sales || []),
            ...(data.rows.purchase || []),
            ...(data.rows.masters || []),
            ...(data.rows.outstanding || []),
          ];
        }
      } catch {
        data = { status: "corrupt", rows: [] };
      }

      return new Response(JSON.stringify(data), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
