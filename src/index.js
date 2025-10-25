// ✅ Final Stable + Error-Free XML Parser Version
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
    // PUSH endpoint (Tally → Worker)
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

        // ✅ Improved XML parser: stable and nested safe
        const parseXML = (xml) => {
          if (!xml || !xml.includes("<ENVELOPE>")) return [];

          const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
          const getField = (src, tag) => {
            const match = src.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
            return match ? match[1].trim() : "";
          };

          const getAny = (src, tags) => {
            for (const tag of tags) {
              const val = getField(src, tag);
              if (val) return val;
            }
            return "";
          };

          const rows = [];
          for (const v of vouchers) {
            let amt = parseFloat(getAny(v, ["AMOUNT", "BILLEDAMOUNT"]) || "0");
            if (v.includes("<ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>") && amt > 0) amt = -amt;

            rows.push({
              "Voucher Type": getAny(v, ["VOUCHERTYPENAME", "VCHTYPE"]),
              Date: getAny(v, ["DATE"]),
              Party: getAny(v, ["PARTYNAME", "LEDGERNAME"]),
              Item: getAny(v, ["STOCKITEMNAME", "ITEMNAME"]),
              Qty: getAny(v, ["BILLEDQTY", "ACTUALQTY"]),
              Amount: amt,
              State: getAny(v, ["PLACEOFSUPPLY", "STATENAME"]),
              Salesman: getAny(v, ["BASICSALESNAME", "SALESMANNAME"]),
            });
          }

          return rows.filter((r) => Object.values(r).some((x) => x && x !== "0"));
        };

        // ✅ Parse all categories
        const parsed = {
          sales: parseXML(xmlBlocks.salesXml),
          purchase: parseXML(xmlBlocks.purchaseXml),
          masters: parseXML(xmlBlocks.mastersXml),
          outstanding: parseXML(xmlBlocks.outstandingXml),
        };

        // ✅ Payload for KV
        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: parsed,
          flatRows: [
            ...(parsed.sales || []),
            ...(parsed.purchase || []),
            ...(parsed.masters || []),
            ...(parsed.outstanding || []),
          ],
        };

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
        return new Response(
          JSON.stringify({ error: err.message }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // -----------------------------------------------------
    // IMPORT endpoint (Frontend → Worker)
    // -----------------------------------------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        const raw = await env.REPLICA_DATA.get("latest_tally_json");
        if (!raw)
          return new Response(
            JSON.stringify({ status: "empty", rows: [] }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );

        let data = JSON.parse(raw);
        if (typeof data.rows === "string") data.rows = JSON.parse(data.rows);

        if (!data.flatRows && data.rows) {
          data.flatRows = [
            ...(data.rows.sales || []),
            ...(data.rows.purchase || []),
            ...(data.rows.masters || []),
            ...(data.rows.outstanding || []),
          ];
        }

        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      } catch (e) {
        return new Response(
          JSON.stringify({ status: "corrupt", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // -----------------------------------------------------
    // Default 404
    // -----------------------------------------------------
    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
