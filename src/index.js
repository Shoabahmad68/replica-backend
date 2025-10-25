// ✅ Final Stable + Fixed XML Parser Version
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

        // ✅ Improved XML parser: supports nested tags and multiple entries
        const parseXML = (xml) => {
          if (!xml || !xml.includes("<ENVELOPE>")) return [];

          const vouchers = xml.match(/<VOUCHER[\s\S]*?<\/VOUCHER>/gi) || [];
          const getAll = (src, tag) => {
            const matches = [...src.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "gi"))];
            return matches.map((m) => m[1].trim());
          };

          const rows = [];
          for (const v of vouchers) {
            const amtArr = getAll(v, "AMOUNT");
            let amt = amtArr.length ? parseFloat(amtArr.pop() || "0") : 0;
            const isNeg = v.includes("<ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>");
            if (isNeg && amt > 0) amt = -amt;

            rows.push({
              "Voucher Type": getAll(v, "VOUCHERTYPENAME")[0] || "",
              Date: getAll(v, "DATE")[0] || "",
              Party: getAll(v, "PARTYNAME")[0] || "",
              Item: getAll(v, "STOCKITEMNAME")[0] || "",
              Qty: getAll(v, "BILLEDQTY")[0] || "",
              Amount: amt,
              State: getAll(v, "PLACEOFSUPPLY")[0] || "",
              Salesman: getAll(v, "BASICSALESNAME")[0] || "",
            });
          }

          // filter out empty rows
          return rows.filter(
            (r) =>
              Object.values(r).some((v) => String(v || "").trim() !== "") &&
              !(Object.values(r).length === 1 && Object.values(r)[0] === "Voucher Type")
          );
        };

        // ✅ Parse all sections
        const parsed = {
          sales: parseXML(xmlBlocks.salesXml),
          purchase: parseXML(xmlBlocks.purchaseXml),
          masters: parseXML(xmlBlocks.mastersXml),
          outstanding: parseXML(xmlBlocks.outstandingXml),
        };

        // ✅ Payload to store
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

        // ✅ Save to Cloudflare KV
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
      const raw = await env.REPLICA_DATA.get("latest_tally_json");
      if (!raw)
        return new Response(
          JSON.stringify({ status: "empty", rows: [] }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );

      let data = {};
      try {
        data = JSON.parse(raw);
        if (typeof data.rows === "string") data.rows = JSON.parse(data.rows);

        if (!data.flatRows && data.rows) {
          data.flatRows = [
            ...(data.rows.sales || []),
            ...(data.rows.purchase || []),
            ...(data.rows.masters || []),
            ...(data.rows.outstanding || []),
          ];
        }
      } catch (e) {
        data = { status: "corrupt", rows: [] };
      }

      return new Response(JSON.stringify(data), {
        headers: { "Content-Type": "application/json", ...cors },
      });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
