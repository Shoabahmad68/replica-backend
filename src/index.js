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
      return new Response("Replica Unified Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return new Response(
        JSON.stringify({
          status: "ok",
          message: "Backend Live",
          time: new Date().toISOString(),
        }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    // ------------------ MAIN PUSH ENDPOINT ------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();

        // --- Base64 + GZIP decode using Web Streams ---
        async function decodeAndDecompress(b64) {
          if (!b64) return "";
          try {
            const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
            const stream = new DecompressionStream("gzip");
            const decompressed = await new Response(
              new Blob([bin]).stream().pipeThrough(stream)
            ).arrayBuffer();
            return new TextDecoder().decode(decompressed);
          } catch (e) {
            console.warn("Decompress failed:", e.message);
            return "";
          }
        }

        const xmlSales = await decodeAndDecompress(body.salesXml);
        const xmlPurchase = await decodeAndDecompress(body.purchaseXml);
        const xmlMasters = await decodeAndDecompress(body.mastersXml);

        // --- Simple XML parsing helpers ---
        const extract = (xml, tag) =>
          xml.match(new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi")) || [];
        const get = (block, tag) => {
          const m = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return m ? m[1].trim() : "";
        };

        const salesRows = [];
        for (const v of extract(xmlSales, "VOUCHER")) {
          salesRows.push({
            Voucher: get(v, "VOUCHERTYPENAME"),
            Date: get(v, "DATE"),
            Party: get(v, "PARTYNAME"),
            Item: get(v, "STOCKITEMNAME"),
            Qty: get(v, "BILLEDQTY"),
            Amount: parseFloat(get(v, "AMOUNT") || "0"),
            Salesman: get(v, "BASICSALESNAME"),
          });
        }

        const purchaseRows = [];
        for (const v of extract(xmlPurchase, "VOUCHER")) {
          purchaseRows.push({
            Voucher: get(v, "VOUCHERTYPENAME"),
            Date: get(v, "DATE"),
            Party: get(v, "PARTYNAME"),
            Item: get(v, "STOCKITEMNAME"),
            Qty: get(v, "BILLEDQTY"),
            Amount: parseFloat(get(v, "AMOUNT") || "0"),
          });
        }

        const masterRows = [];
        for (const l of extract(xmlMasters, "LEDGER")) {
          masterRows.push({
            Type: "Ledger",
            Name: get(l, "NAME"),
            Closing: get(l, "CLOSINGBALANCE"),
          });
        }

        const finalPayload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: {
            sales: salesRows,
            purchase: purchaseRows,
            masters: masterRows,
          },
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(finalPayload));

        return new Response(
          JSON.stringify({
            success: true,
            message: "Full parsed data stored successfully.",
            count: {
              sales: salesRows.length,
              purchase: purchaseRows.length,
              masters: masterRows.length,
            },
          }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      } catch (err) {
        return new Response(
          JSON.stringify({ error: err.message || "Processing failed" }),
          { status: 500, headers: { "Content-Type": "application/json", ...cors } }
        );
      }
    }

    // ------------------ FETCH ENDPOINT ------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const kv = await env.REPLICA_DATA.get("latest_tally_json");
      if (!kv)
        return new Response(
          JSON.stringify({ status: "empty" }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      return new Response(kv, { headers: { "Content-Type": "application/json", ...cors } });
    }

    // Default
    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
