// ✅ FINAL index.js — with GZIP decode + XML parse + Full output
import { gunzipSync } from "fflate";

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
      return new Response(
        JSON.stringify({ status: "ok", time: new Date().toISOString() }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );

    // ---------------- PUSH ENDPOINT ----------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const data = await request.json();
        const decodeBase64 = (b64) => {
          try {
            if (!b64) return "";
            const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
            return new TextDecoder().decode(gunzipSync(bin));
          } catch {
            return "";
          }
        };

        const xmlSales = decodeBase64(data.salesXml);
        const xmlPurchase = decodeBase64(data.purchaseXml);
        const xmlMasters = decodeBase64(data.mastersXml);

        const extractBlocks = (xml, tag) =>
          xml.match(new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi")) || [];
        const getTag = (block, tag) => {
          const match = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return match ? match[1].trim() : "";
        };

        const salesRows = [];
        for (const v of extractBlocks(xmlSales, "VOUCHER")) {
          salesRows.push({
            Voucher: getTag(v, "VOUCHERTYPENAME"),
            Date: getTag(v, "DATE"),
            Party: getTag(v, "PARTYNAME"),
            Item: getTag(v, "STOCKITEMNAME"),
            Qty: getTag(v, "BILLEDQTY"),
            Amount: parseFloat(getTag(v, "AMOUNT") || "0"),
            Salesman: getTag(v, "BASICSALESNAME"),
          });
        }

        const purchaseRows = [];
        for (const v of extractBlocks(xmlPurchase, "VOUCHER")) {
          purchaseRows.push({
            Voucher: getTag(v, "VOUCHERTYPENAME"),
            Date: getTag(v, "DATE"),
            Party: getTag(v, "PARTYNAME"),
            Item: getTag(v, "STOCKITEMNAME"),
            Qty: getTag(v, "BILLEDQTY"),
            Amount: parseFloat(getTag(v, "AMOUNT") || "0"),
          });
        }

        const masterRows = [];
        for (const l of extractBlocks(xmlMasters, "LEDGER")) {
          masterRows.push({
            Type: "Ledger",
            Name: getTag(l, "NAME"),
            Closing: getTag(l, "CLOSINGBALANCE"),
          });
        }

        const payload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: {
            sales: salesRows,
            purchase: purchaseRows,
            masters: masterRows,
          },
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(payload));
        return new Response(
          JSON.stringify({
            success: true,
            message: "Parsed & Stored",
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

    // ---------------- FETCH ENDPOINT ----------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      const kv = await env.REPLICA_DATA.get("latest_tally_json");
      if (!kv)
        return new Response(
          JSON.stringify({ status: "empty" }),
          { headers: { "Content-Type": "application/json", ...cors } }
        );
      return new Response(kv, { headers: { "Content-Type": "application/json", ...cors } });
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
