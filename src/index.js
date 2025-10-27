// index.js — Full unified backend for Replica System
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
      return new Response("Replica Unified Backend Active ✅", { headers: cors });

    // ------------------ TEST ROUTE ------------------
    if (url.pathname === "/api/test") {
      return new Response(
        JSON.stringify({ status: "ok", message: "Backend Live", time: new Date().toISOString() }),
        { headers: { "Content-Type": "application/json", ...cors } }
      );
    }

    // ------------------ MAIN PUSH ENDPOINT ------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json"))
          return new Response("Invalid Content-Type", { status: 400, headers: cors });

        const body = await request.json();

        // Incoming XMLs (may be missing)
        const xmlSales = body.salesXml || "";
        const xmlPurchase = body.purchaseXml || "";
        const xmlMasters = body.mastersXml || "";
        const xmlOutstanding = body.outstandingXml || "";

        // Parser helper
        const extractBlocks = (xml, tag) => xml.match(new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi")) || [];
        const getTag = (block, tag) => {
          const match = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i"));
          return match ? match[1].trim() : "";
        };

        // ---------- SALES ----------
        const salesRows = [];
        if (xmlSales && xmlSales.includes("<VOUCHER")) {
          const vouchers = extractBlocks(xmlSales, "VOUCHER");
          for (const v of vouchers) {
            const amt = parseFloat(getTag(v, "AMOUNT") || "0");
            const isPositive = getTag(v, "ISDEEMEDPOSITIVE");
            const finalAmt = isPositive === "Yes" && amt > 0 ? -amt : amt;
            salesRows.push({
              "Voucher Type": getTag(v, "VOUCHERTYPENAME"),
              Date: getTag(v, "DATE"),
              Party: getTag(v, "PARTYNAME"),
              Item: getTag(v, "STOCKITEMNAME"),
              Qty: getTag(v, "BILLEDQTY"),
              Amount: finalAmt,
              State: getTag(v, "PLACEOFSUPPLY"),
              Salesman: getTag(v, "BASICSALESNAME"),
            });
          }
        }

        // ---------- PURCHASE ----------
        const purchaseRows = [];
        if (xmlPurchase && xmlPurchase.includes("<VOUCHER")) {
          const vouchers = extractBlocks(xmlPurchase, "VOUCHER");
          for (const v of vouchers) {
            purchaseRows.push({
              "Voucher Type": getTag(v, "VOUCHERTYPENAME"),
              Date: getTag(v, "DATE"),
              Party: getTag(v, "PARTYNAME"),
              Item: getTag(v, "STOCKITEMNAME"),
              Qty: getTag(v, "BILLEDQTY"),
              Amount: parseFloat(getTag(v, "AMOUNT") || "0"),
              State: getTag(v, "PLACEOFSUPPLY"),
              Salesman: getTag(v, "BASICSALESNAME"),
            });
          }
        }

        // ---------- MASTERS (LEDGERS + STOCK ITEMS) ----------
        const masterRows = [];
        if (xmlMasters && xmlMasters.includes("<LEDGER")) {
          const ledgers = extractBlocks(xmlMasters, "LEDGER");
          for (const l of ledgers) {
            masterRows.push({
              Type: "Ledger",
              Name: getTag(l, "NAME"),
              Opening: getTag(l, "OPENINGBALANCE"),
              Closing: getTag(l, "CLOSINGBALANCE"),
              Email: getTag(l, "EMAIL"),
            });
          }
        }
        if (xmlMasters && xmlMasters.includes("<STOCKITEM")) {
          const stocks = extractBlocks(xmlMasters, "STOCKITEM");
          for (const s of stocks) {
            masterRows.push({
              Type: "StockItem",
              Name: getTag(s, "NAME"),
              Opening: getTag(s, "OPENINGBALANCE"),
              Closing: getTag(s, "CLOSINGBALANCE"),
            });
          }
        }

        // ---------- OUTSTANDING ----------
        const outstandingRows = [];
        if (xmlOutstanding && xmlOutstanding.includes("<LEDGER")) {
          const ledgers = extractBlocks(xmlOutstanding, "LEDGER");
          for (const l of ledgers) {
            outstandingRows.push({
              Party: getTag(l, "NAME"),
              Closing: getTag(l, "CLOSINGBALANCE"),
              Contact: getTag(l, "EMAIL"),
            });
          }
        }

        // ---------- अगर चारों XML खाली हों तो पुराना data retain करो ----------
        const totalRecords =
          salesRows.length + purchaseRows.length + masterRows.length + outstandingRows.length;
        if (totalRecords === 0) {
          const oldData = await env.REPLICA_DATA.get("latest_tally_json");
          return new Response(
            JSON.stringify({
              success: false,
              message: "Empty data skipped. Old data retained.",
              oldData: !!oldData,
              time: new Date().toISOString(),
            }),
            { headers: { "Content-Type": "application/json", ...cors } }
          );
        }

        // ---------- Excel-style header structure ----------
        const blank = {};
        const salesHeader = {
          "Voucher Type": "Voucher Type",
          Date: "Date",
          Party: "Party",
          Item: "Item",
          Qty: "Qty",
          Amount: "Amount",
          State: "State",
          Salesman: "Salesman",
        };
        const purchaseHeader = { ...salesHeader };
        const masterHeader = {
          Type: "Type",
          Name: "Name",
          Opening: "Opening",
          Closing: "Closing",
          Email: "Email",
        };
        const outstandingHeader = {
          Party: "Party",
          Closing: "Closing",
          Contact: "Contact",
        };

        const combinedPayload = {
          status: "ok",
          time: new Date().toISOString(),
          rows: {
            sales: [blank, salesHeader, ...salesRows],
            purchase: [blank, purchaseHeader, ...purchaseRows],
            masters: [blank, masterHeader, ...masterRows],
            outstanding: [blank, outstandingHeader, ...outstandingRows],
          },
        };

        await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(combinedPayload));

        return new Response(
          JSON.stringify({
            success: true,
            message: "Unified Tally data stored successfully.",
            counts: {
              sales: salesRows.length,
              purchase: purchaseRows.length,
              masters: masterRows.length,
              outstanding: outstandingRows.length,
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

// -------------- FIXED: LATEST FETCH ENDPOINT --------------
if (url.pathname === "/api/imports/latest" && request.method === "GET") {
  const kvValue = await env.REPLICA_DATA.get("latest_tally_json");
  if (!kvValue) {
    return new Response(
      JSON.stringify({ status: "empty", rows: [] }),
      { headers: { "Content-Type": "application/json", ...cors } }
    );
  }

  let parsed;
  try {
    parsed = JSON.parse(kvValue);
  } catch {
    parsed = { raw: kvValue };
  }

  // If compressed XML exists, send decompression-ready JSON
  if (parsed.salesXml || parsed.purchaseXml || parsed.mastersXml) {
    return new Response(
      JSON.stringify({ status: "ok", compressed: true, ...parsed }),
      { headers: { "Content-Type": "application/json", ...cors } }
    );
  }

  // Otherwise just send the object with rows or any detected array
  if (Array.isArray(parsed.rows)) {
    return new Response(
      JSON.stringify({ status: "ok", rows: parsed.rows }),
      { headers: { "Content-Type": "application/json", ...cors } }
    );
  }

  // Fallback: try to detect array-like content
  const arrayCandidate = Object.values(parsed).find((v) => Array.isArray(v));
  if (arrayCandidate) {
    return new Response(
      JSON.stringify({ status: "ok", rows: arrayCandidate }),
      { headers: { "Content-Type": "application/json", ...cors } }
    );
  }

  // Otherwise return as-is
  return new Response(
    JSON.stringify({ status: "ok", raw: parsed }),
    { headers: { "Content-Type": "application/json", ...cors } }
  );
}
