export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (url.pathname === "/")
      return new Response("Replica Backend Active ✅", { headers: cors });

    if (url.pathname === "/api/test")
      return Response.json({ status: "ok", message: "Backend Live", time: new Date().toISOString() }, { headers: cors });

    // ✅ असली Tally डेटा स्टोर करने वाला endpoint
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();
        const decompressedData = {};

        for (const [key, compressedValue] of Object.entries(body)) {
          if (["status", "source", "time", "compressed"].includes(key)) continue;
          if (compressedValue && compressedValue.length > 0) {
            try {
              const decompressed = await decompressGzip(compressedValue);
              decompressedData[key] = decompressed;
            } catch {
              decompressedData[key] = "";
            }
          }
        }

        // ✅ Excel-Style Structured Rows बनाओ
        const parsedRows = {
          sales: buildExcelStyleRows(decompressedData.sales || "", "Sales"),
          purchase: buildExcelStyleRows(decompressedData.purchase || "", "Purchase"),
          receipt: buildExcelStyleRows(decompressedData.receipt || "", "Receipt"),
          payment: buildExcelStyleRows(decompressedData.payment || "", "Payment"),
          journal: buildExcelStyleRows(decompressedData.journal || "", "Journal"),
          debit: buildExcelStyleRows(decompressedData.debitNote || "", "Debit Note"),
          credit: buildExcelStyleRows(decompressedData.creditNote || "", "Credit Note"),
        };

        const totalRows = Object.values(parsedRows).reduce((a, b) => a + b.length, 0);

        const dataToStore = {
          rows: parsedRows,
          storedAt: new Date().toISOString(),
          source: body.source || "tally-pusher",
          totalRows,
          counts: Object.fromEntries(Object.entries(parsedRows).map(([k, v]) => [k, v.length])),
        };

        await env.REPLICA_DATA.put("latest_data", JSON.stringify(dataToStore));

        return Response.json({
          success: true,
          message: "Tally डेटा successfully save हो गया",
          totalRows,
          counts: dataToStore.counts,
          storedAt: dataToStore.storedAt,
        }, { headers: cors });

      } catch (err) {
        return Response.json({ error: "Failed to process data", detail: err.message }, { status: 500, headers: cors });
      }
    }

    // ✅ डेटा लाने वाला endpoint
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        const data = await env.REPLICA_DATA.get("latest_data");
        if (!data) {
          return Response.json({
            status: "empty",
            message: "No data available",
            sales: [], purchase: [], receipt: [], payment: [], journal: [], debit: [], credit: [],
          }, { headers: cors });
        }

        const parsedData = JSON.parse(data);
        return Response.json({
          status: "ok",
          storedAt: parsedData.storedAt,
          source: parsedData.source,
          totalRows: parsedData.totalRows,
          counts: parsedData.counts,
          ...parsedData.rows,
        }, { headers: cors });
      } catch (e) {
        return Response.json({ error: "Failed to fetch data", detail: e.message }, { status: 500, headers: cors });
      }
    }

    // ✅ Summary endpoint
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const data = await env.REPLICA_DATA.get("latest_data");
        if (!data) return Response.json({ status: "no_data", message: "No data available" }, { headers: cors });
        const parsed = JSON.parse(data);
        return Response.json({
          status: "data_available",
          storedAt: parsed.storedAt,
          source: parsed.source,
          totalRows: parsed.totalRows,
          counts: parsed.counts,
          message: "Data is available",
        }, { headers: cors });
      } catch {
        return Response.json({ error: "Summary fetch failed" }, { status: 500, headers: cors });
      }
    }

    return Response.json({ error: "Endpoint not found" }, { status: 404, headers: cors });
  }
};

// ✅ Decompress helper
async function decompressGzip(base64String) {
  const binary = atob(base64String);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  const ds = new DecompressionStream("gzip");
  const writer = ds.writable.getWriter();
  writer.write(bytes);
  writer.close();
  const output = await new Response(ds.readable).arrayBuffer();
  return new TextDecoder().decode(output);
}

// ✅ XML Tag extractor
function extractTag(xml, tag) {
  const m = xml.match(new RegExp(`<${tag}[^>]*>([^<]*)<\/${tag}>`, "i"));
  return m ? m[1].trim() : "";
}

// ✅ Excel-style structured parser (Enhanced for full item mapping)
function buildExcelStyleRows(xmlString, voucherType) {
  if (!xmlString || xmlString.length < 100) return [];
  const rows = [];
  const voucherMatches = xmlString.matchAll(/<VOUCHER[^>]*>([\s\S]*?)<\/VOUCHER>/g);

  for (const match of voucherMatches) {
    const vXML = match[1];
    const base = {
      "Date": formatTallyDate(extractTag(vXML, "DATE")),
      "Vch No.": extractTag(vXML, "VOUCHERNUMBER"),
      "Party Name":
        extractTag(vXML, "PARTYLEDGERNAME") ||
        extractTag(vXML, "PARTYNAME") ||
        extractTag(vXML, "LEDGERNAME") ||
        "Unknown",
      "City/Area":
        extractTag(vXML, "PLACEOFSUPPLY") ||
        extractTag(vXML, "ADDRESS") ||
        extractTag(vXML, "LEDSTATENAME") ||
        "",
      "State": extractTag(vXML, "STATENAME") || "",
      "Salesman":
        extractTag(vXML, "BASICSALESMANNAME") ||
        extractTag(vXML, "SALESMANNAME") ||
        extractTag(vXML, "USERDESCRIPTION") ||
        "Unknown",
      "Vch Type": extractTag(vXML, "VOUCHERTYPENAME") || voucherType,
      "ItemName": "",
      "Item Group": "",
      "Item Category": "",
      "Qty": "",
      "Rate": "",
      "Amount": parseFloat(extractTag(vXML, "AMOUNT") || "0"),
    };

    const itemMatches = vXML.matchAll(
      /<INVENTORYENTRIES\.LIST[^>]*>([\s\S]*?)<\/INVENTORYENTRIES\.LIST>/g
    );

    let found = false;
    for (const iMatch of itemMatches) {
      found = true;
      const iXML = iMatch[1];
      const row = { ...base };
      row["ItemName"] = extractTag(iXML, "STOCKITEMNAME") || "Unknown";
      row["Item Group"] =
        extractTag(iXML, "STOCKITEMGROUPNAME") ||
        extractTag(iXML, "PARENTITEM") ||
        "Unknown";
      row["Item Category"] =
        extractTag(iXML, "CATEGORY") ||
        extractTag(iXML, "STOCKCATEGORY") ||
        extractTag(iXML, "ITEMCATEGORYNAME") ||
        "Unknown";
      row["Qty"] = extractTag(iXML, "ACTUALQTY") || extractTag(iXML, "BILLEDQTY") || "";
      row["Rate"] = extractTag(iXML, "RATE") || "";
      row["Amount"] = parseFloat(extractTag(iXML, "AMOUNT") || "0");
      rows.push(row);
    }

    // अगर कोई inventory entry नहीं मिली तो भी एक बेसिक row add करो
    if (!found) rows.push(base);
  }

  return rows;
}

// ✅ Date formatter
function formatTallyDate(d) {
  if (!d || d.length !== 8) return d;
  return `${d.slice(6, 8)}-${d.slice(4, 6)}-${d.slice(0, 4)}`;
}
