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
      return Response.json({ 
        status: "ok", 
        message: "Backend Live", 
        time: new Date().toISOString() 
      }, { headers: cors });

    // ✅ असली Tally डेटा स्टोर करने वाला endpoint
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        console.log("📥 Tally डेटा आ रहा है...");
        
        const body = await request.json();
        console.log("✅ डेटा मिल गया");
        
        // Decompress करके असली डेटा निकालो
        const decompressedData = {};
        
        // हर field को decompress करो
        for (const [key, compressedValue] of Object.entries(body)) {
          if (key === 'status' || key === 'source' || key === 'time' || key === 'compressed') {
            continue; // Meta fields skip करो
          }
          
          if (compressedValue && compressedValue.length > 0) {
            try {
              // Base64 decode करके decompress करो
              const decompressed = await decompressGzip(compressedValue);
              decompressedData[key] = decompressed;
              console.log(`✅ ${key} decompress हो गया: ${Math.round(decompressed.length/1024)}KB`);
            } catch (e) {
              console.warn(`⚠️ ${key} decompress नहीं हुआ:`, e.message);
              decompressedData[key] = "";
            }
          }
        }

        // अब असली डेटा को parse करके rows में बदलो
        const parsedRows = {
          sales: parseXMLToRows(decompressedData.sales || "", "sales"),
          purchase: parseXMLToRows(decompressedData.purchase || "", "purchase"),
          receipt: parseXMLToRows(decompressedData.receipt || "", "receipt"),
          payment: parseXMLToRows(decompressedData.payment || "", "payment"),
          journal: parseXMLToRows(decompressedData.journal || "", "journal"),
          debit: parseXMLToRows(decompressedData.debitNote || "", "debit"),
          credit: parseXMLToRows(decompressedData.creditNote || "", "credit"),
        };

        // Count निकालो
        const totalRows = Object.values(parsedRows).reduce((sum, arr) => sum + arr.length, 0);
        console.log(`📊 कुल ${totalRows} rows बनी`);

        // KV में स्टोर करो
        const dataToStore = {
          rows: parsedRows,
          storedAt: new Date().toISOString(),
          source: body.source || "tally-pusher",
          totalRows: totalRows,
          counts: {
            sales: parsedRows.sales.length,
            purchase: parsedRows.purchase.length,
            receipt: parsedRows.receipt.length,
            payment: parsedRows.payment.length,
            journal: parsedRows.journal.length,
            debit: parsedRows.debit.length,
            credit: parsedRows.credit.length,
          }
        };

        await env.REPLICA_DATA.put("latest_data", JSON.stringify(dataToStore));
        console.log("💾 KV में डेटा save हो गया");
        
        return Response.json({
          success: true,
          message: "Tally डेटा successfully save हो गया",
          totalRows: totalRows,
          counts: dataToStore.counts,
          storedAt: dataToStore.storedAt
        }, { headers: cors });

      } catch (err) {
        console.error("❌ Error:", err.message);
        return Response.json({ 
          error: "Failed to process data",
          detail: err.message 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    // ✅ डेटा लाने वाला endpoint

// ✅ डेटा लाने वाला endpoint (Final Fixed Version)
if (url.pathname === "/api/imports/latest" && request.method === "GET") {
  try {
    console.log("📤 डेटा भेज रहे हैं...");

    const data = await env.REPLICA_DATA.get("latest_data");

    if (!data) {
      console.log("❌ KV में कोई डेटा नहीं है");
      return Response.json({
        status: "empty",
        message: "No data available",
        sales: [],
        purchase: [],
        receipt: [],
        payment: [],
        journal: [],
        debit: [],
        credit: [],
      }, { headers: cors });
    }

    const parsedData = JSON.parse(data);
    console.log(`✅ ${parsedData.totalRows} rows भेज रहे हैं`);

    // 👇 Flatten करके frontend के लिए साफ output दो
    const { rows } = parsedData;
    return Response.json({
      status: "ok",
      storedAt: parsedData.storedAt,
      source: parsedData.source,
      totalRows: parsedData.totalRows,
      counts: parsedData.counts,
      sales: rows.sales || [],
      purchase: rows.purchase || [],
      receipt: rows.receipt || [],
      payment: rows.payment || [],
      journal: rows.journal || [],
      debit: rows.debit || [],
      credit: rows.credit || [],
    }, { headers: cors });

  } catch (e) {
    console.error("❌ Fetch error:", e.message);
    return Response.json({
      error: "Failed to fetch data",
      detail: e.message,
    }, {
      status: 500,
      headers: cors,
    });
  }
}


    // ✅ Summary endpoint
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const data = await env.REPLICA_DATA.get("latest_data");
        
        if (!data) {
          return Response.json({ 
            status: "no_data",
            message: "No data available" 
          }, { headers: cors });
        }

        const parsed = JSON.parse(data);
        return Response.json({
          status: "data_available",
          storedAt: parsed.storedAt,
          source: parsed.source,
          totalRows: parsed.totalRows,
          counts: parsed.counts,
          message: "Data is available"
        }, { headers: cors });

      } catch (e) {
        return Response.json({ 
          error: "Summary fetch failed" 
        }, { 
          status: 500, 
          headers: cors 
        });
      }
    }

    return Response.json({ 
      error: "Endpoint not found" 
    }, { 
      status: 404, 
      headers: cors 
    });
  }
};

// ✅ Cloudflare Workers में gzip decompress करने वाला function
async function decompressGzip(base64String) {
  try {
    // Base64 को binary में convert करो
    const binaryString = atob(base64String);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    
    // Decompress करो using DecompressionStream
    const ds = new DecompressionStream('gzip');
    const writer = ds.writable.getWriter();
    writer.write(bytes);
    writer.close();
    
    const output = await new Response(ds.readable).arrayBuffer();
    const decoder = new TextDecoder();
    return decoder.decode(output);
    
  } catch (e) {
    console.error("Decompression error:", e.message);
    throw new Error(`Failed to decompress: ${e.message}`);
  }
}

// ✅ XML को rows में convert करने वाला function
function parseXMLToRows(xmlString, voucherType) {
  if (!xmlString || xmlString.length < 100) return [];
  
  const rows = [];
  
  try {
    // VOUCHER tags ढूंढो
    const voucherMatches = xmlString.matchAll(/<VOUCHER[^>]*>([\s\S]*?)<\/VOUCHER>/g);
    
    for (const match of voucherMatches) {
      const voucherXML = match[1];
      
      // Basic fields निकालो
      const row = {
        type: "voucher",
        voucherType: voucherType,
        date: extractTag(voucherXML, "DATE"),
        voucherNumber: extractTag(voucherXML, "VOUCHERNUMBER"),
        reference: extractTag(voucherXML, "REFERENCE"),
        narration: extractTag(voucherXML, "NARRATION"),
        partyName: extractTag(voucherXML, "PARTYNAME"),
        amount: parseFloat(extractTag(voucherXML, "AMOUNT") || "0"),
      };
      
      // ALLLEDGERENTRIES से ledger details निकालो
      const ledgerMatches = voucherXML.matchAll(/<ALLLEDGERENTRIES\.LIST[^>]*>([\s\S]*?)<\/ALLLEDGERENTRIES\.LIST>/g);
      let ledgerIndex = 0;
      
      for (const ledgerMatch of ledgerMatches) {
        const ledgerXML = ledgerMatch[1];
        const ledgerRow = {
          ...row,
          type: "ledger_entry",
          ledgerName: extractTag(ledgerXML, "LEDGERNAME"),
          ledgerAmount: parseFloat(extractTag(ledgerXML, "AMOUNT") || "0"),
          index: ledgerIndex++
        };
        rows.push(ledgerRow);
      }
      
      // INVENTORYENTRIES से item details निकालो
      const itemMatches = voucherXML.matchAll(/<INVENTORYENTRIES\.LIST[^>]*>([\s\S]*?)<\/INVENTORYENTRIES\.LIST>/g);
      let itemIndex = 0;
      
      for (const itemMatch of itemMatches) {
        const itemXML = itemMatch[1];
        const itemRow = {
          ...row,
          type: "item_row",
          StockItemName: extractTag(itemXML, "STOCKITEMNAME"),
          Item: extractTag(itemXML, "STOCKITEMNAME"),
          quantity: parseFloat(extractTag(itemXML, "ACTUALQTY") || "0"),
          rate: parseFloat(extractTag(itemXML, "RATE") || "0"),
          itemAmount: parseFloat(extractTag(itemXML, "AMOUNT") || "0"),
          index: itemIndex++
        };
        rows.push(itemRow);
      }
      
      // अगर कोई entries नहीं मिली तो basic row ही add करो
      if (ledgerIndex === 0 && itemIndex === 0) {
        rows.push(row);
      }
    }
    
    console.log(`✅ ${voucherType}: ${rows.length} rows parsed`);
    
  } catch (e) {
    console.error(`❌ ${voucherType} parsing error:`, e.message);
  }
  
  return rows;
}

// XML tag से value निकालने वाला helper function
function extractTag(xml, tagName) {
  const regex = new RegExp(`<${tagName}[^>]*>([^<]*)<\/${tagName}>`, 'i');
  const match = xml.match(regex);
  return match ? match[1].trim() : "";
}
