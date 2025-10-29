export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    
    // Quick root endpoint
    if (url.pathname === "/") 
      return new Response("Replica Backend Active ✅ | Use /api/push/tally or /api/imports/latest", { headers: cors });
    
    // Test endpoint
    if (url.pathname === "/api/test")
      return Response.json({ 
        status: "ok", 
        message: "Backend Live", 
        time: new Date().toISOString() 
      }, { headers: cors });

    // ------------------ FIXED XML PARSING ------------------
    async function decodeAndDecompress(b64) {
      if (!b64) return "";
      try {
        const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
        const stream = new DecompressionStream("gzip");
        const decompressed = await new Response(new Blob([bin]).stream().pipeThrough(stream)).arrayBuffer();
        return new TextDecoder().decode(decompressed);
      } catch (e) {
        console.warn("Decompress failed, trying plain text:", e?.message);
        try {
          return atob(b64);
        } catch (e2) {
          return "";
        }
      }
    }

    // SIMPLIFIED XML PARSING - More robust
    function extractBlocks(xml, tag) {
      if (!xml || typeof xml !== 'string') return [];
      try {
        const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'gi');
        const matches = [];
        let match;
        while ((match = regex.exec(xml)) !== null) {
          matches.push(match[0]); // Return the full matched block
        }
        return matches;
      } catch (e) {
        console.warn(`extractBlocks failed for ${tag}:`, e?.message);
        return [];
      }
    }

    function getTag(text, tag) {
      if (!text) return "";
      try {
        const regex = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'i');
        const match = text.match(regex);
        return match ? match[1].trim() : "";
      } catch (e) {
        return "";
      }
    }

    // SIMPLIFIED VOUCHER PARSING
    function parseVouchersSimple(xml, type) {
      if (!xml) return [];
      
      const vouchers = extractBlocks(xml, 'VOUCHER');
      console.log(`📊 Found ${vouchers.length} ${type} vouchers`);
      
      const rows = [];
      
      for (const voucherXml of vouchers) {
        try {
          const voucher = {
            type: type,
            VoucherType: getTag(voucherXml, 'VOUCHERTYPENAME') || getTag(voucherXml, 'VCHTYPE') || type,
            VoucherNumber: getTag(voucherXml, 'VOUCHERNUMBER') || 'N/A',
            Date: getTag(voucherXml, 'DATE'),
            PartyName: getTag(voucherXml, 'PARTYNAME') || getTag(voucherXml, 'PARTYLEDGERNAME') || 'Unknown',
            Amount: getTag(voucherXml, 'AMOUNT') || '0',
            Narration: getTag(voucherXml, 'NARRATION') || '',
          };
          
          rows.push(voucher);
          
          // Also parse inventory items if present
          const items = extractBlocks(voucherXml, 'ALLINVENTORYENTRIES.LIST');
          for (const itemXml of items) {
            const item = {
              type: `${type}_item`,
              VoucherType: voucher.VoucherType,
              VoucherNumber: voucher.VoucherNumber,
              Date: voucher.Date,
              StockItemName: getTag(itemXml, 'STOCKITEMNAME') || getTag(itemXml, 'NAME') || 'Unknown Item',
              Quantity: getTag(itemXml, 'BILLEDQTY') || getTag(itemXml, 'ACTUALQTY') || '0',
              Rate: getTag(itemXml, 'RATE') || '0',
              Amount: getTag(itemXml, 'AMOUNT') || '0',
            };
            rows.push(item);
          }
          
        } catch (e) {
          console.warn('Voucher parse error:', e?.message);
        }
      }
      
      return rows;
    }

    // SIMPLIFIED MASTERS PARSING
    function parseMastersSimple(xml) {
      if (!xml) return [];
      
      const masters = extractBlocks(xml, 'LEDGER');
      console.log(`📋 Found ${masters.length} masters`);
      
      return masters.map(ledgerXml => ({
        type: 'master',
        Name: getTag(ledgerXml, 'NAME'),
        Parent: getTag(ledgerXml, 'PARENT'),
        ClosingBalance: getTag(ledgerXml, 'CLOSINGBALANCE'),
        MailingName: getTag(ledgerXml, 'MAILINGNAME'),
      })).filter(m => m.Name); // Only include masters with names
    }

    // ------------------ PUSH ENDPOINT (SIMPLIFIED) ------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        console.log("📥 Received data from pusher...");
        const body = await request.json();
        
        // Decode all XML data
        const decoded = {
          sales: await decodeAndDecompress(body.salesXml),
          purchase: await decodeAndDecompress(body.purchaseXml),
          receipt: await decodeAndDecompress(body.receiptXml),
          payment: await decodeAndDecompress(body.paymentXml),
          journal: await decodeAndDecompress(body.journalXml),
          debit: await decodeAndDecompress(body.debitXml),
          credit: await decodeAndDecompress(body.creditXml),
          masters: await decodeAndDecompress(body.mastersXml),
          outstanding: await decodeAndDecompress(body.outstandingXml),
        };

        console.log("📊 Decoded data sizes:");
        Object.keys(decoded).forEach(key => {
          console.log(`   ${key}: ${decoded[key]?.length || 0} chars`);
        });

        // Parse all data
        const parsedData = {
          sales: parseVouchersSimple(decoded.sales, 'sales'),
          purchase: parseVouchersSimple(decoded.purchase, 'purchase'),
          receipt: parseVouchersSimple(decoded.receipt, 'receipt'),
          payment: parseVouchersSimple(decoded.payment, 'payment'),
          journal: parseVouchersSimple(decoded.journal, 'journal'),
          debit: parseVouchersSimple(decoded.debit, 'debit'),
          credit: parseVouchersSimple(decoded.credit, 'credit'),
          masters: parseMastersSimple(decoded.masters),
          outstanding: [], // We'll fix this later
        };

        // Calculate counts
        const counts = {
          sales: parsedData.sales.length,
          purchase: parsedData.purchase.length,
          receipt: parsedData.receipt.length,
          payment: parsedData.payment.length,
          journal: parsedData.journal.length,
          debit: parsedData.debit.length,
          credit: parsedData.credit.length,
          masters: parsedData.masters.length,
          outstandingReceivable: 0,
          outstandingPayable: 0,
        };

        console.log("✅ Parsed counts:", counts);

        // Prepare final payload
        const finalPayload = {
          status: "ok",
          source: body.source || "tally-pusher",
          time: body.time || new Date().toISOString(),
          counts: counts,
          data: parsedData,
          // Include sample data for debugging
          sample: {
            sales: parsedData.sales.slice(0, 3),
            purchase: parsedData.purchase.slice(0, 3),
            masters: parsedData.masters.slice(0, 3),
          }
        };

        // Save to KV (simplified - single key)
        try {
          const dataStr = JSON.stringify(finalPayload);
          await env.REPLICA_DATA.put("latest_tally_data", dataStr);
          
          // Also save metadata separately
          await env.REPLICA_DATA.put("latest_tally_meta", JSON.stringify({
            storedAt: new Date().toISOString(),
            counts: counts,
            source: body.source || "tally-pusher"
          }));
          
          console.log("💾 Saved to KV successfully");
        } catch (kvError) {
          console.error("❌ KV save error:", kvError?.message);
        }

        return Response.json({
          success: true,
          message: "Data processed and stored",
          counts: counts,
          sample: finalPayload.sample
        }, { headers: cors });

      } catch (err) {
        console.error("❌ Push endpoint error:", err?.message);
        return Response.json({
          error: "Processing failed",
          detail: err?.message
        }, {
          status: 500,
          headers: cors
        });
      }
    }

    // ------------------ FIXED FETCH ENDPOINT ------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        console.log("📤 Fetching latest data...");
        
        const data = await env.REPLICA_DATA.get("latest_tally_data");
        if (!data) {
          return Response.json({
            status: "empty",
            message: "No data available yet. Push data first using /api/push/tally"
          }, { headers: cors });
        }

        const parsedData = JSON.parse(data);
        console.log("✅ Returning data with counts:", parsedData.counts);
        
        return new Response(data, {
          headers: {
            "Content-Type": "application/json",
            ...cors
          }
        });

      } catch (e) {
        console.error("❌ Fetch error:", e?.message);
        return Response.json({
          error: "Failed to fetch data",
          detail: e?.message
        }, {
          status: 500,
          headers: cors
        });
      }
    }

    // Summary endpoint
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_tally_meta");
        if (!meta) {
          return Response.json({
            status: "no_data",
            message: "No data available"
          }, { headers: cors });
        }

        const metaJson = JSON.parse(meta);
        return Response.json({
          status: "data_available",
          storedAt: metaJson.storedAt,
          counts: metaJson.counts,
          source: metaJson.source
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

    return new Response("404 - Use: /api/push/tally (POST) or /api/imports/latest (GET)", {
      status: 404,
      headers: cors
    });
  },
};
