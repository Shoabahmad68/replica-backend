export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    
    // Root endpoint
    if (url.pathname === "/") 
      return new Response("Replica Backend Active ✅ | Use /api/push/tally or /api/imports/latest", { headers: cors });
    
    // Test endpoint
    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({ 
        status: "ok", 
        message: "Backend Live", 
        time: new Date().toISOString() 
      }), {
        headers: { "Content-Type": "application/json", ...cors },
      });

    // ------------------ FIXED FETCH ENDPOINT ------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        console.log("🔍 Fetching latest data from KV...");
        
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        if (!meta) {
          return new Response(JSON.stringify({ 
            status: "empty", 
            message: "No data available" 
          }), { 
            headers: { "Content-Type": "application/json", ...cors } 
          });
        }

        const metaJson = JSON.parse(meta);
        console.log("📊 Metadata found, parts:", metaJson.parts);

        // If data is chunked, assemble it
        if (metaJson.parts && Number.isInteger(metaJson.parts) && metaJson.parts > 0) {
          let merged = "";
          for (let i = 0; i < metaJson.parts; i++) {
            const partKey = `latest_tally_json_part_${i}`;
            const part = await env.REPLICA_DATA.get(partKey);
            if (part) {
              merged += part; // ✅ CORRECT: += 
              console.log(`✅ Loaded part ${i}: ${part.length} chars`);
            }
          }
          
          if (merged) {
            console.log(`🎯 Returning merged data: ${merged.length} chars`);
            return new Response(merged, { 
              headers: { "Content-Type": "application/json", ...cors } 
            });
          }
        }

        // fallback: return metadata
        return new Response(meta, { 
          headers: { "Content-Type": "application/json", ...cors } 
        });

      } catch (e) {
        console.error("❌ Fetch error:", e.message);
        return new Response(JSON.stringify({ 
          error: "Failed to fetch latest", 
          detail: e.message 
        }), { 
          status: 500, 
          headers: { "Content-Type": "application/json", ...cors } 
        });
      }
    }

    // ------------------ PUSH ENDPOINT (Your original working code) ------------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();
        console.log("📥 Received data from pusher");

        // Decode compressed data
        async function decodeAndDecompress(b64) {
          if (!b64) return "";
          try {
            const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
            const stream = new DecompressionStream("gzip");
            const decompressed = await new Response(new Blob([bin]).stream().pipeThrough(stream)).arrayBuffer();
            return new TextDecoder().decode(decompressed);
          } catch (e) {
            console.warn("Decompress failed, trying plain text");
            try {
              return atob(b64);
            } catch (e2) {
              return "";
            }
          }
        }

        // Simple XML parsing
        function extractBlocks(xml, tag) {
          if (!xml) return [];
          const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'gi');
          const matches = [];
          let match;
          while ((match = re.exec(xml)) !== null) {
            matches.push(match[0]);
          }
          return matches;
        }

        function getTag(text, tag) {
          if (!text) return "";
          const re = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'i');
          const m = text.match(re);
          return m ? m[1].trim() : "";
        }

        // Decode all XML data
        const xmlData = {
          sales: await decodeAndDecompress(body.salesXml),
          purchase: await decodeAndDecompress(body.purchaseXml),
          receipt: await decodeAndDecompress(body.receiptXml),
          payment: await decodeAndDecompress(body.paymentXml),
          journal: await decodeAndDecompress(body.journalXml),
          debit: await decodeAndDecompress(body.debitXml),
          credit: await decodeAndDecompress(body.creditXml),
          masters: await decodeAndDecompress(body.mastersXml),
          outstanding: await decodeAndDecompress(body.outstandingXml || body.outstandingReceivableXml),
        };

        console.log("📊 Decoded XML sizes:");
        Object.keys(xmlData).forEach(key => {
          console.log(`   ${key}: ${xmlData[key]?.length || 0} chars`);
        });

        // Parse vouchers
        function parseVouchers(xml, type) {
          if (!xml) return [];
          const vouchers = extractBlocks(xml, 'VOUCHER');
          const rows = [];
          
          for (const voucherXml of vouchers) {
            const voucher = {
              type: type,
              VoucherType: getTag(voucherXml, 'VOUCHERTYPENAME') || type,
              VoucherNumber: getTag(voucherXml, 'VOUCHERNUMBER') || 'N/A',
              Date: getTag(voucherXml, 'DATE'),
              PartyName: getTag(voucherXml, 'PARTYNAME') || 'Unknown',
              Amount: getTag(voucherXml, 'AMOUNT') || '0',
            };
            rows.push(voucher);
          }
          return rows;
        }

        // Parse masters
        function parseMasters(xml) {
          if (!xml) return [];
          const masters = extractBlocks(xml, 'LEDGER');
          return masters.map(ledger => ({
            type: 'master',
            Name: getTag(ledger, 'NAME'),
            Parent: getTag(ledger, 'PARENT'),
            ClosingBalance: getTag(ledger, 'CLOSINGBALANCE'),
          })).filter(m => m.Name);
        }

        // Parse all data
        const parsedData = {
          sales: parseVouchers(xmlData.sales, 'sales'),
          purchase: parseVouchers(xmlData.purchase, 'purchase'),
          receipt: parseVouchers(xmlData.receipt, 'receipt'),
          payment: parseVouchers(xmlData.payment, 'payment'),
          journal: parseVouchers(xmlData.journal, 'journal'),
          debit: parseVouchers(xmlData.debit, 'debit'),
          credit: parseVouchers(xmlData.credit, 'credit'),
          masters: parseMasters(xmlData.masters),
          outstanding: [],
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

        // Final payload
        const finalPayload = {
          status: "ok",
          source: body.source || "tally-pusher-final",
          time: body.time || new Date().toISOString(),
          counts: counts,
          rows: parsedData,
        };

        // Save to KV (single key for simplicity)
        try {
          const dataStr = JSON.stringify(finalPayload);
          await env.REPLICA_DATA.put("latest_tally_data", dataStr);
          
          // Save metadata separately
          await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify({
            storedAt: new Date().toISOString(),
            counts: counts,
            source: body.source || "tally-pusher"
          }));
          
          console.log("💾 Saved to KV successfully");
        } catch (kvError) {
          console.error("❌ KV save error:", kvError.message);
        }

        return new Response(JSON.stringify({
          success: true,
          message: "Data processed and stored",
          counts: counts
        }), {
          headers: { "Content-Type": "application/json", ...cors },
        });

      } catch (err) {
        console.error("❌ Push error:", err.message);
        return new Response(JSON.stringify({ 
          error: "Processing failed",
          detail: err.message 
        }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // Summary endpoint
    if (url.pathname === "/api/summary" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        if (!meta) {
          return new Response(JSON.stringify({ 
            status: "no_data",
            message: "No data available" 
          }), { 
            headers: { "Content-Type": "application/json", ...cors } 
          });
        }

        const metaJson = JSON.parse(meta);
        return new Response(JSON.stringify({
          status: "data_available",
          storedAt: metaJson.storedAt,
          counts: metaJson.counts,
          source: metaJson.source
        }), { 
          headers: { "Content-Type": "application/json", ...cors } 
        });

      } catch (e) {
        return new Response(JSON.stringify({ 
          error: "Summary fetch failed" 
        }), { 
          status: 500, 
          headers: { "Content-Type": "application/json", ...cors } 
        });
      }
    }

    return new Response("404 - Available endpoints: /api/push/tally (POST), /api/imports/latest (GET), /api/summary (GET)", {
      status: 404,
      headers: cors
    });
  },
};
