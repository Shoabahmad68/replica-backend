export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (url.pathname === "/") return new Response("Replica Unified Backend Active ✅", { headers: cors });
    if (url.pathname === "/api/test")
      return new Response(JSON.stringify({ status: "ok", message: "Backend Live", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json", ...cors },
      });

    // utility: base64 gzip decode (web streams)
    async function decodeAndDecompress(b64) {
      if (!b64) return "";
      try {
        const bin = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
        const stream = new DecompressionStream("gzip");
        const decompressed = await new Response(new Blob([bin]).stream().pipeThrough(stream)).arrayBuffer();
        return new TextDecoder().decode(decompressed);
      } catch (e) {
        console.warn("Decompress failed:", e?.message || e);
        // try plain base64->text fallback
        try {
          return atob(b64);
        } catch (e2) {
          return "";
        }
      }
    }

    // small XML helpers using regex (robust for typical Tally XML)
    const extractBlocks = (xml, tag) => {
      if (!xml) return [];
      const re = new RegExp(`<${tag}[\\s\\S]*?<\\/${tag}>`, "gi");
      return xml.match(re) || [];
    };
    const getTag = (text, tag) => {
      if (!text) return "";
      const re = new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i");
      const m = text.match(re);
      return m ? m[1].trim() : "";
    };
    const getAllTags = (text, tag) => {
      if (!text) return [];
      const re = new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "gi");
      const matches = [];
      let m;
      // eslint-disable-next-line no-cond-assign
      while ((m = re.exec(text)) !== null) matches.push(m[1].trim());
      return matches;
    };

    // return array of {tag, value} for simple tags at top-level inside a block
    const getAllTagPairs = (text) => {
      if (!text) return [];
      const re = /<([A-Z0-9_.:-]+)>([\s\S]*?)<\/\1>/gi;
      const out = [];
      let m;
      while ((m = re.exec(text)) !== null) {
        const tag = m[1].trim();
        const val = (m[2] || "").trim();
        // skip tags that contain nested child tags (we only want simple text tags)
        if (!/<[A-Z0-9_.:-]+>/i.test(val)) {
          out.push({ tag, value: val });
        }
      }
      return out;
    };

    // parseVoucher (captures all simple tags + items + ledger entries)
    const parseVoucher = (vXml) => {
      const voucher = {
        VoucherType: getTag(vXml, "VOUCHERTYPENAME"),
        VoucherNumber: getTag(vXml, "VOUCHERNUMBER") || getTag(vXml, "VOUCHERKEY"),
        Date: getTag(vXml, "DATE"),
        PartyName: getTag(vXml, "PARTYNAME") || getTag(vXml, "PARTYGSTIN") || getTag(vXml, "PARTYLEDGERNAME"),
        PartyLedger: getTag(vXml, "PARTYLEDGERNAME"),
        VoucherNarration: getTag(vXml, "NARRATION"),
        VoucherAmount: parseFloat(getTag(vXml, "AMOUNT") || "0") || 0,
        VchType: getTag(vXml, "VCHTYPE") || getTag(vXml, "VOUCHERTYPENAME"),
        InvoiceNo: getTag(vXml, "BILLALLOCATIONS.LIST>NAME") || "",
        Salesman: getTag(vXml, "BASICSALESNAME") || getTag(vXml, "SALESMAN") || getTag(vXml, "BASICSALESMAN") || "",
        Reference: getTag(vXml, "REFERENCE") || getTag(vXml, "INVOICENO") || "",
        __raw: vXml,
      };

      try {
        const pairs = getAllTagPairs(vXml);
        for (const p of pairs) {
          const key = p.tag.replace(/[^A-Za-z0-9_]/g, "_");
          if (!(key in voucher)) voucher[key] = p.value;
        }
      } catch (e) {
        console.warn("getAllTagPairs failed:", e?.message || e);
      }

      const ledgerEntries = [];
      for (const l of extractBlocks(vXml, "LEDGERENTRIES.LIST")) {
        ledgerEntries.push({
          LedgerName: getTag(l, "LEDGERNAME"),
          Amount: parseFloat(getTag(l, "AMOUNT") || "0") || 0,
          Narration: getTag(l, "NARRATION"),
        });
      }
      voucher.LedgerEntries = ledgerEntries;

      const itemRows = [];
      for (const it of extractBlocks(vXml, "ALLINVENTORYENTRIES.LIST")) {
        const item = {
          StockItemName: getTag(it, "STOCKITEMNAME") || getTag(it, "NAME"),
          ItemGroup: getTag(it, "STOCKGROUPNAME") || getTag(it, "ITEMGROUP"),
          ItemCategory: getTag(it, "CATEGORY") || getTag(it, "ITEMCATEGORY"),
          BilledQty: getTag(it, "BILLEDQTY") || getTag(it, "ACTUALQTY") || "",
          AltQty: getTag(it, "ALTQTY") || "",
          Rate: parseFloat(getTag(it, "RATE") || "0") || 0,
          Amount: parseFloat(getTag(it, "AMOUNT") || "0") || 0,
          UOM: getTag(it, "UOM") || getTag(it, "UOMNAME") || "",
          BatchName: getTag(it, "BATCHNAME") || "",
          Godown: getTag(it, "GODOWNNAME") || "",
          Narration: getTag(it, "NARRATION") || "",
        };

        try {
          const ipairs = getAllTagPairs(it);
          for (const p of ipairs) {
            const k = p.tag.replace(/[^A-Za-z0-9_]/g, "_");
            if (!(k in item)) item[k] = p.value;
          }
        } catch (e) {
          console.warn("getAllTagPairs(item) failed:", e?.message || e);
        }

        itemRows.push(item);
      }

      if (itemRows.length === 0) {
        const stockNames = getAllTags(vXml, "STOCKITEMNAME");
        const rates = getAllTags(vXml, "RATE");
        const qtys = getAllTags(vXml, "BILLEDQTY");
        for (let i = 0; i < stockNames.length; i++) {
          itemRows.push({
            StockItemName: stockNames[i] || "",
            BilledQty: qtys[i] || "",
            Rate: parseFloat(rates[i] || "0") || 0,
            Amount: parseFloat((getAllTags(vXml, "AMOUNT")[i] || "0")) || 0,
            ItemGroup: "",
            AltQty: "",
            UOM: "",
          });
        }
      }

      voucher.Items = itemRows;
      return voucher;
    };

    // parse outstanding reports into row list (flexible)
    const parseOutstanding = (xml) => {
      const rows = [];
      for (const b of extractBlocks(xml, "LEDGER")) {
        rows.push({
          Name: getTag(b, "NAME"),
          ClosingBalance: getTag(b, "CLOSINGBALANCE") || getTag(b, "AMOUNT"),
          Age: getTag(b, "AGE") || getTag(b, "DAYS"),
          __raw: b,
        });
      }
      for (const b of extractBlocks(xml, "OUTSTANDINGITEMS.LIST")) {
        rows.push({
          Ref: getTag(b, "REFERENCE") || getTag(b, "NAME"),
          Amount: getTag(b, "AMOUNT"),
          DueDate: getTag(b, "DUEDATE"),
          Days: getTag(b, "DAYS"),
          __raw: b,
        });
      }
      if (rows.length === 0) {
        const names = getAllTags(xml, "NAME");
        const amts = getAllTags(xml, "AMOUNT");
        for (let i = 0; i < Math.max(names.length, amts.length); i++) {
          rows.push({ Name: names[i] || "", Amount: amts[i] || "" });
        }
      }
      return rows;
    };

    // ---------------- main push endpoint ----------------
    if (url.pathname === "/api/push/tally" && request.method === "POST") {
      try {
        const body = await request.json();

        // decode many XML parts if present
        const xmlSales = await decodeAndDecompress(body.salesXml || "");
        const xmlPurchase = await decodeAndDecompress(body.purchaseXml || "");
        const xmlReceipt = await decodeAndDecompress(body.receiptXml || "");
        const xmlPayment = await decodeAndDecompress(body.paymentXml || "");
        const xmlJournal = await decodeAndDecompress(body.journalXml || "");
        const xmlDebit = await decodeAndDecompress(body.debitXml || "");
        const xmlCredit = await decodeAndDecompress(body.creditXml || "");
        const xmlMasters = await decodeAndDecompress(body.mastersXml || "");
        const xmlOutstandingRec = await decodeAndDecompress(body.outstandingReceivableXml || "");
        const xmlOutstandingPay = await decodeAndDecompress(body.outstandingPayableXml || "");
        const rawOriginal = { bodyMeta: { time: body.time || new Date().toISOString(), source: body.source || "tally-pusher" } };

        // helper to parse voucher XML list into detailed rows (voucher-level + item-level)
        const parseVouchersToRows = (xml) => {
          const outVouchers = [];
          for (const v of extractBlocks(xml, "VOUCHER")) {
            const parsed = parseVoucher(v);
            outVouchers.push({
              type: "voucher_summary",
              VoucherType: parsed.VoucherType,
              VoucherNumber: parsed.VoucherNumber,
              Date: parsed.Date,
              PartyName: parsed.PartyName,
              Salesman: parsed.Salesman,
              Amount: parsed.VoucherAmount,
              Narration: parsed.VoucherNarration,
              LedgerEntries: parsed.LedgerEntries,
            });
            for (const it of parsed.Items) {
              outVouchers.push({
                type: "item_row",
                VoucherType: parsed.VoucherType,
                VoucherNumber: parsed.VoucherNumber,
                Date: parsed.Date,
                PartyName: parsed.PartyName,
                StockItemName: it.StockItemName,
                ItemGroup: it.ItemGroup,
                ItemCategory: it.ItemCategory,
                Qty: it.BilledQty,
                AltQty: it.AltQty,
                Rate: it.Rate,
                Amount: it.Amount,
                UOM: it.UOM,
                Salesman: parsed.Salesman,
                Narration: it.Narration || parsed.VoucherNarration,
              });
            }
          }
          return outVouchers;
        };

        const salesRows = parseVouchersToRows(xmlSales);
        const purchaseRows = parseVouchersToRows(xmlPurchase);
        const receiptRows = parseVouchersToRows(xmlReceipt);
        const paymentRows = parseVouchersToRows(xmlPayment);
        const journalRows = parseVouchersToRows(xmlJournal);
        const debitRows = parseVouchersToRows(xmlDebit);
        const creditRows = parseVouchersToRows(xmlCredit);
        const masterRows = [];
        for (const m of extractBlocks(xmlMasters, "LEDGER")) {
          masterRows.push({
            Type: "Ledger",
            Name: getTag(m, "NAME"),
            ClosingBalance: getTag(m, "CLOSINGBALANCE"),
            MailingName: getTag(m, "MAILINGNAME"),
            PrimaryGroup: getTag(m, "PARENT"),
            __raw: m,
          });
        }

        const outstandingReceivableRows = parseOutstanding(xmlOutstandingRec);
        const outstandingPayableRows = parseOutstanding(xmlOutstandingPay);

        // Debug output – check keys
        try {
          const sample = salesRows.slice(0, 6);
          console.log("☑️ sample salesRows count:", salesRows.length, "sample keys:");
          const allKeys = new Set();
          for (const r of salesRows.slice(0, Math.min(salesRows.length, 200))) {
            Object.keys(r).forEach(k => allKeys.add(k));
          }
          console.log(Array.from(allKeys).sort());
          console.log("☑️ sample row[0]:", JSON.stringify(sample[0] || {}, null, 2).slice(0, 2000));
        } catch (e) {
          console.warn("debug print failed", e?.message || e);
        }

        // final normalized payload
        const finalPayload = {
          status: "ok",
          source: rawOriginal.bodyMeta.source,
          time: rawOriginal.bodyMeta.time,
          counts: {
            sales: salesRows.length,
            purchase: purchaseRows.length,
            receipt: receiptRows.length,
            payment: paymentRows.length,
            journal: journalRows.length,
            debit: debitRows.length,
            credit: creditRows.length,
            masters: masterRows.length,
            outstandingReceivable: outstandingReceivableRows.length,
            outstandingPayable: outstandingPayableRows.length,
          },
          rows: {
            sales: salesRows,
            purchase: purchaseRows,
            receipt: receiptRows,
            payment: paymentRows,
            journal: journalRows,
            debit: debitRows,
            credit: creditRows,
            masters: masterRows,
            outstandingReceivable: outstandingReceivableRows,
            outstandingPayable: outstandingPayableRows,
          },
          rawSample: {
            salesHead: xmlSales ? xmlSales.slice(0, 4000) : "",
            purchaseHead: xmlPurchase ? xmlPurchase.slice(0, 4000) : "",
            mastersHead: xmlMasters ? xmlMasters.slice(0, 2000) : "",
          },
        };

        // ---------------- SAFE KV STORE (chunked, overwrite old) ----------------
        try {
          // cleanup previous parts if any
          try {
            const listed = await env.REPLICA_DATA.list({ prefix: "latest_tally_json_part_" });
            for (const k of listed.keys || []) {
              try { await env.REPLICA_DATA.delete(k.name); } catch (_) {}
            }
            // also delete metadata key if exists
            try { await env.REPLICA_DATA.delete("latest_tally_json"); } catch (_) {}
          } catch (e) {
            console.warn("KV cleanup list failed:", e?.message || e);
          }

          const dataStr = JSON.stringify(finalPayload);
          const CHUNK_SIZE = 6_000_000; // ~6 million chars ~ safe under 25MB KV value limit
          let idx = 0;
          for (let i = 0; i < dataStr.length; i += CHUNK_SIZE) {
            const part = dataStr.slice(i, i + CHUNK_SIZE);
            await env.REPLICA_DATA.put(`latest_tally_json_part_${idx}`, part);
            idx++;
          }
          // store metadata so frontend can fetch and reassemble
          const meta = { parts: idx, storedAt: new Date().toISOString(), counts: finalPayload.counts };
          await env.REPLICA_DATA.put("latest_tally_json", JSON.stringify(meta));

          // optional small raw backup (only heads)
          try {
            await env.REPLICA_DATA.put("latest_tally_raw", JSON.stringify({
              received: rawOriginal,
              salesXmlHead: xmlSales ? xmlSales.slice(0, 20000) : "",
              purchaseXmlHead: xmlPurchase ? xmlPurchase.slice(0, 20000) : "",
            }));
          } catch (e) {
            console.warn("kv raw store issue", e?.message || e);
          }

        } catch (e) {
          console.warn("KV chunk store failed:", e?.message || e);
          return new Response(JSON.stringify({ error: "KV store failed", detail: e?.message || e }), { status: 500, headers: { "Content-Type": "application/json", ...cors } });
        }

        return new Response(JSON.stringify({ success: true, message: "Full parsed data stored in chunks.", counts: finalPayload.counts }), {
          headers: { "Content-Type": "application/json", ...cors },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err?.message || "Processing failed" }), {
          status: 500,
          headers: { "Content-Type": "application/json", ...cors },
        });
      }
    }

    // ------------------ FETCH ENDPOINT (assemble chunks) ------------------
    if (url.pathname === "/api/imports/latest" && request.method === "GET") {
      try {
        const meta = await env.REPLICA_DATA.get("latest_tally_json");
        if (!meta) return new Response(JSON.stringify({ status: "empty" }), { headers: { "Content-Type": "application/json", ...cors } });

        const metaJson = JSON.parse(meta);
        if (metaJson.parts && Number.isInteger(metaJson.parts) && metaJson.parts > 0) {
          let merged = "";
          for (let i = 0; i < metaJson.parts; i++) {
            const part = await env.REPLICA_DATA.get(`latest_tally_json_part_${i}`);
            if (part) merged += part;
          }
          return new Response(merged, { headers: { "Content-Type": "application/json", ...cors } });
        }

        // fallback: if meta is actual payload string (older behaviour)
        return new Response(meta, { headers: { "Content-Type": "application/json", ...cors } });

      } catch (e) {
        return new Response(JSON.stringify({ error: "Failed to fetch latest", detail: e?.message || e }), { status: 500, headers: { "Content-Type": "application/json", ...cors } });
      }
    }

    return new Response("404 Not Found", { status: 404, headers: cors });
  },
};
