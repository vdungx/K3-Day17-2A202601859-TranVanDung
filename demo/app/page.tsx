"use client";

import { useState } from "react";

const pipeline = [
  { id: "01", name: "Sources", meta: "CDC · Events · Transcripts", text: "Ba luồng nguồn giữ nguyên event time và ingestion time để có thể replay, đo độ trễ và điều tra." },
  { id: "02", name: "Bronze", meta: "Raw · Append-only", text: "Payload thô được nạp theo từng ngày vận hành. Không sửa dữ liệu nguồn; mọi quyết định chất lượng được đẩy sang Silver." },
  { id: "03", name: "Silver", meta: "Normalize · Deduplicate", text: "CDC được xếp hạng theo event_time và cdc_seq. Priority được chuẩn hóa; record lỗi được tách trước khi chọn trạng thái hợp lệ mới nhất." },
  { id: "04", name: "Gold", meta: "Training · Features · Chunks", text: "Ba mô hình phục vụ AI: tập huấn luyện theo ticket, feature theo ngày/khách hàng và document chunks." },
  { id: "05", name: "Serving", meta: "AI · Dashboard · QA", text: "Dữ liệu ổn định cấp cho mô hình và dashboard. dbt tests, checksum và data invariants bảo vệ đầu ra." },
];

const incidents = [
  {
    id: "idempotency", label: "01", title: "Retry tạo bản ghi trùng", severity: "P1",
    symptom: "38.750 hàng sau 3 lượt, thay vì 12.480. Cả 12.480 ticket đều bị lặp.",
    cause: "Incremental model chỉ append, không có unique_key và merge strategy.",
    fix: "MERGE theo ticket_id; DAG khóa một active run và không tự catch up.",
    result: "12.480 hàng · 0 duplicate · checksum 8dd7c98653",
    trace: [
      ["02:00:00", "Airflow", "Run partition 2026-08-09", "ok"],
      ["02:00:14", "dbt", "Incremental INSERT lặp entity", "bad"],
      ["02:00:18", "guard", "unique(ticket_id) phát hiện duplicate", "bad"],
      ["02:06:31", "dbt", "MERGE ON ticket_id → update in place", "ok"],
      ["02:06:33", "verify", "3/3 checksum trùng khớp", "ok"],
    ],
  },
  {
    id: "late", label: "02", title: "Late-arriving events", severity: "P2",
    symptom: "Thiếu 455 cặp (event_date, customer_id) ở các ngày quá khứ.",
    cause: "Watermark chỉ đi tới; event cũ ingest muộn không bao giờ được tính lại.",
    fix: "Lookback 3 ngày theo ceil(P99); delete+insert bằng khóa kép.",
    result: "9.100 cặp · P99 2,726 ngày · checksum 3db448685c",
    trace: [
      ["D+0", "event", "event_date = 2026-08-12", "ok"],
      ["D+3", "ingest", "_ingested_at = 2026-08-15", "warn"],
      ["D+3", "watermark", "Mở lại window [08-12 → 08-15]", "ok"],
      ["D+3", "dbt", "DELETE+INSERT grain date + customer", "ok"],
      ["D+3", "verify", "455 feature rows được phục hồi", "ok"],
    ],
  },
  {
    id: "contract", label: "03", title: "Schema drift ở priority", severity: "P1",
    symptom: "6.606 priority sai/NULL; pipeline cũ vẫn xanh và quarantine rỗng.",
    cause: "Backend đổi số sang nhãn; try_cast làm mất nhãn hợp lệ và nhận số ngoài miền.",
    fix: "Một macro CASE dùng chung cho accepted/rejected; contract + accepted_values.",
    result: "12.480 ticket sạch · 312 record quarantine · 11/11 tests",
    trace: [
      ["10:41:03", "source", "priority_raw = 'urgent'", "warn"],
      ["10:41:04", "macro", "urgent → 1 (accepted)", "ok"],
      ["10:41:05", "source", "priority_raw = 'P1'", "bad"],
      ["10:41:06", "router", "P1 → UNKNOWN_LABEL", "bad"],
      ["10:41:07", "quarantine", "Giữ raw payload + reject_reason", "ok"],
    ],
  },
  {
    id: "crash", label: "04", title: "Consumer chết giữa batch", severity: "P0",
    symptom: "Commit offset trước write gây mất dữ liệu khi process chết ở batch 7.",
    cause: "At-most-once delivery và write không có khóa chống replay.",
    fix: "Commit DuckDB trước offset; upsert theo primary key event_id.",
    result: "20.000/20.000 · không mất · không trùng · replay an toàn",
    trace: [
      ["batch 07", "write", "UPSERT 500 events", "ok"],
      ["batch 07", "database", "COMMIT durable transaction", "ok"],
      ["batch 07", "process", "SIGKILL trước offset commit", "bad"],
      ["restart", "consumer", "Replay batch 07 từ offset 3.000", "warn"],
      ["restart", "upsert", "ON CONFLICT DO UPDATE · 0 duplicate", "ok"],
    ],
  },
  {
    id: "smallfiles", label: "05", title: "Small-file bottleneck", severity: "P2",
    symptom: "Dashboard mở 5.000 file nhỏ và quét 5 triệu hàng cho một ngày.",
    cause: "Không partition; predicate strftime không cho phép pruning.",
    fix: "Compact 14 partition event_date; sort; row group 2.048; predicate sargable.",
    result: "5.000 → 14 files · 5.000.000 → 9.324 rows · 536,3×",
    trace: [
      ["query", "planner", "strftime(event_time) → full scan", "bad"],
      ["compact", "parquet", "PARTITION_BY event_date", "ok"],
      ["compact", "layout", "SORT customer_name, event_time", "ok"],
      ["query", "pruning", "event_date = 2026-08-09", "ok"],
      ["result", "hash", "4379e4c5d9f3 · không đổi", "ok"],
    ],
  },
];

const evidence = [
  ["gold_training_set", "12.480", "8dd7c98653", "1 ticket / row"],
  ["gold_feature_daily", "9.100", "3db448685c", "3-day lookback"],
  ["gold_doc_chunks", "31.200", "92d8e50131", "control table"],
  ["quarantine_tickets", "312", "ebb89036fb", "rejected CDC"],
];

export default function Home() {
  const [activeStage, setActiveStage] = useState(2);
  const [activeIncident, setActiveIncident] = useState(0);
  const current = incidents[activeIncident];

  return (
    <main>
      <nav className="nav">
        <a className="brand" href="#top" aria-label="Về đầu trang"><span className="brandMark">D17</span><span>PIPELINE / FIELD REPORT</span></a>
        <div className="navLinks"><a href="#flow">Luồng dữ liệu</a><a href="#incidents">Trace lỗi</a><a href="#evidence">Kiểm chứng</a></div>
        <div className="navStatus"><i /> SYSTEM HEALTHY</div>
      </nav>

      <section className="hero" id="top">
        <div className="eyebrow"><span>LAB 17</span><span>DATA PIPELINE ENGINEERING</span><span>17.08.2026</span></div>
        <div className="heroGrid">
          <div>
            <h1>Pipeline đúng<br />khi <em>chạy lại.</em></h1>
            <p className="heroLead">Một hệ thống dữ liệu cho nền tảng AI hỗ trợ khách hàng—được sửa để chịu được retry, dữ liệu về muộn, schema drift và crash giữa batch.</p>
            <div className="heroActions"><a className="primary" href="#flow">Xem kiến trúc <span>↓</span></a><a className="secondary" href="#incidents">Mở incident trace</a></div>
          </div>
          <div className="scoreCard">
            <div className="scoreTop"><span>FINAL VERIFICATION</span><span className="live"><i /> PASSED</span></div>
            <div className="score"><strong>4</strong><span>/ 4</span></div>
            <div className="scoreLine"><span>Pipeline runs</span><b>3 / 3 stable</b></div>
            <div className="scoreLine"><span>dbt tests</span><b>11 / 11 pass</b></div>
            <div className="scoreLine"><span>Duplicate tickets</span><b>0</b></div>
          </div>
        </div>
        <div className="metrics">
          <div><small>TRAINING ROWS</small><strong>12.480</strong><span>exact grain</span></div>
          <div><small>FEATURE PAIRS</small><strong>9.100</strong><span>late data recovered</span></div>
          <div><small>QUARANTINED</small><strong>312</strong><span>fully traceable</span></div>
          <div><small>SCAN REDUCTION</small><strong>536,3×</strong><span>same result hash</span></div>
        </div>
      </section>

      <section className="section dark" id="flow">
        <header className="sectionHead"><div><span className="sectionNo">01 / ARCHITECTURE</span><h2>Luồng dữ liệu,<br />từ raw đến AI.</h2></div><p>Nhấn vào từng tầng để xem trách nhiệm. Mọi nhánh đều giữ đủ dấu vết để replay và điều tra.</p></header>
        <div className="flow">
          {pipeline.map((stage, i) => (
            <button key={stage.id} className={`flowNode ${activeStage === i ? "active" : ""}`} onClick={() => setActiveStage(i)} aria-pressed={activeStage === i}>
              <span className="nodeNo">{stage.id}</span><b>{stage.name}</b><small>{stage.meta}</small>{i < pipeline.length - 1 && <span className="connector">→</span>}
            </button>
          ))}
        </div>
        <div className="stageDetail"><div><span>ACTIVE LAYER / {pipeline[activeStage].id}</span><h3>{pipeline[activeStage].name}</h3></div><p>{pipeline[activeStage].text}</p><code>{activeStage === 0 ? "tickets_cdc.jsonl · events.jsonl · transcripts.jsonl" : activeStage === 1 ? "load_day(run_date) → bronze_*" : activeStage === 2 ? "normalize → validate → rank → route" : activeStage === 3 ? "merge · delete+insert · table" : "AI training · daily features · dashboard"}</code></div>
        <div className="branchDiagram">
          <div className="branchInput">priority_raw</div><div className="branchRule"><span>normalize_priority()</span></div>
          <div className="branchOutputs"><div><i className="okDot" /><b>VALID</b><span>Silver → Gold</span><small>1 · urgent · high · medium · low</small></div><div><i className="badDot" /><b>REJECTED</b><span>Quarantine</span><small>NULL · P1 · unknown · out of range</small></div></div>
        </div>
      </section>

      <section className="section incidents" id="incidents">
        <header className="sectionHead light"><div><span className="sectionNo">02 / INCIDENT PLAYBOOK</span><h2>Không chỉ sửa lỗi.<br />Trace đến tận gốc.</h2></div><p>Năm failure mode thực tế, mỗi case có triệu chứng, nguyên nhân, thay đổi và bằng chứng sau sửa.</p></header>
        <div className="incidentLayout">
          <div className="incidentList" role="tablist" aria-label="Các sự cố">
            {incidents.map((item, i) => <button key={item.id} role="tab" aria-selected={activeIncident === i} className={activeIncident === i ? "active" : ""} onClick={() => setActiveIncident(i)}><span>{item.label}</span><b>{item.title}</b><small>{item.severity}</small></button>)}
          </div>
          <article className="incidentDetail" role="tabpanel">
            <div className="incidentTitle"><div><span>INC-{current.label} · {current.severity}</span><h3>{current.title}</h3></div><span className="resolved">RESOLVED</span></div>
            <div className="diagnosis">
              <div><small>TRIỆU CHỨNG</small><p>{current.symptom}</p></div><div><small>NGUYÊN NHÂN GỐC</small><p>{current.cause}</p></div><div><small>THAY ĐỔI</small><p>{current.fix}</p></div><div className="result"><small>KẾT QUẢ</small><p>{current.result}</p></div>
            </div>
            <div className="trace"><div className="traceHead"><span>EXECUTION TRACE</span><span>5 EVENTS</span></div>{current.trace.map(([time, source, message, state], i) => <div className="traceRow" key={`${time}-${i}`}><time>{time}</time><span className={`traceState ${state}`}>{state === "ok" ? "✓" : state === "bad" ? "!" : "~"}</span><code>{source}</code><p>{message}</p></div>)}</div>
          </article>
        </div>
      </section>

      <section className="section dark compare">
        <header className="sectionHead"><div><span className="sectionNo">03 / PERFORMANCE</span><h2>Ít file hơn.<br />Ít quét hơn.</h2></div><p>Layout Parquet được tổ chức lại theo cách truy vấn thực sự sử dụng dữ liệu, trong khi result hash giữ nguyên.</p></header>
        <div className="compareGrid"><div className="before"><span>BEFORE</span><strong>5.000.000</strong><small>ROWS SCANNED</small><div className="bar"><i /></div><p>5.000 files · full scan<br />strftime(event_time)</p></div><div className="arrow">→</div><div className="after"><span>AFTER</span><strong>9.324</strong><small>ROWS SCANNED</small><div className="bar"><i /></div><p>14 partitions · pruning<br />event_date = DATE</p></div></div>
        <div className="perfFoot"><span><b>130.683</b> rows preserved</span><span><b>4379e4c5d9f3</b> result hash</span><span><b>2.048</b> row group size</span><span><b>536,3×</b> less scanning</span></div>
      </section>

      <section className="section evidence" id="evidence">
        <header className="sectionHead light"><div><span className="sectionNo">04 / EVIDENCE</span><h2>Chứng minh bằng<br />dữ liệu, không cảm tính.</h2></div><p>Ba lượt chạy liên tiếp tạo cùng số hàng và checksum. Các invariant độc lập bảo vệ chất lượng dữ liệu.</p></header>
        <div className="evidenceGrid"><div className="verifyCard"><div className="verifyHeader"><span>make verify</span><b><i /> ALL CHECKS PASS</b></div>{evidence.map((row) => <div className="verifyRow" key={row[0]}><code>{row[0]}</code><strong>{row[1]}</strong><span>{row[2]}</span><small>{row[3]}</small><b>✓</b></div>)}</div><div className="guardrails"><span>GUARDRAILS</span><div><b>01</b><p><strong>Entity idempotency</strong>MERGE theo natural key, không phụ thuộc scheduler.</p></div><div><b>02</b><p><strong>Time-aware incrementals</strong>Phân biệt event time với ingestion time.</p></div><div><b>03</b><p><strong>Observable rejection</strong>Không âm thầm cast NULL; lỗi có reason rõ ràng.</p></div><div><b>04</b><p><strong>Crash-safe delivery</strong>Durable write trước offset, replay bằng upsert.</p></div></div></div>
      </section>

      <footer><div><span className="brandMark invert">D17</span><p>Data Pipeline Engineering<br /><small>Trần Văn Dũng · 2A202601859</small></p></div><div className="footerStatus"><i /> VERIFIED · 4/4 · 11/11 TESTS</div><a href="#top">BACK TO TOP ↑</a></footer>
    </main>
  );
}
