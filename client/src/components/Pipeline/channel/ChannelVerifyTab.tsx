import React from 'react';
import { AlertTriangle, CheckCircle2, Clock, MessageSquare } from 'lucide-react';
import { useSetRecoilState } from 'recoil';
import type { PipelineChannelRun, PipelineVerifyRow } from '~/data-provider';
import OwnFlowVerifyReport, {
  type OwnFlowProcessingVerifyPayload,
  type OwnFlowRuleVerifyRow,
} from '~/components/Pipeline/channel/OwnFlowVerifyReport';
import BillVerifyReport, {
  type BillMergeReportPayload,
} from '~/components/Pipeline/channel/BillVerifyReport';
import {
  pipelineCopilotOpenAtom,
  pipelineCopilotPrefillAtom,
} from '~/store/pipeline';

function SeverityIcon({ severity }: { severity: PipelineVerifyRow['severity'] }) {
  if (severity === 'pass') return <CheckCircle2 className="h-3.5 w-3.5 text-green-400" />;
  if (severity === 'warning') return <AlertTriangle className="h-3.5 w-3.5 text-amber-400" />;
  return <Clock className="h-3.5 w-3.5 text-text-secondary" />;
}

export default function ChannelVerifyTab({
  taskId,
  channelId,
  run,
}: {
  taskId: string;
  channelId: string;
  run: PipelineChannelRun | undefined;
}) {
  const setOpen = useSetRecoilState(pipelineCopilotOpenAtom);
  const setPrefill = useSetRecoilState(pipelineCopilotPrefillAtom);
  void taskId;

  const askAgent = (row: PipelineVerifyRow) => {
    setPrefill({
      question: `渠道 ${channelId} 校验行 ${row.row_id}（${row.summary}）为什么是 ${row.severity}？引用对应规则与文件原始行回答。`,
      channel_id: channelId,
      run_id: run?.run_id,
      verify_row_id: row.row_id,
      stamp: Date.now(),
    });
    setOpen(true);
  };

  if (!run) {
    return (
      <div className="p-6 text-sm text-text-secondary">
        尚无运行结果，请先执行一次。
      </div>
    );
  }
  const rows = run.verify_summary?.rows ?? [];
  const warnings = run.verify_summary?.warnings ?? [];
  const note = run.verify_summary?.note ?? null;

  const ownFlowProcessingVerify = run.verify_summary?.metrics
    ?.own_flow_processing_verify as OwnFlowProcessingVerifyPayload | undefined;
  const showOwnFlowRuleReport =
    channelId === 'own_flow' &&
    ownFlowProcessingVerify &&
    Array.isArray(ownFlowProcessingVerify.rules) &&
    ownFlowProcessingVerify.rules.length > 0;

  const billMergeReport = run.verify_summary?.metrics?.bill_merge_report as
    | BillMergeReportPayload
    | undefined;
  const showBillMergeReport =
    channelId === 'bill' &&
    billMergeReport &&
    billMergeReport.summary &&
    typeof billMergeReport.summary.success === 'number' &&
    Array.isArray(billMergeReport.blocks) &&
    typeof billMergeReport.raw_log === 'string';

  /** 结构化报告已覆盖的条目不再重复出现在表格（如 bill.bank.* / own.rule.*） */
  let legacyVerifyRows = rows;
  if (showOwnFlowRuleReport) {
    legacyVerifyRows = legacyVerifyRows.filter((r) => !String(r.row_id).startsWith('own.rule.'));
  }
  if (showBillMergeReport) {
    legacyVerifyRows = legacyVerifyRows.filter((r) => !(r.rule_ref ?? '').startsWith('bill.bank.'));
  }

  const askRuleAgent = (rule: OwnFlowRuleVerifyRow) => {
    const seq = rule.规则序号;
    const match =
      seq !== undefined ? rows.find((x) => x.row_id === `own.rule.${seq}`) : undefined;
    if (match) {
      askAgent(match);
      return;
    }
    setPrefill({
      question: `自有流水校验规则 ${seq ?? '?'}：${rule.渠道 ?? ''} / ${rule.文件 ?? ''} — ${rule.说明 ?? ''}。为什么是「${rule.状态 ?? ''}」？`,
      channel_id: channelId,
      run_id: run.run_id,
      stamp: Date.now(),
    });
    setOpen(true);
  };

  return (
    <div className="space-y-3 p-4">
      <div className="rounded-lg border border-border-light bg-surface-primary p-3 text-xs text-text-secondary">
        当前显示最新一次 run（<span className="font-mono">{run.run_id.slice(0, 8)}</span>）的校验摘要。
        {showOwnFlowRuleReport
          ? ' 下方验证报告与 Allline「执行匹配」视图一致；规则行可点「Agent」追问。'
          : showBillMergeReport
            ? ' 账单分段视图与 Allline Streamlit「导入与合并」一致；其余条目仍可表格追问 Agent。'
            : ' 每行可点击「问 Agent」，Agent 会调用工具读取规则、文件、日志后回答。'}
      </div>

      {warnings.length > 0 && (
        <div className="rounded-lg border border-green-500/30 bg-green-500/5 p-3 text-sm text-text-primary">
          <div className="font-medium">告警</div>
          <ul className="mt-1 list-inside list-disc space-y-0.5 text-xs">
            {warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      {note && (
        <div className="rounded-lg border border-border-light bg-surface-secondary p-3 font-mono text-xs leading-relaxed text-text-secondary whitespace-pre-wrap">
          {note}
        </div>
      )}

      {showBillMergeReport ? <BillVerifyReport payload={billMergeReport} /> : null}

      {showOwnFlowRuleReport ? (
        <OwnFlowVerifyReport
          payload={ownFlowProcessingVerify}
          onAskAgent={askRuleAgent}
        />
      ) : null}

      {legacyVerifyRows.length === 0 ? (
        !showOwnFlowRuleReport && !showBillMergeReport ? (
          <div className="rounded-lg border border-dashed border-border-medium p-6 text-center text-sm text-text-secondary">
            本次执行未产生校验行
          </div>
        ) : null
      ) : (
        <div className="overflow-hidden rounded-lg border border-border-light">
          <div className="border-b border-border-light bg-surface-secondary px-3 py-2 text-xs text-text-secondary">
            {showOwnFlowRuleReport || showBillMergeReport ? '其他校验条目' : '校验条目'}
          </div>
          <table className="w-full text-sm">
            <thead className="bg-surface-secondary text-xs text-text-secondary">
              <tr>
                <th className="px-3 py-2 text-left">严重度</th>
                <th className="px-3 py-2 text-left">摘要</th>
                <th className="px-3 py-2 text-left">规则</th>
                <th className="px-3 py-2 text-left">文件</th>
                <th className="px-3 py-2 text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {legacyVerifyRows.map((r) => (
                <tr key={r.row_id} className="border-t border-border-light">
                  <td className="px-3 py-2">
                    <span className="inline-flex items-center gap-1.5 text-xs text-text-secondary">
                      <SeverityIcon severity={r.severity} />
                      {r.severity}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-xs text-text-primary">{r.summary}</td>
                  <td className="px-3 py-2 font-mono text-xs text-text-secondary">
                    {r.rule_ref ?? '—'}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-text-secondary">
                    {r.file_ref ?? '—'}
                  </td>
                  <td className="px-3 py-2 text-right">
                    <button
                      type="button"
                      onClick={() => askAgent(r)}
                      className="inline-flex items-center gap-1 rounded-md border border-green-500/40 bg-green-500/5 px-2 py-1 text-xs text-green-400 hover:bg-green-500/15"
                      title="带上当前 run / verify_row_id 上下文打开 Copilot"
                    >
                      <MessageSquare className="h-3 w-3" />
                      问 Agent
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
