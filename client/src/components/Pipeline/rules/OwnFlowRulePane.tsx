import React from 'react';
import RulePane from './RulePane';

/**
 * Special pane for own_flow_processing — upload-driven table + banner on「处理」列 DSL。
 */
export default function OwnFlowRulePane() {
  return (
    <RulePane
      kind="own_flow_processing"
      hideColumnTools
      ruleExcelImport={{ variant: 'own_flow_template', scope: 'own_flow_processing' }}
      bannerNote={
        <div className="space-y-2">
          <div>
            校验脚本会从<strong>「处理」</strong>列解析匹配 DSL（与 allline{' '}
            <code className="font-mono">_parse_match_condition</code> 一致），形如{' '}
            <code className="mx-1 rounded bg-surface-primary px-1 py-0.5 font-mono">
              列名=值
            </code>
            ，多条件用 <code className="font-mono">AND</code> / <code className="font-mono">OR</code>
            ，分组可用括号。
          </div>
          <p className="text-[11px] leading-relaxed text-text-secondary">
            本页表格来自 RuleStore 快照{' '}
            <code className="break-all rounded bg-surface-primary px-1 py-0.5 font-mono text-[10px]">
              {'{PIPELINE_DATA_DIR}/rules/files/own_flow_processing/current.json'}
            </code>
            ；仅修改同级的{' '}
            <code className="break-all rounded bg-surface-primary px-1 py-0.5 font-mono text-[10px]">
              rules/files/rules/处理表.csv
            </code>{' '}
            不会更新此处，请在本页保存或通过「上传模版工作簿」重新导入。
          </p>
        </div>
      }
    />
  );
}
