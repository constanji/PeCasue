import React, { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  type PipelineRuleExcelImportConfig,
  type PipelineRuleKind,
} from '~/data-provider';
import { cn } from '~/utils';
import RulePane from '../rules/RulePane';
import OwnFlowRulePane from '../rules/OwnFlowRulePane';
import AllocationTemplatesPane from '../rules/AllocationTemplatesPane';
import PasswordBookPane from '../rules/PasswordBookPane';

interface SubTabDef {
  id: PipelineRuleKind;
  label: string;
  hint: string;
}

const GROUP_OWN_FLOW_MAPPING = {
  label: '自有流水 Mapping',
  tabs: [
    {
      id: 'account_mapping' as const,
      label: '账户 Mapping',
      hint: '银行账号 → 主体/分行映射（模版表「账户对应主体分行mapping表」）',
    },
    {
      id: 'fee_mapping' as const,
      label: '费项 Mapping',
      hint: '原始描述 → 费项分类 / 入账科目（「账单及自有流水费项mapping表」）',
    },
    {
      id: 'special_branch_mapping' as const,
      label: '特殊渠道 主体分行',
      hint: '自有流水：特殊主体分行匹配',
    },
    {
      id: 'own_flow_processing' as const,
      label: '自有流水 处理表',
      hint: '渠道/主体/文件 处理与备注规则（含条件 DSL）',
    },
  ],
};

const GROUP_CUSTOMER_MAPPING = {
  label: '客资流水 Mapping',
  tabs: [
    {
      id: 'customer_mapping' as const,
      label: '客资流水 MAPPING',
      hint: '模版工作表「客资流水MAPPING」→ RuleStore + mapping/*.csv',
    },
    {
      id: 'customer_fee_mapping' as const,
      label: '客资费项 mapping',
      hint: '「客资流水费项mapping表」',
    },
    {
      id: 'customer_branch_mapping' as const,
      label: '客资分行 mapping',
      hint: '「客资流水分行mapping」',
    },
  ],
};

const FLAT_TABS: SubTabDef[] = [
  { id: 'fx', label: '汇率', hint: '各币种对 USD 的折算率' },
  {
    id: 'result_template',
    label: '分摊基数模版',
    hint: 'QuickBI / CitiHK / 成本分摊 workbook（磁盘固定路径），上传并保持齐全',
  },
  { id: 'password_book', label: '密码簿', hint: '选择流水线渠道 + 密码，保存至 rules/password_book.enc（Fernet）' },
];

const ALL_RULE_TAB_LIST: SubTabDef[] = [
  ...GROUP_OWN_FLOW_MAPPING.tabs,
  ...GROUP_CUSTOMER_MAPPING.tabs,
  ...FLAT_TABS,
];

const OWN_FLOW_MAPPING_KINDS = new Set<PipelineRuleKind>([
  'account_mapping',
  'fee_mapping',
  'special_branch_mapping',
  'own_flow_processing',
]);
const CUSTOMER_MAPPING_KINDS = new Set<PipelineRuleKind>([
  'customer_mapping',
  'customer_fee_mapping',
  'customer_branch_mapping',
]);

type PrimaryNavKey = 'own_flow_mapping' | 'customer_mapping' | PipelineRuleKind;

const PRIMARY_ROW: { key: PrimaryNavKey; label: string; enterKind: PipelineRuleKind }[] = [
  { key: 'own_flow_mapping', label: GROUP_OWN_FLOW_MAPPING.label, enterKind: 'account_mapping' },
  { key: 'customer_mapping', label: GROUP_CUSTOMER_MAPPING.label, enterKind: 'customer_mapping' },
  ...FLAT_TABS.map((t) => ({ key: t.id as PrimaryNavKey, label: t.label, enterKind: t.id })),
];

function primaryKeyForActive(kind: PipelineRuleKind): PrimaryNavKey {
  if (OWN_FLOW_MAPPING_KINDS.has(kind)) return 'own_flow_mapping';
  if (CUSTOMER_MAPPING_KINDS.has(kind)) return 'customer_mapping';
  return kind;
}

const isValid = (s: string | null): s is PipelineRuleKind =>
  !!s && ALL_RULE_TAB_LIST.some((t) => t.id === s);

/** Upload-centric tabs: hide manual column schema controls (content comes from Excel). */
const KINDS_HIDE_COLUMN_TOOLS: PipelineRuleKind[] = [
  'account_mapping',
  'fee_mapping',
  'fx',
  'customer_mapping',
  'customer_fee_mapping',
  'customer_branch_mapping',
];

function ruleExcelImportForKind(kind: PipelineRuleKind): PipelineRuleExcelImportConfig | undefined {
  if (kind === 'fx') return { variant: 'fx' };
  if (kind === 'account_mapping') {
    return { variant: 'own_flow_template', scope: 'account_mapping' };
  }
  if (kind === 'fee_mapping') {
    return { variant: 'own_flow_template', scope: 'fee_mapping' };
  }
  if (kind === 'customer_mapping') {
    return { variant: 'customer_flow_template', scope: 'customer_mapping' };
  }
  if (kind === 'customer_fee_mapping') {
    return { variant: 'customer_flow_template', scope: 'customer_fee_mapping' };
  }
  if (kind === 'customer_branch_mapping') {
    return { variant: 'customer_flow_template', scope: 'customer_branch_mapping' };
  }
  return undefined;
}

export default function RulesTab() {
  const [params, setParams] = useSearchParams();
  const subTabParam = params.get('rule');
  const [active, setActive] = useState<PipelineRuleKind>(
    isValid(subTabParam) ? subTabParam : 'account_mapping',
  );

  const allTabsById = useMemo(() => {
    const m = new Map<PipelineRuleKind, SubTabDef>();
    ALL_RULE_TAB_LIST.forEach((t) => m.set(t.id, t));
    return m;
  }, []);

  useEffect(() => {
    if (isValid(subTabParam) && subTabParam !== active) setActive(subTabParam);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subTabParam]);

  const setSubTab = (k: PipelineRuleKind) => {
    setActive(k);
    const next = new URLSearchParams(params);
    next.set('rule', k);
    setParams(next, { replace: false });
  };

  const activeMeta = allTabsById.get(active);
  const activePrimary = primaryKeyForActive(active);

  const navBtnClass = (id: PipelineRuleKind) =>
    cn(
      'relative px-3 py-1.5 text-xs font-medium transition-colors rounded-md',
      active === id
        ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
        : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
    );

  const primaryNavBtnClass = (key: PrimaryNavKey) =>
    cn(
      'relative px-3 py-1.5 text-xs font-medium transition-colors rounded-md',
      activePrimary === key
        ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
        : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
    );

  const showOwnFlowSub = OWN_FLOW_MAPPING_KINDS.has(active);
  const showCustomerSub = CUSTOMER_MAPPING_KINDS.has(active);

  const handlePrimaryNav = (row: (typeof PRIMARY_ROW)[number]) => {
    if (row.key === 'own_flow_mapping') {
      if (OWN_FLOW_MAPPING_KINDS.has(active)) return;
      setSubTab('account_mapping');
      return;
    }
    if (row.key === 'customer_mapping') {
      if (CUSTOMER_MAPPING_KINDS.has(active)) return;
      setSubTab('customer_mapping');
      return;
    }
    setSubTab(row.enterKind);
  };

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <header className="shrink-0 border-b border-border-light bg-surface-primary">
        <nav className="flex flex-wrap gap-1.5 px-4 py-3" aria-label="规则类别">
          {PRIMARY_ROW.map((row) => (
            <button
              key={row.key}
              type="button"
              onClick={() => handlePrimaryNav(row)}
              className={primaryNavBtnClass(row.key)}
            >
              {row.label}
            </button>
          ))}
        </nav>
        {(showOwnFlowSub || showCustomerSub) && (
          <nav
            className="flex flex-wrap gap-1.5 border-t border-border-light bg-surface-secondary/40 px-4 py-2"
            aria-label="Mapping 明细"
          >
            {(showOwnFlowSub ? GROUP_OWN_FLOW_MAPPING.tabs : GROUP_CUSTOMER_MAPPING.tabs).map((t) => (
              <button
                key={t.id}
                type="button"
                onClick={() => setSubTab(t.id)}
                className={navBtnClass(t.id)}
              >
                {t.label}
              </button>
            ))}
          </nav>
        )}
        {activeMeta && (
          <div className="border-t border-border-light bg-gradient-to-r from-surface-secondary via-surface-secondary to-surface-primary px-4 py-2.5 pl-5 shadow-[inset_4px_0_0_0_rgba(34,197,94,0.45)]">
            <p className="text-xs font-medium leading-snug text-text-primary">{activeMeta.hint}</p>
          </div>
        )}
      </header>
      <div className="flex-1 overflow-hidden">
        {active === 'result_template' ? (
          <AllocationTemplatesPane />
        ) : active === 'own_flow_processing' ? (
          <OwnFlowRulePane />
        ) : active === 'password_book' ? (
          <PasswordBookPane />
        ) : active === 'special_branch_mapping' ? (
          <RulePane
            kind="special_branch_mapping"
            viewOnly
            hideColumnTools
            bannerNote={
              <div className="text-xs leading-relaxed text-text-secondary">
                自有流水场景下的<strong className="text-text-primary">特殊主体分行匹配</strong>
                规则页：展示按渠道等维度匹配主体与分行维度时的参考数据。
              </div>
            }
          />
        ) : (
          <RulePane
            kind={active}
            hideColumnTools={KINDS_HIDE_COLUMN_TOOLS.includes(active)}
            ruleExcelImport={ruleExcelImportForKind(active)}
          />
        )}
      </div>
    </div>
  );
}
