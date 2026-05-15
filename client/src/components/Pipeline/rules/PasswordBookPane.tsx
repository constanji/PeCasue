import React, { useMemo, useState } from 'react';
import { Eye, EyeOff, Pencil, Save, Trash2 } from 'lucide-react';
import {
  usePipelineChannelCatalog,
  usePutRule,
  useRule,
  type PipelineRuleTable,
} from '~/data-provider';
import { useAuthContext } from '~/hooks/AuthContext';
import { cn } from '~/utils';

/** 与 backend `password_book.upsert` / 新 UI 约定一致 */
const PATTERN_CHANNEL_DEFAULT = '__channel_default__';

const TABLE_COLUMNS = ['scope', 'pattern', 'password', '备注', 'expires_at'] as const;

type BookRow = Record<string, unknown>;

type ChannelEntry = {
  id: string;
  channelId: string;
  password: string;
  note: string;
  keepMasked: boolean;
};

type SavedRowDisplay = {
  channelId: string;
  label: string;
  /** API 返回的原始 password 字段（管理员可为明文，否则常为 ••••） */
  passwordRaw: string;
  note: string;
};

function splitLegacyAndChannels(
  rows: BookRow[],
  catalogIds: Set<string>,
): { legacy: BookRow[]; channels: Map<string, BookRow> } {
  const legacy: BookRow[] = [];
  const channels = new Map<string, BookRow>();
  for (const raw of rows) {
    const scope = String(raw.scope ?? '').trim();
    const pat = String(raw.pattern ?? '').trim();
    const defPat = pat === '' || pat === PATTERN_CHANNEL_DEFAULT;
    if (defPat && scope && catalogIds.has(scope)) {
      channels.set(scope, raw as BookRow);
    } else {
      legacy.push(raw as BookRow);
    }
  }
  return { legacy, channels };
}

function buildSavedDisplayList(
  rows: BookRow[],
  catalogIds: Set<string>,
  channelOptions: { channel_id: string; display_name: string }[],
): SavedRowDisplay[] {
  const map = new Map<string, SavedRowDisplay>();
  for (const raw of rows) {
    const scope = String(raw.scope ?? '').trim();
    const pat = String(raw.pattern ?? '').trim();
    const defPat = pat === '' || pat === PATTERN_CHANNEL_DEFAULT;
    if (!defPat || !scope || !catalogIds.has(scope)) continue;
    const meta = channelOptions.find((c) => c.channel_id === scope);
    const pwc = raw.password;
    const pw = typeof pwc === 'string' ? pwc : '';
    map.set(scope, {
      channelId: scope,
      label: meta ? `${meta.display_name} · ${scope}` : scope,
      passwordRaw: pw,
      note: String(raw['备注'] ?? ''),
    });
  }
  return [...map.values()].sort((a, b) => a.channelId.localeCompare(b.channelId));
}

export default function PasswordBookPane() {
  const { user } = useAuthContext();
  const catalogQ = usePipelineChannelCatalog();
  const ruleQ = useRule('password_book', { userRole: user?.role ?? null });
  const putMut = usePutRule('password_book');

  const catalogIds = useMemo(
    () => new Set((catalogQ.data?.channels ?? []).map((c) => c.channel_id)),
    [catalogQ.data?.channels],
  );
  const channelOptions = useMemo(
    () => [...(catalogQ.data?.channels ?? [])].sort((a, b) =>
      a.display_name.localeCompare(b.display_name, 'zh-CN'),
    ),
    [catalogQ.data?.channels],
  );

  /** 仅「新增区」草稿，不从服务端回填 */
  const [draftAdds, setDraftAdds] = useState<ChannelEntry[]>([]);
  const [inlineEdit, setInlineEdit] = useState<
    | {
        channelId: string;
        password: string;
        note: string;
        keepMasked: boolean;
      }
    | null
  >(null);

  const [globalReveal, setGlobalReveal] = useState(false);
  const [rowReveal, setRowReveal] = useState<Set<string>>(() => new Set());

  const serverMasked = ruleQ.data?.masked !== false;

  const savedDisplayList = useMemo(
    () =>
      buildSavedDisplayList(
        (ruleQ.data?.table?.rows ?? []) as BookRow[],
        catalogIds,
        channelOptions,
      ),
    [ruleQ.data?.table?.rows, catalogIds, channelOptions],
  );

  const savedIds = useMemo(() => new Set(savedDisplayList.map((r) => r.channelId)), [savedDisplayList]);

  const draftChannelIds = useMemo(() => new Set(draftAdds.map((d) => d.channelId)), [draftAdds]);

  const usedChannels = useMemo(() => {
    const u = new Set<string>();
    savedIds.forEach((id) => u.add(id));
    draftChannelIds.forEach((id) => u.add(id));
    return u;
  }, [savedIds, draftChannelIds]);

  const canAdd = channelOptions.some((c) => !usedChannels.has(c.channel_id));

  const legacyRows = useMemo(() => {
    const allRows = (ruleQ.data?.table?.rows ?? []) as BookRow[];
    return splitLegacyAndChannels(allRows, catalogIds).legacy;
  }, [ruleQ.data?.table?.rows, catalogIds]);

  const currentTable = ruleQ.data?.table as PipelineRuleTable | undefined;

  const buildTableFromMaps = (
    legacy: BookRow[],
    channelMap: Map<string, BookRow>,
  ): PipelineRuleTable => {
    const channelSorted = [...channelMap.values()].sort((a, b) =>
      String(a.scope ?? '').localeCompare(String(b.scope ?? '')),
    );
    return {
      columns: [...TABLE_COLUMNS],
      rows: [...legacy, ...channelSorted],
      note: currentTable?.note ?? null,
      meta: currentTable?.meta,
    };
  };

  /** 合并服务端数据 + 顶部草稿行 */
  const buildTableWithDrafts = (): PipelineRuleTable => {
    const allRows = (currentTable?.rows ?? []) as BookRow[];
    const { legacy, channels } = splitLegacyAndChannels(allRows, catalogIds);
    const next = new Map(channels);
    for (const e of draftAdds) {
      const trimmed = e.password.trim();
      const passwordOut = trimmed === '' && e.keepMasked ? '••••' : trimmed;
      next.set(e.channelId, {
        scope: e.channelId,
        pattern: PATTERN_CHANNEL_DEFAULT,
        password: passwordOut,
        备注: e.note,
        expires_at: null,
      });
    }
    return buildTableFromMaps(legacy, next);
  };

  const persistTable = async (table: PipelineRuleTable) => {
    await putMut.mutateAsync({ table });
  };

  const handleSaveDrafts = async () => {
    await persistTable(buildTableWithDrafts());
    setDraftAdds([]);
  };

  const deleteSavedChannel = async (channelId: string) => {
    const allRows = (currentTable?.rows ?? []) as BookRow[];
    const { legacy, channels } = splitLegacyAndChannels(allRows, catalogIds);
    channels.delete(channelId);
    await persistTable(buildTableFromMaps(legacy, channels));
    setInlineEdit((e) => (e?.channelId === channelId ? null : e));
    setRowReveal((prev) => {
      const n = new Set(prev);
      n.delete(channelId);
      return n;
    });
  };

  const saveInlineEdit = async () => {
    if (!inlineEdit) return;
    const allRows = (currentTable?.rows ?? []) as BookRow[];
    const { legacy, channels } = splitLegacyAndChannels(allRows, catalogIds);
    const trimmed = inlineEdit.password.trim();
    const passwordOut =
      trimmed === '' && inlineEdit.keepMasked ? '••••' : trimmed;
    channels.set(inlineEdit.channelId, {
      scope: inlineEdit.channelId,
      pattern: PATTERN_CHANNEL_DEFAULT,
      password: passwordOut,
      备注: inlineEdit.note,
      expires_at: null,
    });
    await persistTable(buildTableFromMaps(legacy, channels));
    setInlineEdit(null);
  };

  const startEdit = (row: SavedRowDisplay) => {
    setInlineEdit({
      channelId: row.channelId,
      password: '',
      note: row.note,
      keepMasked: row.passwordRaw === '••••' || (!!row.passwordRaw && serverMasked),
    });
  };

  const toggleRowReveal = (channelId: string) => {
    setRowReveal((prev) => {
      const n = new Set(prev);
      if (n.has(channelId)) n.delete(channelId);
      else n.add(channelId);
      return n;
    });
  };

  const addDraft = () => {
    const firstFree = channelOptions.find((c) => !usedChannels.has(c.channel_id));
    if (!firstFree) return;
    setDraftAdds((prev) => [
      ...prev,
      {
        id: `${firstFree.channel_id}-${Date.now()}`,
        channelId: firstFree.channel_id,
        password: '',
        note: '',
        keepMasked: false,
      },
    ]);
  };

  const updateDraft = (id: string, patch: Partial<ChannelEntry>) => {
    setDraftAdds((prev) => prev.map((e) => (e.id === id ? { ...e, ...patch } : e)));
  };

  const onDraftChannelChange = (id: string, newChannelId: string) => {
    const cur = draftAdds.find((e) => e.id === id);
    if (!cur || cur.channelId === newChannelId) return;
    if (usedChannels.has(newChannelId) && newChannelId !== cur.channelId) return;
    updateDraft(id, { channelId: newChannelId });
  };

  function passwordDisplayText(row: SavedRowDisplay): React.ReactNode {
    const raw = row.passwordRaw?.trim() ?? '';
    if (!raw) return <span className="text-text-tertiary">未设置</span>;
    if (serverMasked || raw === '••••') {
      return (
        <span className="text-text-tertiary">
          ••••（服务端已掩码；管理员账号下可拉取明文后再用下方「显示」）
        </span>
      );
    }
    const show = globalReveal || rowReveal.has(row.channelId);
    return (
      <span className={cn('break-all font-mono text-[11px]', show ? 'text-text-primary' : 'text-text-secondary')}>
        {show ? raw : '••••'}
      </span>
    );
  }

  if (catalogQ.isLoading || ruleQ.isLoading) {
    return <div className="p-4 text-sm text-text-secondary">加载渠道与密码簿…</div>;
  }
  if (catalogQ.error) {
    return (
      <div className="p-4 text-sm text-text-primary">
        {(catalogQ.error as Error).message}
      </div>
    );
  }
  if (ruleQ.error) {
    return <div className="p-4 text-sm text-text-primary">{(ruleQ.error as Error).message}</div>;
  }

  return (
    <div className="h-full overflow-auto p-4 pb-10">
      <div className="mb-4 rounded-xl border border-green-500/25 bg-surface-primary px-4 py-3 text-xs leading-relaxed text-text-secondary shadow-sm ring-1 ring-inset ring-green-500/10">
        <p className="font-medium text-text-primary">维护说明</p>
        <ol className="mt-2 list-decimal space-y-1 pl-5">
          <li>在<strong className="text-text-primary">新增区</strong>选择渠道并填写密码，点击「保存到 rules」写入磁盘</li>
          <li>已保存记录在下表管理：<strong className="text-text-primary">查看 / 编辑 / 删除</strong></li>
          <li>
            管理员读取接口可返回明文；非管理员仅见掩码。明文显示可用表头「显示明文」或单行眼睛图标（须已成功拉取明文）。
          </li>
        </ol>
      </div>

      <div className="mb-2 border-b border-border-light pb-2">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-text-secondary">
          新增渠道密码
        </h3>
        <p className="mt-1 text-[11px] text-text-tertiary">
          仅用于添加尚未保存的渠道；已存在的请在下方表格中编辑。
        </p>
      </div>

      <div className="mb-3 flex flex-wrap items-center justify-end gap-2">
        <span className="mr-auto text-xs text-text-secondary">
          草稿 <span className="font-mono text-text-primary">{draftAdds.length}</span> 条
        </span>
        <button
          type="button"
          disabled={draftAdds.length === 0 || putMut.isLoading}
          onClick={() => void handleSaveDrafts()}
          className="inline-flex h-9 items-center gap-2 rounded-md bg-green-600 px-4 text-xs font-medium text-white hover:bg-green-500 disabled:opacity-40"
        >
          <Save className="h-3.5 w-3.5" />
          {putMut.isLoading ? '保存中…' : '保存到 rules'}
        </button>
        <button
          type="button"
          disabled={!canAdd}
          onClick={addDraft}
          className="rounded-md border border-border-light bg-surface-secondary px-3 py-1.5 text-xs font-medium text-text-primary hover:bg-surface-hover disabled:opacity-40"
        >
          + 添加渠道
        </button>
      </div>

      <div className="space-y-3">
        {draftAdds.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border-medium px-4 py-6 text-center text-xs text-text-secondary">
            无草稿。点击「+ 添加渠道」后在此填写，再按「保存到 rules」写入{' '}
            <code className="font-mono text-[10px]">password_book.enc</code>。
          </div>
        ) : (
          draftAdds.map((e) => (
            <div
              key={e.id}
              className="rounded-lg border border-border-light bg-surface-primary p-3"
            >
              <div className="flex flex-col gap-3 sm:flex-row sm:items-stretch sm:gap-3">
                <div className="flex min-w-0 flex-1 flex-col">
                  <span className="h-5 shrink-0 text-[11px] font-medium leading-5 text-text-secondary">
                    渠道来源
                  </span>
                  <select
                    value={e.channelId}
                    onChange={(ev) => onDraftChannelChange(e.id, ev.target.value)}
                    aria-label="渠道来源"
                    title="渠道来源"
                    className="mt-1 box-border h-10 w-full rounded-md border border-border-medium bg-surface-secondary px-2.5 text-sm text-text-primary"
                  >
                    {channelOptions.map((c) => (
                      <option
                        key={c.channel_id}
                        value={c.channel_id}
                        disabled={usedChannels.has(c.channel_id) && c.channel_id !== e.channelId}
                      >
                        {c.display_name} ({c.channel_id})
                      </option>
                    ))}
                  </select>
                  <p className="mt-1 min-h-[1.25rem]" aria-hidden>
                    &nbsp;
                  </p>
                </div>
                <div className="flex min-w-0 flex-1 flex-col">
                  <span className="h-5 shrink-0 text-[11px] font-medium leading-5 text-text-secondary">密码</span>
                  <input
                    type="password"
                    autoComplete="new-password"
                    value={e.password}
                    onChange={(ev) => updateDraft(e.id, { password: ev.target.value, keepMasked: false })}
                    placeholder={e.keepMasked ? '已保存 · 留空则不修改' : '输入密码'}
                    aria-label="密码"
                    className="mt-1 box-border h-10 w-full rounded-md border border-border-medium bg-surface-secondary px-2.5 font-mono text-sm text-text-primary"
                  />
                  <p className="mt-1 min-h-[1.25rem]" aria-hidden>
                    &nbsp;
                  </p>
                </div>
                <div className="flex min-w-0 flex-1 flex-col sm:max-w-xs">
                  <span className="h-5 shrink-0 text-[11px] font-medium leading-5 text-text-secondary">
                    备注（可选）
                  </span>
                  <input
                    value={e.note}
                    onChange={(ev) => updateDraft(e.id, { note: ev.target.value })}
                    aria-label="备注"
                    placeholder="可选"
                    className="mt-1 box-border h-10 w-full rounded-md border border-border-medium bg-surface-secondary px-2.5 text-sm text-text-primary"
                  />
                  <p className="mt-1 min-h-[1.25rem]" aria-hidden>
                    &nbsp;
                  </p>
                </div>
              </div>
            </div>
          ))
        )}
      </div>

      <section className="mt-10 border-t border-border-light pt-6">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wide text-text-secondary">
              已保存的渠道密码
            </h3>
            <p className="mt-1 text-[11px] text-text-tertiary">
              数据来自 <code className="font-mono text-[10px] text-green-500/90">password_book.enc</code>
              ；在此查看、改密或删除。
            </p>
          </div>
          {!serverMasked ? (
            <button
              type="button"
              onClick={() => setGlobalReveal((v) => !v)}
              className="inline-flex items-center gap-1.5 rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-[11px] text-text-secondary hover:bg-surface-hover"
            >
              {globalReveal ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
              {globalReveal ? '隐藏明文' : '显示明文'}
            </button>
          ) : (
            <span className="text-[10px] text-text-tertiary">当前接口返回掩码，无法本地解密</span>
          )}
        </div>

        {savedDisplayList.length === 0 ? (
          <div className="mt-3 rounded-lg border border-dashed border-border-medium px-4 py-6 text-center text-xs text-text-secondary">
            暂无已保存的渠道。请先在「新增区」添加并保存。
          </div>
        ) : (
          <div className="mt-3 overflow-hidden rounded-lg border border-border-light">
            <table className="w-full text-left text-xs">
              <thead className="bg-surface-secondary text-[11px] text-text-secondary">
                <tr>
                  <th className="px-3 py-2 font-medium">渠道</th>
                  <th className="px-3 py-2 font-medium">密码</th>
                  <th className="px-3 py-2 font-medium">备注</th>
                  <th className="w-48 px-3 py-2 text-right font-medium">操作</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-light bg-surface-primary">
                {savedDisplayList.map((row) =>
                  inlineEdit?.channelId === row.channelId ? (
                    <tr key={`${row.channelId}-edit`} className="bg-surface-secondary/40">
                      <td className="px-3 py-2 align-top font-medium text-text-primary">{row.label}</td>
                      <td className="px-3 py-2 align-top" colSpan={2}>
                        <div className="flex flex-col gap-2 sm:flex-row">
                          <input
                            type="password"
                            autoComplete="new-password"
                            value={inlineEdit.password}
                            onChange={(ev) =>
                              setInlineEdit((s) => (s ? { ...s, password: ev.target.value, keepMasked: false } : s))
                            }
                            placeholder={inlineEdit.keepMasked ? '留空保留原密码' : '新密码'}
                            className="box-border h-9 w-full rounded-md border border-border-medium bg-surface-primary px-2 font-mono text-[11px]"
                          />
                          <input
                            value={inlineEdit.note}
                            onChange={(ev) =>
                              setInlineEdit((s) => (s ? { ...s, note: ev.target.value } : s))
                            }
                            placeholder="备注"
                            className="box-border h-9 w-full rounded-md border border-border-medium bg-surface-primary px-2 text-[11px]"
                          />
                        </div>
                      </td>
                      <td className="px-3 py-2 align-top text-right">
                        <div className="flex flex-wrap justify-end gap-1.5">
                          <button
                            type="button"
                            disabled={putMut.isLoading}
                            onClick={() => void saveInlineEdit()}
                            className="rounded-md bg-green-600 px-2.5 py-1 text-[11px] text-white hover:bg-green-500 disabled:opacity-50"
                          >
                            保存
                          </button>
                          <button
                            type="button"
                            onClick={() => setInlineEdit(null)}
                            className="rounded-md border border-border-light px-2.5 py-1 text-[11px] text-text-secondary hover:bg-surface-hover"
                          >
                            取消
                          </button>
                        </div>
                      </td>
                    </tr>
                  ) : (
                    <tr key={row.channelId} className="text-text-primary">
                      <td className="px-3 py-2.5 font-medium">{row.label}</td>
                      <td className="max-w-[min(24rem,40vw)] px-3 py-2.5">
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="min-w-0 flex-1">{passwordDisplayText(row)}</div>
                          {!serverMasked && row.passwordRaw && row.passwordRaw !== '••••' ? (
                            <button
                              type="button"
                              onClick={() => toggleRowReveal(row.channelId)}
                              className="shrink-0 rounded border border-border-light p-1 text-text-secondary hover:bg-surface-hover"
                              title={rowReveal.has(row.channelId) ? '隐藏' : '查看本行'}
                              aria-label="切换本行明文显示"
                            >
                              {rowReveal.has(row.channelId) ? (
                                <EyeOff className="h-3.5 w-3.5" />
                              ) : (
                                <Eye className="h-3.5 w-3.5" />
                              )}
                            </button>
                          ) : null}
                        </div>
                      </td>
                      <td className="px-3 py-2.5 text-text-secondary">{row.note || '—'}</td>
                      <td className="px-3 py-2.5 text-right">
                        <div className="inline-flex flex-wrap justify-end gap-1.5">
                          <button
                            type="button"
                            onClick={() => startEdit(row)}
                            className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-[11px] text-text-secondary hover:bg-surface-hover"
                          >
                            <Pencil className="h-3 w-3" />
                            编辑
                          </button>
                          <button
                            type="button"
                            onClick={() => {
                              if (
                                !confirm(`确定从密码簿删除渠道「${row.channelId}」？不可恢复。`)
                              )
                                return;
                              void deleteSavedChannel(row.channelId);
                            }}
                            className="inline-flex items-center gap-1 rounded-md border border-red-500/35 px-2 py-1 text-[11px] text-red-400 hover:bg-red-500/10"
                          >
                            <Trash2 className="h-3 w-3" />
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  ),
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {legacyRows.length > 0 && (
        <details className="mt-6 rounded-lg border border-border-light bg-surface-secondary/30 p-3 text-xs">
          <summary className="cursor-pointer font-medium text-text-secondary">
            其他规则（{legacyRows.length} 条，旧版 scope/pattern，将随保存原样保留）
          </summary>
          <pre className="mt-2 max-h-48 overflow-auto rounded-md bg-surface-primary p-2 font-mono text-[10px] text-text-tertiary">
            {JSON.stringify(legacyRows, null, 2)}
          </pre>
        </details>
      )}

      {putMut.isError && (
        <div className="mt-4 text-xs text-red-400">{(putMut.error as Error).message}</div>
      )}
    </div>
  );
}
