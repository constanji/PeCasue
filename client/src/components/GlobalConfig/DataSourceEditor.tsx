import React, { useEffect } from 'react';
import { Button, useToastContext } from '@because/client';
import { useForm, Controller } from 'react-hook-form';
import { useLocalize } from '~/hooks';
import { cn, defaultTextProps } from '~/utils';
import { useTestConnectionMutation } from '~/data-provider/DataSources';
import type { DataSource, DataSourceCreateParams } from '@because/data-provider';
import { X, TestTube } from 'lucide-react';

interface DataSourceEditorProps {
  dataSource?: DataSource;
  onSave: (data: DataSourceCreateParams) => Promise<void>;
  onCancel: () => void;
}

export default function DataSourceEditor({ dataSource, onSave, onCancel }: DataSourceEditorProps) {
  const localize = useLocalize();
  const { showToast } = useToastContext();
  const testMutation = useTestConnectionMutation();
  const [isTesting, setIsTesting] = React.useState(false);

  const {
    control,
    handleSubmit,
    formState: { isSubmitting, isDirty },
    reset,
    watch,
    setValue,
  } = useForm<DataSourceCreateParams>({
        defaultValues: dataSource
      ? {
          name: dataSource.name,
          type: dataSource.type,
          host: dataSource.host,
          port: dataSource.port,
          database: dataSource.database,
          username: dataSource.username,
          password: '', // 不显示密码
          connectionPool: dataSource.connectionPool,
          ssl: dataSource.ssl || {
            enabled: false,
            rejectUnauthorized: true,
            ca: null,
            cert: null,
            key: null,
          },
          status: dataSource.status || 'active',
        }
      : {
          name: '',
          type: 'mysql',
          host: '',
          port: 3306,
          database: '',
          username: '',
          password: '',
          connectionPool: {
            min: 0,
            max: 10,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 10000,
          },
          ssl: {
            enabled: false,
            rejectUnauthorized: true,
            ca: null,
            cert: null,
            key: null,
          },
          status: 'active',
        },
  });

  useEffect(() => {
    if (dataSource) {
      reset({
        name: dataSource.name,
        type: dataSource.type,
        host: dataSource.host,
        port: dataSource.port,
        database: dataSource.database,
        username: dataSource.username,
        password: '', // 不显示密码
        connectionPool: dataSource.connectionPool,
        ssl: dataSource.ssl || {
          enabled: false,
          rejectUnauthorized: true,
          ca: null,
          cert: null,
          key: null,
        },
        status: dataSource.status || 'active',
      });
    }
  }, [dataSource, reset]);

  const onSubmit = async (data: DataSourceCreateParams) => {
    try {
      await onSave(data);
    } catch (error) {
      // 错误已在父组件处理
    }
  };

  const handleTestConnection = async () => {
    const formData = watch();
    if (!formData.host || !formData.port || !formData.database || !formData.username || !formData.password) {
      showToast({
        message: '请先填写所有必填字段',
        status: 'error',
      });
      return;
    }

    setIsTesting(true);
    try {
      const result = await testMutation.mutateAsync(formData);
      if (result.success) {
        showToast({
          message: '连接测试成功',
          status: 'success',
        });
      } else {
        showToast({
          message: `连接测试失败: ${result.error || '未知错误'}`,
          status: 'error',
        });
      }
    } catch (error) {
      showToast({
        message: `连接测试失败: ${error instanceof Error ? error.message : '未知错误'}`,
        status: 'error',
      });
    } finally {
      setIsTesting(false);
    }
  };

  const type = watch('type');

  return (
    <div className="flex h-full flex-col">
      <form onSubmit={handleSubmit(onSubmit)} className="flex h-full flex-col">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold">{dataSource ? '编辑数据源' : '创建数据源'}</h3>
          <div className="flex gap-2">
            <Button
              type="button"
              onClick={onCancel}
              className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
            >
              取消
            </Button>
            <Button
              type="button"
              onClick={handleTestConnection}
              disabled={isTesting}
              className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
            >
              <TestTube className="h-4 w-4" />
              {isTesting ? '测试中...' : '测试连接'}
            </Button>
            <Button
              type="submit"
              disabled={!isDirty || isSubmitting}
              className="btn btn-primary relative flex items-center gap-2 rounded-lg px-3 py-2"
            >
              {isSubmitting ? '保存中...' : '保存'}
            </Button>
          </div>
        </div>

        <div className="flex-1 overflow-auto">
          <div className="space-y-6">
            {/* 基本信息 */}
            <div className="rounded-lg border border-border-light bg-surface-primary p-6">
              <h4 className="mb-6 text-base font-semibold text-text-primary">基本信息</h4>
              <div className="space-y-5">
                <div>
                  <label className="mb-2 block text-sm font-medium text-text-primary">
                    数据源名称 <span className="text-red-500">*</span>
                  </label>
                  <Controller
                    name="name"
                    control={control}
                    rules={{ required: '数据源名称是必需的' }}
                    render={({ field }) => (
                      <input
                        {...field}
                        className={cn(defaultTextProps, 'w-full')}
                        placeholder="例如：生产数据库"
                      />
                    )}
                  />
                </div>

                <div>
                  <label className="mb-2 block text-sm font-medium text-text-primary">
                    数据库类型 <span className="text-red-500">*</span>
                  </label>
                  <Controller
                    name="type"
                    control={control}
                    rules={{ required: '数据库类型是必需的' }}
                    render={({ field }) => (
                      <select {...field} className={cn(defaultTextProps, 'w-full')}>
                        <option value="mysql">MySQL</option>
                        <option value="postgresql">PostgreSQL</option>
                      </select>
                    )}
                  />
                </div>

                <div>
                  <label className="mb-2 block text-sm font-medium text-text-primary">
                    状态 <span className="text-red-500">*</span>
                  </label>
                  <Controller
                    name="status"
                    control={control}
                    render={({ field }) => (
                      <select {...field} className={cn(defaultTextProps, 'w-full')}>
                        <option value="active">已启用</option>
                        <option value="inactive">已禁用</option>
                      </select>
                    )}
                  />
                </div>
              </div>
            </div>

            {/* 连接信息 */}
            <div className="rounded-lg border border-border-light bg-surface-primary p-6">
              <h4 className="mb-6 text-base font-semibold text-text-primary">连接信息</h4>
              <div className="space-y-5">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      主机地址 <span className="text-red-500">*</span>
                    </label>
                    <Controller
                      name="host"
                      control={control}
                      rules={{ required: '主机地址是必需的' }}
                      render={({ field }) => (
                        <>
                          <input
                            {...field}
                            className={cn(defaultTextProps, 'w-full')}
                            placeholder="localhost 或 IP地址 或 域名"
                          />
                          <p className="mt-1.5 text-xs text-text-secondary">
                            支持本地数据库（localhost）和远程数据库（IP地址或域名）
                          </p>
                        </>
                      )}
                    />
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      端口 <span className="text-red-500">*</span>
                    </label>
                    <Controller
                      name="port"
                      control={control}
                      rules={{
                        required: '端口是必需的',
                        min: { value: 1, message: '端口必须大于0' },
                        max: { value: 65535, message: '端口必须小于65536' },
                      }}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="number"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder={type === 'mysql' ? '3306' : '5432'}
                          onChange={(e) => field.onChange(parseInt(e.target.value) || 0)}
                        />
                      )}
                    />
                  </div>
                </div>

                <div>
                  <label className="mb-2 block text-sm font-medium text-text-primary">
                    数据库名 <span className="text-red-500">*</span>
                  </label>
                  <Controller
                    name="database"
                    control={control}
                    rules={{ required: '数据库名是必需的' }}
                    render={({ field }) => (
                      <input
                        {...field}
                        className={cn(defaultTextProps, 'w-full')}
                        placeholder="数据库名称"
                      />
                    )}
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      用户名 <span className="text-red-500">*</span>
                    </label>
                    <Controller
                      name="username"
                      control={control}
                      rules={{ required: '用户名是必需的' }}
                      render={({ field }) => (
                        <input
                          {...field}
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder="数据库用户名"
                        />
                      )}
                    />
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      密码 <span className="text-red-500">*</span>
                    </label>
                    <Controller
                      name="password"
                      control={control}
                      rules={{
                      required: dataSource ? false : '密码是必需的',
                      validate: (value) => {
                        if (dataSource && !value) {
                          return true; // 编辑时密码可以为空
                        }
                        if (!dataSource && !value) {
                          return '密码是必需的';
                        }
                        return true;
                      },
                    }}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="password"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder={dataSource ? '留空则不修改密码' : '数据库密码'}
                        />
                      )}
                    />
                    {dataSource && (
                      <p className="mt-1.5 text-xs text-text-secondary">留空则不修改密码</p>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* SSL配置 */}
            <div className="rounded-lg border border-border-light bg-surface-primary p-6">
              <h4 className="mb-6 text-base font-semibold text-text-primary">SSL/TLS 配置（可选）</h4>
              <div className="space-y-5">
                <div>
                  <Controller
                    name="ssl.enabled"
                    control={control}
                    render={({ field }) => (
                      <label className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={field.value || false}
                          onChange={(e) => {
                            field.onChange(e.target.checked);
                            if (!e.target.checked) {
                              setValue('ssl.rejectUnauthorized', true);
                              setValue('ssl.ca', null);
                              setValue('ssl.cert', null);
                              setValue('ssl.key', null);
                            }
                          }}
                          className="h-4 w-4 rounded border-gray-300"
                        />
                        <span className="text-sm font-medium text-text-primary">启用 SSL/TLS 连接</span>
                      </label>
                    )}
                  />
                  <p className="mt-1.5 text-xs text-text-secondary">
                    对于远程数据库连接，建议启用 SSL/TLS 以保护数据传输安全
                  </p>
                </div>

                {watch('ssl.enabled') && (
                  <>
                    <div>
                      <Controller
                        name="ssl.rejectUnauthorized"
                        control={control}
                        render={({ field }) => (
                          <label className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={field.value !== undefined ? field.value : true}
                              onChange={(e) => field.onChange(e.target.checked)}
                              className="h-4 w-4 rounded border-gray-300"
                            />
                            <span className="text-sm font-medium text-text-primary">验证服务器证书</span>
                          </label>
                        )}
                      />
                      <p className="mt-1.5 text-xs text-text-secondary">
                        取消勾选将允许自签名证书（不推荐用于生产环境）
                      </p>
                    </div>

                    <div>
                      <label className="mb-2 block text-sm font-medium text-text-primary">
                        CA 证书（可选）
                      </label>
                      <Controller
                        name="ssl.ca"
                        control={control}
                        render={({ field }) => (
                          <textarea
                            {...field}
                            value={field.value || ''}
                            onChange={(e) => field.onChange(e.target.value || null)}
                            className={cn(defaultTextProps, 'w-full min-h-[100px] font-mono text-xs')}
                            placeholder="PEM格式的CA证书内容（可选）"
                          />
                        )}
                      />
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="mb-2 block text-sm font-medium text-text-primary">
                          客户端证书（可选）
                        </label>
                        <Controller
                          name="ssl.cert"
                          control={control}
                          render={({ field }) => (
                            <textarea
                              {...field}
                              value={field.value || ''}
                              onChange={(e) => field.onChange(e.target.value || null)}
                              className={cn(defaultTextProps, 'w-full min-h-[100px] font-mono text-xs')}
                              placeholder="PEM格式的客户端证书内容（可选）"
                            />
                          )}
                        />
                      </div>
                      <div>
                        <label className="mb-2 block text-sm font-medium text-text-primary">
                          客户端私钥（可选）
                        </label>
                        <Controller
                          name="ssl.key"
                          control={control}
                          render={({ field }) => (
                            <textarea
                              {...field}
                              value={field.value || ''}
                              onChange={(e) => field.onChange(e.target.value || null)}
                              className={cn(defaultTextProps, 'w-full min-h-[100px] font-mono text-xs')}
                              placeholder="PEM格式的客户端私钥内容（可选）"
                            />
                          )}
                        />
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>

            {/* 连接池配置 */}
            <div className="rounded-lg border border-border-light bg-surface-primary p-6">
              <h4 className="mb-6 text-base font-semibold text-text-primary">连接池配置</h4>
              <div className="space-y-5">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      最小连接数
                    </label>
                    <Controller
                      name="connectionPool.min"
                      control={control}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="number"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder="0"
                          onChange={(e) => field.onChange(parseInt(e.target.value) || 0)}
                        />
                      )}
                    />
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      最大连接数
                    </label>
                    <Controller
                      name="connectionPool.max"
                      control={control}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="number"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder="10"
                          onChange={(e) => field.onChange(parseInt(e.target.value) || 10)}
                        />
                      )}
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      空闲超时（毫秒）
                    </label>
                    <Controller
                      name="connectionPool.idleTimeoutMillis"
                      control={control}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="number"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder="30000"
                          onChange={(e) => field.onChange(parseInt(e.target.value) || 30000)}
                        />
                      )}
                    />
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">
                      连接超时（毫秒）
                    </label>
                    <Controller
                      name="connectionPool.connectionTimeoutMillis"
                      control={control}
                      render={({ field }) => (
                        <input
                          {...field}
                          type="number"
                          className={cn(defaultTextProps, 'w-full')}
                          placeholder="10000"
                          onChange={(e) => field.onChange(parseInt(e.target.value) || 10000)}
                        />
                      )}
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </form>
    </div>
  );
}

