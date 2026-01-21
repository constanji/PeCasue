import { useMutation, useQueryClient } from '@tanstack/react-query';
import { dataService, QueryKeys } from '@because/data-provider';
import type { UseMutationResult } from '@tanstack/react-query';
import type {
  DataSource,
  DataSourceCreateParams,
  DataSourceUpdateParams,
  DataSourceResponse,
  DataSourceTestResponse,
} from '@because/data-provider';

/**
 * Create a new data source
 */
export const useCreateDataSourceMutation = (): UseMutationResult<
  DataSourceResponse,
  Error,
  DataSourceCreateParams
> => {
  const queryClient = useQueryClient();
  return useMutation((data: DataSourceCreateParams) => dataService.createDataSource(data), {
    onSuccess: () => {
      queryClient.invalidateQueries([QueryKeys.dataSources]);
    },
  });
};

/**
 * Update a data source
 */
export const useUpdateDataSourceMutation = (): UseMutationResult<
  DataSourceResponse,
  Error,
  { id: string; data: DataSourceUpdateParams }
> => {
  const queryClient = useQueryClient();
  return useMutation(
    ({ id, data }: { id: string; data: DataSourceUpdateParams }) =>
      dataService.updateDataSource({ id, data }),
    {
      onSuccess: (response, variables) => {
        // 使用 API 返回的完整数据更新缓存
        if (response.success && response.data) {
          // 如果返回的数据中没有 isPublic，从 variables.data 中恢复
          const updatedDataSource = {
            ...response.data,
            isPublic: response.data.isPublic !== undefined 
              ? response.data.isPublic 
              : variables.data.isPublic
          };
          
          // 更新列表缓存
          queryClient.setQueryData<DataSourceListResponse>([QueryKeys.dataSources], (oldData) => {
            if (!oldData?.data) {
              return oldData;
            }
            return {
              ...oldData,
              data: oldData.data.map((ds) =>
                ds._id === variables.id ? updatedDataSource : ds
              ),
            };
          });
          
          // 更新单个数据源缓存
          queryClient.setQueryData<DataSourceResponse>([QueryKeys.dataSource, variables.id], () => {
            return {
              ...response,
              data: updatedDataSource
            };
          });
        }
        
        // 不立即 invalidateQueries，因为我们已经用 setQueryData 更新了缓存
        // 立即 invalidateQueries 会触发重新获取，如果后端返回的数据有问题，会覆盖我们刚更新的缓存
        // 如果需要同步，可以在后台静默刷新（使用 refetchType: 'none' 不会触发重新获取）
        // queryClient.invalidateQueries([QueryKeys.dataSources], { refetchType: 'none' });
      },
    },
  );
};

/**
 * Delete a data source
 */
export const useDeleteDataSourceMutation = (): UseMutationResult<
  { success: boolean; message?: string },
  Error,
  { id: string }
> => {
  const queryClient = useQueryClient();
  return useMutation(({ id }: { id: string }) => dataService.deleteDataSource({ id }), {
    onSuccess: () => {
      queryClient.invalidateQueries([QueryKeys.dataSources]);
    },
  });
};

/**
 * Test data source connection
 */
export const useTestDataSourceConnectionMutation = (): UseMutationResult<
  DataSourceTestResponse,
  Error,
  { id: string }
> => {
  const queryClient = useQueryClient();
  return useMutation(({ id }: { id: string }) => dataService.testDataSourceConnection({ id }), {
    onSuccess: (response, variables) => {
      // 刷新数据源列表和详情，以更新测试结果
      queryClient.invalidateQueries([QueryKeys.dataSources]);
      queryClient.invalidateQueries([QueryKeys.dataSource, variables.id]);
    },
  });
};

/**
 * Test connection with provided config (without saving)
 */
export const useTestConnectionMutation = (): UseMutationResult<
  DataSourceTestResponse,
  Error,
  DataSourceCreateParams
> => {
  return useMutation((data: DataSourceCreateParams) => dataService.testConnection(data));
};

