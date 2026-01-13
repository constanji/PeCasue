import { useSetRecoilState } from 'recoil';
import type { QueryClient } from '@tanstack/react-query';
import { QueryKeys, Tools } from '@because/data-provider';
import type { TAttachment, EventSubmission, MemoriesResponse } from '@because/data-provider';
import { handleMemoryArtifact } from '~/utils/memory';
import store from '~/store';

export default function useAttachmentHandler(queryClient?: QueryClient) {
  const setAttachmentsMap = useSetRecoilState(store.messageAttachmentsMap);

  return ({ data }: { data: TAttachment; submission: EventSubmission }) => {
    const { messageId } = data;

    // 调试日志
    console.log('[useAttachmentHandler] ========== Attachment Received ==========');
    console.log('[useAttachmentHandler] MessageId:', messageId);
    console.log('[useAttachmentHandler] Attachment Type:', data.type);
    console.log('[useAttachmentHandler] ToolCallId:', data.toolCallId);
    console.log('[useAttachmentHandler] Has _chartData:', !!data._chartData);
    if (data._chartData) {
      console.log('[useAttachmentHandler] _chartData:', data._chartData);
    }
    console.log('[useAttachmentHandler] Full Attachment:', data);
    if (data.type === Tools.ui_resources) {
      console.log('[useAttachmentHandler] UI Resources:', data[Tools.ui_resources]);
      console.log('[useAttachmentHandler] UI Resources Count:', Array.isArray(data[Tools.ui_resources]) ? data[Tools.ui_resources].length : (data[Tools.ui_resources]?.data?.length ?? 0));
    }
    console.log('[useAttachmentHandler] =========================================');

    if (queryClient && data?.filepath && !data.filepath.includes('/api/files')) {
      queryClient.setQueryData([QueryKeys.files], (oldData: TAttachment[] | undefined) => {
        return [data, ...(oldData || [])];
      });
    }

    if (queryClient && data.type === Tools.memory && data[Tools.memory]) {
      const memoryArtifact = data[Tools.memory];

      queryClient.setQueryData([QueryKeys.memories], (oldData: MemoriesResponse | undefined) => {
        if (!oldData) {
          return oldData;
        }

        return handleMemoryArtifact({ memoryArtifact, currentData: oldData }) || oldData;
      });
    }

    setAttachmentsMap((prevMap) => {
      const messageAttachments =
        (prevMap as Record<string, TAttachment[] | undefined>)[messageId] || [];
      const updatedMap = {
        ...prevMap,
        [messageId]: [...messageAttachments, data],
      };
      console.log('[useAttachmentHandler] Updated AttachmentsMap:', {
        messageId,
        attachmentsCount: updatedMap[messageId]?.length ?? 0,
        hasChartData: !!data._chartData,
        allMessageIds: Object.keys(updatedMap),
      });

      // 额外记录这个 messageId 的所有 attachments
      console.log('[useAttachmentHandler] All attachments for messageId:', messageId, updatedMap[messageId]);

      return updatedMap;
    });
  };
}
