import { memo, useState, useEffect, useCallback, type MouseEvent } from 'react';
import { imageExtRegex, Tools } from '@because/data-provider';
import type { TAttachment, TFile, TAttachmentMetadata } from '@because/data-provider';
import { useToastContext } from '@because/client';
import FileContainer from '~/components/Chat/Input/Files/FileContainer';
import Image from '~/components/Chat/Messages/Content/Image';
import FileTextPreviewDialog from './FileTextPreviewDialog';
import { useAuthContext } from '~/hooks/AuthContext';
import { useLocalize } from '~/hooks';
import { useFileDownload } from '~/data-provider';
import { cn } from '~/utils';

const FileAttachment = memo(({ attachment }: { attachment: Partial<TAttachment> }) => {
  const [isVisible, setIsVisible] = useState(false);
  const [previewOpen, setPreviewOpen] = useState(false);
  const localize = useLocalize();
  const { showToast } = useToastContext();
  const { user } = useAuthContext();
  const fileId = (attachment as TFile).file_id;
  const { refetch: downloadOriginal } = useFileDownload(user?.id, fileId, { original: true });

  const handleDownload = useCallback(
    async (e: MouseEvent<HTMLButtonElement>) => {
      e.preventDefault();
      if (!user?.id || !fileId) {
        showToast({ status: 'error', message: localize('com_ui_download_error') });
        return;
      }
      try {
        const result = await downloadOriginal();
        const url = result.data;
        if (url == null || url === '') {
          showToast({ status: 'error', message: localize('com_ui_download_error') });
          return;
        }
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', attachment.filename ?? 'file');
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      } catch (err) {
        console.error('[FileAttachment] download failed', err);
        showToast({ status: 'error', message: localize('com_ui_download_error') });
      }
    },
    [
      user?.id,
      fileId,
      downloadOriginal,
      attachment.filename,
      localize,
      showToast,
    ],
  );

  const extension = attachment.filename?.split('.').pop();

  useEffect(() => {
    const timer = setTimeout(() => setIsVisible(true), 50);
    return () => clearTimeout(timer);
  }, []);

  if (!attachment.filepath) {
    return null;
  }

  const handleCardClick = (e: MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    if (fileId) {
      setPreviewOpen(true);
    } else {
      void handleDownload(e);
    }
  };

  return (
    <div
      className={cn(
        'file-attachment-container',
        'transition-all duration-300 ease-out',
        isVisible ? 'translate-y-0 opacity-100' : 'translate-y-2 opacity-0',
      )}
      style={{
        transformOrigin: 'center top',
        willChange: 'opacity, transform',
        WebkitFontSmoothing: 'subpixel-antialiased',
      }}
    >
      <FileContainer
        file={attachment}
        onClick={handleCardClick}
        overrideType={extension}
        containerClassName="max-w-fit"
        buttonClassName="bg-surface-secondary hover:cursor-pointer hover:bg-surface-hover active:bg-surface-secondary focus:bg-surface-hover hover:border-border-heavy active:border-border-heavy"
      />
      <FileTextPreviewDialog
        open={previewOpen}
        onOpenChange={setPreviewOpen}
        attachment={attachment}
      />
    </div>
  );
});

const ImageAttachment = memo(({ attachment }: { attachment: TAttachment }) => {
  const [isLoaded, setIsLoaded] = useState(false);
  const { width, height, filepath = null } = attachment as TFile & TAttachmentMetadata;

  useEffect(() => {
    setIsLoaded(false);
    const timer = setTimeout(() => setIsLoaded(true), 100);
    return () => clearTimeout(timer);
  }, [attachment]);

  return (
    <div
      className={cn(
        'image-attachment-container',
        'transition-all duration-500 ease-out',
        isLoaded ? 'scale-100 opacity-100' : 'scale-[0.98] opacity-0',
      )}
      style={{
        transformOrigin: 'center top',
        willChange: 'opacity, transform',
        WebkitFontSmoothing: 'subpixel-antialiased',
      }}
    >
      <Image
        altText={attachment.filename || 'attachment image'}
        imagePath={filepath ?? ''}
        height={height ?? 0}
        width={width ?? 0}
        className="mb-4"
      />
    </div>
  );
});

export default function Attachment({ attachment }: { attachment?: TAttachment }) {
  if (!attachment) {
    return null;
  }
  if (attachment.type === Tools.web_search) {
    return null;
  }

  const { width, height, filepath = null } = attachment as TFile & TAttachmentMetadata;
  const isImage = attachment.filename
    ? imageExtRegex.test(attachment.filename) && width != null && height != null && filepath != null
    : false;

  if (isImage) {
    return <ImageAttachment attachment={attachment} />;
  } else if (!attachment.filepath) {
    return null;
  }
  return <FileAttachment attachment={attachment} />;
}

export function AttachmentGroup({ attachments }: { attachments?: TAttachment[] }) {
  if (!attachments || attachments.length === 0) {
    return null;
  }

  const fileAttachments: TAttachment[] = [];
  const imageAttachments: TAttachment[] = [];

  attachments.forEach((attachment) => {
    const { width, height, filepath = null } = attachment as TFile & TAttachmentMetadata;
    const isImage = attachment.filename
      ? imageExtRegex.test(attachment.filename) &&
        width != null &&
        height != null &&
        filepath != null
      : false;

    if (isImage) {
      imageAttachments.push(attachment);
    } else if (attachment.type !== Tools.web_search) {
      fileAttachments.push(attachment);
    }
  });

  return (
    <>
      {fileAttachments.length > 0 && (
        <div className="my-2 flex flex-wrap items-center gap-2.5">
          {fileAttachments.map((attachment, index) =>
            attachment.filepath ? (
              <FileAttachment attachment={attachment} key={`file-${index}`} />
            ) : null,
          )}
        </div>
      )}
      {imageAttachments.length > 0 && (
        <div className="mb-2 flex flex-wrap items-center">
          {imageAttachments.map((attachment, index) => (
            <ImageAttachment attachment={attachment} key={`image-${index}`} />
          ))}
        </div>
      )}
    </>
  );
}
