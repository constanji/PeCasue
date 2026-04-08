import { useMemo, memo, useState, type MouseEvent } from 'react';
import type { TFile, TMessage } from '@because/data-provider';
import FileContainer from '~/components/Chat/Input/Files/FileContainer';
import FileTextPreviewDialog from '~/components/Chat/Messages/Content/Parts/FileTextPreviewDialog';
import { useAttachmentLink } from '~/components/Chat/Messages/Content/Parts/LogLink';
import Image from './Image';

/** 用户消息里的文档附件：点击打开文本预览（与 Attachment 路径一致，需带 file_id） */
function UserMessageFile({ file }: { file: TFile }) {
  const [previewOpen, setPreviewOpen] = useState(false);
  const { handleDownload } = useAttachmentLink({
    href: file.filepath ?? '',
    filename: file.filename ?? '',
  });

  const handleClick = (e: MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    if (file.file_id) {
      setPreviewOpen(true);
    } else {
      void handleDownload(e);
    }
  };

  return (
    <>
      <FileContainer file={file} onClick={handleClick} />
      <FileTextPreviewDialog
        open={previewOpen}
        onOpenChange={setPreviewOpen}
        attachment={file}
      />
    </>
  );
}

const Files = ({ message }: { message?: TMessage }) => {
  const imageFiles = useMemo(() => {
    return message?.files?.filter((file) => file.type?.startsWith('image/')) || [];
  }, [message?.files]);

  const otherFiles = useMemo(() => {
    return message?.files?.filter((file) => !(file.type?.startsWith('image/') === true)) || [];
  }, [message?.files]);

  return (
    <>
      {otherFiles.length > 0 &&
        otherFiles.map((file) => <UserMessageFile key={file.file_id} file={file as TFile} />)}
      {imageFiles.length > 0 &&
        imageFiles.map((file) => (
          <Image
            key={file.file_id}
            imagePath={file.preview ?? file.filepath ?? ''}
            height={file.height ?? 1920}
            width={file.width ?? 1080}
            altText={file.filename ?? 'Uploaded Image'}
            placeholderDimensions={{
              height: `${file.height ?? 1920}px`,
              width: `${file.height ?? 1080}px`,
            }}
            // n={imageFiles.length}
            // i={i}
          />
        ))}
    </>
  );
};

export default memo(Files);
