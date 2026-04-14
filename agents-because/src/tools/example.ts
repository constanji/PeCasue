import { tool } from '@langchain/core/tools';
import { z } from 'zod';

/** Demo tool: returns a random image URL payload (used by scripts/image). */
export const fetchRandomImageTool = tool(
  async () => {
    const url = `https://picsum.photos/seed/${Date.now()}/200/300`;
    return JSON.stringify({ url, note: 'random placeholder image' });
  },
  {
    name: 'fetch_random_image',
    description:
      'Fetches a random image URL for demonstration and testing scripts.',
    schema: z.object({}),
  }
);

/** Demo tool: returns only a URL string. */
export const fetchRandomImageURL = tool(
  async () => `https://picsum.photos/seed/${Date.now()}/200/300`,
  {
    name: 'fetch_random_image_url',
    description: 'Returns a random image URL string.',
    schema: z.object({}),
  }
);
