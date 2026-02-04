const { OpenAI } = require('openai');

class ModelClient {
  constructor(endpointConfig, modelConfig) {
    this.endpointConfig = endpointConfig;
    this.modelConfig = modelConfig;
    this.client = this.createClient();
  }

  createClient() {
    const { type, baseURL, apiKey, name } = this.endpointConfig;

    if (!apiKey) {
      throw new Error(`API key is required for endpoint: ${name || type}`);
    }
    if (!baseURL) {
      throw new Error(`Base URL is required for endpoint: ${name || type}`);
    }

    if (type === 'openai' || type === 'custom') {
      return new OpenAI({
        apiKey,
        baseURL,
      });
    }

    throw new Error(`Unsupported endpoint type: ${type}. Supported types: 'openai', 'custom'`);
  }

  async generate(prompt, options = {}) {
    const model = this.modelConfig.model || 'gpt-3.5-turbo';
    const temperature = options.temperature || 0.1;
    const max_tokens = options.max_tokens || 2000;

    const timeoutMs = 60000;
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Request timeout after 60 seconds')), timeoutMs);
    });

    try {
      const requestPromise = this.client.chat.completions.create({
        model,
        messages: [{ role: 'user', content: prompt }],
        temperature,
        max_tokens,
      });
      const response = await Promise.race([requestPromise, timeoutPromise]);
      return response.choices[0]?.message?.content || '';
    } catch (error) {
      if (error.status === 401) {
        throw new Error(`Authentication failed: Invalid API key for endpoint ${this.endpointConfig.name || 'unknown'}`);
      }
      if (error.status === 429) {
        throw new Error('Rate limit exceeded: Too many requests. Please try again later.');
      }
      if (error.status === 500) {
        throw new Error('Server error: The API server returned an error. Please check your endpoint configuration.');
      }
      if (error.message && error.message.includes('timeout')) {
        throw new Error('Request timeout: The API did not respond within 60 seconds.');
      }
      throw new Error(`Model generation failed: ${error.message}`);
    }
  }
}

module.exports = ModelClient;
