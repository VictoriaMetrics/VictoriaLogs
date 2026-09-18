export const getResponseError = async (response: Response): Promise<string> => {
  const text = await response.text();

  try {
    const json = JSON.parse(text);
    return typeof json?.error === "string" ? json.error : text;
  } catch {
    return text;
  }
};
